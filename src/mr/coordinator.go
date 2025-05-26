package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "strconv"
import "time"
import "fmt"
import "path/filepath"
import  "io"

type xqxWorker struct {
	id            int       // 标识符
	status        int       // 状态 0-空闲 1-工作
	lastHeartbeat time.Time // 新增: 上次心跳的时间戳
}

type xqxTask struct {
	id       int    // 标识符
	mor      int    // 任务类型 0-Map 1-Reduce 2-没有任务
	status   int    // 状态 0-未开始 1-进行中 2-已完成
	where    int    // 位于哪个机器上
	fileName string // Map的话则是（pg-being_ernest.txt），Reduce的话则是索引 “1”
}

type Coordinator struct {
	workers       []*xqxWorker
	tasks         []*xqxTask // 左侧放Map任务，右侧放Reduce任务
	nReduce, nMap int
	rootPath,sourcePath      string
	mu            sync.Mutex
	workerTimeout time.Duration // 新增: 定义 worker 超时时长
	watchTimeout time.Duration // 新增: 定义监控间隔
	logFile       *os.File // 保存文件句柄
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// Master向Worker提供的注册服务
//
func (c *Coordinator) XqxRegister(args *XqxRegisterArgs,reply *struct{ /* 空回复 */ }) error {
	c.mu.Lock() // 加锁
	defer c.mu.Unlock() // 保证方法退出前解锁

	// 检查 worker 是否已经注册过
	for _, w := range c.workers {
		if w.id == args.WorkerId {
			log.Printf("Worker %d 尝试重新注册。更新心跳时间。", args.WorkerId)
			w.lastHeartbeat = time.Now() // 更新心跳
			return nil
		}
	}

	c.workers = append(c.workers, &xqxWorker{id: args.WorkerId, status: 0, lastHeartbeat: time.Now()}) // 修改: 初始化 lastHeartbeat
	log.Printf("Worker %d 注册成功。", args.WorkerId)
	return nil
}

//
// Master向Worker提供的任务申请服务
// 输入：worker id 输出：任务类型、文件位置
func (c *Coordinator) XqxPullTask(args *XqxPullTaskArgs, reply *XqxPullTaskReply) error {
	c.mu.Lock() // 加锁
	defer c.mu.Unlock() // 保证方法退出前解锁
	
	// 当 worker 拉取任务时，更新其最后心跳时间
	workerFound := false
	for _, w := range c.workers {
		if w.id == args.Id { // 注意: 根据你的 XqxPullTaskArgs 定义，这里应该是 args.Id
			w.lastHeartbeat = time.Now()
			workerFound = true
			break
		}
	}

	if !workerFound {
		log.Printf("未注册的 worker %d 尝试拉取任务。忽略。", args.Id)
		reply.Mor = 2 // 没有任务
		return nil   // 或者返回一个错误
	}

	allMapTasksDone := true
	// 首先分配可用的 Map 任务
	for taskIndex, task := range c.tasks { // 使用 taskIndex 确保修改的是原始 task
		if task.mor == 0 { // Map 任务
			if task.status == 0 { // 未分配
				log.Printf("分配 Map 任务 %s (内部任务真实ID %d) 给 worker %d", task.fileName, taskIndex, args.Id)
				c.tasks[taskIndex].status = 1         // 标记为进行中
				c.tasks[taskIndex].where = args.Id    // 分配给此 worker
				reply.Mor = c.tasks[taskIndex].mor
				reply.RootPath = c.rootPath
				reply.SourcePath = c.sourcePath
				reply.MapFileName = c.tasks[taskIndex].fileName
				reply.TaskId = c.tasks[taskIndex].id // 修改: 发送任务的原始ID给worker
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap

				for _, w := range c.workers {
					if w.id == args.Id {
						w.status = 1 // 标记 worker 为工作中
						break
					}
				}
				return nil
			}
			if task.status != 2 { // 如果任何一个map任务未完成
				allMapTasksDone = false
			}
		}
	}

	if !allMapTasksDone {
		// log.Printf("Worker %d 没有立即可用的 Map 任务，但 Map 阶段未完成。", args.Id)
		reply.Mor = 2 //暂时没有任务，worker 应该重试
		return nil
	}

	// 如果所有 Map 任务都完成了，分配 Reduce 任务
	// log.Println("所有 Map 任务已完成。检查 Reduce 任务。")
	for taskIndex, task := range c.tasks { // 使用 taskIndex 确保修改的是原始 task
		if task.mor == 1 { // Reduce 任务
			if task.status == 0 { // 未分配
				log.Printf("分配 Reduce 任务 %s (内部任务真实ID %d) 给 worker %d", task.fileName, taskIndex, args.Id)
				c.tasks[taskIndex].status = 1      // 标记为进行中
				c.tasks[taskIndex].where = args.Id // 分配给此 worker
				reply.Mor = c.tasks[taskIndex].mor
				// 对于 Reduce 任务，fileName 是 reduce 任务的索引 (字符串形式)
				reply.ReduceTaskIndex, _ = strconv.Atoi(c.tasks[taskIndex].fileName) // 修改: 发送 reduce 任务索引
				reply.TaskId = c.tasks[taskIndex].id // 修改: 发送任务的原始ID给worker
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				reply.RootPath = c.rootPath
				reply.SourcePath = c.sourcePath
				for _, w := range c.workers {
					if w.id == args.Id {
						w.status = 1 // 标记 worker 为工作中
						break
					}
				}
				return nil
			}
		}
	}

	// 如果所有任务（Map 和 Reduce）已分配或完成，或者当前没有适合此 worker 的任务
	log.Printf("当前没有适合 worker %d 的任务。", args.Id)
	reply.Mor = 3 // 没有任务 / 如果所有任务都完成了，可以发出任务完成信号
	for _, worker := range c.workers {
		if worker.id == args.Id {
			worker.status = 0 // 如果没有分配任务，则标记为空闲
		}
	}
	return nil
}

// Worker 心跳的 RPC 处理函数
func (c *Coordinator) XqxHeartbeat(args *XqxHeartbeatArgs, reply *XqxHeartbeatReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果所有任务已完成，让Worker下线
	done := true
	reply.Exit = false
	for _, task := range c.tasks {
		if task.status != 2 { // 如果有任何任务未完成
			done = false
		}
	}
	if done {
		reply.Exit = true
	}

	found := false
	for _, worker := range c.workers {
		if worker.id == args.WorkerId {
			worker.lastHeartbeat = time.Now()
			// log.Printf("收到来自 worker %d 的心跳", args.WorkerId) // 可选: 用于详细日志
			found = true
			break
		}
	}

	if !found {
		log.Printf("收到来自未注册 worker %d 的心跳", args.WorkerId)
		// 可选: 可以在这里自动注册它，或者返回错误
		return fmt.Errorf("worker %d 未注册", args.WorkerId) // 或者返回一个错误
	}

	return nil
}

// 定期检查死亡 worker 的 goroutine
func (c *Coordinator) checkDeadWorkers() {
	log.Printf("启动心跳检测机制")
	for {
		// 定期检查，例如，超时时间的一半。这个间隔可以调整。
		// 如果 workerTimeout 设置为5分钟，这里就是2.5分钟检查一次。
		// 更频繁的检查可以更快发现死节点，但也会增加 Coordinator 的负载。
		// 实际应用中这个检查间隔需要权衡。
		checkInterval := c.workerTimeout / 2
		// if checkInterval < 5*time.Second { // 避免过于频繁的检查
		//	checkInterval = 5 * time.Second
		//}
		time.Sleep(checkInterval)
		c.mu.Lock()
		survivingWorkers := []*xqxWorker{}
		for _, worker := range c.workers {
			if time.Since(worker.lastHeartbeat) > c.workerTimeout {
				log.Printf("Worker %d 超时。上次心跳: %v。当前时间: %v", worker.id, worker.lastHeartbeat, time.Now())
				// 重新分配正在此死亡 worker 上运行的任务
				for taskIndex, task := range c.tasks {
					if task.where == worker.id { // 任务正在此 worker 上运行
						if task.mor == 0 { // Map任务无论是进行中还是已完成都要重新运行，因为理论上Map是在本地磁盘存储中间文件
							c.tasks[taskIndex].status = 0 // 标记为未分配
							c.tasks[taskIndex].where = -1  // 取消 worker 分配
							log.Printf("将死亡 worker %d 上的任务 ID %d (类型 %d) 重新加入队列", worker.id, task.id, task.mor)
						}else{ // Reduce任务只需要重新分配正在运行中的任务
							if task.status == 1 {
								c.tasks[taskIndex].status = 0 // 标记为未分配
								c.tasks[taskIndex].where = -1  // 取消 worker 分配
								log.Printf("将死亡 worker %d 上的任务 ID %d (类型 %d) 重新加入队列", worker.id, task.id, task.mor)
							}
						}
					}
				}
			} else {
				survivingWorkers = append(survivingWorkers, worker)
			}
		}
		c.workers = survivingWorkers // 更新存活的 worker 列表
		c.mu.Unlock()
	}
}

// Worker 完成任务的处理函数
func (c *Coordinator) XqxEndWorkNotify(args *XqxEndTaskNotifyArgs, reply *struct{ /* 空回复 */ }) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 标记此Worker为空闲Worker
	for i, w := range c.workers {
		if w.id == args.WorkerId {
			c.workers[i].lastHeartbeat = time.Now()
			c.workers[i].status = 0
			break
		}
	}

	// 标记此任务为完成状态
	for i, t := range c.tasks {
		if t.where == args.WorkerId && t.status == 1{
			c.tasks[i].status = 2
			log.Printf("Worker %d 上的任务 ID %d (类型 %d) 已完成", args.WorkerId, t.id, t.mor)
			break
		}
	}
	
	return nil
}

// 定期监控管理的资源
func (c *Coordinator) showResource() {
	log.Printf("启动资源监控机制")
	for {
		time.Sleep(c.watchTimeout)
		c.mu.Lock()
		log.Println("----------------------------------分布式系统当前状态----------------------------------")
		for _, worker := range c.workers {
			status := "空闲"
			if worker.status == 1{
				status = "工作"
			}
			log.Printf("Worker-%d: %s", worker.id, status)
		}
		for _, task := range c.tasks {
			var tStatus string
			var mor string
			switch task.status{
			case 0:
				tStatus = "未开始"
			case 1:
				tStatus = "进行中"
			case 2:
				tStatus = "已完成"
			}
			switch task.mor{
			case 0:
				mor = "Map"
			case 1:
				mor = "Reduce"
			}
			log.Printf("Task-%d: Type %s, Status %s, Loc %d, Filename %s", task.id, mor, tStatus, task.where, task.fileName)
		}
		log.Println("----------------------------------分布式系统当前状态----------------------------------")
		c.mu.Unlock()
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	// 1. 注册RPC服务：将 Coordinator 结构体 c 的方法注册为 RPC 服务，Go 的 net/rpc 包会自动查找 c 中符合特定签名规则的导出方法 (首字母大写)，并将它们暴露给 RPC 客户端。
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	// 2. 创建并绑定Unix域套接字:一种在同一主机内进行进程间通信（IPC）的高效方式，比 TCP/IP 更轻量，适合本地服务间通信
	sockname := coordinatorSock() // coordinatorSock():返回套接字文件路径（如/var/tmp/mr-coordinator），用于后续绑定
	os.Remove(sockname) // 移除可能存在的旧套接字文件
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// 3. 启动HTTP服务器处理RPC请求
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.tasks {
		if task.status != 2 { // 如果有任何任务未完成
			return false
		}
	}
	log.Println("所有任务已完成。作业完成。")
	// 任务完成后关闭日志文件
	if c.logFile != nil {
		c.logFile.Close()
	}
	return true
}

// 在 Coordinator 中添加 Close 方法（或在 Done() 中处理）
func (c *Coordinator) Close() {
	if c.logFile != nil {
		c.logFile.Close()
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	// 已知：len(files)个Map任务，nReduce个Reduce任务
	// Master职责：
	// 1.管理Worker（状态，负责的是Map任务还是Reduce任务，负责第几个Map任务或Reduce任务）
	// 2.管理Map任务（是否完成，位于哪个Worker上）
	// 3.管理Reduce任务（是否完成，位于哪个Worker上）
	dir, err := os.Getwd()
	if err != nil {
        panic(err)
    }
	logFile, err := os.OpenFile(filepath.Join(dir, "master.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	// defer logFile.Close()
	c := Coordinator{
		workers:       []*xqxWorker{},
		tasks:         []*xqxTask{},
		nReduce:       nReduce,
		nMap:          len(files),
		workerTimeout: 5 * time.Second,
		rootPath:      dir, // 生成文件的目录
		sourcePath:      "/home/xqx/workspace/6.5840/src/main", // 原始文件的目录
		logFile:      logFile, // 原始文件的目录
		watchTimeout:  5 * time.Second,
	}
	// 设置默认日志器
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(multiWriter)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Printf("初始化 Coordinator，包含 %d 个 Map 任务和 %d 个 Reduce 任务。", len(files), nReduce)
	log.Printf("RootPath：%s", c.rootPath)
	// 初始化Map任务信息
	// Map 任务的 ID 从 0 到 len(files)-1
	log.Printf("-------------------初始化Map任务-------------------")
	for i, file := range files {
		c.tasks = append(c.tasks, &xqxTask{id: i, mor: 0, status: 0, where: -1, fileName: filepath.Base(file)})
		log.Printf("任务ID: %d 文件名称: %s", i, filepath.Base(file))
	}
	// 初始化Reduce任务信息
	// Reduce 任务的 ID 可以从 len(files) 开始，以确保在 c.tasks 中 ID 的唯一性（如果需要全局唯一ID）
	// 或者 Reduce 任务可以有自己独立的 ID 序列（0 to nReduce-1），fileName 存储 Reduce 任务的索引
	log.Printf("-------------------初始化Reduce任务-------------------")
	for i := 0; i < nReduce; i++ {
		taskId := len(files) + i // 为了在 c.tasks 数组中有一个唯一的 id
		c.tasks = append(c.tasks, &xqxTask{id: taskId, mor: 1, status: 0, where: -1, fileName: strconv.Itoa(i)})
		log.Printf("任务ID: %d ", taskId)
	}
	go c.checkDeadWorkers() // 新增: 启动 worker 检查 goroutine
	go c.showResource()
	c.server()

	log.Println("Coordinator 初始化完毕，服务器已启动。")
	return &c
}
