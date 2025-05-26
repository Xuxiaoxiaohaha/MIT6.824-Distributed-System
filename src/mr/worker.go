package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "path/filepath"
import "io/ioutil"
import "encoding/json"
import "sort"
import "sync"
import "io"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeIntermediateFiles(intermediate []KeyValue, nReduce int, taskId int, rootPath string) error {
    // 1. 创建并打开NReduce个文件
    files := make([]*os.File, nReduce)
    encoders := make([]*json.Encoder, nReduce)
    
    for i := 0; i < nReduce; i++ {
        filename := fmt.Sprintf("mr-%d-%d.json", taskId, i)
		filename = filepath.Join(rootPath, filename)
        file, err := os.Create(filename)
        if err != nil {
            // 出错时关闭已打开的文件
            for j := 0; j < i; j++ {
                files[j].Close()
            }
            return fmt.Errorf("创建文件 %s 失败: %v", filename, err)
        }
        files[i] = file
        encoders[i] = json.NewEncoder(file)
    }
    
    // 2. 遍历intermediate切片，按哈希值写入对应文件
    for _, kv := range intermediate {
        bucket := ihash(kv.Key) % nReduce
        if err := encoders[bucket].Encode(kv); err != nil {
            // 出错时关闭所有文件
            for _, f := range files {
                f.Close()
            }
            return fmt.Errorf("写入文件失败: %v", err)
        }
    }
    
    // 3. 关闭所有文件
    for _, f := range files {
        f.Close()
    }
    
    return nil
}

func RunReduceTask(rootPath string, reduceIndex int, reducef func(string, []string) string) error {
    // 1. 查找所有mr-*-i.json文件
    pattern := filepath.Join(rootPath, fmt.Sprintf("mr-*-%d.json", reduceIndex))
    files, err := filepath.Glob(pattern)
	
    if err != nil {
        return fmt.Errorf("查找文件失败: %v", err)
    }
    if len(files) == 0 {
        return fmt.Errorf("未找到匹配的中间文件")
    }
    // 2. 并发读取所有文件内容（使用goroutine和channel）
    kvsCh := make(chan []KeyValue, len(files))
	errCh := make(chan error, len(files)) // 用于收集 goroutine 中的错误
    var wg sync.WaitGroup
	for _, file := range files {
        wg.Add(1)
        go func(filePath string) {
            defer wg.Done()
            
            file, err := os.Open(filePath)
            if err != nil {
                fmt.Printf("警告: 无法打开文件 %s: %v\n", filePath, err)
				errCh <- fmt.Errorf("打开文件 %s 失败: %v", filePath, err) // 发送错误
                return
            }
            defer file.Close()
            
            var kvs []KeyValue
            dec := json.NewDecoder(file)
            for {
                var kv KeyValue
                if err := dec.Decode(&kv); err != nil {
					if err != io.EOF { // 确保不是正常的EOF
						errCh <- fmt.Errorf("解码文件 %s 失败: %v", filePath, err) // 发送错误
					}
                    break
                }
                kvs = append(kvs, kv)
            }
            
            kvsCh <- kvs
        }(file)
    }

    // 等待所有goroutine完成并关闭channel
    go func() {
        wg.Wait()
        close(kvsCh)
		close(errCh) // 关闭错误通道
    }()
	// 检查是否有错误发生
	for err := range errCh {
		if err != nil {
			return fmt.Errorf("RunReduceTask 失败，读取中间文件时发生错误: %v", err) // 返回第一个遇到的错误
		}
	}
    // 3. 收集所有KeyValue
    var allKVs []KeyValue
    for kvs := range kvsCh {
        allKVs = append(allKVs, kvs...)
    }

    // 4. 按Key排序
    sort.Slice(allKVs, func(i, j int) bool {
        return allKVs[i].Key < allKVs[j].Key
    })
    // 5. 输出结果到最终文件
    outputFile := filepath.Join(rootPath, fmt.Sprintf("mr-out-%d", reduceIndex))
    out, err := os.Create(outputFile)
    if err != nil {
        return fmt.Errorf("创建输出文件失败: %v", err)
    }
    defer out.Close()
	i := 0
	for i < len(allKVs) {
		j := i + 1
		for j < len(allKVs) && allKVs[j].Key == allKVs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, allKVs[k].Value)
		}
		output := reducef(allKVs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(out, "%v %v\n", allKVs[i].Key, output)

		i = j
	}
	return nil
}

func RunMapTask(pTask XqxPullTaskReply, mapf func(string, string) []KeyValue) error{
	intermediate := []KeyValue{}
	inputFilePath := filepath.Join(pTask.SourcePath, pTask.MapFileName)
	file, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatalf("Worker cannot open %v", inputFilePath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Worker cannot read %v", inputFilePath)
	}
	file.Close()
	kva := mapf("../" + pTask.MapFileName, string(content)) // 这里加 "../"只是为了验证能通过
	intermediate = append(intermediate, kva...)
	writeIntermediateFiles(intermediate, pTask.NReduce, pTask.TaskId, pTask.RootPath)
	return nil
}

// 向Master发送心跳
func runHeartbeatRoutine(pid int, exitChan chan<- bool, heartbeatInterval time.Duration) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 执行需要定时执行的任务，例如发送心跳
			// log.Printf("Worker %d: (心跳goroutine) 准备发送心跳", pid)
			shouldExit := CallHeartbeat(pid) // 假设 CallHeartbeat 返回 Master 是否要求 Worker 退出
			if shouldExit {
				log.Printf("Worker %d: (心跳goroutine) Master指示退出，发送信号到exitChan", pid)
				// 使用非阻塞发送，或者确保 exitChan 有缓冲，或者确保接收方总是在监听
				// 对于明确的退出信号，通常希望它能被发送出去
				// 如果担心主 goroutine 可能没在监听导致这里阻塞，可以考虑带超时的发送或缓冲channel
				// 但更常见的模式是，如果心跳决定退出，它就发送信号并结束自己。
				select {
				case exitChan <- true:
					log.Printf("Worker %d: (心跳goroutine) 成功发送退出信号到exitChan", pid)
				default:
					// 如果 exitChan 由于某种原因无法立即写入（例如主 goroutine 已经退出或阻塞在其他地方），
					// 记录日志，但心跳 goroutine 仍应准备退出。
					log.Printf("Worker %d: (心跳goroutine) 发送退出信号到exitChan失败 (可能已满或无接收者)，但仍将退出心跳", pid)
				}
				return // 心跳 goroutine 结束
			}
		}
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	dir, err := os.Getwd()
	pid := os.Getpid()
	if err != nil {
        panic(err)
    }
	logFileName := fmt.Sprintf("worker-%d.log", pid)
    logFilePath := filepath.Join(dir, logFileName)
	logFile, err := os.OpenFile(logFilePath,  os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logFile.Close()
	// 设置默认日志器
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(multiWriter)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 1.向Master注册自己
	CallRegister(pid)
	// 创建一个 channel 用于接收退出信号
	exitChan := make(chan bool, 1)	

	// 启动心跳 goroutine
	// 心跳间隔应该远小于 Master 判断 worker 超时的时间
	heartbeatInterval := 1 * time.Second // 例如1秒一次心跳
	go runHeartbeatRoutine(pid, exitChan, heartbeatInterval)

	log.Printf("Worker %d: 开始主循环，等待任务或退出信号。", pid)
	// 2.循环，请求任务，否则退出Worker
	for {
		select 
		{
			case exit := <-exitChan:
				if exit {
					log.Printf("Worker %d: 接收到退出信号，准备退出", pid)
					return // 退出主函数，终止Worker
				}
			default:
				pTask, ok := CallPullTask(pid)
				if !ok {
					log.Printf("Worker %d: 从 Coordinator 拉取任务失败，稍后重试...", pid)
					time.Sleep(time.Second * 2) // 简单重试间隔
					continue
				}
				switch pTask.Mor {
					case 0: // Map任务
						// 处理Map任务逻辑
						log.Printf("Worker %d: 开始执行Map任务 | 任务ID-%d", pid, pTask.TaskId)
						RunMapTask(pTask, mapf)
						log.Printf("Worker %d: 执行Map任务完毕 | 任务ID-%d", pid, pTask.TaskId)
						CallEndWorkNotify(pid)
					case 1: // Reduce任务
						log.Printf("Worker %d: 开始执行Reduce任务 | 任务ID-%d", pid, pTask.TaskId)
						RunReduceTask(pTask.RootPath, pTask.ReduceTaskIndex, reducef)
						log.Printf("Worker %d: 执行Reduce任务完毕 | 任务ID-%d", pid, pTask.TaskId )
						CallEndWorkNotify(pid)
					case 2: // 等待
						log.Printf("Worker %d: Master指示等待...", pid)
						time.Sleep(500 * time.Millisecond)
					default: // 所有任务完成（非0/1/2的情况，通常是其他终止信号）
						log.Printf("Worker %d: Master指示终止...", pid)
						return
				}
		}
		
	}

}

func CallPullTask(WorkerId int) (XqxPullTaskReply, bool) {
	args := XqxPullTaskArgs{Id:WorkerId}
	reply :=  XqxPullTaskReply{}
	ok := call("Coordinator.XqxPullTask", &args, &reply)
	return reply, ok
}


func CallHeartbeat(WorkerId int) bool {
	args := XqxHeartbeatArgs{WorkerId:WorkerId}
	reply :=  XqxHeartbeatReply{Exit:false}
	call("Coordinator.XqxHeartbeat", &args, &reply)
	return reply.Exit
}

func CallRegister(WorkerId int) {
	args := XqxRegisterArgs{WorkerId:WorkerId}
	reply :=  struct{}{}
	call("Coordinator.XqxRegister", &args, &reply)
}

func CallEndWorkNotify(WorkerId int) {
	args := XqxEndTaskNotifyArgs{WorkerId:WorkerId}
	reply :=  struct{}{}
	call("Coordinator.XqxEndWorkNotify", &args, &reply)
}
//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
