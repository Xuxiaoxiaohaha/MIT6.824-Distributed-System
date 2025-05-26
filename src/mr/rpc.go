package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// 新增: 向Master注册 的参数和返回值类型
type XqxRegisterArgs struct {
	WorkerId int
}


// 新增: 定义申请任务 RPC 的参数和返回值类型
type XqxPullTaskArgs struct {
	Id int // 请求任务的 Worker ID
}
type XqxPullTaskReply struct {
	Mor             int    // 0 Map, 1 Reduce, 2 无任务/等待
	MapFileName     string // Map 任务的文件名
	ReduceTaskIndex int    // Reduce 任务的索引 (例如 0, 1, ...)
	TaskId          int    // 分配的任务的唯一 ID
	SourcePath      string // 中间文件的根路径 (如果 worker 需要)
	NReduce         int    // Reduce 任务总数
	NMap            int    // Map 任务总数
	RootPath        string // 原始文件的目录
}


// 新增: 定义心跳 RPC 的参数和返回值类型
type XqxHeartbeatArgs struct {
	WorkerId int
}
type XqxHeartbeatReply struct {
	// Master 可以选择性地通过这里回复指令或确认
	Exit bool
}

// 新增: 完成任务通知Master 的参数和返回值类型
type XqxEndTaskNotifyArgs struct {
	WorkerId int
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
