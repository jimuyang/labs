package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

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

//type JobType int

var WaitJob = &Job{
	Type: JobTypeWait,
}

var FinishJob = &Job{
	Type: JobTypeFinish,
}

const (
	JobTypeMap    = 1
	JobTypeReduce = 2
	JobTypeWait   = 3 // please wait
	JobTypeFinish = 4 // no more job
)

type Job struct {
	Id         int32
	Type       int32
	Inputs     []string
	NReduce    int       // map: nReduce reduce: x
	CreateTime time.Time // job创建时间

	WorkerId       int32      // 分配worker id
	DistributeTime *time.Time // 分配时间
	FinishTime     *time.Time // 完成时间
	Outputs        []string   // mr-workerId-X
}

func (job *Job) TBD() bool {
	if job.WorkerId == 0 {
		return true
	}
	if job.FinishTime != nil {
		return false
	}
	if job.DistributeTime == nil {
		return true
	}
	if time.Now().Sub(*job.DistributeTime) > WorkerTimeout {
		return true
	}
	return false
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
