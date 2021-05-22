package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	files   []string // 输入文件
	nReduce int      // number of reduce tasks to use.

	idGen int32 // uniq id
	phase int   // current phase

	jobLock *sync.Mutex // job lock
	jobs    []*Job      // running jobs
	jobDone int         // job done count
}

type CoordinatorPhase int

const (
	CoordinatorPhaseInit   = 0
	CoordinatorPhaseMap    = 1
	CoordinatorPhaseReduce = 2
	CoordinatorPhaseDone   = 9
)

const (
	WorkerTimeout = time.Second * 10
)

func (c *Coordinator) getUniqId() int32 {
	return atomic.AddInt32(&c.idGen, 1)
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

// 取回1个job
func (c *Coordinator) GetJob(_ *ExampleArgs, gotJob *Job) error {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	log.Println("GetJob called")

	var res *Job
	defer func() {
		*gotJob = *res
	}()

	if c.phase == CoordinatorPhaseDone {
		res = FinishJob
		return nil
	}

	for _, job := range c.jobs {
		if job.TBD() {
			res = job
			break
		}
	}
	if res == nil {
		res = WaitJob
		return nil
	}

	//
	workerId := c.getUniqId()
	now := time.Now()
	res.WorkerId = workerId
	res.DistributeTime = &now
	return nil
}

func (c *Coordinator) JobDone(doneJob *Job, _ *ExampleReply) error {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	log.Println("JobDone called")

	var foundJob *Job
	for _, job := range c.jobs {
		if job.Id == doneJob.Id {
			foundJob = job
			break
		}
	}
	if foundJob == nil {
		fmt.Println("cannot find job: ", doneJob)
		return nil
	}
	if foundJob.WorkerId != doneJob.WorkerId {
		fmt.Println("worker changed, sorry: ", doneJob)
		return nil
	}

	now := time.Now()
	foundJob.FinishTime = &now
	foundJob.Outputs = doneJob.Outputs
	c.jobDone++

	if c.jobDone == len(c.jobs) {
		switch c.phase {
		case CoordinatorPhaseMap:
			c.mapDone()
		case CoordinatorPhaseReduce:
			c.reduceDone()
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()

	return c.phase == CoordinatorPhaseDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nReduce: nReduce,
		jobLock: &sync.Mutex{},
	}
	c.initMapJob()
	c.server()
	return &c
}

// init map jobs
func (c *Coordinator) initMapJob() {
	c.jobs = make([]*Job, 0, len(c.files))
	c.jobDone = 0
	for _, file := range c.files {
		c.jobs = append(c.jobs, &Job{
			Id:         c.getUniqId(),
			Type:       JobTypeMap,
			Inputs:     []string{file},
			NReduce:    c.nReduce,
			CreateTime: time.Now(),
		})
	}

	c.phase = CoordinatorPhaseMap
	log.Println("map phase!")
}

// init reduce jobs
func (c *Coordinator) mapDone() {
	fileMap := make(map[int][]string)
	for _, mapJob := range c.jobs {
		for _, file := range mapJob.Outputs {
			splits := strings.Split(file, "-") // mr-workerId-X
			x, _ := strconv.Atoi(splits[len(splits)-1])
			fileMap[x] = append(fileMap[x], file)
		}
	}
	fmt.Println("initReduceJobs ", fileMap)

	c.jobs = make([]*Job, 0, len(c.files))
	c.jobDone = 0
	for x, files := range fileMap {
		c.jobs = append(c.jobs, &Job{
			Id:         c.getUniqId(),
			Type:       JobTypeReduce,
			Inputs:     files,
			NReduce:    x,
			CreateTime: time.Now(),
		})
	}
	c.phase = CoordinatorPhaseReduce
	log.Println("reduce phase!")
}

func (c *Coordinator) reduceDone() {
	// rename the reduce output
	for _, reduceJob := range c.jobs {
		output := reduceJob.Outputs[0]
		x := reduceJob.NReduce
		if err := os.Rename(output, fmt.Sprintf("mr-out-%d", x)); err != nil {
			fmt.Errorf("rename %s err: %v", output, err)
			break
		}
	}

	c.phase = CoordinatorPhaseDone
	log.Println("done phase!")
}
