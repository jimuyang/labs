package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		job := CallGetJob()

		switch job.Type {
		case JobTypeMap:
			fmt.Println("got map job: ", job.Inputs)
			intermediate := make([]KeyValue, 0)
			for _, filename := range job.Inputs {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
			}

			filenames := make([]string, 0, job.NReduce)
			ofiles := make([]*os.File, 0, job.NReduce)
			encs := make([]*json.Encoder, 0, job.NReduce)
			for i := 0; i < job.NReduce; i++ {
				filename := fmt.Sprintf("mr-%d-%d", job.WorkerId, i)
				filenames = append(filenames, filename)
				ofile, _ := os.Create(filename)
				encs = append(encs, json.NewEncoder(ofile))
				ofiles = append(ofiles, ofile)
			}

			for _, kv := range intermediate {
				x := ihash(kv.Key) % job.NReduce
				if err := encs[x].Encode(&kv); err != nil {
					log.Fatal("encode err")
				}
			}

			for _, ofile := range ofiles {
				ofile.Close()
			}
			job.Outputs = filenames
			CallJobDone(job)

		case JobTypeReduce:
			log.Println("got reduce job: ", job.Inputs)

			kva := make([]KeyValue, 0)
			for _, filename := range job.Inputs {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				_ = file.Close()
			}
			sort.Sort(ByKey(kva))

			oname := fmt.Sprintf("mr-%d-%d", job.WorkerId, job.NReduce)
			ofile, _ := os.Create(oname)

			for i := 0; i < len(kva); {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			_ = ofile.Close()
			job.Outputs = []string{oname}
			CallJobDone(job)

		case JobTypeWait:
			log.Println("got wait job: wait 1 second")
			time.Sleep(time.Second)
		case JobTypeFinish:
			log.Println("job finished!")
			return
		default:
			return
		}
	}
}

func CallGetJob() *Job {
	job := Job{}
	call("Coordinator.GetJob", &ExampleArgs{}, &job)
	return &job
}

func CallJobDone(doneJob *Job) {
	call("Coordinator.JobDone", doneJob, &ExampleReply{})
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
