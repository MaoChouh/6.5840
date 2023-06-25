package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	IDLE int = iota
	RUNNING
	COMPLETED
)

type Coordinator struct {
	// Your definitions here.
	MapState                   map[string]int
	ReduceState                map[int]int
	ready_reduce_map_files_map map[int][]string
	nReduce                    int
	sem                        chan int
}

// Your code here -- RPC handlers for the worker to call.

func DeleteSlice(a []string, elem string) []string {
	j := 0
	for _, v := range a {
		if v != elem {
			a[j] = v
			j++
		}
	}
	return a[:j]
}

func (c *Coordinator) GetReducerNum(args *GetReudcerNumArgs, reply *GetReudcerNumReply) error {
	<-c.sem
	defer func() {
		c.sem <- 1
	}()
	reply.Nreduce = c.nReduce
	// fmt.Printf("reply.nReduce in Server is : %v\n", reply.Nreduce)
	return nil
}

func (c *Coordinator) PullMapFile(args *PullMapFileArgs, reply *PullMapFileReply) error {
	<-c.sem
	defer func() {
		c.sem <- 1
	}()

	if args.FinishedMapFile != "" {
		if _, ok := c.MapState[args.FinishedMapFile]; ok {
			delete(c.MapState, args.FinishedMapFile)
			for _, file_name := range args.IntermediateFiles {
				ss := strings.Split(file_name, "/")
				if len(ss) != 2 {
					log.Fatalf("IntermediateFiles len must be 2")
				}
				reduce_idx_str := strings.Split(ss[0], "_")[1]
				reduce_idx, _ := strconv.Atoi(reduce_idx_str)
				c.ready_reduce_map_files_map[reduce_idx] = append(c.ready_reduce_map_files_map[reduce_idx], file_name)
			}
			if len(c.MapState) == 0 {
				for k := range c.ReduceState {
					delete(c.ReduceState, k)
				}
				for i := 0; i < c.nReduce; i++ {
					c.ReduceState[i] = -1
				}
			}
		}
	}
	reply.PreMapFile = ""
	for file_name, process_time := range c.MapState {
		if process_time == -1 {
			reply.PreMapFile = file_name
			c.MapState[file_name]++
			break
		}
	}

	return nil

}

func (c *Coordinator) PullReduceFiles(args *PullReduceArgs, reply *PullReduceReply) error {
	<-c.sem
	defer func() {
		c.sem <- 1
	}()

	if args.FinishedReduceGroupId != -1 {
		if _, ok := c.ReduceState[args.FinishedReduceGroupId]; ok {
			delete(c.ReduceState, args.FinishedReduceGroupId)
		} else {
			// NOTE(huangjiaming): delete verbose output_file
			err := os.RemoveAll(args.OutputFile)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	reply.PreReduceId = -1
	for group_id, process_time := range c.ReduceState {
		if process_time == -1 {
			reply.PreReduceId = group_id
			reply.PreReduceFiles = c.ready_reduce_map_files_map[group_id]
			c.ReduceState[group_id]++
			break
		}
	}

	return nil
}

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
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1235")
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
	// ret := false

	// Your code here.
	<-c.sem
	defer func() {
		c.sem <- 1
	}()

	ret := len(c.MapState) == 0 && len(c.ReduceState) == 0

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapState:                   map[string]int{},
		ReduceState:                map[int]int{},
		ready_reduce_map_files_map: map[int][]string{},
		nReduce:                    nReduce,
		sem:                        make(chan int),
	}

	for _, file := range files {
		c.MapState[file] = -1
	}

	// Your code here.
	go func() {
		c.sem <- 1
	}()

	go func() {
		for {
			<-c.sem
			for ready_map_file, _ := range c.MapState {
				if c.MapState[ready_map_file] > -1 {
					if c.MapState[ready_map_file] > 10 {
						c.MapState[ready_map_file] = -1
					} else {
						c.MapState[ready_map_file]++
					}
				}
			}
			for reduce_id, _ := range c.ReduceState {
				if c.ReduceState[reduce_id] > -1 {
					if c.ReduceState[reduce_id] > 10 {
						c.ReduceState[reduce_id] = -1
					} else {
						c.ReduceState[reduce_id]++
					}
				}
			}
			// fmt.Println("[WatchDog] Mapstate\n", c.MapState, "\nReducestate\n", c.ReduceState)
			c.sem <- 1

			time.Sleep(time.Second)
		}

	}()

	c.server()
	return &c
}
