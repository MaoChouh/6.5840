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
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func PullMapFile(finished_map_file string, intermediate_files []string) string {
	args := PullMapFileArgs{}
	if finished_map_file != "" {
		args.FinishedMapFile = finished_map_file
		args.IntermediateFiles = intermediate_files
	}
	reply := PullMapFileReply{}
	ok := call("Coordinator.PullMapFile", &args, &reply)
	if !ok {
		os.Exit(0)
	}
	return reply.PreMapFile
}

func GetReudcerNum() int {
	args := GetReudcerNumArgs{}
	reply := GetReudcerNumReply{}
	ok := call("Coordinator.GetReducerNum", &args, &reply)
	if !ok {
		os.Exit(0)
	}
	return reply.Nreduce
}

func PullReduceFile(finished_reduce_groupid int, output_file string) (int, []string) {
	args := PullReduceArgs{}
	args.FinishedReduceGroupId = finished_reduce_groupid
	args.OutputFile = output_file

	reply := PullReduceReply{}
	ok := call("Coordinator.PullReduceFiles", &args, &reply)
	if !ok {
		os.Exit(0)
	}
	return reply.PreReduceId, reply.PreReduceFiles
}

func ReadLocalFile(filename string) []byte {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return content
}

func MapPhase(mapf func(string, string) []KeyValue,
	nReduce int, ready_map_file string) []string {
	kva := mapf(ready_map_file, string(ReadLocalFile(ready_map_file)))
	tmp_file_list := []*os.File{}
	json_encoder_list := []*json.Encoder{}
	for i := 0; i < nReduce; i++ {
		sub_path := "reduce_" + strconv.Itoa(i)
		err := os.MkdirAll(sub_path, 0777)
		if err != nil && !os.IsExist(err) {
			log.Fatal(err)
		}
		json_path := "map_out_*.json.tmp"
		tmp_file, err := ioutil.TempFile(sub_path, json_path)
		if err != nil {
			log.Fatal(err)
		}
		tmp_file_list = append(tmp_file_list, tmp_file)
		json_encoder_list = append(json_encoder_list, json.NewEncoder(tmp_file))
	}
	for _, kv := range kva {
		reduce_idx := ihash(kv.Key) % nReduce
		enc := json_encoder_list[reduce_idx]
		tmp_file := tmp_file_list[reduce_idx]
		err := enc.Encode(&kv)
		if err != nil {
			tmp_file.Close()
			log.Fatal(err)
		}
	}
	ready_reduce_map_file_list := []string{}
	for _, tmp_file := range tmp_file_list {
		if err := tmp_file.Close(); err != nil {
			log.Fatal(err)
		}
		ready_reduce_map_file := tmp_file.Name()[:len(tmp_file.Name())-4]

		if err := os.Rename(tmp_file.Name(), ready_reduce_map_file); err != nil {
			log.Fatal(err)
		}
		ready_reduce_map_file_list = append(ready_reduce_map_file_list, ready_reduce_map_file)
	}

	return ready_reduce_map_file_list
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ReducePhase(reducef func(string, []string) string, reduce_id int, reduce_inputs []string) string {
	intermediate := []KeyValue{}
	for _, file_name := range reduce_inputs {
		input_file, err := os.Open(file_name)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(input_file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reduce_id)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	return oname

}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	nReduce := GetReudcerNum()
	// fmt.Printf("[worker]nReduce %v\n", nReduce)

	func() {
		ready_map_file := ""
		map_output_file_list := []string{}
		finished_reduce_group_id := -1
		output_file := ""

		for {
			if finished_reduce_group_id == -1 {
				ready_map_file = PullMapFile(ready_map_file, map_output_file_list)
				if ready_map_file != "" {
					map_output_file_list = MapPhase(mapf, nReduce, ready_map_file)
				} else {
					reduce_inputs := []string{}
					finished_reduce_group_id, reduce_inputs = PullReduceFile(finished_reduce_group_id, output_file)
					if finished_reduce_group_id != -1 {
						ReducePhase(reducef, finished_reduce_group_id, reduce_inputs)
					}
				}
			} else {
				reduce_inputs := []string{}
				finished_reduce_group_id, reduce_inputs = PullReduceFile(finished_reduce_group_id, output_file)
				if finished_reduce_group_id != -1 {
					output_file = ReducePhase(reducef, finished_reduce_group_id, reduce_inputs)
				}
			}
		}
	}()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1235")
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
