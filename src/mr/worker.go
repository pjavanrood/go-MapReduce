package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getNMapReduce() []int {
	var args, nmap, nreduce int
	ok := call("Coordinator.GetNMap", &args, &nmap)
	if !ok {
		return make([]int, 0)
	}
	ok = call("Coordinator.GetNReduce", &args, &nreduce)
	if !ok {
		return make([]int, 0)
	}
	return []int{nmap, nreduce}
}

func getWork() (*Work, error) {
	args := os.Getuid()
	reply := new(Work)
	ok := call("Coordinator.GetWork", &args, &reply)
	if ok {
		return reply, nil
	} else {
		return nil, errors.New("failed to GET work")
	}
}

func postWork(work *Work) bool {
	args := WorkResult{
		Filename:  make([]string, 0),
		Id:        work.Id,
		WorkerUid: work.WorkerUid,
	}
	args.Filename = append(args.Filename, work.Filename...)
	var reply bool
	ok := call("Coordinator.PostWork", &args, &reply)
	if !ok {
		fmt.Println("Error! Failed to POST work")
		return false
	}
	return true
}

func removeFiles(files []string) {
	for _, file := range files {
		e := os.Remove(file)
		if e != nil {
			fmt.Printf("Failed to remove %v\n", file)
		}
	}
}

func handleMap(work *Work, mapf func(string, string) []KeyValue) {
	filename := work.Filename[0]
	content, err := os.ReadFile(filename)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	createdFiles := make([]string, 0)
	kva := mapf(filename, string(content))
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		b := ihash(kv.Key) % nReduce
		buckets[b] = append(buckets[b], kv)
	}
	for bucket, kvb := range buckets {
		if len(kvb) == 0 {
			continue
		}
		outputFileName := fmt.Sprintf("mr-tempout-%v-%v-%v", bucket, work.Id, work.WorkerUid)
		outputFile, err := os.Create(outputFileName)
		if err != nil {
			fmt.Println("Error creating file:", err)
			removeFiles(createdFiles)
			return
		}
		createdFiles = append(createdFiles, outputFileName)
		encoder := json.NewEncoder(outputFile)
		err = encoder.Encode(kvb)
		if err != nil {
			fmt.Println("Error Encoding JSON:", err)
			removeFiles(createdFiles)
			return
		}
	}
	work.Filename = createdFiles
	ok := postWork(work)
	if !ok {
		removeFiles(createdFiles)
	}
}

func handleReduce(work *Work, reducef func(string, []string) string) {
	//fmt.Printf("Reducing: %v\n", work.Filename)
	bucket := work.Id - nMap
	if bucket < 0 {
		fmt.Printf("Invalid Bucket! %v\n", bucket)
		return
	}
	kvMap := make(map[string][]string)
	for _, filename := range work.Filename {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		dec := json.NewDecoder(file)
		var kvb []KeyValue
		if err = dec.Decode(&kvb); err != nil {
			fmt.Println(err.Error())
			file.Close()
			continue
		}
		file.Close()
		for _, kv := range kvb {
			values, ok := kvMap[kv.Key]
			if !ok {
				kvMap[kv.Key] = make([]string, 0)
				values = kvMap[kv.Key]
			}
			values = append(values, kv.Value)
			kvMap[kv.Key] = values
		}
	}
	reducedKV := make([]KeyValue, 0)
	for key, values := range kvMap {
		reduced := reducef(key, values)
		reducedKV = append(reducedKV, KeyValue{key, reduced})
	}
	outputFileName := fmt.Sprintf("mr-out-%v", bucket)
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	for _, kv := range reducedKV {
		_, err = outputFile.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
		if err != nil {
			fmt.Println("Error writing to file:", err)
		}
	}
	//encoder := json.NewEncoder(outputFile)
	//err = encoder.Encode(reducedKV)
	//if err != nil {
	//	fmt.Println("Error Encoding JSON:", err)
	//	outputFile.Close()
	//	return
	//}
	outputFile.Close()
	work.Filename = []string{outputFileName}
	ok := postWork(work)
	if !ok {
		removeFiles([]string{outputFileName})
	}
	return
}

var nMap, nReduce int

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	nMapReduce := getNMapReduce()
	if len(nMapReduce) == 0 {
		fmt.Println("Error! Couldn't get NReduce")
		return
	}
	nMap, nReduce = nMapReduce[0], nMapReduce[1]
	for {
		time.Sleep(time.Second)
		work, err := getWork()
		//fmt.Println(work)
		if err != nil {
			fmt.Println(err.Error())
			continue
		} else if work.Task == Finish {
			//fmt.Println("Done!...")
			break
		} else if len(work.Filename) == 0 {
			//fmt.Println("No More Work!...")
			continue
		}
		switch work.Task {
		case Map:
			handleMap(work, mapf)
		case Reduce:
			handleReduce(work, reducef)
		default:
			fmt.Printf("Invalid Task Type %v\n", work.Task)
		}
	}
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
