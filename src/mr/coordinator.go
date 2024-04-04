package mr

import (
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Finish
)

type StateType int

const (
	IDLE StateType = iota
	InProgress
	Completed
)

type Work struct {
	Task      TaskType
	State     StateType
	Filename  []string
	Id        int
	WorkerUid int
	mu        sync.RWMutex
}

func (w *Work) copyTo(dest *Work) {
	dest.Task = w.Task
	dest.State = w.State
	dest.Filename = make([]string, 0)
	dest.Filename = append(dest.Filename, w.Filename...)
	dest.Id = w.Id
	dest.WorkerUid = w.WorkerUid
}

type Coordinator struct {
	worksMap      map[StateType][]*Work
	nReduce, nMap int
	mu            sync.RWMutex
	done          bool
}

func (c *Coordinator) GetNMap(request *int, response *int) error {
	*response = c.nMap
	return nil
}

func (c *Coordinator) GetNReduce(request *int, response *int) error {
	*response = c.nReduce
	return nil
}

func (c *Coordinator) GetWork(request *int, response *Work) error {
	uid := *request
	c.mu.RLock()
	done := c.done
	noWork := len(c.worksMap[IDLE]) == 0
	c.mu.RUnlock()
	if done {
		response.Task = Finish
	} else if noWork {
		response.Filename = make([]string, 0)
	} else {
		c.mu.Lock()
		work := c.worksMap[IDLE][0]
		c.worksMap[IDLE] = c.worksMap[IDLE][1:]
		c.worksMap[InProgress] = append(c.worksMap[InProgress], work)
		c.mu.Unlock()
		work.mu.Lock()
		work.WorkerUid = uid
		work.State = InProgress
		work.copyTo(response)
		work.mu.Unlock()
		go func() {
			time.Sleep(10 * time.Second)
			work.mu.RLock()
			completed := work.State == Completed
			work.mu.RUnlock()
			if !completed {
				c.mu.Lock()
				idx := slices.IndexFunc(
					c.worksMap[InProgress], func(w *Work) bool {
						return w == work
					})
				if idx < 0 {
					c.mu.Unlock()
					return
				}
				c.worksMap[InProgress] = append(c.worksMap[InProgress][:idx], c.worksMap[InProgress][idx+1:]...)
				work.mu.Lock()
				work.State = IDLE
				work.mu.Unlock()
				c.worksMap[IDLE] = append(c.worksMap[IDLE], work)
				c.mu.Unlock()
			}
		}()
	}
	return nil
}

func (c *Coordinator) PostWork(req *WorkResult, res *bool) error {
	c.mu.Lock()
	idx := slices.IndexFunc(
		c.worksMap[InProgress], func(w *Work) bool {
			return w.Id == req.Id && w.WorkerUid == req.WorkerUid
		})
	if idx < 0 {
		c.mu.Unlock()
		*res = false
		return nil
	}
	work := c.worksMap[InProgress][idx]
	c.worksMap[InProgress] = append(c.worksMap[InProgress][:idx], c.worksMap[InProgress][idx+1:]...)
	work.mu.Lock()
	work.State = Completed
	work.Filename = append(work.Filename, req.Filename...)
	work.mu.Unlock()
	c.worksMap[Completed] = append(c.worksMap[Completed], work)
	if len(c.worksMap[Completed]) == c.nMap && len(c.worksMap[IDLE]) == 0 {
		c.initWorksReduce()
	}
	if len(c.worksMap[IDLE]) == 0 && len(c.worksMap[InProgress]) == 0 {
		c.reduce()
	}
	c.mu.Unlock()
	*res = true
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.RLock()
	ret := c.done
	c.mu.RUnlock()

	//fmt.Println(c.worksMap)

	return ret
}

func (c *Coordinator) initWorksMap(files []string) {
	c.worksMap[IDLE] = make([]*Work, 0)
	c.worksMap[InProgress] = make([]*Work, 0)
	c.worksMap[Completed] = make([]*Work, 0)
	for i, filename := range files {
		work := Work{
			Filename: []string{filename},
			Task:     Map,
			State:    IDLE,
			Id:       i,
		}
		//fmt.Println(work)
		c.worksMap[IDLE] = append(c.worksMap[IDLE], &work)
	}
}

func (c *Coordinator) initWorksReduce() {
	createdFiles := make([]string, 0)
	for _, work := range c.worksMap[Completed] {
		createdFiles = append(createdFiles, work.Filename[1:]...)
	}
	fileBuckets := make([][]string, c.nReduce)
	for _, file := range createdFiles {
		splitName := strings.Split(file, "-")
		if len(splitName) != 5 {
			fmt.Printf("Error in [initWorkReduce]: Invalid filename %v\n", file)
			continue
		}
		bucket, err := strconv.Atoi(splitName[2])
		if err != nil {
			fmt.Printf("Error in [initWorkReduce]: Invalid filename %v\n", file)
			continue
		}
		fileBuckets[bucket] = append(fileBuckets[bucket], file)
	}
	for bucket, files := range fileBuckets {
		if len(files) == 0 {
			continue
		}
		work := Work{
			Filename: make([]string, 0),
			Task:     Reduce,
			State:    IDLE,
			Id:       c.nMap + bucket,
		}
		work.Filename = append(work.Filename, files...)
		//fmt.Println(work)
		c.worksMap[IDLE] = append(c.worksMap[IDLE], &work)
	}
}

func (c *Coordinator) reduce() {
	c.done = true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		worksMap: make(map[StateType][]*Work),
		nMap:     len(files),
		nReduce:  nReduce,
		done:     false,
	}
	c.initWorksMap(files)

	//fmt.Println(nReduce)
	//fmt.Println(files)

	c.server()
	return &c
}
