package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState int64

const (
	UNSTARTED TaskState = iota
	RUNNING
	UNASSIGNED
	DONE
)

type TaskInfo struct {
	startTime time.Time
	state     TaskState
}

const StrugglerTimeout = 5 * time.Second

type Coordinator struct {
	// Your definitions here.
	fileNames []string
	nReduce   int

	// map of task indexes and infos
	mapInfos map[int]TaskInfo
	// set of indexes of done map tasks (if done s[i] == true else s[i] doesn't exist)
	doneMaps map[int]bool

	// map of task indexes and infos
	reduceInfos map[int]TaskInfo
	// set of indexes of done map tasks (if done s[i] == true else s[i] doesn't exist)
	doneReduces map[int]bool

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *interface{}, reply *TaskArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.doneMaps) != len(c.fileNames) {
		mapsState := c.getMapsState(reply)
		if mapsState == UNASSIGNED {
			return nil
		}
		if mapsState == RUNNING {
			reply.Action = WAIT
			return nil
		}
	}

	if len(c.doneReduces) != c.nReduce {
		reduceState := c.getReducesState(reply)
		if reduceState == UNASSIGNED {
			return nil
		}
		if reduceState != RUNNING {
			reply.Action = WAIT
			return nil
		}
	}
	if len(c.doneReduces) == c.nReduce {
		reply.Action = FINISHED
	}

	return nil

}

func (c *Coordinator) getMapsState(reply *TaskArgs) TaskState {
	for i, info := range c.mapInfos {
		if c.doneMaps[i] {
			continue
		}
		if info.state == RUNNING {
			if time.Since(info.startTime) > StrugglerTimeout {
				c.mapInfos[i] = TaskInfo{time.Now(), UNSTARTED}
				info = c.mapInfos[i]
			}
		}
		if info.state == UNSTARTED {
			c.mapInfos[i] = TaskInfo{time.Now(), RUNNING}
			reply.Index = i
			reply.FileName = c.fileNames[i]
			reply.Action = MAP
			reply.NReduce = c.nReduce
			return UNASSIGNED
		}
	}
	return RUNNING
}

func (c *Coordinator) getReducesState(reply *TaskArgs) TaskState {
	for i, info := range c.reduceInfos {
		if c.doneReduces[i] {
			continue
		}
		if info.state == RUNNING {
			if time.Since(info.startTime) > StrugglerTimeout {
				c.reduceInfos[i] = TaskInfo{time.Now(), UNSTARTED}
				info = c.reduceInfos[i]
			}
		}
		if info.state == UNSTARTED {
			c.reduceInfos[i] = TaskInfo{time.Now(), RUNNING}
			reply.Index = i
			reply.Action = REDUCE
			reply.NMap = len(c.fileNames)
			return UNASSIGNED
		}
	}
	return RUNNING
}

func (c *Coordinator) MarkAsDone(args *TaskDone, reply *interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Action == MAP {
		c.mapInfos[args.Index] = TaskInfo{state: DONE}
		c.doneMaps[args.Index] = true
	} else if args.Action == REDUCE {
		c.reduceInfos[args.Index] = TaskInfo{state: DONE}
		c.doneReduces[args.Index] = true
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.doneReduces) == c.nReduce
}

func (c *Coordinator) fillMaps() {
	for i := range c.fileNames {
		c.mapInfos[i] = TaskInfo{time.Time{}, UNSTARTED}
	}
}

func (c *Coordinator) fillReduces() {
	for i := 0; i < c.nReduce; i++ {
		c.reduceInfos[i] = TaskInfo{time.Time{}, UNSTARTED}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.fileNames = files
	c.nReduce = nReduce

	c.mapInfos = make(map[int]TaskInfo)
	c.doneMaps = make(map[int]bool)

	c.reduceInfos = make(map[int]TaskInfo)
	c.doneReduces = make(map[int]bool)

	c.fillMaps()
	c.fillReduces()
	c.server()
	return &c
}
