package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "path/filepath"
import "fmt"
import "time"

// Master : data struct for a master
type Master struct {
	// Your definitions here.
	mapTask2File []string		// input filename of maptasks
	mapTasks []int				// maptask id
	reduceTasks []int			// reducetask id
	nMap int
	nReduce int
	mapStatus map[int]bool		// if map succeed
	reduceStatus map[int]bool	// if reduce succeed
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// AskJob : rfc calling logic to assign MR jobs to workers.
func (m *Master) AskJob(args *MRArgs, reply *MRReply) error {

	m.mu.Lock()
	defer m.mu.Unlock()
	reply.nReduce = m.nReduce
	if len(m.mapStatus) == m.nMap && len(m.reduceStatus) == m.nReduce {
		reply.jobName = "end"
	} else if len(m.mapStatus) == m.nMap && len(m.reduceTasks) > 0 {
		// map phase finished, reduce task has left
		reduceJobID := m.reduceTasks[len(m.reduceTasks) - 1]
		m.reduceTasks = m.reduceTasks[:len(m.reduceTasks) - 1]
		reply.jobName = "reduce"
		reduceFiles, _ := filepath.Glob(fmt.Sprintf("mr-*-%d", reduceJobID))
		reply.inpFiles = reduceFiles
		reply.outpFile = fmt.Sprintf("mr-out-%d", reduceJobID)
		go m.checkReduce(reduceJobID, reply.outpFile)
	} else if len(m.mapTasks) > 0 {
		// map task has left
		mapJobID := m.mapTasks[len(m.mapTasks) - 1]
		m.mapTasks = m.mapTasks[:len(m.mapTasks) - 1]
		reply.jobName = "map"
		reply.inpFiles = []string{m.mapTask2File[mapJobID]}	//TODO: concrete inpFiles
		reply.outpFile = fmt.Sprintf("mr-%d-", mapJobID)
		go m.checkMap(mapJobID, reply.outpFile + "*")
		// checkMap
	} else {
		// neither all job finished nor map/reduce job available
		reply.jobName = "wait"
	}
	return nil
}

func (m *Master) checkMap(jobID int, filePattern string) bool {
	time.Sleep(10 * 1000 * time.Millisecond)
	m.mu.Lock()
	defer m.mu.Unlock()
	check := checkFilePatternExist(filePattern)
	if check {
		// map execution succeeded.
		m.mapStatus[jobID] = true
	} else {
		// map failed.
		m.mapTasks = append(m.mapTasks, jobID)
	}
	return check
}

func (m *Master) checkReduce(jobID int, filePattern string) bool {
	time.Sleep(10 * 1000 * time.Millisecond)
	m.mu.Lock()
	defer m.mu.Unlock()
	check := checkFilePatternExist(filePattern)
	if check {
		// reduce execution succeeded.
		m.reduceStatus[jobID] = true
	} else {
		m.reduceTasks = append(m.reduceTasks, jobID)
	}
	return check
}

func checkFilePatternExist(pattern string) bool {
	matchedNames, _ := filepath.Glob(pattern)
	return matchedNames == nil
}

// Example : an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done : main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if len(m.mapStatus) == m.nMap && len(m.reduceStatus) == m.nReduce {
		ret = true
	}

	return ret
}

// MakeMaster : create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nMap = len(files)
	m.nReduce = nReduce
	m.mapTask2File = files
	m.reduceTasks = make([]int, nReduce)
	for i := range m.mapTasks {
		m.mapTasks[i] = i
	}
	for i := range m.reduceTasks {
		m.reduceTasks[i] = i
	}
	m.mapStatus = make(map[int]bool)
	m.reduceStatus = make(map[int]bool)

	m.server()
	return &m
}
