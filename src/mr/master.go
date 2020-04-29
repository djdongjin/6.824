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
import "strings"
import "io/ioutil"


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
	reply.NReduce = m.nReduce
	if len(m.mapStatus) == m.nMap && len(m.reduceStatus) == m.nReduce {
		reply.JobName = "end"
	} else if len(m.mapStatus) == m.nMap && len(m.reduceTasks) > 0 {
		// map phase finished, reduce task has left
		reduceJobID := m.reduceTasks[len(m.reduceTasks) - 1]
		m.reduceTasks = m.reduceTasks[:len(m.reduceTasks) - 1]
		reply.JobName = "reduce"
		reply.JobID = reduceJobID
		reduceFiles, _ := filepath.Glob(fmt.Sprintf("mr-*-%d", reduceJobID))
		reply.InpFiles = reduceFiles
		reply.OutpFile = fmt.Sprintf("mr-out-%d", reduceJobID)
		go m.checkReduce(reduceJobID, reply.OutpFile)
	} else if len(m.mapTasks) > 0 {
		// map task has left
		mapJobID := m.mapTasks[len(m.mapTasks) - 1]
		m.mapTasks = m.mapTasks[:len(m.mapTasks) - 1]
		reply.JobName = "map"
		reply.JobID = mapJobID
		reply.InpFiles = []string{m.mapTask2File[mapJobID]}
		reply.OutpFile = fmt.Sprintf("mr-%d-", mapJobID)
		go m.checkMap(mapJobID, reply.OutpFile + "*")
	} else {
		// neither all job finished nor map/reduce job available
		reply.JobName = "wait"
		reply.JobID = 0
	}
	return nil
}

func (m *Master) checkMap(jobID int, filePattern string) bool {
	time.Sleep(10 * time.Second)
	m.mu.Lock()
	defer m.mu.Unlock()
	check := checkFilePatternExist(filePattern, m.nReduce)
	if check {
		// map execution succeeded.
		m.mapStatus[jobID] = true
		log.Printf("Master checkout Map %d\n", jobID)
	} else {
		// map failed.
		m.mapTasks = append(m.mapTasks, jobID)
		log.Printf("Master push back Map %d\n", jobID)
	}
	return check
}

func (m *Master) checkReduce(jobID int, filePattern string) bool {
	time.Sleep(10 * time.Second)
	m.mu.Lock()
	defer m.mu.Unlock()
	check := checkFilePatternExist(filePattern, 1)
	if check {
		// reduce execution succeeded.
		m.reduceStatus[jobID] = true
		log.Printf("Master checkout Reduce %d\n", jobID)
		cachedFileName, _ := filepath.Glob(fmt.Sprintf("mr-*-%d", jobID))
		for _, fname := range cachedFileName {
			if !strings.Contains(fname, "mr-out") {
				os.Remove(fname)
			}
		}
	} else {
		m.reduceTasks = append(m.reduceTasks, jobID)
		log.Printf("Master push back Reduce %d\n", jobID)
	}
	return check
}

func checkFilePatternExist(pattern string, cnt int) bool {
	matchedNames, _ := filepath.Glob(pattern)
	ret := matchedNames != nil && len(matchedNames) >= cnt
	if !ret {
		for _, nm := range matchedNames {
			err := os.Remove(nm)
			if err != nil {
				log.Fatalf("Error on deleting file %v\n", nm)
			} else {
				log.Printf("Delete file %v", nm)
			}
		}
	}
	return ret
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
	m.mu.Lock()
	defer m.mu.Unlock()
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
	log.SetOutput(ioutil.Discard)
	// Your code here.
	m.nMap = len(files)
	m.nReduce = nReduce
	m.mapTask2File = files
	m.mapTasks = make([]int, m.nMap)
	for i := range m.mapTasks {
		m.mapTasks[i] = i
	}
	m.reduceTasks = make([]int, m.nReduce)
	for i := range m.reduceTasks {
		m.reduceTasks[i] = i
	}
	m.mapStatus = make(map[int]bool)
	m.reduceStatus = make(map[int]bool)
	m.server()
	return &m
}
