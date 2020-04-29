package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Add your RPC definitions here.

// MRArgs : we didn't use args from worker to master
type MRArgs struct {

}

// MRReply : return struct from master to worker
type MRReply struct {
	JobName string		// map, reduce, wait, end
	JobID int
	InpFiles []string	// for map, it has only one file
	OutpFile string		// for map, mr-x-; for reduce, mr-out-y
	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
