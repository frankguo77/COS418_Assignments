package mapreduce

import (
	"sync"
	"fmt"
)

func (mr *Master) handleTask(req *DoTaskArgs, wg *sync.WaitGroup) {
	workerName := <-mr.registerChannel
	ok := call(workerName, "Worker.DoTask", req, nil)
	if ok == false {
		fmt.Printf("DoTask: RPC %s error\n", workerName)
		mr.handleTask(req, wg)
		return
	}

	wg.Done()
	mr.registerChannel <- workerName
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//


	var wg sync.WaitGroup
	// <-mr.registerChannel
	for i := 0; i < ntasks; i++ {
		// nworks := len(mr.workers)
	    // fmt.Printf("Nworks: %d\n", nworks)
		// idx := i % 	nworks
		// fmt.Println("wait3")
		
		// fmt.Println("wait4")
		req := &DoTaskArgs{JobName: mr.jobName, Phase: phase, TaskNumber: i, NumOtherPhase: nios}
		if phase == mapPhase {
			req.File = mr.files[i]
		}			

		wg.Add(1)
		go mr.handleTask(req, &wg)
	}

	wg.Wait()

	// fmt.Println("wait1")
	// wg.Wait()
	// fmt.Println("wait2")

	 

	// type DoTaskArgs struct {
	// 	JobName    string
	// 	File       string   // input file, only used in map tasks
	// 	Phase      jobPhase // are we in mapPhase or reducePhase?
	// 	TaskNumber int      // this task's index in the current phase
	
	// 	// NumOtherPhase is the total number of tasks in other phase; mappers
	// 	// need this to compute the number of output bins, and reducers needs
	// 	// this to know how many input files to collect.
	// 	NumOtherPhase int
	// }
	

	debug("Schedule: %v phase done\n", phase)
}
