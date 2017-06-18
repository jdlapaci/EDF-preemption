package main

import (
	"EDFpreemption/constant"
	"fmt"
	"EDFpreemption/scheduler"
	"EDFpreemption/task"
	"time"
)

func main() {
	apps := []*task.App{}

	// Task specifications
	taskSpecs := []task.TaskSpec{
		task.TaskSpec{
			Period:           4 * time.Second,
			TotalRunTimeMean: 1 * time.Second,
			TotalRunTimeStd:  300 * time.Millisecond,
			RelativeDeadline: 3 * time.Second,
		},
		task.TaskSpec{
			Period:           2 * time.Second,
			TotalRunTimeMean: 1 * time.Second,
			TotalRunTimeStd:  300 * time.Millisecond,
			RelativeDeadline: 2 * time.Second,
		},
	}

	// Create all applications
	for i, taskSpec := range taskSpecs {
		apps = append(apps, task.NewApp(fmt.Sprintf("app%d", i), taskSpec))
	}


	// Create and initialize the scheduler
	sched := scheduler.NewScheduler()
	// To be implemented, initialization process

	// Create all workers
	sched.AllWorkerBuf.CreateWorkerPool()
	for _,worker:=range sched.AllWorkerBuf.Pool {
		worker.ReturnChan=sched.ReturnChan
	}
	sched.FreeWorkerBuf.Pool=sched.AllWorkerBuf.Pool


	// Start workers 
	for _,w := range sched.FreeWorkerBuf.Pool{
		go w.TaskProcessLoop()
	}

	// Start the scheduler
	sched.Start2()
	sched.Start1()

	// Start all applications
	for _, app := range apps {
		app.TaskChan=sched.TaskChan
		app.Start()
	}

	time.Sleep(constant.TEST_TIME)

	// Stop all applications
	for _, app := range apps {
		app.Stop()
	}

	// Stop the scheduler
	sched.Stop()
}
