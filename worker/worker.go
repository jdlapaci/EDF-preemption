package worker

import (
	"EDFpreemption/constant"
	"log"
	"sync"
	"EDFpreemption/task"
	"time"
)

type TaskQueue struct {
	Queue []*task.Task
	Lock  *sync.Mutex
}

type WorkerPool struct {
	Pool []*Worker
	Lock *sync.Mutex
}

// Worker is the agent to process tasks
type Worker struct {
	WorkerID 	int
	TaskChan 	chan *task.Task
	StopChan 	chan interface{}
	FlagChan	chan interface{}
	PreemptFlag	chan *task.Task
	ReturnChan	chan *task.Task
	CurrentTask	*task.Task
}


func NewWorker(workerid int) *Worker{
	return &Worker{
		WorkerID:		workerid,
		TaskChan:		make(chan *task.Task,constant.TASK_CHAN_SIZE),
		StopChan:		make(chan interface{},1),
		FlagChan:		make(chan interface{},1),
		PreemptFlag:	make(chan *task.Task,constant.TASK_CHAN_SIZE),
		ReturnChan:		make(chan *task.Task,1),
		CurrentTask:	new(task.Task),
	}
}


func (wp *WorkerPool) CreateWorkerPool() {
	for i:=0;i<constant.WORKER_NR;i++ {
		wp.Pool=append(wp.Pool,NewWorker(i))
	}
}



// TaskProcessLoop processes tasks with preemption
func (w *Worker) TaskProcessLoop() {
	log.Printf("Worker<%d>: Task processor starts\n", w.WorkerID)
loop:
	for {
		select {
		case t := <-w.TaskChan:
			// This worker receives a new task to run
			w.ProcessPreempt(t)
			w.FlagChan <-0 						// this will alert the scheduler that this worker is ready to be put back into its FreeWorkerBuf
			log.Printf("worker<%d> is finished processing App<%s>/Task<%d> and sends flag\n",w.WorkerID,w.CurrentTask.AppID,w.CurrentTask.TaskID)
		case <-w.StopChan:
			// Receive signal to stop
			w.FlagChan <-0
			break loop
		}
	}
	log.Printf("Worker<%d>: Task processor ends\n", w.WorkerID)
}

// Process runs a task on a worker with preemption
func (w *Worker) ProcessPreempt(t *task.Task) {
	w.CurrentTask=t 										// Update the worker's current running task
	log.Printf("Worker <%d>: App<%s>/Task<%d> starts (ddl %v)\n", w.WorkerID, t.AppID, t.TaskID, t.Deadline)
	loop:
		for {
			time.Sleep(constant.CHECK_PREEMPT_INTERVAL)
			t.RunTime+=constant.CHECK_PREEMPT_INTERVAL
			if t.RunTime>=t.TotalRunTime {
				log.Printf("Worker <%d>: App<%s>/Task<%d> ends\n", w.WorkerID, t.AppID, t.TaskID)
				break loop
			}else {
				select {
					case preempt_task:= <-w.PreemptFlag:		// If a task is received through the PreemptFlag channel,
						log.Printf("worker<%d> preempted with App<%s>/Task<%d>\n",w.WorkerID,preempt_task.AppID,preempt_task.TaskID)
						w.ReturnChan <-t						// the current task being processed is sent through the worker's ReturnChan and put back into the TaskBuf
						log.Printf("App<%s>/Task<%d> is returned through worker<%d> ReturnChan\n",t.AppID,t.TaskID,w.WorkerID)
						w.ProcessPreempt(preempt_task)			// and the worker starts processing the new task
						break loop
					default:
						continue
				}
			}
		}
}
