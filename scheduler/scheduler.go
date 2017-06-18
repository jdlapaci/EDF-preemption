package scheduler

import (
	"EDFpreemption/constant"
	"log"
	"EDFpreemption/task"
	"EDFpreemption/worker"
	"fmt"
)



// Scheduler dispatches tasks to workers
type Scheduler struct {
	TaskChan      		chan *task.Task
	WorkerChan    		chan *worker.Worker
	ReturnChan			chan *task.Task
	StopChan1      		chan interface{}
	StopChan1_verify	chan interface{}
	StopChan2			chan interface{}
	StopChan2_verify	chan interface{}
	FreeWorkerBuf 		*worker.WorkerPool
	CurrentWorkerBuf	*worker.WorkerPool
	AllWorkerBuf  		*worker.WorkerPool
	TaskBuf       		*worker.TaskQueue
}

func NewScheduler() *Scheduler{
	return &Scheduler{
		TaskChan:			make(chan *task.Task,constant.TASK_CHAN_SIZE),
		WorkerChan:			make(chan *worker.Worker,constant.WORKER_NR),
		ReturnChan:			make(chan *task.Task,constant.TASK_CHAN_SIZE),
		StopChan1:			make(chan interface{},1),
		StopChan1_verify:	make(chan interface{},1),
		StopChan2:			make(chan interface{},1),
		StopChan2_verify:	make(chan interface{},1),
		FreeWorkerBuf:		new(worker.WorkerPool),
		CurrentWorkerBuf:	new(worker.WorkerPool),
		AllWorkerBuf:		new(worker.WorkerPool),
		TaskBuf:			new(worker.TaskQueue),
	}
}



func (s *Scheduler) Insert_into_TaskStack(task *task.Task) {
	log.Printf("Insert_into_TaskStack method starts\n")
	if len(s.TaskBuf.Queue)==0 {
		s.TaskBuf.Queue=append(s.TaskBuf.Queue,task)
	}else {
		for i:=0;i<len(s.TaskBuf.Queue); {
			if task.Deadline.Before(s.TaskBuf.Queue[i].Deadline) {
				log.Printf("new task deadline is before task<%d> of the queue\n",i)
				s.TaskBuf.Queue=append(s.TaskBuf.Queue,task)
				copy(s.TaskBuf.Queue[i+1:],s.TaskBuf.Queue[i:])
				s.TaskBuf.Queue[i]=task
				break
			}else {
				if i==len(s.TaskBuf.Queue)-1 {
					log.Printf("new task deadline is the latest of the queue\n")
					s.TaskBuf.Queue=append(s.TaskBuf.Queue,task)
					break
				}else {
					i++
				}
			}
		}
	}
	log.Printf("App<%s>/Task<%d> inserted into queue\n",task.AppID,task.TaskID)
	fmt.Println("Task Queue: ", s.TaskBuf.Queue)
}

// ScheduleLoop runs the scheduling algorithm inside a goroutine
func (s *Scheduler) ScheduleLoop() {
	log.Printf("ScheduleLoop starts\n")
	loop:
		for {
			select {
				case newTask:= <-s.TaskChan:
					s.Insert_into_TaskStack(newTask)
					if len(s.FreeWorkerBuf.Pool) != 0 {
						w:=s.FreeWorkerBuf.Pool[0]
						s.FreeWorkerBuf.Pool=s.FreeWorkerBuf.Pool[1:]
						log.Printf("worker<%d> pulled from FreeWorkerBuf\n",w.WorkerID)
						fmt.Println("FreeWorkerBuf: ", s.FreeWorkerBuf.Pool)
						s.CurrentWorkerBuf.Pool=append(s.CurrentWorkerBuf.Pool,w)
						log.Printf("worker<%d> added to CurrentWorkerBuf\n",w.WorkerID)
						fmt.Println("CurrentWorkerBuf: ", s.CurrentWorkerBuf.Pool)
						w.TaskChan <-s.TaskBuf.Queue[0]
						log.Printf("App<%s>/Task<%d> taken out of queue\n",s.TaskBuf.Queue[0].AppID,s.TaskBuf.Queue[0].TaskID)
						s.TaskBuf.Queue=s.TaskBuf.Queue[1:]
						fmt.Println("Task Queue: ", s.TaskBuf.Queue)
					}else {
						preempt_condition:=false
						x:=s.TaskBuf.Queue[0]
						var y *worker.Worker
						for _,current_worker:=range s.CurrentWorkerBuf.Pool {
							if x.Deadline.Before(current_worker.CurrentTask.Deadline) {
								preempt_condition=true
								y=current_worker
								x=y.CurrentTask
							}
						}
						if preempt_condition==true {
							log.Printf("preempt_condition is true\n")
							y.PreemptFlag <-s.TaskBuf.Queue[0]
							log.Printf("App<%s>/Task<%d> taken out of queue\n",s.TaskBuf.Queue[0].AppID,s.TaskBuf.Queue[0].TaskID)
							s.TaskBuf.Queue=s.TaskBuf.Queue[1:]
							fmt.Println("Task Queue: ", s.TaskBuf.Queue)
						}
					}
				case freeWorker:= <-s.WorkerChan:
					for i,current_worker:=range s.CurrentWorkerBuf.Pool {
						if freeWorker.WorkerID==current_worker.WorkerID {
							s.CurrentWorkerBuf.Pool=append(s.CurrentWorkerBuf.Pool[:i],s.CurrentWorkerBuf.Pool[i+1:]...)
							log.Printf("worker<%d> removed from CurrentWorkerBuf\n",freeWorker.WorkerID)
							fmt.Println("CurrentWorkerBuf: ", s.CurrentWorkerBuf.Pool)
							break
						}
					}
					s.FreeWorkerBuf.Pool=append(s.FreeWorkerBuf.Pool,freeWorker)
					log.Printf("worker added to FreeWorkerBuf\n")
					fmt.Println("FreeWorkerBuf: ", s.FreeWorkerBuf.Pool)
				case returned_task:= <-s.ReturnChan:
					log.Printf("scheduler receives returned preempted App<%s>/Task<%d>\n",returned_task.AppID,returned_task.TaskID)
					s.Insert_into_TaskStack(returned_task)
				case <-s.StopChan1:
					log.Printf("received signal through StopChan1\n")
					for {
						select {
							case freeWorker:= <-s.WorkerChan:
								for i,current_worker:=range s.CurrentWorkerBuf.Pool {
									if freeWorker.WorkerID==current_worker.WorkerID {
										s.CurrentWorkerBuf.Pool=append(s.CurrentWorkerBuf.Pool[:i],s.CurrentWorkerBuf.Pool[i+1:]...)
										log.Printf("worker<%d> removed from CurrentWorkerBuf\n",freeWorker.WorkerID)
										fmt.Println("CurrentWorkerBuf: ", s.CurrentWorkerBuf.Pool)
										break
									}
								}
								s.FreeWorkerBuf.Pool=append(s.FreeWorkerBuf.Pool,freeWorker)
								log.Printf("worker added to FreeWorkerBuf\n")
								fmt.Println("FreeWorkerBuf: ", s.FreeWorkerBuf.Pool)
							default:
								if len(s.TaskBuf.Queue) != 0 {
									if len(s.FreeWorkerBuf.Pool) != 0 {
										w:=s.FreeWorkerBuf.Pool[0]
										s.FreeWorkerBuf.Pool=s.FreeWorkerBuf.Pool[1:]
										log.Printf("worker<%d> pulled from FreeWorkerBuf\n",w.WorkerID)
										fmt.Println("FreeWorkerBuf: ", s.FreeWorkerBuf.Pool)
										s.CurrentWorkerBuf.Pool=append(s.CurrentWorkerBuf.Pool,w)
										log.Printf("worker<%d> added to CurrentWorkerBuf\n",w.WorkerID)
										fmt.Println("CurrentWorkerBuf: ", s.CurrentWorkerBuf.Pool)
										w.TaskChan <-s.TaskBuf.Queue[0]
										log.Printf("App<%s>/Task<%d> taken out of queue\n",s.TaskBuf.Queue[0].AppID,s.TaskBuf.Queue[0].TaskID)
										s.TaskBuf.Queue=s.TaskBuf.Queue[1:]
										fmt.Println("Task Queue: ", s.TaskBuf.Queue)
									}
								}else {
									break loop 
								}
						}
					}
			}
		}
	log.Printf("ScheduleLoop stops\n")
	s.StopChan1_verify <-0
}

func (s *Scheduler) CheckWorkerFlagChans() {
	log.Printf("CheckWorkerFlagChans starts\n")
	loop:
		for {
			select {
				case <-s.StopChan2:
					log.Printf("received signal through StopChan2\n")
					break loop
				default:
					for i:=0;i<len(s.AllWorkerBuf.Pool); {
						select {
							case <-s.AllWorkerBuf.Pool[i].FlagChan:
								log.Printf("received flag from worker<%d>\n",s.AllWorkerBuf.Pool[i].WorkerID)
								s.WorkerChan <-s.AllWorkerBuf.Pool[i]
								log.Printf("sent worker<%d> into s.WorkerChan\n",s.AllWorkerBuf.Pool[i].WorkerID)
								i++
							default:
								i++
						}
					}
			}
		}
		log.Printf("CheckWorkerFlagChans stops\n")
		s.StopChan2_verify <-0
}

// Start starts the scheduler

func (s *Scheduler) Start1() {
	go s.ScheduleLoop()
}

func (s *Scheduler) Start2() {
	go s.CheckWorkerFlagChans()
}

// Stop stops the scheduler
func (s *Scheduler) Stop() {
	s.StopChan1 <- 0
	<-s.StopChan1_verify
	s.StopChan2 <- 0
	<-s.StopChan2_verify
	log.Printf("the two verify channels work\n")
	for _, w := range s.AllWorkerBuf.Pool {
		w.StopChan <- 0
		<-w.FlagChan
		log.Printf("worker<%d> received flag to stop\n",w.WorkerID)
	}
}