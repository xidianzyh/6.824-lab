package mapreduce

import (
	"fmt"
	"net/rpc"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	// 同步语义
	var wg sync.WaitGroup
	wg.Add(ntasks)
	var workerTask map[string][]int = make(map[string][]int)
	taskChan := make(chan int)
	closeChan := make(chan bool)

	go func() {
		for i := 0; i < ntasks; i++ {
			taskChan <- i
		}
	}()

	//关键点：将任务处理也放在 goroutine 中，不会阻塞整个任务。
	go func() {
		TaskClose:
		for {
			select {
				case taskId := <- taskChan:
					workerAddr := <-registerChan
					mapFile := mapFiles[taskId]
					// workerTask 没有被并发处理
					workerTask[workerAddr] = append(workerTask[workerAddr], taskId)
					args := DoTaskArgs{jobName, mapFile, phase, taskId, n_other}
					fmt.Printf("分配任务%d 给worker %s\n", taskId, workerAddr)
					go func() {
						client, err := rpc.Dial("unix", workerAddr)
						if err != nil {
							if len(workerTask[workerAddr]) > 1 {
								wg.Add(len(workerTask[workerAddr]) - 1)
							}
							fmt.Printf("worker%s失败，重新分配任务%v\n", workerAddr, workerTask[workerAddr])
							for _, taskNo := range workerTask[workerAddr] {
								taskChan <- taskNo
							}
							return
						}
						var reply struct{}
						client.Call("Worker.DoTask", &args, &reply)
						client.Close()
						wg.Done()
						registerChan <- workerAddr
					}()
			case <- closeChan:
				break TaskClose
			}
		}
	}()

	//主线程等待所有任务的执行
	wg.Wait()
	// 关闭整个程序的执行
	closeChan <- true
	fmt.Printf("Schedule: %v done\n", phase)
}
