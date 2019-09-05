package mapreduce

import (
	"fmt"
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
	//

	// 等待ntasks任务结束函数
	wg := sync.WaitGroup{}
	wg.Add(ntasks)

	for i := 0; i < ntasks; i++ {
		// 执行任务准备参数
		args := DoTaskArgs{
			JobName:       jobName,
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		}

		// 只有 mapPhase 需要设置 args.File
		if phase == mapPhase {
			args.File = mapFiles[i]
		}

		go func(args DoTaskArgs) {
			for {
				// 获取worker地址
				worker := <-registerChan
				// 发送任务至 worker 中
				// 成功则标记并释放资源
				// 失败则无限循环不断尝试
				if call(worker, "Worker.DoTask", args, nil) {
					// 标记结束
					wg.Done()
					// worke闲置，worker 重新进入分配队列
					registerChan <- worker
					// 任务成功并资源回收完毕，中断循环
					break
				}
			}
		}(args)
	}

	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
