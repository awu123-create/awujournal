+++
date = '2026-02-22T16:42:03+08:00'
draft = false
title = 'Lab1 MapReduce'
tags = ["mit6.824", "mapreduce", "lab"]
categories = ["MIT 6.824", "Distributed Systems"]
weight = 1
description = "实现一个简化版 MapReduce：Master/Worker、任务分配、容错与超时处理。"
showToc = true
comments = true
+++

# lab1 MapReduce

## 思路

---

由于是刚开始学习分布式系统，所以我认为现阶段掌握整体架构的设计比代码的规整性更重要，于是我选择使用加锁的方式来处理并发而不是使用channel。

在lab1的源代码中，已经实现了一个单进程的顺序版的MapReduce，我们需要将其改写成分布式的MapReduce，master进程和worker进程之间使用RPC进行通信。

我的实现思路如下：

1. 在rpc.go中定义最小RPC协议，RequestArgs/RequestReply用于worker进程向master进程拉取任务，ReportArgs/ReportReply用于worker进程向master进程报告任务的完成情况
2. 定义Master结构体的字段，用于分配任务；然后实现给worker进程分配任务的AssignTask方法
3. 实现Worker函数，使用一个无尽循环不断RPC调用AssignTask方法，从master进程取任务，根据返回的任务类型（Map/Reduce/Wait/Exit）做出相应的动作，然后返回状态给master进程
4. 实现供master进程和worker进程main函数调用的其余函数和方法，如MakeMaster函数初始化Master结构体并调用server方法启动RPC服务和Done方法判断整体的MapReduce任务是否执行完成

## 实现

---

### RPC结构

worker进程从master进程获取任务时，需要知道当前获取到的是什么任务，对于Map任务需要知道当前任务的ID、输入文件和Reduce任务的总数（用于哈希分桶）；对于Reduce任务需要知道当前任务的ID、Map任务的总数（获取所有需要由此Reduce任务处理的中间文件）。

worker进程在获取任务并进行处理后，可能成功也可能不成功，其需要将任务的完成情况报告给master进程，若不成功的话ReportTask方法就将此任务的状态设置为Idle，等待下一次分配，若成功则将状态设置为Completed。

所以RPC结构体定义如下：

```go
type TaskType int

const(
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type RequestArgs struct{

}

type RequestReply struct{
	ID int			// 任务ID
	Type TaskType	// 任务类型
	FileName string	// map任务的输入文件
	NumMap int		// map任务的总数
	NumReduce int	// reduce任务的总数
}

type ReportArgs struct{
	Type TaskType	// 任务类型
	ID int			// 任务ID
	Ok bool		// 任务是否成功完成
}

type ReportReply struct{}
```

### worker接收信息

worker需要做的就是轮询master，拉任务以进行处理，对于Map任务和Reduce任务分别实现doMapTask方法和doReduceTask方法，在某一阶段任务都被worker进程拉取且还在处理时，等待一段时间再继续轮询。report函数封装了RPC调用返回任务处理信息的逻辑。

```go
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		// 拉任务
		request:=RequestArgs{}
		reply:=RequestReply{}
		ok:=call("Master.AssignTask",&request,&reply)
		if !ok{
			time.Sleep(200*time.Millisecond)
			continue
		}

		// 执行任务
		if reply.Type==MapTask{
			err:=doMapTask(reply.ID,reply.FileName,reply.NumReduce,mapf)
			report(MapTask,reply.ID,err==nil)
		}else if reply.Type==ReduceTask{
			err:=doReduceTask(reply.ID,reply.NumMap,reducef)
			report(ReduceTask,reply.ID,err==nil)
		}else if reply.Type==WaitTask{
			// 等待一段时间
			time.Sleep(time.Second)
		}else{
			// 没有任务了，退出
			break
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()
	
}

func report(taskType TaskType,taskId int,ok bool)  {
	args:=ReportArgs{
		Type:taskType,
		ID:taskId,
		Ok:ok,
	}
	reply:=ReportReply{}
	call("Master.ReportTask",&args,&reply)
}
```

<aside>
💡

需要注意的是，在Map任务和Reduce任务的处理过程中，对于文件的写入操作都要是**原子性的**，否则可能会产生这种情况：worker在进行任务到一半时，因为某种原因挂掉，导致文件中只写入了部分数据，后续别的worker再处理此任务时，文件中会有重复的数据。

这里面使用创建临时文件的方法。先在同目录下使用os包中的CreateTemp方法创建一个临时文件，然后把完整内容写入并使用Close方法确保落盘；只有全部写成功后，才调用Rename方法将临时文件原子地替换为最终文件；若处理时遇到错误，就清除临时文件。

</aside>

### master进程分配任务

在lab1任务要求中，说有一个master进程和多个worker进程，这些worker进程都会向master进程请求任务，所以对于分配任务的AssignTask方法需要并发处理，同时任务要求中还有对任务超时的处理。

```go
type Master struct {
	// Your definitions here.
	mu sync.Mutex	// 保证RPC处理函数的互斥访问
	files []string	// 输入文件列表
	nReduce int		// reduce任务数量
	phase int		// 当前阶段：0-map阶段，1-reduce阶段，2-完成
	mapTasks []TaskMeta
	reduceTasks []TaskMeta
}

type State int

const(
	Idle State = iota
	InProgress
	Completed
)

type TaskMeta struct{
	state State
	startTime time.Time
}
```

这里对于Map任务和Reduce任务的状态我使用了最简单的方法来处理，分别维护一个任务切片。在任务分配开始前，需要先检查是否有超时的任务，超时就将任务状态设置为Idle，等待别的worker进程来取此任务进行处理。此外，AssignTask方法还负责阶段的推进，流程是Map→Reduce→Completed，只有某一阶段的任务全部完成了才可以进入下一阶段。事实上还可以在master进程中再创建一个goroutine来在后台检测超时任务。

## 总结

lab1是6.824中最简单的一个lab，主要是用于熟悉go的语法，入门理解分布式的原理。

- 同个键经过Map分割映射后必定落在相同的桶下。
- 大任务划分成不同的桶后变成子任务，经过Reduce操作再合并结果

当前版本的实现比较粗糙，后面会改进为使用channel来代替lock，实现lock-free（虽然channel底层也包含lock），且精炼代码，使用更高效的方法来处理。