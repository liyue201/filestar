package sectorstorage

import (
	"context"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

func (m *Manager) WorkerStats() map[uint64]storiface.WorkerStats {
	m.sched.workersLk.RLock()
	defer m.sched.workersLk.RUnlock()

	out := map[uint64]storiface.WorkerStats{}

	for id, handle := range m.sched.workers {
		out[uint64(id)] = storiface.WorkerStats{
			Info:       handle.info,
			MemUsedMin: handle.active.memUsedMin,
			MemUsedMax: handle.active.memUsedMax,
			GpuUsed:    handle.active.gpuUsed,
			CpuUse:     handle.active.cpuUse,
		}
	}

	return out
}

func (m *Manager) WorkerJobs() map[uint64][]storiface.WorkerJob {
	m.sched.workersLk.RLock()
	defer m.sched.workersLk.RUnlock()

	out := map[uint64][]storiface.WorkerJob{}

	for id, handle := range m.sched.workers {
		out[uint64(id)] = handle.wt.Running()

		handle.wndLk.Lock()
		for wi, window := range handle.activeWindows {
			for _, request := range window.todo {
				out[uint64(id)] = append(out[uint64(id)], storiface.WorkerJob{
					ID:      0,
					Sector:  request.sector,
					Task:    request.taskType,
					RunWait: wi + 1,
					Start:   request.start,
				})
			}
		}
		handle.wndLk.Unlock()
	}

	return out
}

func taskIntersects(supportedTasks map[sealtasks.TaskType]struct{}, taskTypes []sealtasks.TaskType) bool {
	for _, t := range taskTypes {
		if _, ok := supportedTasks[t]; ok {
			return true
		}
	}
	return false
}

func taskContains(taskTypes []sealtasks.TaskType, taskType sealtasks.TaskType) bool {
	for _, t := range taskTypes {
		if t == taskType {
			return true
		}
	}
	return false
}

func (m *Manager) taskCountAndLimitOf(taskTypes []sealtasks.TaskType) (totalTaskLimit int, count int) {
	jobm := m.WorkerJobs()
	for _, jobs := range jobm {
		for _, job := range jobs {
			if taskContains(taskTypes, job.Task) {
				count++
			}
		}
	}

	m.sched.workersLk.RLock()
	defer m.sched.workersLk.RUnlock()

	//task in queue
	for sqi := 0; sqi < m.sched.schedQueue.Len(); sqi++ {
		task := (* m.sched.schedQueue)[sqi]
		if taskContains(taskTypes, task.taskType) {
			count++
		}
	}

	for id, wh := range m.sched.workers {
		taskLimit := wh.taskLimitOf(taskTypes)
		totalTaskLimit += taskLimit
		log.Infof("id: %v,  limit: %v", id, taskLimit)
	}

	return
}

func (wh *workerHandle) taskCount() (count int) {
	jobs := wh.wt.Running()
	count = len(jobs)

	wh.wndLk.Lock()
	defer wh.wndLk.Unlock()

	for _, window := range wh.activeWindows {
		count += len(window.todo)
	}
	return
}

func (wh *workerHandle) taskCountOf(taskTypes []sealtasks.TaskType) (count int) {
	jobs := wh.wt.Running()
	for _, job := range jobs {
		if taskContains(taskTypes, job.Task) {
			count++
		}
	}

	wh.wndLk.Lock()
	defer wh.wndLk.Unlock()

	for _, window := range wh.activeWindows {
		for _, job := range window.todo {
			if taskContains(taskTypes, job.taskType) {
				count++
			}
		}
	}
	return
}

func (wh *workerHandle) taskLimitOf(taskTypes []sealtasks.TaskType) int {
	supportedTasks, err := wh.w.TaskTypes(context.Background())
	if err != nil {
		return 0
	}
	if taskIntersects(supportedTasks, taskTypes) {
		return wh.taskLimit()
	}
	return 0
}

func (wh *workerHandle) taskLimit() int {
	info, err := wh.w.Info(context.Background())
	if err != nil {
		return 0
	}
	if info.TaskLimit >= 0 {
		return info.TaskLimit
	}
	return wh.globalTaskLimitPerWorker
}
