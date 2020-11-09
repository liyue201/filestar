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

func (wh *workerHandle) taskLimitOf(taskType sealtasks.TaskType) int {
	if wh.supportedTaskType == nil {
		return 0
	}
	if _, ok := wh.supportedTaskType[taskType]; !ok {
		return 0
	}
	switch taskType {
	case sealtasks.TTAddPiece:
		return wh.info.SellerConf.ApTaskLimit
	case sealtasks.TTPreCommit1:
		return wh.info.SellerConf.P1TaskLimit
	case sealtasks.TTPreCommit2:
		return wh.info.SellerConf.P2TaskLimit
	case sealtasks.TTCommit2:
		return wh.info.SellerConf.C2TaskLimit
	}
	return 0
}

func (wh *workerHandle) updateInfo() {
	wh.lk.Lock()
	defer wh.lk.Unlock()

	info, err := wh.w.Info(context.Background())
	if err == nil {
		res := wh.info.Resources
		info.Resources = res
		wh.info = info
	}
	taskTypes, err := wh.w.TaskTypes(context.Background())
	if err == nil {
		wh.supportedTaskType = taskTypes
	}
}
