package storiface

import (
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
)

type WorkerInfo struct {
	Hostname   string
	SellerConf SealerConfig
	Resources  WorkerResources
}

type WorkerResources struct {
	MemPhysical uint64
	MemSwap     uint64

	MemReserved uint64 // Used by system / other processes

	CPUs uint64 // Logical cores
	GPUs []string
}

type WorkerStats struct {
	Info WorkerInfo

	MemUsedMin uint64
	MemUsedMax uint64
	GpuUsed    bool   // nolint
	CpuUse     uint64 // nolint
}

type WorkerJob struct {
	ID     uint64
	Sector abi.SectorID
	Task   sealtasks.TaskType

	RunWait int // 0 - running, 1+ - assigned
	Start   time.Time
}

type SealerConfig struct {
	ApTaskLimit int
	P1TaskLimit int
	P2TaskLimit int
	C2TaskLimit int
}
