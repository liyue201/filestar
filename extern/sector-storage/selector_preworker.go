package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
)

type preWorkerSelector struct {
	preWorkerUrl string
}

func newPreWorkSelector(preWorkerUrl string) *preWorkerSelector {
	return &preWorkerSelector{
		preWorkerUrl: preWorkerUrl,
	}
}

func (s *preWorkerSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	if whnd.url != s.preWorkerUrl {
		return false, nil
	}
	return true, nil
}

func (s *preWorkerSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return true, nil
}

var _ WorkerSelector = &preWorkerSelector{}
