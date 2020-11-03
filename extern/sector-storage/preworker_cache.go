package sectorstorage

import (
	"encoding/json"
	"github.com/filecoin-project/go-state-types/abi"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
)

func preWorkerCacheFile() string {
	minerPath := os.Getenv("LOTUS_MINER_PATH")
	if minerPath == "" {
		minerPath = os.TempDir()
	}
	return minerPath + "/preworker"
}

func loadPreWorkerMap(preWorker *sync.Map) {
	filePath := preWorkerCacheFile()
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Errorf("loadPreWorkerMap: %v, file:%v", err, filePath)
		return
	}
	m := make(map[string]string)
	err = json.Unmarshal(data, &m)
	if err != nil {
		return
	}
	for k, v := range m {
		sectorId, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			continue
		}
		preWorker.Store(abi.SectorNumber(sectorId), v)
	}
}

func preWorkerSaveFunc(preWorker *sync.Map) func() {
	mutex := sync.Mutex{}
	return func() {
		mutex.Lock()
		defer mutex.Unlock()
		savePreWorkerMap(preWorker)
	}
}

func savePreWorkerMap(preWorker *sync.Map) {
	m := make(map[string]interface{})
	preWorker.Range(func(key, value interface{}) bool {
		v := key.(abi.SectorNumber)
		m[v.String()] = value
		return true
	})
	data, err := json.Marshal(m)
	if err != nil {
		log.Errorf("savePreWorkerMap: %v", err)
		return
	}
	filePath := preWorkerCacheFile()
	err = ioutil.WriteFile(filePath, data, 0666)
	if err != nil {
		log.Errorf("savePreWorkerMap: %v, file:%v", err, filePath)
	}
}
