package sectorstorage

import (
	"encoding/json"
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

func loadPreWorkerMap(preWorker sync.Map) {
	filePath := preWorkerCacheFile()
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
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
		preWorker.Store(sectorId, v)
	}
}

func preWorkerSaveFunc(preWorker sync.Map) func() {
	mutex := sync.Mutex{}
	return func() {
		mutex.Lock()
		defer mutex.Unlock()
		savePreWorkerMap(preWorker)
	}
}

func savePreWorkerMap(preWorker sync.Map) {
	m := make(map[interface{}]interface{})
	preWorker.Range(func(key, value interface{}) bool {
		m[key] = value
		return true
	})
	data, err := json.Marshal(m)
	if err != nil {
		return
	}
	filePath := preWorkerCacheFile()
	ioutil.WriteFile(filePath, data, 0666)
}
