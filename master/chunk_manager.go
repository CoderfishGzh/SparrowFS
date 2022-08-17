package master

import (
	"SparrowFS"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

// chunkManager , the struct to manager chunks
type chunkManager struct {
	mu sync.RWMutex

	chunk map[SparrowFS.ChunkHandle]*chunkInfo
	file  map[SparrowFS.Path]*fileInfo

	// the list of handle that need to be replicated
	replicasNeedList []SparrowFS.ChunkHandle

	numChunkHandle SparrowFS.ChunkHandle
}

type chunkInfo struct {
	mu sync.RWMutex
	// the set of replica locations
	location []SparrowFS.ServerAddress
	// the primary chunkserver
	primary SparrowFS.ServerAddress
	// the lease expire time
	expire time.Time
	// the version of the chunk
	version SparrowFS.ChunkVersion
	// the checksum of the chunk
	checksum SparrowFS.Checksum
	// the path of the chunk
	path SparrowFS.Path
}

type fileInfo struct {
	mu      sync.RWMutex
	handles []SparrowFS.ChunkHandle
}

func newChunkManager() *chunkManager {
	cm := &chunkManager{
		chunk: make(map[SparrowFS.ChunkHandle]*chunkInfo),
		file:  make(map[SparrowFS.Path]*fileInfo),
	}

	log.Info("new chunk manager")
	return cm
}

// GetChunk , get the chunk of the path and the index of the chunk
// return the chunk handle of the chunk,nil
// failed return -1, and error
func (cm *chunkManager) GetChunk(path SparrowFS.Path, index SparrowFS.ChunkIndex) (SparrowFS.ChunkHandle, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// get the file from the file map
	fileInfo, ok := cm.file[path]
	if !ok {
		// not this file
		return -1, fmt.Errorf("file not found %s", path)
	}

	// get the chunk from the chunk map
	if index < 0 || int(index) >= len(fileInfo.handles) {
		// not this chunk
		return -1, fmt.Errorf("chunk index out of range %d", index)
	}

	return fileInfo.handles[index], nil

}

func (cm *chunkManager) RegisterReplica(handle SparrowFS.ChunkHandle, adder SparrowFS.ServerAddress, useLock bool) error {
	var ck *chunkInfo
	var ok bool

	if useLock {
		cm.mu.RLock()
		ck, ok = cm.chunk[handle]
		cm.mu.RUnlock()

		cm.mu.Lock()
		defer cm.mu.Unlock()
	} else {
		ck, ok = cm.chunk[handle]
	}

	if !ok {
		return fmt.Errorf("cannot find chunk %v", handle)
	}

	ck.location = append(ck.location, adder)
	return nil
}



