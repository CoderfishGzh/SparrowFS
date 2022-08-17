package master

import (
	"SparrowFS"
	"SparrowFS/utils"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

// ChunkServerManager is a struct that manages the chunk servers.
type ChunkServerManager struct {
	mu      sync.RWMutex
	servers map[SparrowFS.ServerAddress]*ChunkServerInfo
}

type ChunkServerInfo struct {
	// the last time that chunk server send the heart beat to master
	lastHeartbeat time.Time
	// the chunks that the server has
	chunks map[SparrowFS.ChunkHandle]bool
	//
	garbage []SparrowFS.ChunkHandle
}

func newChunkServerManager() *ChunkServerManager {
	log.Println("newChunkServerManager")
	return &ChunkServerManager{
		servers: make(map[SparrowFS.ServerAddress]*ChunkServerInfo),
	}
}

// add a chunk to the chunk server list
func (csm *ChunkServerManager) addChunk(chunkServerList []SparrowFS.ServerAddress, chunk SparrowFS.ChunkHandle) {
	csm.mu.Lock()
	defer csm.mu.Unlock()

	for _, v := range chunkServerList {
		sv, ok := csm.servers[v]
		if ok {
			// if server exit, add the chunk to the server's chunk list
			sv.chunks[chunk] = true
		} else {
			log.Println("add chunk in removed server ", sv)
		}
	}
}

// Heartbeat
// return true: the server is new, false: the server is old
func (csm *ChunkServerManager) Heartbeat(addr SparrowFS.ServerAddress, reply SparrowFS.HeartbeatReply) bool {
	csm.mu.Lock()
	defer csm.mu.Unlock()

	sv, ok := csm.servers[addr]
	// if not exit in the servers list, that the server is the new one
	// add the server to the servers list
	if !ok {
		log.Println("New chunk server", addr)
		// add the server to the servers list
		csm.servers[addr] = &ChunkServerInfo{time.Now(), make(map[SparrowFS.ChunkHandle]bool), nil}
		return true
	} else {
		// the server not the new one, update the last heart beat time
		// return the garbage list to chunk server
		reply.Garbage = csm.servers[addr].garbage
		csm.servers[addr].garbage = make([]SparrowFS.ChunkHandle, 0)
		sv.lastHeartbeat = time.Now()
		return false
	}
}

// ChooseServers Choose servers to store the new chunk , called when a new chunk is created
func (csm *ChunkServerManager) ChooseServers(nums int) ([]SparrowFS.ServerAddress, error) {
	if nums > len(csm.servers) {
		return nil, fmt.Errorf("not enough servers for %v replics", nums)
	}

	csm.mu.RLock()
	var all, ret []SparrowFS.ServerAddress
	for a, _ := range csm.servers {
		all = append(all, a)
	}

	choose, err := utils.Sample(len(all), nums)
	if err != nil {
		return nil, err
	}

	for _, v := range choose {
		ret = append(ret, all[v])
	}

	return ret, nil
}

// RemoveServers Check the dead servers and remove them from the servers list
// return the list of chunk handle
// according to the last heartbeat time
func (csm *ChunkServerManager) RemoveServers(address SparrowFS.ServerAddress) (handles []SparrowFS.ChunkHandle, err error) {
	csm.mu.Lock()
	defer csm.mu.Unlock()

	err = nil
	sv, ok := csm.servers[address]
	if !ok {
		err = fmt.Errorf("Cannot find chunk server %v", address)
		return
	}

	// save the chunks
	for h, v := range sv.chunks {
		// if the chunk is true
		if v {
			// save the chunk handle
			handles = append(handles, h)
		}
	}

	delete(csm.servers, address)
	return
}

// CheckServers return the Dead servers
// according to the last heartbeat time and the ServerTimeout const
func (csm *ChunkServerManager) CheckServers() []SparrowFS.ServerAddress {
	csm.mu.RLock()
	defer csm.mu.RUnlock()

	var ret []SparrowFS.ServerAddress
	// get the time now
	now := time.Now()
	for k, v := range csm.servers {
		// if the time now - the last heartbeat time > ServerTimeout
		// that the server is dead
		if v.lastHeartbeat.Add(SparrowFS.ServerTimeout).Before(now) {
			// save the dead server
			ret = append(ret, k)
		}
	}

	return ret
}

// AddChunk , register a chunk to the chunk server list
func (csm *ChunkServerManager) AddChunk(addrs []SparrowFS.ServerAddress, chunk_handle SparrowFS.ChunkHandle) {
	csm.mu.Lock()
	defer csm.mu.Unlock()

	for _, v := range addrs {
		sv, ok := csm.servers[v]
		if ok {
			sv.chunks[chunk_handle] = true
		} else {
			log.Warning("AddChunk in removed server ", sv)
		}

	}

}

// AddGarbage , add the removed chunk into the garbage list of the chunk server
func (csm *ChunkServerManager) AddGarbage(adders SparrowFS.ServerAddress, chunkHandle SparrowFS.ChunkHandle) {
	csm.mu.Lock()
	defer csm.mu.Unlock()

	// get the server info
	sv, ok := csm.servers[adders]
	if ok {
		sv.garbage = append(sv.garbage, chunkHandle)
	}
}

// ChooseReReplication , choose a server to re-replicate the chunk
// return the 'from' and 'to' server address, chunk replicate from 'from' to 'to'
// called when a chunk's replicas number is less than the required number
// TODO： the sample algorithm to find 'from' and 'to' server is not good, need to improve
func (csm *ChunkServerManager) ChooseReReplication(handle SparrowFS.ChunkHandle) (from, to SparrowFS.ServerAddress, err error) {
	csm.mu.RLock()
	defer csm.mu.RUnlock()

	from = ""
	to = ""
	err = nil

	// cycle the servers list,
	for address, serverInfo := range csm.servers {
		// find which server has this chunk handle
		if serverInfo.chunks[handle] {
			from = address
		} else {
			// find which server has no this chunk handle
			to = address
		}

		// if get the 'from' and 'to' server address, return
		if from != "" && to != "" {
			return
		}
	}

	// if not get the 'from' and 'to' server address, return error
	err = fmt.Errorf("not enough servers to re-replicate %v", handle)
	return
}
