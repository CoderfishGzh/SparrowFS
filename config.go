package SparrowFS

import "time"

type ChunkHandle int64
type ServerAddress string
type Path string
type offset int64
type ChunkIndex int
type ChunkVersion int64
type Checksum int64

const (
	// master
	ServerTimeout = 10 * time.Second
)
