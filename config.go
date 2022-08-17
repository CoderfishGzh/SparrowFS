package SparrowFS

import "time"

type ChunkHandle int64
type ServerAddress string

const (
	// master
	ServerTimeout = 10 * time.Second
)
