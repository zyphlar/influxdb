package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type WAL struct {
	baseDir    string
	shardCount int
	shards     []*os.File
}

func NewWAL(baseDir string, shardCount int) (*WAL, error) {
	wal := &WAL{baseDir: baseDir, shardCount: shardCount, shards: make([]*os.File, shardCount, shardCount)}
	for i := 0; i < shardCount; i++ {
		fileDir := filepath.Join(baseDir, fmt.Sprintf("%d", i))
		err := os.MkdirAll(fileDir, 0744)
		if err != nil {
			return nil, err
		}

		wal.shards[i], err = os.OpenFile(filepath.Join(fileDir, "WAL", "wal.log"), os.O_RDWR|os.O_CREATE, 0660)
		if err != nil {
			return nil, err
		}
	}
	return wal, nil
}

func (self *WAL) Log(series *string, shard *int, values []*ColumnValue) error {
	return nil
}

func (self *WAL) Commit(id *int, shard *int) error {
	return nil
}

func (self *WAL) Close() {
	for _, l := range self.shards {
		l.Close()
	}
}

func (self *WAL) syncFiles() {
	for {
		time.Sleep(time.Second)
		for _, l := range self.shards {
			l.Sync()
		}
	}
}
