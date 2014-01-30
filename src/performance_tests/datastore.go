package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/jmhodges/levigo"
	"math"
	"os"
	"path/filepath"
)

type Datastore struct {
	baseDir      string
	shardCount   int
	readOptions  *levigo.ReadOptions
	writeOptions *levigo.WriteOptions
	shards       []*levigo.DB
}

type ColumnValue struct {
	ColumnId       int
	Value          []byte
	Time           int64
	SequenceNumber int64
}

func (self *ColumnValue) key() []byte {
	keyBuffer := bytes.NewBuffer(make([]byte, 0, 24))
	binary.Write(keyBuffer, binary.BigEndian, int64(self.ColumnId))
	binary.Write(keyBuffer, binary.BigEndian, convertTimestampToUint(&self.Time))
	binary.Write(keyBuffer, binary.BigEndian, uint64(self.SequenceNumber))
	return keyBuffer.Bytes()
}

const (
	BLOOM_FILTER_BITS_PER_KEY = 8
	TWO_FIFTY_SIX_KILOBYTES   = 256 * 1024
	ONE_MEGABYTE              = 1024 * 1024
	ONE_GIGABYTE              = ONE_MEGABYTE * 1024
)

func NewDataStore(baseDir string, shardCount int) (*Datastore, error) {
	ds := &Datastore{
		baseDir:      baseDir,
		shardCount:   shardCount,
		readOptions:  levigo.NewReadOptions(),
		writeOptions: levigo.NewWriteOptions(),
		shards:       make([]*levigo.DB, shardCount, shardCount),
	}

	lru := levigo.NewLRUCache(1024)
	opts := levigo.NewOptions()
	opts.SetCache(lru)
	opts.SetCreateIfMissing(true)
	opts.SetBlockSize(TWO_FIFTY_SIX_KILOBYTES)
	filter := levigo.NewBloomFilter(BLOOM_FILTER_BITS_PER_KEY)
	opts.SetFilterPolicy(filter)

	for i := 0; i < shardCount; i++ {
		dbDir := filepath.Join(baseDir, "DB", fmt.Sprintf("%d", i))
		err := os.MkdirAll(dbDir, 0744)
		if err != nil {
			return nil, err
		}

		db, err := levigo.Open(dbDir, opts)
		if err != nil {
			return nil, err
		}
		ds.shards[i] = db
	}
	return ds, nil
}

func (self *Datastore) WriteColumnValues(shard int, values []*ColumnValue) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()

	for _, val := range values {
		wb.Put(val.key(), val.Value)
	}

	self.shards[shard].Write(self.writeOptions, wb)
	return nil
}

func (self *Datastore) ReadValues(columnId, limit int, yield func(*ColumnValue)) error {
	if self.shardCount == 1 {
		return self.readFromSingleShard(columnId, limit, yield)
	}
	iterators := make([]*levigo.Iterator, self.shardCount, self.shardCount)
	columnIdBuff := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(columnIdBuff, binary.BigEndian, int64(columnId))
	columnIdKey := columnIdBuff.Bytes()

	for i := 0; i < self.shardCount; i++ {
		iterators[i] = self.shards[i].NewIterator(self.readOptions)
		iterators[i].Seek(columnIdKey)
		defer iterators[i].Close()
	}
	var iteratorToAdvance int
	for {
		nextTime := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
		var iteratorsToRemove []int
		iteratorToAdvance = -1
		for i, iterator := range iterators {
			key := iterator.Key()
			if !bytes.Equal(key[:8], columnIdKey) {
				// this iterator is done, remove it
				if iteratorsToRemove == nil {
					iteratorsToRemove = make([]int, 0)
				}
				index := i
				iteratorsToRemove = append(iteratorsToRemove, index)
			} else {
				time := key[8:16]
				if bytes.Compare(time, nextTime) < 1 {
					nextTime = time
					index := i
					iteratorToAdvance = index
				}
			}
		}
		if iteratorToAdvance == -1 {
			return nil
		}
		it := iterators[iteratorToAdvance]
		key := it.Key()
		columnId := int(bytesToInt(key[:8]))

		var t uint64
		binary.Read(bytes.NewBuffer(key[8:16]), binary.BigEndian, &t)
		time := convertUintTimestampToInt64(&t)
		sequenceNumber := bytesToInt(key[16:])
		yield(&ColumnValue{ColumnId: columnId, Time: time, SequenceNumber: sequenceNumber, Value: it.Value()})
		limit--

		it.Next()
		if !it.Valid() {
			if iteratorsToRemove == nil {
				iteratorsToRemove = make([]int, 0)
			}
			iteratorsToRemove = append(iteratorsToRemove, iteratorToAdvance)
		}

		if len(iteratorsToRemove) > 0 {
			newIterators := make([]*levigo.Iterator, 0)
			for i, iterator := range iterators {
				shouldAppend := true
				for _, index := range iteratorsToRemove {
					if i == index {
						shouldAppend = false
					}
				}
				if shouldAppend {
					newIterators = append(newIterators, iterator)
				}
			}
			iterators = newIterators
		}

		if limit == 0 || len(iterators) == 0 {
			return nil
		}
	}
	return nil
}

func (self *Datastore) readFromSingleShard(columnId, limit int, yield func(*ColumnValue)) error {
	columnIdBuff := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(columnIdBuff, binary.BigEndian, int64(columnId))
	columnIdKey := columnIdBuff.Bytes()

	iterator := self.shards[0].NewIterator(self.readOptions)
	iterator.Seek(columnIdKey)
	defer iterator.Close()

	for {
		if !iterator.Valid() {
			return nil
		}
		key := iterator.Key()
		if !bytes.Equal(key[:8], columnIdKey) {
			return nil
		}
		columnId := int(bytesToInt(key[:8]))

		var t uint64
		binary.Read(bytes.NewBuffer(key[8:16]), binary.BigEndian, &t)
		time := convertUintTimestampToInt64(&t)
		sequenceNumber := bytesToInt(key[16:])
		yield(&ColumnValue{ColumnId: columnId, Time: time, SequenceNumber: sequenceNumber, Value: iterator.Value()})
		limit--
		if limit == 0 {
			return nil
		}
	}
	return nil
}

func (self *Datastore) Close() {
	for _, ldb := range self.shards {
		ldb.Close()
	}
}

func (self *Datastore) Open() error {
	lru := levigo.NewLRUCache(1024)
	opts := levigo.NewOptions()
	opts.SetCache(lru)
	opts.SetBlockSize(TWO_FIFTY_SIX_KILOBYTES)
	filter := levigo.NewBloomFilter(BLOOM_FILTER_BITS_PER_KEY)
	opts.SetFilterPolicy(filter)

	for i := 0; i < len(self.shards); i++ {
		dbDir := filepath.Join(self.baseDir, "DB", fmt.Sprintf("%d", i))
		err := os.MkdirAll(dbDir, 0744)
		if err != nil {
			return nil
		}

		db, err := levigo.Open(dbDir, opts)
		if err != nil {
			return nil
		}
		self.shards[i] = db
	}
	return nil
}

func convertTimestampToUint(t *int64) uint64 {
	if *t < 0 {
		return uint64(math.MaxInt64 + *t + 1)
	}
	return uint64(*t) + uint64(math.MaxInt64) + uint64(1)
}

func convertUintTimestampToInt64(t *uint64) int64 {
	if *t > uint64(math.MaxInt64) {
		return int64(*t-math.MaxInt64) - int64(1)
	}
	return int64(*t) - math.MaxInt64 - int64(1)
}

func bytesToInt(value []byte) int64 {
	var n uint64
	binary.Read(bytes.NewBuffer(value), binary.BigEndian, &n)
	return int64(n)
}
