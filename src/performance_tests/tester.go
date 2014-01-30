package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"time"
)

const (
	SHARD_COUNT        = 200
	VALUES_PER_WRITE   = 10
	COLUMN_COUNT       = 10
	WRITES_PER_COLUMN  = 100000
	WRITER_ROUTINES    = 10
	DAY_IN_SECONDS     = 86400
	YEAR_IN_DAYS       = 365
	UNIQUE_VALUE_COUNT = 10000
)

var MAX_SEQUENCE = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UnixNano())

	baseDir := "/tmp/influxdb/perf_test"
	err := os.RemoveAll(baseDir)
	if err != nil {
		panic(err)
	}

	ds, err := NewDataStore(baseDir, SHARD_COUNT)
	if err != nil {
		panic(err)
	}

	startTime := time.Now()
	totalWrites := 0
	totalValues := 0
	values := make([]*ColumnValue, VALUES_PER_WRITE, VALUES_PER_WRITE)
	startingTime := time.Now().Unix() - DAY_IN_SECONDS*YEAR_IN_DAYS*20

	randomValues := make([][]byte, UNIQUE_VALUE_COUNT, UNIQUE_VALUE_COUNT)
	for i := 0; i < UNIQUE_VALUE_COUNT; i++ {
		randomValues[i] = randomBytes()
	}

	for i := 0; i < WRITES_PER_COLUMN; i++ {
		for columnId := 1; columnId < COLUMN_COUNT+1; columnId++ {
			shardId := rand.Intn(SHARD_COUNT)
			totalWrites += 1
			for valId := 0; valId < VALUES_PER_WRITE; valId++ {
				value := &ColumnValue{}
				value.ColumnId = columnId
				value.SequenceNumber = int64(totalValues)
				value.Time = startingTime
				value.Value = randomValues[totalWrites%UNIQUE_VALUE_COUNT]
				values[valId] = value
				startingTime += 1
				totalValues += 1
			}
			ds.WriteColumnValues(shardId, values)
			totalWrites += 1
			if totalWrites%100000 == 0 {
				fmt.Printf("Performed %d writes.\n", totalWrites)
			}
		}
	}
	elapsed := time.Now().Sub(startTime)
	fmt.Printf("Finished %d writes in %.3f seconds for %f per second\n", totalWrites, elapsed.Seconds(), float64(totalWrites)/elapsed.Seconds())

	// ds.Close()
	// err = ds.Open()
	// if err != nil {
	// 	panic(err)
	// }

	valCount := 0
	f := func(val *ColumnValue) {
		valCount += 1
	}
	startTime = time.Now()
	err = ds.ReadValues(3, WRITES_PER_COLUMN, f)
	elapsed = time.Now().Sub(startTime)
	fmt.Printf("Read %d vals in %.3f seconds for %f per second\n", valCount, elapsed.Seconds(), float64(valCount)/elapsed.Seconds())
	if err != nil {
		fmt.Println("ERROR READING: ", err)
	}

	ds.Close()
	fmt.Println("Done.", ds)
	time.Sleep(30 * time.Second)
}

func randomBytes() []byte {
	n := rand.Int63()
	buff := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(buff, binary.BigEndian, n)
	return buff.Bytes()
	//	return MAX_SEQUENCE
}
