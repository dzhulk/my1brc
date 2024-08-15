package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

type City struct {
	min   int
	max   int
	sum   int
	count int
	name  string
	hash  uint
}

type Chunk struct {
	start, end int
}

const (
	Kb        int  = 1024
	Mb        int  = 1024 * Kb
	pieceSize int  = 4 * Mb
	slots     uint = 4096
)

var collisionsCount int64

func newMyMap() []*City {
	return make([]*City, slots)
}

func mapSet(mMap []*City, hash uint, city *City) {
	slot := hash % slots

	if mMap[slot] == nil {
		mMap[slot] = city
		return
	} else {
		//atomic.AddInt64(&collisionsCount, 1)
		for i := slot + 1; i < uint(len(mMap)); i++ {
			if mMap[i] == nil {
				mMap[i] = city
				return
			} else {
				//atomic.AddInt64(&collisionsCount, 1)
			}
		}
	}

	panic("no slots found")
}

func mapGet(mMap []*City, hash uint) (city *City) {
	slot := hash % slots
	if mMap[slot] != nil {
		if mMap[slot].hash == hash {
			city = mMap[slot]
			return
		}
		for i := slot + 1; i < uint(len(mMap)); i++ {
			if mMap[i] != nil && mMap[i].hash == hash {
				city = mMap[i]
				return
			}
		}
	}
	return
}

var citiesMap map[uint]*City = make(map[uint]*City)
var chunksChan chan Chunk = make(chan Chunk, 4000)

func CutFile(b []byte) {
	offset := 0
	for offset < len(b) {
		pieceLen := pieceSize
		if pieceLen > len(b)-offset {
			pieceLen = len(b) - offset
		} else {
			for b[offset+pieceLen] != '\n' {
				pieceLen++
			}
			pieceLen++
		}
		chunksChan <- Chunk{offset, offset + pieceLen}
		offset += pieceLen
	}
	close(chunksChan)
}

func perform(wg *sync.WaitGroup, b []byte, localMap []*City) {
	defer wg.Done()

	for {
		chunk, open := <-chunksChan
		if !open {
			return
		}
		if chunk.start == 0 && chunk.end == 0 { // zero value
			continue
		}
		parsePiece(localMap, b[chunk.start:chunk.end])
	}
}

func main() {
	start := time.Now()
	filename := os.Args[1]

	file, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	fileSize := int(stat.Size())
	//fmt.Printf("File size: %d\n", fileSize)
	b, err := syscall.Mmap(int(file.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Fatal(err)
	}
	defer syscall.Munmap(b)
	//fmt.Printf("File mmap size: %d\n", len(b))

	wg := sync.WaitGroup{}
	workersCount := runtime.NumCPU()
	localMaps := make([][]*City, workersCount)
	for w := 0; w < workersCount; w++ {
		wg.Add(1)
		localMaps[w] = newMyMap()
		go perform(&wg, b, localMaps[w])
	}
	CutFile(b)
	wg.Wait()
	mergePrint(localMaps)
	fmt.Println(time.Since(start))
	fmt.Printf("Collision count: %d\n", collisionsCount)
}

func mergePrint(localMaps [][]*City) {
	for _, localMap := range localMaps {
		for _, city := range localMap {
			if city == nil {
				continue
			}
			global := citiesMap[city.hash]
			if global == nil {
				citiesMap[city.hash] = city
			} else {
				if city.max > global.max {
					global.max = city.max
				}
				if city.min < global.min {
					global.min = city.min
				}
				global.sum += city.sum
				global.count += city.count
			}
		}
	}
	printResults()
}

func parsePiece(localMap []*City, data []byte) {
	offset := 0
	for i, b := range data {
		if b == '\n' {
			index := offset
			sem := 0
			hash := uint(17)
			for ; data[index] != ';'; index++ {
				hash = (hash * 31) + uint(data[index])
			}
			sem = index
			index++
			neg := 1
			if data[index] == '-' {
				neg = -1
				index++
			}
			num := int(data[index] - '0')
			index++
			if data[index] != '.' {
				num = num*10 + int(data[index]-'0')
				index++
			}
			index++
			num = num*10 + int(data[index]-'0')
			temp := num * neg

			city := mapGet(localMap, hash)

			if city != nil {
				if temp > city.max {
					city.max = temp
				}
				if temp < city.min {
					city.min = temp
				}
				city.sum += temp
				city.count++
			} else {
				mapSet(localMap, hash, &City{
					min:   temp,
					max:   temp,
					sum:   temp,
					count: 1,
					name:  string(data[offset:sem]),
					hash:  hash,
				})
			}
			offset = i + 1
		}
	}
}

func printResults() {
	var results []*City
	for _, v := range citiesMap {
		results = append(results, v)
	}

	slices.SortFunc(results[:], func(a, b *City) int {
		return strings.Compare(a.name, b.name)
	})

	buf := bytes.NewBufferString("{\n")
	for i, v := range results {
		fmt.Fprintf(buf, "%s=%.1f/%.1f/%.1f", v.name, float64(v.min)*0.1, float64(v.sum)/float64(v.count)*0.1, float64(v.max)*0.1)
		if i != len(results)-1 {
			fmt.Fprint(buf, ", ")
		}
	}
	fmt.Fprint(buf, "}")
	fmt.Println(buf.String())
}
