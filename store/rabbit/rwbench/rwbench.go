package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coinexchain/randsrc"
	"github.com/dterei/gotsc"
	sha256 "github.com/minio/sha256-simd"

	"github.com/smartbch/moeingads"
	"github.com/smartbch/moeingads/datatree"
	"github.com/smartbch/moeingads/store"
	"github.com/smartbch/moeingads/store/rabbit"
)

const (
	BatchSize    = 10000
	JobSize      = 200
	SamplePos    = 99
	SampleStripe = 125

	Stripe        = 64
	ReadBatchSize = 64 * Stripe
)

type KVPair struct {
	Key, Value []byte
}

func ReadSamples(fname string, kvCount int, batchHandler func(batch []KVPair)) int {
	file, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var readBatch [ReadBatchSize]KVPair
	idx := 0
	totalRun := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, " ")
		if len(fields) != 3 || fields[0] != "SAMPLE" {
			panic("Invalid line: " + line)
		}
		k, err := base64.StdEncoding.DecodeString(fields[1])
		if err != nil {
			panic(err)
		}
		v, err := base64.StdEncoding.DecodeString(fields[2])
		if err != nil {
			panic(err)
		}
		readBatch[idx] = KVPair{k, v}
		idx++
		if idx == ReadBatchSize {
			idx = 0
			batchHandler(readBatch[:])
			totalRun++
			if totalRun*ReadBatchSize >= kvCount {
				break
			}
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
	return totalRun
}

var (
	GuardStart = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	GuardEnd   = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
)

func main() {
	if len(os.Args) != 4 || (os.Args[1] != "rp" && os.Args[1] != "rs" && os.Args[1] != "w") {
		fmt.Printf("Usage: %s w <rand-source-file> <kv-count>\n", os.Args[0])
		fmt.Printf("Usage: %s rp <sample-file> <kv-count>\n", os.Args[0])
		fmt.Printf("Usage: %s rs <sample-file> <kv-count>\n", os.Args[0])
		return
	}
	kvCount, err := strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}

	fmt.Printf("Before Start %d\n", time.Now().UnixNano())
	mads, err := moeingads.NewMoeingADS("./moeingads4test", false, [][]byte{GuardStart, GuardEnd})
	if err != nil {
		panic(err)
	}
	root := store.NewRootStore(mads, nil)
	defer root.Close()

	if os.Args[1] == "w" {
		randFilename := os.Args[2]
		rs := randsrc.NewRandSrcFromFile(randFilename)
		RandomWrite(root, rs, kvCount)
		fmt.Printf("Phase1: %d\n", Phase1Time)
		fmt.Printf("Phase2: %d\n", Phase2Time)
		fmt.Printf("Phase1: %d\n", moeingads.Phase1Time)
		fmt.Printf("Phase2: %d\n", moeingads.Phase2Time)
		fmt.Printf("Phase1: %d\n", datatree.Phase1Time)
		fmt.Printf("Phase2: %d\n", datatree.Phase2Time)
		return
	}

	fmt.Printf("After Load %f\n", float64(time.Now().UnixNano())/1000000000.0)
	sampleFilename := os.Args[2]
	var totalRun int
	trunk := root.GetTrunkStore(1000).(*store.TrunkStore)
	if os.Args[1] == "rp" {
		totalRun = ReadSamples(sampleFilename, kvCount, func(batch []KVPair) {
			checkPar(trunk, batch)
		})
	}
	if os.Args[1] == "rs" {
		totalRun = ReadSamples(sampleFilename, kvCount, func(batch []KVPair) {
			rbt := rabbit.NewRabbitStore(trunk)
			checkSer(rbt, batch)
		})
	}
	fmt.Printf("totalRun: %d\n", totalRun)
	fmt.Printf("Finished %f\n", float64(time.Now().UnixNano())/1000000000.0)
}

func GetRandKV(touchedShortKeys map[[rabbit.KeySize]byte]struct{}, rs randsrc.RandSrc) (k, v []byte) {
	for {
		var sk1, sk2, sk3, sv1, sv2, sv3 [rabbit.KeySize]byte
		k = rs.GetBytes(32)
		k1 := sha256.Sum256(k)
		k2 := sha256.Sum256(k1[:])
		k3 := sha256.Sum256(k2[:])
		copy(sk1[:], k1[:])
		copy(sk2[:], k2[:])
		copy(sk3[:], k3[:])
		//sk1[0] |= 0x1
		//sk2[0] |= 0x1
		//sk3[0] |= 0x1
		v = rs.GetBytes(32)
		v1 := sha256.Sum256(v)
		v2 := sha256.Sum256(v1[:])
		v3 := sha256.Sum256(v2[:])
		copy(sv1[:], v1[:])
		copy(sv2[:], v2[:])
		copy(sv3[:], v3[:])
		//sv1[0] |= 0x1
		//sv2[0] |= 0x1
		//sv3[0] |= 0x1
		if _, ok := touchedShortKeys[sk1]; ok {
			continue
		}
		if _, ok := touchedShortKeys[sk2]; ok {
			continue
		}
		if _, ok := touchedShortKeys[sk3]; ok {
			continue
		}
		if _, ok := touchedShortKeys[sv1]; ok {
			continue
		}
		if _, ok := touchedShortKeys[sv2]; ok {
			continue
		}
		if _, ok := touchedShortKeys[sv3]; ok {
			continue
		}
		touchedShortKeys[sk1] = struct{}{}
		touchedShortKeys[sk2] = struct{}{}
		touchedShortKeys[sk3] = struct{}{}
		touchedShortKeys[sv1] = struct{}{}
		touchedShortKeys[sv2] = struct{}{}
		touchedShortKeys[sv3] = struct{}{}
		break
	}
	return
}

var Phase1Time, Phase2Time /*, Phase3Time*/ uint64

func RandomWrite(root *store.RootStore, rs randsrc.RandSrc, count int) {
	tscOverhead := gotsc.TSCOverhead()
	file, err := os.OpenFile("./sample.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	numBatch := count / BatchSize
	for i := 0; i < numBatch; i++ {
		root.SetHeight(int64(i))
		trunk := root.GetTrunkStore(1000).(*store.TrunkStore)
		if i%20 == 0 {
			fmt.Printf("Now %d of %d, %d\n", i, numBatch, root.ActiveCount())
		}
		var kList [BatchSize][]byte
		var vList [BatchSize][]byte
		touchedShortKeys := make(map[[rabbit.KeySize]byte]struct{}, 6*BatchSize)
		for j := 0; j < BatchSize; j++ {
			k, v := GetRandKV(touchedShortKeys, rs)
			kList[j] = k
			vList[j] = v
			if (j % SampleStripe) == SamplePos {
				s := fmt.Sprintf("SAMPLE %s %s\n", base64.StdEncoding.EncodeToString(k),
					base64.StdEncoding.EncodeToString(v))
				_, err := file.Write([]byte(s))
				if err != nil {
					panic(err)
				}
			}
		}
		var rbtList [BatchSize / JobSize]rabbit.RabbitStore
		var wg sync.WaitGroup
		wg.Add(len(rbtList))
		for x := 0; x < len(rbtList); x++ {
			go func(x, start int) {
				rbt := rabbit.NewRabbitStore(trunk)
				for y := 0; y < JobSize; y++ {
					rbt.Set(kList[start+y], vList[start+y])
				}
				rbtList[x] = rbt
				wg.Done()
			}(x, x*JobSize)
		}
		wg.Wait()
		start := gotsc.BenchStart()
		for _, rbt := range rbtList {
			rbt.Close()
			rbt.WriteBack()
		}
		Phase1Time += gotsc.BenchEnd() - start - tscOverhead
		start = gotsc.BenchStart()
		trunk.Close(true)
		Phase2Time += gotsc.BenchEnd() - start - tscOverhead
	}
}

func checkPar(trunk *store.TrunkStore, batch []KVPair) {
	if len(batch) != ReadBatchSize {
		panic(fmt.Sprintf("invalid size %d %d", len(batch), ReadBatchSize))
	}
	var wg sync.WaitGroup
	for i := 0; i < ReadBatchSize/Stripe; i++ {
		wg.Add(1)
		go func(start, end int) {
			rbt := rabbit.NewRabbitStore(trunk)
			for _, pair := range batch[start:end] {
				v := rbt.Get(pair.Key)
				if !bytes.Equal(v, pair.Value) {
					fmt.Printf("Not Equal for %v: ref: %v actual: %v\n", pair.Key, pair.Value, v)
				}
			}
			rbt.Close()
			rbt.WriteBack()
			wg.Done()
		}(i*Stripe, (i+1)*Stripe)
	}
	wg.Wait()
}

func checkSer(rbt rabbit.RabbitStore, batch []KVPair) {
	if len(batch) != ReadBatchSize {
		panic("invalid size")
	}
	for _, pair := range batch {
		v := rbt.Get(pair.Key)
		if !bytes.Equal(v, pair.Value) {
			fmt.Printf("Not Equal for %v: ref: %v actual: %v\n", pair.Key, pair.Value, v)
		}
	}
}
