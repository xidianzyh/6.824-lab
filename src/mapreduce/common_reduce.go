package mapreduce

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	var mapData []KeyValue
	for  i := 0; i < nMap; i++ {
		reduceFile := reduceName(jobName, i, reduceTask)
		// read kv from redcuceFile
		mapData = append(mapData, readData(reduceFile) ...)
	}

	//sort and combine
	var keySlice []string
	var keyM = make(map[string]bool)
	for _, kv := range mapData {
		if _, ok := keyM[kv.Key]; !ok {
			keyM[kv.Key] = true
			keySlice = append(keySlice, kv.Key)
		}
	}
	sort.Strings(keySlice)

	mergeData := make(map[string][]string)
	for _, kv := range mapData {
		if _, ok := mergeData[kv.Key]; !ok {
			mergeData[kv.Key] = make([]string, 0)
		}
		mergeData[kv.Key] = append(mergeData[kv.Key], kv.Value)
	}

	//write data to file
	outF, err := os.Create(outFile)
	if err != nil {
		panic(err)
	}
	defer outF.Close()

	bufWriter := bufio.NewWriter(outF)
	enc := json.NewEncoder(bufWriter)

	for _, key := range keySlice {
		valSlice := mergeData[key]
		valReduce := reduceF(key, valSlice)
		kv :=KeyValue{key, valReduce}
		enc.Encode(kv)
	}

	bufWriter.Flush()
}


func readData(reduceFile string) []KeyValue  {
	result := make([]KeyValue, 0, 1024)
	//read all file data
	binData, err := ioutil.ReadFile(reduceFile)
	if err != nil {
		panic(err)
	}

	// construct a reader from byte array (can log the position)
	r := bytes.NewReader(binData)
	//deserialize data
	for {
		//Read Head
		var keyLen, valLen uint32
		err = binary.Read(r, binary.LittleEndian, &keyLen)
		if err != nil {
			break
		}
		binary.Read(r, binary.LittleEndian, &valLen)

		//read data
		var keyByte []byte = make([]byte, keyLen)
		var valByte []byte = make([]byte, valLen)

		if keyLen > 0 {
			io.ReadFull(r, keyByte)
		}
		if keyLen > 0 {
			io.ReadFull(r, valByte)
		}

		var kv KeyValue
		kv.Key = string(keyByte)
		kv.Value = string(valByte)

		result = append(result, kv)
	}

	return result
}












