package mapreduce

import (
	"encoding/json"
	"io"
	"log"
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

	keyValues := make(map[string][]string)

	for m := 0; m < nMap; m++ {
		// 打开文件
		intermediateFile, err := os.Open(reduceName(jobName, m, reduceTask))
		if err != nil {
			log.Fatal(err)
		}
		// 完成所有后关闭文件
		defer intermediateFile.Close()

		// JSON流式解码器
		dec := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				// 读取到文件末尾
				if err == io.EOF {
					break
				}
				log.Fatal(err)
			}

			_, ok := keyValues[kv.Key]
			// key 不存在
			if !ok {
				keyValues[kv.Key] = make([]string, 0)
			}
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
	}

	// key 升序排序
	var keys []string
	for k, _ := range keyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 创建输出文件
	outputFile, err := os.Create(outFile)
	if err != nil {
		log.Fatal(err)
	}
	defer outputFile.Close()

	// 流式编码器
	enc := json.NewEncoder(outputFile)

	// 调用 reduceF
	for _, key := range keys {
		value := keyValues[key]
		err := enc.Encode(KeyValue{key, reduceF(key, value)})
		if err != nil {
			log.Fatal(err)
		}
	}

}
