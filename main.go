package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
)

type City struct {
	name  string
	min   int64
	max   int64
	sum   int64
	count int
}

func main() {
	if len(os.Args) == 1 {
		log.Println("Please provide the file as the first arg.")
		return
	}
	filename := os.Args[1]
	log.Printf("Reading file: %s\n", filename)

	// f, err := os.Create("./profiles/execution_profile.prof")
	// if err != nil {
	// 	log.Fatal("could not create trace execution profile: ", err)
	// }
	// defer f.Close()
	// trace.Start(f)
	// defer trace.Stop()

	// f2, err := os.Create("./profiles/cpuprofile.prof")
	// if err != nil {
	// 	log.Fatal("could not create CPU profile: ", err)
	// }
	// defer f2.Close()
	// if err := pprof.StartCPUProfile(f2); err != nil {
	// 	log.Fatal("could not start CPU profile: ", err)
	// }
	// defer pprof.StopCPUProfile()

	processFile(filename)

	log.Println("Done")

}

func processFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	cityHash := make(map[string]*City)
	buffStream := make(chan []byte, 15)
	computeStream := make(chan map[string]*City, 10)
	chunkSize := 64 * 1024 * 1024 // 64MB

	var wg sync.WaitGroup

	// spawn workers to consume (process) file chunks read
	for i := 0; i < runtime.NumCPU()-1; i++ {
		wg.Add(1)
		go func() {
			for c := range buffStream {
				processChunk(c, computeStream)
			}
			wg.Done()
		}()
	}

	// Read the file
	go func() {
		buff := make([]byte, chunkSize)
		rest := []byte{}

		for {
			readSize, err := file.Read(buff)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Fatal(err)
			}
			if readSize == 0 {
				break
			}

			toSend := make([]byte, readSize)
			copy(toSend, buff[:readSize])

			lastNewLineIdx := bytes.LastIndex(buff, []byte{'\n'})

			toSend = append(rest, buff[:lastNewLineIdx+1]...)
			rest = make([]byte, len(buff[lastNewLineIdx+1:]))
			copy(rest, buff[lastNewLineIdx+1:])

			buffStream <- toSend
		}
		close(buffStream)

		wg.Wait()
		// Wait the process chunk to end before closing the compute stream

		close(computeStream)
	}()

	processMeasurement(computeStream, cityHash)

	// Create the result
	keys := make([]string, 0)
	for k := range cityHash {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var out strings.Builder
	for _, cityName := range keys {
		city := cityHash[cityName]
		out.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f, ",
			city.name,
			round(float64(city.min)/10.0),
			round(float64(city.sum)/10.0/float64(city.count)),
			round(float64(city.max)/10.0),
		))
	}
	return strings.TrimSuffix(out.String(), ", ")
}

func processChunk(chunk []byte, computeStream chan map[string]*City) {
	tmpHash := make(map[string]*City)
	chunkSize := len(chunk)

	semiColonChar := byte(';')
	breakLine := byte('\n')

	i := 0
	lineStartIdx := 0 // idx of
	for {
		if i >= chunkSize-1 {
			break
		}

		// Found a semi-colon, we can get the city name and temp based on position
		if chunk[i] == semiColonChar {
			city := string(chunk[lineStartIdx:i])
			bitesToEndLine := 0
			// Find the breakline, it can be at most 6 bytes given the temp format
			// TODO: This can be faster parsing manually
			for _, b := range chunk[i : i+6] {
				if b == breakLine {
					break
				}
				bitesToEndLine += 1
			}
			// get the temp account for sign
			temp := customStringToIntParser(string(chunk[i+1 : i+bitesToEndLine]))

			// log.Printf("FOUND SEMICOLON City is %s and temp %s\n", city, temp)
			// move the pointer to after the breakline
			i += bitesToEndLine + 1
			lineStartIdx = i

			if val, ok := tmpHash[city]; ok {
				if temp < val.min {
					val.min = temp
				}
				if temp > val.max {
					val.max = temp
				}
				val.sum += temp
				val.count += 1
				tmpHash[city] = val
			} else {
				tmpHash[city] = &City{
					name:  city,
					min:   temp,
					max:   temp,
					count: 1,
					sum:   temp,
				}
			}
		}
		i += 1
	}
	computeStream <- tmpHash
}

func processMeasurement(
	computeStream chan map[string]*City,
	cityHash map[string]*City,
) {
	for tmpHash := range computeStream {
		for cityName, cityObj := range tmpHash {
			if new, ok := cityHash[cityObj.name]; ok {
				if new.min < cityHash[cityName].min {
					cityHash[cityName].min = new.min
				}
				if new.max > cityHash[cityName].max {
					cityHash[cityName].max = new.max
				}
				cityHash[cityName].count += new.count
				cityHash[cityName].sum += new.sum
			} else {
				cityHash[cityName] = cityObj
			}
		}
	}
}

// input: string containing signed number in the range [-99.9, 99.9]
// output: signed int in the range [-999, 999]
func customStringToIntParser(input string) (output int64) {
	var isNegativeNumber bool
	if input[0] == '-' {
		isNegativeNumber = true
		input = input[1:]
	}

	switch len(input) {
	case 3:
		output = int64(input[0])*10 + int64(input[2]) - int64('0')*11
	case 4:
		output = int64(input[0])*100 + int64(input[1])*10 + int64(input[3]) - (int64('0') * 111)
	}

	if isNegativeNumber {
		return -output
	}
	return
}

func round(x float64) float64 {
	rounded := math.Round(x * 10)
	if rounded == -0.0 {
		return 0.0
	}
	return rounded / 10
}
