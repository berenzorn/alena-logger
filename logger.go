package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	lz42 "github.com/pierrec/lz4"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/gzip"
	"github.com/segmentio/kafka-go/lz4"
	"github.com/segmentio/kafka-go/snappy"
	"github.com/segmentio/kafka-go/zstd"
)

type Config struct {
	Debug         bool   `json:"debug"`
	Brokers       string `json:"brokers"`
	BrokersLeader string `json:"brokers_leader"`
	Topic         string `json:"topic"`
	Logpath       string `json:"log_path"`
	Logmask       string `json:"log_mask"`
	Balancer      int    `json:"balancer"`
	Compression   int    `json:"compression"`
	RequiredAcks  int    `json:"required_acks"`
	BucketSize    int    `json:"bucket_size"`
	BucketTicker  int    `json:"bucket_ticker"`
	SendTicker    int    `json:"send_ticker"`
	ContainerSize int    `json:"container_size"`
	WaitTime      int    `json:"wait_time"`
	TimeOnErr     int    `json:"time_on_err"`
}

var hostname string
var waitGroup sync.WaitGroup

//var cacheFileName = "db.dat"
var offset int64 = 0

func check(err error) {
	if err != nil {
		fmt.Println(err)
	}
}

type Routine struct {
	Name string
	End  chan bool
}

type FileSeeker struct {
	Routine
}
type FileReader struct {
	Routine
}
type BucketCollector struct {
	Routine
}
type KafkaWriter struct {
	Routine
}

func getOffset() string {
	_, offset := time.Now().Zone()
	offset /= 3600
	offsetStr := strconv.Itoa(offset)
	if offset <= 9 {
		offsetStr = "0" + offsetStr
	}
	offsetStr = "+" + offsetStr
	offsetStr += ":00"
	return offsetStr
}

// периодичность разных проверок, от WaitTime секунд до 2 минут
func (rt *Routine) waitingDuration(waitTime int) int {
	waitTime *= 2
	if waitTime > 120 {
		waitTime = 120
	}
	return waitTime
}

// ищем самый свежий лог по дате модификации и отправляем в канал
func (rt *Routine) fileSeeker(config *Config, file chan string, name chan bool) {
	ticker := time.NewTicker(time.Duration(config.WaitTime) * time.Second)
	var currentFile = ""
	var newestFile = ""
	var latestTime int64 = 0
	var waitTime = config.WaitTime

	for {
		select {
		case <-rt.End:
			waitGroup.Done()
			return
		case <-name:
			file <- currentFile
		case <-ticker.C:
		F:
			files, err := filepath.Glob(config.Logpath + string(os.PathSeparator) + config.Logmask)
			check(err)
			if len(files) == 0 {
				// если вдруг нет никаких с LogMask - сидим ждём
				currentFile = ""
				latestTime = 0
				fmt.Println("No log files. Waiting...")
				time.Sleep(time.Duration(waitTime) * time.Second)
				waitTime = rt.waitingDuration(waitTime)
				continue
			} else {
				waitTime = config.WaitTime
			}

			for _, f := range files {
				fi, err := os.Stat(f)
				if err != nil {
					// если в доли секунды с момента проверки что-то изменилось,
					// безусловно нужна проверка файлов заново, не дожидаясь тикера
					fmt.Println("Something went wrong, checking log files again...")
					currentFile = ""
					latestTime = 0
					goto F
				}
				newTime := fi.ModTime().Unix()
				if newTime > latestTime {
					latestTime = newTime
					newestFile = f
				}
			}
			currentFile = newestFile
		}
	}
}

// читаем файл, пришедший из f, и формируем корзинки
func (rt *Routine) fileReader(config *Config, f chan string, name chan bool, sc chan kafka.Message) {
	var file = ""
	var err error
	var logFile *os.File
	var kmsg kafka.Message
	var rd *bufio.Reader

	//cacheFile, err := os.Create(cacheFileName)
	//check(err)
	//
	//defer func(f *os.File) {
	//	err := f.Close()
	//	check(err)
	//}(cacheFile)

	defer func(f *os.File) {
		err := f.Close()
		check(err)
	}(logFile)

	var multiline string
	var line string

	for {
		select {
		case <-rt.End:
			waitGroup.Done()
			return
		case file = <-f:
			_ = logFile.Close()
			logFile, err = os.OpenFile(file, os.O_RDONLY, os.ModePerm)
			if err != nil {
				fmt.Printf("Can't open POS log %s. Waiting...\n", file)
				file = ""
				name <- true
				time.Sleep(time.Duration(config.WaitTime) * time.Second)
				break
			}
			offset = 0
			_, _ = logFile.Seek(offset, 0)
			rd = bufio.NewReader(logFile)
		default:
			if file == "" {
				name <- true
				time.Sleep(time.Duration(config.WaitTime) * time.Second)
				break
			}

			for {
				line, err = rd.ReadString('\n')
				if err != nil {
					if err == io.EOF {
						time.Sleep(time.Duration(config.WaitTime) * time.Second)
						line = ""
						break
					} else {
						line = ""
						fmt.Println(err)
						break
					}
				}
				if len(line) <= 1 {
					continue
				}
				if multiline == "" {
					multiline = line
				} else {
					if len(line) > 11 {
						match, _ := regexp.MatchString(
							"20\\d{2}-\\d{2}-\\d{2}\\s", line[:11])
						if !match {
							multiline += line
							continue
						} else {
							break
						}
					} else {
						multiline += line
						continue
					}
				}
			}
			if len(multiline) > 0 {
				multiline = strings.Replace(multiline, `\`, `\\`, -1)
				multiline = strings.Replace(multiline, `/`, `\/`, -1)
				multiline = strings.Replace(multiline, `"`, `\"`, -1)
				multiline = strings.Replace(multiline, "\b", `\b`, -1)
				multiline = strings.Replace(multiline, "\f", `\f`, -1)
				multiline = strings.Replace(multiline, "\n", `\n`, -1)
				multiline = strings.Replace(multiline, "\r", `\r`, -1)
				multiline = strings.Replace(multiline, "\t", `\t`, -1)
				multiline = strings.Replace(multiline, `\u003c`, "<", -1)
				multiline = strings.Replace(multiline, `\u003e`, ">", -1)
				multiline = strings.Replace(multiline, `\u0026`, "&", -1)
				multiline = strings.Replace(multiline, `\u003d`, "=", -1)

				multiline = prepareMessage(multiline)
				kmsg = kafka.Message{
					Key:   []byte(hostname),
					Value: []byte(multiline),
				}
				sc <- kmsg
			}
			multiline = line
		}
	}
}

func (rt *Routine) bucketCollector(config *Config, sc chan kafka.Message, send chan []byte) {
	ticker := time.NewTicker(time.Duration(config.BucketTicker) * time.Second)
	var bucket []kafka.Message
	for {
		select {
		case <-rt.End:
			waitGroup.Done()
			return
		case str := <-sc:
			bucket = append(bucket, str)
		case <-ticker.C:
			if len(bucket) > config.BucketSize / 2 {
				rt.sendBucket(bucket, send)
				bucket = nil
			}
		default:
			if len(bucket) >= config.BucketSize {
				rt.sendBucket(bucket, send)
				bucket = nil
			}
		}
	}
}

func (rt *Routine) sendBucket(bucket []kafka.Message, send chan []byte) {
	bytes := serializeBucket(bucket)
	barc := make([]byte, len(bytes))
	n, _ := lz42.CompressBlockHC(bytes, barc, 0)
	if n > 0 {
		barc = append(barc[:n], []byte("Ո")...) // 213 136
		send <- barc[:n+2]
	} else {
		bytes = append(bytes, []byte("Ս")...) // 213 141
		send <- bytes
	}
}

func serializeBucket(kmsg []kafka.Message) []byte {
	var bucket []byte
	for _, message := range kmsg {
		bucket = append(bucket, serialize(message)...)
	}
	return bucket
}

func deserializeBucket(bytes []byte) []kafka.Message {
	var kmsgs []kafka.Message
	var offset int64 = 0

	var keyLength int
	var valueLength int64

	for {
		keyLength = int(bytes[offset+2])
		if bytes[offset+3] == 0 {
			valueLength = int64(bytes[offset+4])
		} else {
			str := hex.EncodeToString(bytes[offset+3 : offset+5])
			valueLength, _ = strconv.ParseInt(str, 16, 16)
		}
		length := int64(keyLength) + valueLength + 5
		kmsgs = append(kmsgs, deserialize(bytes, offset, int64(keyLength), valueLength))
		offset += length
		if offset == int64(len(bytes)) {
			break
		}
	}
	return kmsgs
}

func serialize(kmsg kafka.Message) []byte {
	var bucket []byte
	value := make([]byte, 2)
	binary.BigEndian.PutUint16(value, uint16(len(kmsg.Value)))
	bucket = append(bucket, []byte("˂")...)      // 203 130
	bucket = append(bucket, byte(len(kmsg.Key))) // 1 byte
	bucket = append(bucket, value...)            // 2 bytes
	bucket = append(bucket, kmsg.Key...)
	bucket = append(bucket, kmsg.Value...)
	return bucket
}

func deserialize(bkmsg []byte, offset int64, key int64, val int64) kafka.Message {
	message := kafka.Message{
		Key:   nil,
		Value: nil,
	}
	message.Key = bkmsg[offset+5 : offset+key+5]
	message.Value = bkmsg[offset+key+5 : offset+key+5+val]
	return message
}

func prepareMessage(line string) string {
	message := "{"
	message += "\"message\":\"" + line + "\","
	message += "\"beat\":{"
	message += "\"hostname\":\"" + hostname + "\","
	message += "\"timezone\":\"" + getOffset() + "\""
	message += "}"
	message += "}"
	return message
}

func (rt *Routine) kafkaWriter(config *Config, send chan []byte) {
	ticker := time.NewTicker(time.Duration(config.SendTicker) * time.Second)
	brokers := strings.Split(config.Brokers, ";")

	dialer := &kafka.Dialer{
		Timeout: time.Duration(config.WaitTime),
	}

	var Compressor kafka.CompressionCodec = nil
	if config.Compression == 1 {
		Compressor = &gzip.CompressionCodec{}
	} else if config.Compression == 2 {
		Compressor = &snappy.CompressionCodec{}
	} else if config.Compression == 3 {
		Compressor = &lz4.CompressionCodec{}
	} else if config.Compression == 4 {
		Compressor = &zstd.CompressionCodec{}
	}

	var Balancer kafka.Balancer = nil
	if config.Balancer == 1 {
		Balancer = &kafka.LeastBytes{}
	} else if config.Balancer == 2 {
		Balancer = &kafka.CRC32Balancer{}
	} else if config.Balancer == 3 {
		Balancer = &kafka.RoundRobin{}
	} else if config.Balancer == 4 {
		Balancer = &kafka.Murmur2Balancer{}
	}

	for {
		select {
		case <-rt.End:
			waitGroup.Done()
			return
		case <-ticker.C:
			_, err := dialer.LookupLeader(context.Background(),
				"tcp", config.BrokersLeader, config.Topic, 0)
			if err != nil {
				continue
			} else {
				w := kafka.NewWriter(kafka.WriterConfig{
					Brokers:           brokers,
					Topic:             config.Topic,
					Balancer:          Balancer,
					RequiredAcks:      config.RequiredAcks,
					CompressionCodec:  Compressor,
					BatchTimeout:      2 * time.Second,
					Dialer:            dialer,
					WriteTimeout:      10 * time.Second,
					ReadTimeout:       10 * time.Second,
					RebalanceInterval: 15 * time.Second,
					IdleConnTimeout:   9 * time.Minute,
				})
				for i := 0; i < config.ContainerSize; i++ {
					bytes := <-send
					if bytes[len(bytes)-1] == 136 && bytes[len(bytes)-2] == 213 {
						unarc := make([]byte, len(bytes)*12)
						n, _ := lz42.UncompressBlock(bytes[:len(bytes)-2], unarc)
						bytes = nil
						bytes = unarc[:n]
						kmsgs := deserializeBucket(bytes)
						_ = w.WriteMessages(context.Background(), kmsgs...)
					} else if bytes[len(bytes)-1] == 141 && bytes[len(bytes)-2] == 213 {
						kmsgs := deserializeBucket(bytes[:len(bytes)-2])
						_ = w.WriteMessages(context.Background(), kmsgs...)
					}
					time.Sleep(250 * time.Millisecond)
				}
				_ = w.Close()
			}
		}
	}
}

func (rt *Routine) stop() {
	log.Printf("Routine [%s] is stopping", rt.Name)
	rt.End <- true
}

func main() {
	file, err := os.Open("config.json")
	check(err)
	defer func(f *os.File) {
		err := f.Close()
		check(err)
	}(file)
	decoder := json.NewDecoder(file)
	config := Config{}
	err = decoder.Decode(&config)
	check(err)

	var chanSignal = make(chan os.Signal, 1)
	hostname, err = os.Hostname()

	// имена файлов от Seeker к Reader
	chanFile := make(chan string, 1)
	// запрос на новое имя файла к Seeker
	chanName := make(chan bool, 1)
	// строки от Reader к сборке корзинок
	chanString := make(chan kafka.Message, 1)
	// контейнер раз в пять минут от сборки корзинок к Writer
	chanSend := make(chan []byte, config.ContainerSize)
	signal.Notify(chanSignal, syscall.SIGINT, syscall.SIGTERM)

	var pointers []*Routine

	fs := FileSeeker{Routine: Routine{Name: "FileSeeker", End: make(chan bool, 1)}}
	fr := FileReader{Routine: Routine{Name: "FileReader", End: make(chan bool, 1)}}
	bc := BucketCollector{Routine: Routine{Name: "BucketCollector", End: make(chan bool, 1)}}
	kw := KafkaWriter{Routine: Routine{Name: "KafkaWriter", End: make(chan bool, 1)}}

	pointers = append(pointers, &fs.Routine)
	pointers = append(pointers, &fr.Routine)
	pointers = append(pointers, &bc.Routine)
	pointers = append(pointers, &kw.Routine)

	waitGroup.Add(4)
	go fs.fileSeeker(&config, chanFile, chanName)
	go fr.fileReader(&config, chanFile, chanName, chanString)
	go fr.bucketCollector(&config, chanString, chanSend)
	go kw.kafkaWriter(&config, chanSend)

	for {
		//SIGTERM - все на выход
		s := <-chanSignal
		log.Printf("Got signal: %s.. stopping", s)
		for _, pts := range pointers {
			pts.stop()
		}
		waitGroup.Wait()
		os.Exit(0)
	}
}
