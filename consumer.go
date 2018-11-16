/*===============================================================
*   Copyright (C) 2018 All rights reserved.
*
*   FileName：main.go
*   Author：chl
*   Date： 2018-08-01
*   Description：
*
================================================================*/
package main

import (
	"sync"
	"fmt"
	"github.com/Shopify/sarama"
	"flag"
	"strconv"
	"time"
	"sync/atomic"
)

var (
	wg sync.WaitGroup
)

func Consumer() {
	counts := flag.String("counts", "3", "every topic consumer data counts")
	flag.Parse()

	num, err := strconv.ParseUint(*counts, 10, 64)
	_ = num
	if err != nil {
		fmt.Print("err0")
		return
	}

	topics := []string{"chlTopic"}

	for _, topic := range topics {

		config := sarama.NewConfig()
		config.ClientID = "65tgbytg6tvb6tgbt"
		consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
		if err != nil {
			fmt.Print("newConsumer err")
			return
		}
		defer consumer.Close()

		partitionList, err := consumer.Partitions(topic)
		if err != nil {
			fmt.Print("err1")
			return
		}
		//count := uint64(0)
		//_ = count
		var mutex sync.Mutex
		_ = mutex
		for partition := range partitionList {
			//tpc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
			tpc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetOldest)
			if err != nil {
				fmt.Print("err2")
				return
			}
			defer tpc.AsyncClose()
			defer func(partition int){
				fmt.Println("testDefer...", partition)
			}(partition)
			fmt.Println("testChl...")

			count := uint64(0)
			_ = count
			wg.Add(1)
			go func(tpc sarama.PartitionConsumer, topic string) {
				defer wg.Done()
				for msg := range tpc.Messages() {

					mutex.Lock()
					atomic.AddUint64(&count, 1)
					if atomic.LoadUint64(&count) <= num {
						fmt.Printf("Topic:%s, Partition:%d, Offset:%d, Key:%s, Value:%s\n", topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
					} else {
						mutex.Unlock()
						fmt.Println("chl")
						break
					}
					mutex.Unlock()
					time.Sleep(time.Millisecond * 100)
				}
			}(tpc, topic)
		}
	}

	wg.Wait()
}

func main() {
	Consumer()
}
