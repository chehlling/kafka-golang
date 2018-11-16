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
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"sync"

	"github.com/Shopify/sarama"
)

var (
	test33Topic = "log-chl"
	wg          sync.WaitGroup
)

func Producer() {
	//_ = sarama1
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		fmt.Print("err: ", err)
		return
	}
	defer producer.Close()

	fi, err := os.Open("chl.txt")
	if err != nil {
		fmt.Print("err: ", err)
		return
	}
	defer fi.Close()
	br := bufio.NewReader(fi)

	msg := &sarama.ProducerMessage{
		Topic:     "chlTopic",
		Partition: int32(-1),
		Key:       sarama.StringEncoder("key"),
	}
	_ = msg

	var value []string
	for {
		a, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		vRegexp := regexp.MustCompile(`.*Value:(.*)`)
		value = vRegexp.FindStringSubmatch(string(a))
		if len(value) >= 2 {
			msg.Value = sarama.ByteEncoder(value[1])
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			fmt.Println("Send message Fail")
		}
		fmt.Printf("Partition = %d, offset=%d\n", partition, offset)
	}
}

func main() {
	Producer()
}
