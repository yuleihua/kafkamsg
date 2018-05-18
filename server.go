package main

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

var wkctx WorkerInfo

type WorkerInfo struct {
	consumer   sarama.Consumer
	interval   time.Duration
	tickerTime time.Duration
	tps        []string
	wg         sync.WaitGroup
	lock       sync.Mutex
	dir        string
	isNewest   bool
}

func Setup(sk *KafkaServer, dir, offset string) error {
	if sk == nil {
		return errors.New("invalid parameter")
	}

	conf := sarama.NewConfig()
	conf.Net.DialTimeout = time.Duration(sk.Timeout) * time.Second
	consumer, err := sarama.NewConsumer(sk.Addrs, conf)
	if err != nil {
		log.Fatalf("Kafka newConsumer error, addr:%v, error:%v", sk.Addrs, err)
	}
	wkctx.consumer = consumer
	wkctx.dir = dir
	wkctx.tps = sk.Topics
	wkctx.interval = 200 * time.Millisecond
	if offset == "newest" {
		wkctx.isNewest = true
	}
	log.Infof("server initiation end")
	return nil
}

func readHandle(topic string, pidx int32) {
	defer wkctx.wg.Done()

	file, err := os.OpenFile(fmt.Sprintf("%v/%v_%v", wkctx.dir, topic, pidx), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("open file failed", err.Error())
	}
	defer file.Close()

	offsetFlag := sarama.OffsetOldest
	if wkctx.isNewest {
		offsetFlag = sarama.OffsetNewest
	}
	pc, err := wkctx.consumer.ConsumePartition(topic, pidx, offsetFlag)
	if err != nil {
		log.Fatalf("Kafka ConsumePartition error, topic:%v, error:%v", topic, err)
	}
	defer pc.Close()

	log.Infof("reader worker topic[%s] start", topic)
	count := 0

	for {
		select {
		case msg := <-pc.Messages():
			//log.Infof("Partition:%d, Offset:%d, Key:%s, Value:%s", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			if topic != msg.Topic {
				log.Errorf("topic is not match")
				continue
			}
			log.Infof("Receive msg,%v:%v", string(msg.Key), string(msg.Value))
			file.WriteString(string(msg.Value))
			file.WriteString("\n")
		}
	}
	log.Warnf("topic[%v:%v:%d] stop", topic, pidx, count)
}

func Run() error {
	for _, t := range wkctx.tps {
		if t != "" {
			tpList, err := wkctx.consumer.Partitions(t)
			if err != nil {
				log.Fatalf("Failed to get topic(%s) list of partitions:%v", t, err)
			}
			log.Infof("get topic(%s) list of partitions:%v", t, tpList)
			for pidx := range tpList {
				go readHandle(t, int32(pidx))
				wkctx.wg.Add(1)
			}
		}
	}
	return nil
}

func Shutdown() error {
	wkctx.wg.Wait()

	if wkctx.consumer != nil {
		wkctx.consumer.Close()
	}
	log.Warnf("server shutdown end")
	return nil
}
