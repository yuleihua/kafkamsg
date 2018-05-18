package main

import (
	"flag"
	"strings"

	log "github.com/sirupsen/logrus"
)

type KafkaServer struct {
	Addrs   []string `toml:"addrs"`
	Topics  []string `toml:"topics"`
	Timeout int      `toml:"timeout"`
}

var (
	topics   string
	hosts    string
	dir      string
	offset   string
	loglevel int
)

func init() {
	flag.StringVar(&topics, "t", "venus", "topic setting")
	flag.StringVar(&hosts, "h", "127.0.0.1:9092", "broker setting")
	flag.IntVar(&loglevel, "l", 5, "log level")
	flag.StringVar(&dir, "d", "./kdata", "output directory setting")
	flag.StringVar(&offset, "o", "oldest", "offset, oldest or newest")
}

func main() {
	flag.Parse()

	// setting logger
	log.SetFormatter(&log.TextFormatter{
		DisableColors:   true,
		TimestampFormat: "2006/01/02-15:04:05.000",
	})
	log.SetLevel(log.Level(loglevel))

	addrList := strings.Split(hosts, ",")
	tpList := strings.Split(topics, ",")
	vs := &KafkaServer{
		Addrs:   addrList,
		Topics:  tpList,
		Timeout: 200,
	}

	log.Infof("%v", vs)
	// handle
	if err := Setup(vs, dir, offset); err != nil {
		log.Fatalf("Setup error:%v", err)
	}

	if err := Run(); err != nil {
		log.Errorf("%v", err)
	}

	if err := Shutdown(); err != nil {
		log.Errorf("shutdown error:%v", err)
	}

	log.Error("shutting down end")
}
