package main

import (
	"flag"
	"fmt"
	"go-kafka/src"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
)

var (
	conf  src.Config
	mode  string
	topic string
)

func init() {
	flag.StringVar(&mode, "mode", "producer", "producer or consumer")
	flag.StringVar(&topic, "topic", "go_test", "topic name")
	flag.Parse()

	yamlFile, _ := ioutil.ReadFile("config.yaml")
	err := yaml.Unmarshal(yamlFile, &conf)
	if err != nil {
		panic(err)
	}

}

func main() {
	if mode == "producer" {
		producer()
	} else {
		consumer()
	}
}

func producer() {
	producer, _ := src.NewProducer(conf, "go_test", 0)
	_ = producer.Push([]byte("{\"name\":\"test\"}"), nil)
}

func consumer() {
	var topics = []src.Topic{
		{
			Name: "go_test",
		},
	}
	consumer, _ := src.NewConsumer(conf, "test-id", topics)

	loop := true
	go func() {
		var c = make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
		<-c

		fmt.Println("quit")
		loop = false
	}()

	for loop {
		data, err := consumer.Pull(200)
		if err != nil {
			panic(err)
		}

		if data == "" {
			continue
		}

		fmt.Println(data)
		_ = consumer.Commit()
	}
}
