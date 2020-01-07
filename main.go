package main

import (
	"go-kafka/src"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

func main() {
	var conf src.Config

	yamlFile, _ := ioutil.ReadFile("config.yaml")
	err := yaml.Unmarshal(yamlFile, &conf)
	if err != nil {
		panic(err)
	}

	producer, _ := src.NewProducer(conf, "go_test", 0)
	_ = producer.Push([]byte("{\"name\":\"test\"}"), nil)
}
