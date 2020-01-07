package src

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Config *kafka.ConfigMap

type Producer struct {
	Handler        *kafka.Producer
	TopicPartition kafka.TopicPartition
	DeliverChan    chan kafka.Event
}

func (producer *Producer) Push(message []byte, key []byte) error {
	err := producer.Handler.Produce(&kafka.Message{
		TopicPartition: producer.TopicPartition,
		Value:          message,
		Key:            key,
	}, producer.DeliverChan)

	if err != nil {
		return err
	}

	event := (<-producer.DeliverChan).(*kafka.Message)
	if event.TopicPartition.Error != nil {
		return fmt.Errorf("push failed: %v\n", event.TopicPartition.Error)
	}

	return nil
}

func (producer *Producer) Close() {
	for producer.Handler.Len() > 0 {
		producer.Handler.Flush(1)
	}

	close(producer.DeliverChan)
	producer.Handler.Close()
}

func NewProducer(config Config, topic string, partition int32) (*Producer, error) {
	kafkaProducer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}

	topicPartition := kafka.TopicPartition{
		Topic:     &topic,
		Partition: partition,
	}

	producer := &Producer{
		Handler:        kafkaProducer,
		TopicPartition: topicPartition,
		DeliverChan:    make(chan kafka.Event),
	}

	return producer, nil
}
