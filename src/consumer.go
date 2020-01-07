package src

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"sync"
)

type Consumer struct {
	Handler  *kafka.Consumer
	Messages []*kafka.Message
	Mux      sync.Mutex
}

type Topic struct {
	Name      string
	Partition int32
	Offset    int64
}

func (consumer *Consumer) Pull(timeout int) (string, error) {
	ev := consumer.Handler.Poll(timeout)
	if ev == nil {
		return "", nil
	}

	switch e := ev.(type) {
	case *kafka.Message:
		consumer.Messages = append(consumer.Messages, e)
		return string(e.Value), nil
	case kafka.Error:
		return "", e
	default:
		return "", nil
	}
}

func (consumer *Consumer) Commit() error {
	defer consumer.Mux.Unlock()
	consumer.Mux.Lock()

	for i := 0; len(consumer.Messages) > 0; {
		if _, err := consumer.Handler.CommitMessage(consumer.Messages[i]); err != nil {
			return err
		}
		consumer.Messages = append(consumer.Messages[i+1:])
	}

	return nil
}

func (consumer *Consumer) Close() error {
	if err := consumer.Handler.Close(); err != nil {
		return err
	}

	return nil
}

func NewConsumer(config Config, groupId string, topics []Topic) (*Consumer, error) {
	(*config)["group.id"] = groupId
	c, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	topicPartitions := make([]kafka.TopicPartition, 0, len(topics))
	topicNames := make([]string, 0, len(topics))

	for _, topic := range topics {
		if topic.Offset == 0 {
			topicNames = append(topicNames, topic.Name)
		} else {
			topicPartitions = append(topicPartitions, kafka.TopicPartition{
				Topic:     &topic.Name,
				Partition: topic.Partition,
				Offset:    kafka.Offset(topic.Offset),
			})
		}
	}

	if len(topicNames) > 0 {
		if err = c.SubscribeTopics(topicNames, nil); err != nil {
			return nil, err
		}
	}

	if len(topicPartitions) > 0 {
		if err = c.Assign(topicPartitions); err != nil {
			return nil, err
		}
	}

	consumer := &Consumer{
		Handler: c,
	}
	return consumer, nil
}
