package main

import (
	"fmt"
	ck "github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

func main() {
	consumer, err := ck.NewConsumer(&ck.ConfigMap{
		"bootstrap.servers":        "localhost:9092",
		"group.id":                 "test-confluent-consumer",
		"enable.auto.commit":       "false",
		"auto.offset.reset":        "earliest",
		"reconnect.backoff.ms":     50,
		"reconnect.backoff.max.ms": 60000,
		"log.connection.close":     false,
	})
	if err != nil {
		fmt.Printf("Failed to construct consumer: %v\n", err)
		return
	}
	consumer.Subscribe("test_topic", nil)
	for {
		msg, err := consumer.ReadMessage(5 * time.Second)
		if err != nil {
			fmt.Printf("Failed to read message: %v\n", err)
		} else {
			fmt.Printf("%v\n", string(msg.Value))
		}
	}
}
