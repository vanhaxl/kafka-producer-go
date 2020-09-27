package main

import (
	"fmt"
	"github.com/Shopify/sarama"
)

func main() {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// brokers := []string{"192.168.59.103:9092"}
	brokers := []string{"localhost:9092","localhost:9093","localhost:9094","localhost:9095"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		// Should not reach here
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			// Should not reach here
			panic(err)
		}
	}()

	topic := "helloworld"
	for i := 0; i< 5; i++ {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder("Something Cool"),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			panic(err)
		}

		fmt.Printf("Message is sent to topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	}

}