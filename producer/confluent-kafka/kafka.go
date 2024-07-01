package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"stand/models"
	"strconv"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	bootstrapServers string = "192.168.168.87:30763"
	topic            string = "test-topic"
)

func produce(p *kafka.Producer, i int) {
	loan := models.New()
	data, err := json.Marshal(loan)
	if err != nil {
		log.Fatal(err)
	}
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(strconv.Itoa(i)),
		Value:          data,
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, nil)

	log.Printf("Sended message #%d (leadId: %d, currentStatus: %s) to topic %q\n", i, loan.LeadId, loan.CurrentStatus, topic)

	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrQueueFull {
			// Producer queue is full, wait 1s for messages
			// to be delivered then try again.
			time.Sleep(time.Second)
		}
	}
}

func main() {
	log.SetFlags(log.Lmsgprefix | log.Ldate | log.Lmicroseconds)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}

	log.Printf("Created Producer %v\n", p)

	// Listen to all the events on the default events channel
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				// The message delivery report, indicating success or
				// permanent failure after retries have been exhausted.
				// Application level retries won't help since the client
				// is already configured to do that.
				m := ev
				var loan models.Loan
				err = json.Unmarshal(m.Value, &loan)
				if err != nil {
					log.Fatal(err)
				}
				if m.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					log.Printf("Delivered message #%s (leadId: %d, currentStatus: %s) to topic %q [%d] at offset %v\n",
					string(m.Key), loan.LeadId, loan.CurrentStatus, *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case kafka.Error:
				// Generic client instance-level errors, such as
				// broker connection failures, authentication issues, etc.
				//
				// These errors should generally be considered informational
				// as the underlying client will automatically try to
				// recover from any errors encountered, the application
				// does not need to take action on them.
				log.Printf("Error: %v\n", ev)
			default:
				log.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	// initialize a counter
	i := 0
	go func() {
		for range time.Tick(100 * time.Millisecond) {
			produce(p, i)
			i++
		}
	}()
	go func() {
		for range time.Tick(50 * time.Millisecond) {
			produce(p, i)
			i++
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	// Flush and close the producer and the events channel
	for p.Flush(10000) > 0 {
		log.Println("Still waiting to flush outstanding messages")
	}
	log.Println("Closing producer")
	p.Close()
}
