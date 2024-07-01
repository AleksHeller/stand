package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"stand/models"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	bootstrapServers string = "192.168.168.87:30763"
	topics                  = []string{"test-topic"}
	group            string = "test-group"
)

func main() {
	log.SetFlags(log.Lmsgprefix | log.Ldate | log.Lmicroseconds)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		// Avoid connecting to IPv6 brokers:
		// This is needed for the ErrAllBrokersDown show-case below
		// when using localhost brokers on OSX, since the OSX resolver
		// will return the IPv6 addresses first.
		// You typically don't need to specify this configuration property.
		"broker.address.family": "v4",
		"group.id":              group,
		"session.timeout.ms":    6000,
		// Start reading from the first message of each assigned
		// partition if there are no previously committed offsets
		// for this group.
		"auto.offset.reset": "earliest",
		// Whether or not we store offsets automatically.
		"enable.auto.offset.store": false,
	})

	if err != nil {
		log.Fatalf("Failed to create consumer: %s\n", err)
	}

	log.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		log.Printf("Error subscribing to %q: %s\n", topics, err)
	}

	run := true
	for run {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				var loan models.Loan
				err = json.Unmarshal(e.Value, &loan)
				if err != nil {
					log.Fatal(err)
				}
				// Process the message received.
				log.Printf("Received message #%s (leadId: %d, currentStatus: %s) on topic %q [%d] at offset %v %v %v\n",
					string(e.Key), loan.LeadId, loan.CurrentStatus, *e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset, e.TimestampType, e.Timestamp)
				if e.Headers != nil {
					log.Printf("Headers: %v\n", e.Headers)
				}

				// We can store the offsets of the messages manually or let
				// the library do it automatically based on the setting
				// enable.auto.offset.store. Once an offset is stored, the
				// library takes care of periodically committing it to the broker
				// if enable.auto.commit isn't set to false (the default is true).
				// By storing the offsets manually after completely processing
				// each message, we can ensure atleast once processing.
				_, err := c.StoreMessage(e)
				if err != nil {
					log.Printf("Error storing offset after message %s:\n", e.TopicPartition)
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				log.Printf("Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				log.Printf("Ignored %v\n", e)
			}
		}
	}

	log.Println("Closing consumer")
	c.Close()
}
