package main

import (
	"context"
	"encoding/json"
	"log"
	"stand/models"

	"github.com/segmentio/kafka-go"
)

const (
	topic         = "test-topic"
	brokerAddress = "192.168.168.87:30763"
)

func main() {
	log.SetFlags(log.Lmsgprefix | log.Ldate | log.Lmicroseconds)
	// create a new context
	ctx := context.Background()
	// initialize a new reader with the brokers and topic
	// the groupID identifies the consumer and prevents
	// it from receiving duplicate messages
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: "test-group",
	})
	defer r.Close()
	for {
		// the `ReadMessage` method blocks until we receive the next event
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			log.Fatal("could not read message",err)
		}
		var loan models.Loan
		err = json.Unmarshal(msg.Value, &loan)
		if err != nil {
			log.Fatal(err)
		}
		// after receiving the message, log its value
		log.Printf("received message: %s (leadId: %d, currentStatus: %s)\n", string(msg.Key), loan.LeadId, loan.CurrentStatus)
	}
}