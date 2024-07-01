package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"stand/models"
	"syscall"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topic         = "test-topic"
	brokerAddress = "192.168.168.87:30763"
)

func produce(ctx context.Context, w *kafka.Writer, i int) {
		loan := models.New()
		data, err := json.Marshal(loan)
		if err != nil {
			log.Fatal(err)
		}
		// each kafka message has a key and value. The key is used
		// to decide which partition (and consequently, which broker)
		// the message gets published on
		err = w.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(i)),
			// create an arbitrary message payload for the value
			Value: data,
		})
		if err != nil {
			log.Fatal("could not write message", err)
		}

		// log a confirmation once the message is written
		log.Printf("writes message: %d (leadId: %d, currentStatus: %s)", i, loan.LeadId, loan.CurrentStatus)
}

func main() {
	log.SetFlags(log.Lmsgprefix | log.Ldate | log.Lmicroseconds)
	// intialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer w.Close()
	// create a new context
	ctx := context.Background()
	// initialize a counter
	i := 0
	go func() {
		for range time.Tick(100*time.Millisecond) {
			produce(ctx, w, i)
			i++
		}
	}()
	go func() {
		for range time.Tick(50*time.Millisecond) {
			produce(ctx, w, i)
			i++
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
}