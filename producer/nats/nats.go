package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"stand/models"
	"syscall"
	"time"

	nats "github.com/nats-io/nats.go"
)

func msgPublish(js nats.JetStreamContext) {
	loan := models.New()
	data, err := json.Marshal(loan)
	if err != nil {
		log.Fatal(err)
	}

	subj, msg := "test", data

	ackF, err := js.PublishAsync(subj, msg)
	if err != nil {
		log.Fatalf("Error during async publish: %v\n", err)
	}

	select {
	case err := <-ackF.Err():
		log.Fatal("Failed to publish:", err)
	case <-time.After(5 * time.Second):
		log.Fatal("Did not receive an ack")
	case ack := <-ackF.Ok():
		// Message successfully published.
		log.Printf("Published msg (leadId: %d, currentStatus: %s) with sequence number %d on stream %q", loan.LeadId, loan.CurrentStatus, ack.Sequence, ack.Stream)
	}
}

func main() {
	var URL string = "nats://192.168.168.87:32067"
	log.SetFlags(log.Lmsgprefix | log.Ldate | log.Lmicroseconds)
	// Connect Options.
	opts := []nats.Option{nats.Name("NATS JetStream Example Publisher")}

	// Connect to NATS
	nc, err := nats.Connect(URL, opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// Create JetStream Context
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}
	// Создаем новый поток, если его еще нет
	// _, err = js.AddStream(&nats.StreamConfig{
	// 	Name:     "ORDERS",
	// 	Subjects: []string{"ORDERS.*"},
	// })
	// if err != nil && err != nats.ErrStreamNameAlreadyInUse {
	// 	log.Fatal(err)
	// }

	go func() {
		for range time.Tick(100 * time.Millisecond) {
			msgPublish(js)
		}
	}()
	go func() {
		for range time.Tick(50 * time.Millisecond) {
			msgPublish(js)
		}
	}()

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	log.Fatalf("Exiting")
}
