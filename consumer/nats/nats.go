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

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		log.Printf("Disconnected due to: %s, will attempt reconnects for %.0fm", err, totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Fatalf("Exiting: %v", nc.LastError())
	}))
	return opts
}

func main() {
	var URL string = "nats://192.168.168.87:32067"
	log.SetFlags(log.Lmsgprefix | log.Ldate | log.Lmicroseconds)
	// Connect Options.
	opts := []nats.Option{nats.Name("NATS JetStream Example Subscriber")}
	opts = setupConnOptions(opts)

	// Connect to NATS
	nc, err := nats.Connect(URL, opts...)
	if err != nil {
		log.Fatal(err)
	}

	// Create JetStream Context
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	// Create a Consumer
	_, err = js.AddConsumer("test", &nats.ConsumerConfig{
		Durable:        "test-consumer",
		DeliverGroup:   "test-consumer",
	})
	if err != nil && err != nats.ErrConsumerNameAlreadyInUse {
		log.Fatal(err)
	}	

	subj, queue, i := "test", "test-consumer", 0
	mcb := func(msg *nats.Msg) {
		i++
		var loan models.Loan
		err := json.Unmarshal(msg.Data, &loan)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("[#%d] Received msg (leadId: %d, currentStatus: %s) with metadata %v\n", i, loan.LeadId, loan.CurrentStatus, msg.Reply)
	}
	_, err = js.QueueSubscribe(subj, queue, mcb)
	if err != nil {
		log.Printf("Error subscribing to %q: %s\n", subj, err)
	}

	nc.Flush()

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening on [%s], queue group [%s]", subj, queue)

	// Setup the interrupt handler to drain so we don't miss
	// requests when scaling down.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	log.Println()
	log.Printf("Draining...")
	nc.Drain()
	log.Fatalf("Exiting")
}
