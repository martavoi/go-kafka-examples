package main

import (
	"context"
	"fmt"
	"go-kafka-examples/internal/sample"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sysCh := make(chan os.Signal, 1)
	signal.Notify(sysCh, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	loopDone := make(chan any)
	go func(ctx context.Context, done chan<- any) {
		for i := 0; true; i++ {
			batch := sample.NewRndKakfaSampleMsgBatch(10)
			fmt.Printf("Batch #%d===============================\n", i)
			for i, msg := range batch {
				fmt.Printf("Message #%d is %v\n", i, msg)
			}

			topic := "sample.1"
			server := "localhost"
			err := produceBatch(topic, server, batch)
			if err != nil {
				fmt.Printf("Error upon produceBatch: %v", err)
			}

			if ctx.Err() != nil {
				fmt.Printf("Aborting. Next batch #%d won't be executed\n", i+1)
				done <- 1
				break
			}
			<-time.After(2 * time.Second)
		}
	}(ctx, loopDone)

	<-sysCh
	cancel()

	<-loopDone
}

func produceBatch(topic string, server string, batch []sample.KafkaSampleMsg) error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": server,
	})

	if err != nil {
		return err
	}

	defer p.Close()

	wg := sync.WaitGroup{}
	// batch kafka produce
	for _, msg := range batch {

		wg.Add(1)
		go func(msg sample.KafkaSampleMsg) {
			defer wg.Done()
			delCh := make(chan kafka.Event)
			defer close(delCh)

			mJson, err := msg.MarshallJson()
			if err != nil {
				fmt.Printf("Failed to marshall: %v. Error: %v\n", msg, err)
			}

			for {
				err = p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Key:            []byte(msg.Id),
					Value:          mJson,
				}, delCh)

				if err != nil {
					fmt.Printf("Produce failed on msg.Id: %s. Retrying in 5s...\n", msg.Id)
					<-time.After(5 * time.Second)
					continue
				}

				break
			}

			e := <-delCh
			m := e.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				fmt.Printf("Delivered message to topic %s [%d] at offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}
		}(msg)
	}

	wg.Wait()

	return nil
}
