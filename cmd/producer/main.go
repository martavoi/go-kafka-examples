package main

import (
	"context"
	"fmt"
	kafkago "go-kafka-examples/internal/kafka-batch-producer"
	"go-kafka-examples/internal/sample"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := "sample.1"
	server := "localhost"

	p, err := kafkago.NewBulkProducer(&kafka.ConfigMap{"bootstrap.servers": server})
	if err != nil {
		fmt.Printf("Error upon producer creation: %v", err)
		os.Exit(1)
	}

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

			buildKafkaMsgs := func(msgs []sample.KafkaSampleMsg) ([]*kafka.Message, error) {
				res := make([]*kafka.Message, len(msgs))
				for i, v := range msgs {
					jsonBytes, err := v.MarshallJson()
					if err != nil {
						return nil, fmt.Errorf("marshall error: %w", err)
					}
					res[i] = &kafka.Message{
						Key:            []byte(v.Id),
						Value:          jsonBytes,
						TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					}
				}

				return res, nil
			}

			kafkaMsgs, err := buildKafkaMsgs(batch)
			if err != nil {
				fmt.Printf("Error during messages serialization: %v", err)
				os.Exit(1)
			}

			for {
				delClbck := func(ctx context.Context, msg *kafka.Message) error {
					fmt.Printf("Msg delivered key = %s value = %s \n", msg.Key, msg.Value)
					return nil
				}
				err = p.ProduceBatch(ctx, kafkaMsgs, delClbck)
				if err != nil {
					fmt.Printf("Produce batch error: %v, retrying in 5s", err)
					<-time.After(5 * time.Second)
					continue
				} else {
					break
				}
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
