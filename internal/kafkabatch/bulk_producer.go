package kafkabatch

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type BulkProducer struct {
	*kafka.Producer
}

func NewBulkProducer(conf *kafka.ConfigMap) (*BulkProducer, error) {
	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, err
	}

	return &BulkProducer{Producer: p}, nil
}

type MsgDelClbck func(ctx context.Context, msg *kafka.Message) error

func (p *BulkProducer) ProduceBatch(ctx context.Context, batch []*kafka.Message, clbck MsgDelClbck) error {
	batchSize := len(batch)
	delCh := make(chan kafka.Event, batchSize)
	defer close(delCh)
	for _, msg := range batch {
		err := p.Produce(msg, delCh)
		if err != nil {
			return fmt.Errorf("error %w while producing message key = %s", err, msg.Key)
		}
	}

	for i := 0; i < batchSize; i++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("batch context cancelled, %d out of %d delivered", i, batchSize)
		case e := <-delCh:
			if clbck != nil {
				m := e.(*kafka.Message)
				clbck(ctx, m)
			}
		}
	}

	return nil
}
