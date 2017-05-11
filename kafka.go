package imgevent

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

// Producer of kafka
type Producer struct {
	sarama.SyncProducer
}

type Event interface {
	buildEvent() (*sarama.ProducerMessage, error)
}

// NewProducer create kafka producer
func NewProducer() (*Producer, error) {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		return nil, errors.New("Missing KAFKA_BROKERS env")
	}
	sp, err := sarama.NewSyncProducer(strings.Split(kafkaBrokers, ","), nil)
	if err != nil {
		return nil, err
	}
	return &Producer{sp}, nil
}

// Produce kafka event
func (p *Producer) Produce(e Event) error {
	var partition int32
	var offset int64
	msg, err := e.buildEvent()
	if err != nil {
		return err
	}
	partition, offset, err = p.SyncProducer.SendMessage(msg)
	if err == nil {
		log.Printf("Produced: %s metadata: {partition:%d, offset:%d}\n", fmt.Sprint(e), partition, offset)
	}
	return err
}
