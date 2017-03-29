package imgevent

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

// UploadEvent structure of kafka
type UploadEvent struct {
	Topic     string
	ImgID     string
	Timestamp string
}

// Producer of kafka
type Producer struct {
	sarama.SyncProducer
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
func (p *Producer) Produce(e *UploadEvent) error {
	val := fmt.Sprintf("{\"ImgID\":%s,\"Timestamp\":%s}", e.ImgID, e.Timestamp)
	msg := &sarama.ProducerMessage{Topic: e.Topic, Key: sarama.StringEncoder(e.ImgID), Value: sarama.StringEncoder(val)}
	partition, offset, err := p.SyncProducer.SendMessage(msg)
	if err == nil {
		log.Printf("Produced: %s metadata: {partition:%d, offset:%d}\n", fmt.Sprint(e), partition, offset)
	}
	return err
}
