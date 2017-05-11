package imgevent

import (
	"errors"

	"github.com/Shopify/sarama"
)

// UploadEvent structure of kafka
type UploadEvent struct {
	Topic     string
	ImgID     string
	Timestamp string
}

func (e *UploadEvent) buildEvent() (*sarama.ProducerMessage, error) {
	if e.ImgID == "" {
		return nil, errors.New("Missing ImgID")
	}
	msg := &sarama.ProducerMessage{Topic: e.Topic, Key: sarama.StringEncoder(e.ImgID), Value: sarama.StringEncoder(e.ImgID)}
	return msg, nil
}
