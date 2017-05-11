package imgevent

import (
	"errors"

	"encoding/json"

	"github.com/Shopify/sarama"
)

// UploadEvent structure of kafka
type AdImageEvent struct {
	Topic     string
	ImgID     string `json:"media_id"`
	AdID      string `json:"ad_id"`
	Timestamp string
}

func (e *AdImageEvent) buildEvent() (*sarama.ProducerMessage, error) {
	if e.ImgID == "" {
		return nil, errors.New("Missing ImgID")
	}
	if e.AdID == "" {
		return nil, errors.New("Missing AdID")
	}
	je, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	msg := &sarama.ProducerMessage{Topic: e.Topic, Key: sarama.StringEncoder(e.ImgID), Value: sarama.StringEncoder(string(je))}
	return msg, nil
}
