package imgevent

import "testing"

func TestCreateProducer(t *testing.T) {
	_, err := NewProducer()
	if err != nil {
		t.Fatal("Producing event to kafka was fail on error", err.Error())
	}
}

func TestProduceUploadEvent(t *testing.T) {
	p, _ := NewProducer()
	e := &UploadEvent{
		Topic:     "test-imgevent",
		ImgID:     "test-id",
		Timestamp: "1490762174",
	}
	err := p.Produce(e)
	if err != nil {
		t.Fatal("Producing event to kafka was fail on error", err.Error())
	}
}
