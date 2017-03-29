package imgevent

import "testing"

func TestBuildUploadEvent(t *testing.T) {
	e := &UploadEvent{
		Topic:     "test-imgevent",
		ImgID:     "test-id",
		Timestamp: "1490762174",
	}
	_, err := e.buildEvent()
	if err != nil {
		t.Fatal("Building upload event was fail on error", err.Error())
	}
}

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
