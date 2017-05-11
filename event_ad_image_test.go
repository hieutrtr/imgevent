package imgevent

import "testing"

func TestBuildAdImageEvent(t *testing.T) {
	e := &AdImageEvent{
		Topic: "test-adimgevent",
		ImgID: "test-ad-image-id",
		AdID:  "test-ad-image-id",
	}
	_, err := e.buildEvent()
	if err != nil {
		t.Fatal("Building upload event was fail on error", err.Error())
	}
}

func TestProduceAdImageEvent(t *testing.T) {
	p, _ := NewProducer()
	e := &AdImageEvent{
		Topic: "test-adimgevent",
		ImgID: "test-ad-image-id",
		AdID:  "test-ad-image-id",
	}
	err := p.Produce(e)
	if err != nil {
		t.Fatal("Producing event to kafka was fail on error", err.Error())
	}
}
