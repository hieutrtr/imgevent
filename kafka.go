package imgevent

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	avro "github.com/elodina/go-avro"
	gavro "github.com/elodina/go-kafka-avro"
)

var (
	schemaRegistry = os.Getenv("KAFKA_SCHEMA_REGISTRY")
)

// FnProcess function to process consumer's events
type FnProcess func(*sarama.ConsumerMessage) error

// Consumer use to consume upload events
type Consumer struct {
	consumer *cluster.Consumer
	process  FnProcess
}

// Producer of kafka
type Producer struct {
	sarama.SyncProducer
}

type Event interface {
	buildEvent() (*sarama.ProducerMessage, error)
}

// NewUploadConsumer create consumer of upload event
func NewConsumer(offsetInit int64, fn FnProcess) *Consumer {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	kafkaTopics := os.Getenv("KAFKA_TOPICS")
	kafkaConsGroup := os.Getenv("KAFKA_CONSUMER_GROUP")

	if kafkaBrokers == "" {
		panic("Missing KAFKA_BROKERS env")
	}
	if kafkaTopics == "" {
		panic("Missing KAFKA_TOPICS env")
	}
	if kafkaConsGroup == "" {
		panic("Missing KAFKA_CONSUMER_GROUP env")
	}

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = offsetInit
	config.Group.Return.Notifications = true

	cons, err := cluster.NewConsumer(strings.Split(kafkaBrokers, ","), kafkaConsGroup, strings.Split(kafkaTopics, ","), config)
	if err != nil {
		panic("Cannot start consumer with error " + fmt.Sprint(err))
	}
	return &Consumer{
		consumer: cons,
		process:  fn,
	}
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

func AvroDecode(obj []byte, key string) (interface{}, error) {
	if schemaRegistry == "" {
		return nil, errors.New("Schema Registry was not configed")
	}
	decoder := gavro.NewKafkaAvroDecoder(schemaRegistry)
	decoded, err := decoder.Decode(obj)
	if err != nil {
		return nil, err
	}
	decodedRecord, ok := decoded.(*avro.GenericRecord)
	if ok == false {
		return nil, errors.New("Cannot decode avro record")
	}
	return decodedRecord.Get(key), nil
}

// Consume upload events
func (c *Consumer) Consume() {
	for {
		select {
		case msg := <-c.consumer.Messages():
			err := c.process(msg)
			if err != nil {
				fmt.Println(err)
			}
			c.consumer.MarkOffset(msg, "") // mark message as processed
		}
	}
}
