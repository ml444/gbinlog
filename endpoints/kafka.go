package endpoints

import (
	"encoding/json"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/ml444/gbinlog/handler"
	"github.com/ml444/gbinlog/util"
)

var DefaultEndpoint *KafkaEndPoint

type KafkaEndPoint struct {
	Addrs     []string
	Cfg       sarama.Config
	client    sarama.Client
	producer  sarama.AsyncProducer
	onSuccess func(msg *sarama.ProducerMessage)
	onFailure func(err *sarama.ProducerError)

	TopicCfgMap map[string]TopicCfg
}

func NewKafkaEndpoint(kafkaCfg KafkaConfig) (*KafkaEndPoint, error) {
	p := &KafkaEndPoint{
		Addrs:       kafkaCfg.Addrs,
		Cfg:         sarama.Config(kafkaCfg.Cfg),
		onSuccess:   kafkaCfg.OnSuccess,
		onFailure:   kafkaCfg.OnFailure,
		TopicCfgMap: kafkaCfg.TopicConfigMap,
	}
	err := p.Init()
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *KafkaEndPoint) Init() error {
	client, err := sarama.NewClient(p.Addrs, &p.Cfg)
	if err != nil {
		return err
	}
	p.client = client

	var producer sarama.AsyncProducer
	producer, err = sarama.NewAsyncProducerFromClient(p.client)
	if err != nil {
		return err
	}
	p.producer = producer
	if p.Cfg.Producer.Return.Successes && p.onSuccess != nil {
		go func() {
			for msg := range p.producer.Successes() {
				p.onSuccess(msg)
			}
		}()
	}
	if p.Cfg.Producer.Return.Errors && p.onFailure != nil {
		go func() {
			for errMsg := range p.producer.Errors() {
				p.onFailure(errMsg)
			}
		}()
	}

	return nil
}

func (p *KafkaEndPoint) Ping() error {
	return p.client.RefreshMetadata()
}

func (p *KafkaEndPoint) Send(data *handler.BinlogEvent) error {
	topic, partition := p.prepare(data.Table)
	bMsg, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return p.emit(topic, partition, bMsg)
}

func (p *KafkaEndPoint) emit(topic string, partition int32, msgData []byte) error {
	if p.producer == nil {
		return errors.New("please use Init before ProduceMsg")
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Value:     sarama.StringEncoder(msgData),
	}

	p.producer.Input() <- msg
	return nil
}

func (p *KafkaEndPoint) prepare(tableName string) (topic string, partition int32) {
	topicCfg := p.TopicCfgMap[tableName]
	topic = topicCfg.Topic
	hashValue := p.getHashValue()
	partition = util.GetPartition(hashValue, topicCfg.Shard)
	return
}

func (p *KafkaEndPoint) getHashValue() uint64 {
	return 0
}
func (p *KafkaEndPoint) Close() (err error) {
	if p.producer != nil {
		err = p.producer.Close()
	}
	if p.client != nil {
		err = p.client.Close()
	}
	return err
}
