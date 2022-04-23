package endpoints

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/ml444/gbinlog/handler"
)
type KafkaCfg sarama.Config
type ElasticsearchConfig elasticsearch.Config
type EsBulkIdxCfg esutil.BulkIndexerConfig

type Config struct {
	EndpointType  int
	Kafka         KafkaConfig
	Elasticsearch EsConfig
}

type KafkaConfig struct {
	Addrs          []string
	Cfg            KafkaCfg
	OnSuccess      func(msg *sarama.ProducerMessage)
	OnFailure      func(err *sarama.ProducerError)
	TopicConfigMap map[string]TopicCfg `json:"topic_config_map"`
}

type TopicCfg struct {
	Topic string `json:"topic"`
	Shard int    `json:"shard_count"`
}

type EsConfig struct {
	BulkIndexerCfg EsBulkIdxCfg
	ElasticsearchCfg ElasticsearchConfig

	OnSuccess func(context.Context, esutil.BulkIndexerItem, esutil.BulkIndexerResponseItem)
	OnFailure func(context.Context, esutil.BulkIndexerItem, esutil.BulkIndexerResponseItem, error)
	Hook func(data *handler.BinlogEvent) (item BulkItem, err error)
}

