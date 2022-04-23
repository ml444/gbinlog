package endpoints

import (
	"context"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/ml444/gbinlog/handler"
)

type BulkItem esutil.BulkIndexerItem
type EsEndpoint struct {
	Cli         *elasticsearch.Client
	bulkIndexer esutil.BulkIndexer

	OnSuccess func(context.Context, esutil.BulkIndexerItem, esutil.BulkIndexerResponseItem)
	OnFailure func(context.Context, esutil.BulkIndexerItem, esutil.BulkIndexerResponseItem, error)

	Hook func(data *handler.BinlogEvent) (item BulkItem, err error)
}

func NewEsEndpoint(cfg EsConfig) (*EsEndpoint, error) {
	esCli, err := elasticsearch.NewClient(elasticsearch.Config(cfg.ElasticsearchCfg))
	if err != nil {
		return nil, err
	}
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig(cfg.BulkIndexerCfg))
	if err != nil {
		return nil, err
	}
	e:= &EsEndpoint{
		Cli:         esCli,
		bulkIndexer: bi,
		OnSuccess:   cfg.OnSuccess,
		OnFailure:   cfg.OnFailure,
		Hook: cfg.Hook,
	}
	return e, nil
}

func (e *EsEndpoint) Send(data *handler.BinlogEvent) error {
	item, err := e.Hook(data)
	if err != nil {
		return err
	}
	return e.Add(item)
}

func (e *EsEndpoint) Add(item BulkItem) error {
	if item.OnSuccess == nil {
		item.OnSuccess = e.OnSuccess
	}
	if item.OnFailure == nil {
		item.OnFailure = e.OnFailure
	}
	return e.bulkIndexer.Add(context.Background(), esutil.BulkIndexerItem(item))
}
func (e *EsEndpoint) Close() (err error) {
	return e.bulkIndexer.Close(context.Background())
}





