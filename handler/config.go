package handler

import (
	"github.com/ml444/gbinlog/endpoints"
	"github.com/ml444/gbinlog/storage"
)

type Config struct {
	PosStorage     storage.PosStorageConfig
	SerializerType int
	Endpoint       endpoints.Config

	includeDbNameMap map[string]struct{}
	includeTablesMap map[string]struct{}
}
