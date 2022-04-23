package storage

import (
	"errors"
)

const (
	FileStorageType  = 1
	RedisStorageType = 2
)

type IPosStorage interface {
	Get() (*Position, error)
	Rewrite(pos *Position) error
	Close()
}

type Position struct {
	Name string `json:"name"`
	Pos  uint32 `json:"pos"`
}

type PosStorageConfig struct {
	StorageType int
	FileConfig  struct {
		Filepath string
	}
	RedisConfig struct {
		Uri string
	}
}

func NewPosStorage(storageCfg PosStorageConfig) (IPosStorage, error) {
	switch storageCfg.StorageType {
	case FileStorageType:
		filepath := storageCfg.FileConfig.Filepath
		if filepath == "" {
			return nil, errors.New("filepath of position isn't configured")
		}
		return NewFilePosStorage(filepath)
	case RedisStorageType:
		uri := storageCfg.RedisConfig.Uri
		if uri == "" {
			return nil, errors.New("redis.uri of position isn't configured")
		}
		return NewRedisPosStorage(uri)
	default:
		return nil, errors.New("storage type is error")
	}
}
