package storage

type RedisPosStorage struct {
	uri string
}

func (s *RedisPosStorage) Get() (*Position, error) {
	panic("implement me")
}

func (s *RedisPosStorage) Rewrite(pos *Position) error {
	panic("implement me")
}

func (s *RedisPosStorage) Close() {
	panic("implement me")
}

func NewRedisPosStorage(uri string) (*RedisPosStorage, error) {
	c := &RedisPosStorage{uri: uri}
	return c, nil
}
