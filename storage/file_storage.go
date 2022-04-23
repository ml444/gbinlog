package storage

import (
	"encoding/json"
	"fmt"
	"github.com/ml444/glog"
	"io"
	"os"
	"path"
)


type FilePosStorage struct {
	filepath string
	file     *os.File
}


func (s *FilePosStorage) open() error {
	fileDir := path.Dir(s.filepath)
	// TODO Umask
	err := os.MkdirAll(fileDir, 0775)
	if err != nil {
		fmt.Printf("make dir fail, path %s, err %s", err)
		os.Exit(1)
	}
	file, err := os.OpenFile(s.filepath, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		log.Errorf("Err: %v\n", err)
		return err
	}
	s.file = file
	return nil
}

func (s *FilePosStorage) Get() (*Position, error) {
	if s.file == nil {
		err := s.open()
		if err != nil {
			log.Errorf("Err: %v\n", err)
			return nil, err
		}
	}
	var b = make([]byte, 64)
	_, err := s.file.Seek(0, io.SeekStart)
	if err != nil {
		log.Errorf("Err:%s \n", err)
		return nil, err
	}
	n, err := s.file.Read(b)
	if err != nil && err != io.EOF {
		log.Errorf("Err: %v\n", err)
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	if n > 64 {
		log.Error("Err: b > 64.")
	}
	//index := bytes.IndexByte(b, 0)
	data := b[:n]
	var pos = &Position{}
	err = json.Unmarshal(data, pos)
	if err != nil {
		log.Errorf("Err: %v %s \n", err, string(b))
		return nil, err
	}
	return pos, nil
}

func (s *FilePosStorage) Rewrite(pos *Position) error {
	if s.file == nil {
		err := s.open()
		if err != nil {
			log.Errorf("Err: %v\n", err)
			return err
		}
	}
	b, err := json.Marshal(pos)
	if err != nil {
		log.Errorf("Err: %v\n", err)
		return err
	}
	_ = s.file.Truncate(0)
	//n, _ := s.file.Seek(0, io.SeekEnd)
	_, err = s.file.WriteAt(b, 0)
	if err != nil {
		log.Errorf("Err: %v\n", err)
		return err
	}
	return nil
}

func (s *FilePosStorage) Close() {
	_ = s.file.Close()
}

func NewFilePosStorage(filepath string) (*FilePosStorage, error) {
	c := &FilePosStorage{filepath: filepath}
	err := c.open()
	if err != nil {
		log.Errorf("Err: %v\n", err)
		return nil, err
	}
	return c, nil
}
