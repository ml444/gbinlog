package gbinlog

import (
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/ml444/gbinlog/endpoints"
	"github.com/ml444/gbinlog/handler"
	"github.com/ml444/gbinlog/storage"
	"github.com/ml444/glog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var canalList []*canal.Canal

func Run(config Config) {
	var doneChan = make(chan bool, 1)
	var once sync.Once


	defer func() {
		close(doneChan)
	}()

	go func() {
		log.Info("===> Start signal monitoring")
		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-doneChan:
			log.Error("terminating: doneChan")
			once.Do(closeAll)
		case s := <-sigterm:
			log.Errorf("terminating: signal[%v]", s)
			once.Do(closeAll)
		}
	}()

	cfg := canal.NewDefaultConfig()
	cfg.Addr = config.Db.Addr
	cfg.User = config.Db.User
	cfg.Password = config.Db.Pwd
	cfg.Dump.ExecutionPath = ""
	//cfg.Dump.TableDB = "db1"
	//cfg.Dump.Tables = []string{"table1"}
	//cfg.Dump.Databases = s.Conf.DbNames


	posStorage, err := storage.NewPosStorage(config.Handler.PosStorage)
	if err != nil {
		log.Errorf("Err: %v\n", err)
		return
	}
	pos, err := posStorage.Get()
	if err != nil {
		log.Errorf("Err: %v\n", err)
		return
	}

	// Start canal
	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Errorf("err:%s", err)
		return
	}
	defer c.Close()

	c.SetEventHandler(handler.NewEventHandler(config.Handler))
	if pos == nil || pos.Name == "" {
		log.Error("err: pos == nil || pos.Name is null")
		err = c.Run()
	} else {
		log.Infof("====> get pos.name: %s, pos.position: %d", pos.Name, pos.Pos)
		err = c.RunFrom(mysql.Position{
			Name: pos.Name,
			Pos:  pos.Pos,
		})
	}

	if err != nil {
		log.Errorf("err:%s", err)
		return
	}

	time.Sleep(1 * time.Second)
}

func closeAll() {
	for _, c := range canalList {
		log.Warnf("Close Canal %v", c)
		c.Close()
	}
	if endpoints.DefaultEndpoint != nil {
		log.Info("===> close DefaultEndpoint")
		endpoints.DefaultEndpoint.Close()
	}
	//if endpoints.UserInScopeEndpoint != nil {
	//	log.Info("===> close UserInScopeEndpoint")
	//	endpoints.UserInScopeEndpoint.Close()
	//}

}
