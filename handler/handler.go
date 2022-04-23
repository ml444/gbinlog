package handler

import (
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/ml444/gbinlog/endpoints"
	"github.com/ml444/gbinlog/serializer"
	"github.com/ml444/gbinlog/storage"
)

type EventHandler struct {
	canal.DummyEventHandler
	posStorage storage.IPosStorage
	serializer serializer.ISerializer
	endpoint   endpoints.IEndpoint

	dbNameMap map[string]struct{}
	tablesMap map[string]struct{}
}

func NewEventHandler(handlerCfg Config) *EventHandler {
	ps, err := storage.NewPosStorage(handlerCfg.PosStorage)
	if err != nil {
		panic(err)
	}
	s := serializer.NewSerializer(handlerCfg.SerializerType)
	endpoint, err := endpoints.NewEndPoint(handlerCfg.Endpoint)
	if err != nil {
		panic(err)
	}
	dbNameMap := handlerCfg.includeDbNameMap
	if dbNameMap == nil {
		dbNameMap = map[string]struct{}{}
	}
	tablesMap := handlerCfg.includeTablesMap
	if tablesMap == nil {
		tablesMap = map[string]struct{}{}
	}
	return &EventHandler{
		posStorage: ps,
		serializer: s,
		dbNameMap:  dbNameMap,
		tablesMap:  tablesMap,
		endpoint: endpoint,
	}
}

func (h *EventHandler) OnRow(e *canal.RowsEvent) (err error) {
	if h.isSkip(e) {
		return nil
	}
	dbName := e.Table.Schema
	tableName := e.Table.Name
	action := e.Action

	var timestamp uint32
	if e.Header != nil {
		timestamp = e.Header.Timestamp
	}
	bgEvent := &BinlogEvent{
		Schema:    dbName,
		Table:     tableName,
		Action:    action,
		Timestamp: timestamp,
		//Data:      nil,
	}

	switch action {
	case InsertAction, DeleteAction:
		for _, row := range e.Rows {
			bgEvent.Data = [][]interface{}{row}
			if h.isExcludeData() {
				return nil
			}
		}
	case UpdateAction:
		for i := 0; i < len(e.Rows); i += 2 {
			beforeRow := e.Rows[i]
			row := e.Rows[i+1]
			bgEvent.Data = [][]interface{}{beforeRow, row}
			if h.isExcludeData() {
				return nil
			}
		}
	}
	return h.sendData(bgEvent)
}

func (h *EventHandler) OnRotate(ev *replication.RotateEvent) error {
	//log.Infof("===>Save pos: %s %d \n", ev.NextLogName, ev.Position)
	err := h.posStorage.Rewrite(&storage.Position{
		Name: string(ev.NextLogName),
		Pos:  uint32(ev.Position),
	})
	if err != nil {
		//log.Errorf("Err: %v\n", err)
		return err
	}
	return nil
}
func (h *EventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	if force {
		//log.Infof("===>Save pos: %s %d \n", pos.Name, pos.Pos)
		err := h.posStorage.Rewrite(&storage.Position{
			Name: pos.Name,
			Pos:  pos.Pos,
		})
		if err != nil {
			//log.Errorf("Err: %v\n", err)
			return err
		}
	}

	return nil
}
func (h *EventHandler) String() string {
	return "EventHandler"
}

func (h *EventHandler) sendData(data *BinlogEvent) error {
	return h.endpoint.Send(data)
}

func (h *EventHandler) isSkip(e *canal.RowsEvent) bool {
	if e == nil || e.Table == nil {
		return true
	}
	if _, ok := h.dbNameMap[e.Table.Schema]; !ok {
		return true
	}
	if _, ok := h.tablesMap[e.Table.Name]; !ok {
		return true
	}
	return false
}
func (h *EventHandler) isExcludeData() bool {
	return false
}
