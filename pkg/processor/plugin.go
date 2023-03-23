package processor

import (
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type Plugin interface {
	Init() error
	Process(fileName string, cfg interface{}) error
}

type MockPlugin struct {
	log log.Logger
}

func (m *MockPlugin) Init() error {
	level.Debug(m.log).Log("msg", "mock init")
	return nil
}

func (m *MockPlugin) Process(filename string, cfg interface{}) error {
	level.Debug(m.log).Log("msg", "mock start process")
	time.Sleep(5 * time.Second)
	level.Debug(m.log).Log("msg", "mock stop process")
	return nil
}
