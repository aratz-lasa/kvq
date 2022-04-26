package internal

import (
	"fmt"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

type MetaManager struct {
	io    QueueIO
	queue string

	data Metadata
}

func NewMetaManager(io QueueIO, queue string) (*MetaManager, error) {
	return &MetaManager{queue: queue, data: Metadata{}, io: io}, nil
}

func (meta *MetaManager) Run() {
	msgs, err := meta.io.Recv(meta.queue, false)
	if err != nil {
		logrus.Error(fmt.Sprintf("[meta] error %v", err.Error()))
		return
	}
	logrus.Info("[meta] running")

	for msg := range msgs {
		go meta.io.Send(msg.ReplyTo, "", meta.data, true)
	}
	logrus.Info("[meta] stopped")

}

func (meta *MetaManager) addScale(amount int64) {
	atomic.AddInt64(&meta.data.Scale, amount)
	logrus.Debug(fmt.Sprintf("[meta] new scale: %v", meta.data.Scale))
}
