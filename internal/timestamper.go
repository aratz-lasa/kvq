package internal

import (
	"fmt"
	"time"

	"github.com/segmentio/fasthash/fnv1a"
	"github.com/sirupsen/logrus"
)

type Timestamper struct {
	io    QueueIO
	queue string

	workers []string
}

func NewTimestamper(io QueueIO, queue string, workers []string) (*Timestamper, error) {
	return &Timestamper{queue: queue, workers: workers, io: io}, nil
}

func (t *Timestamper) Run() {
	msgs, err := t.io.Recv(t.queue, false)
	if err != nil {
		logrus.Error(fmt.Sprintf("[timestamper %v] error %v", t.queue, err.Error()))
		return
	}

	logrus.Info(fmt.Sprintf("[timestamper %v] running", t.queue))

	for msg := range msgs {
		req, err := decodeRequest(msg)
		if err != nil {
			logrus.Error(err.Error())
			continue
		}
		req.Kv.Value.Timestamp = time.Now().UnixMicro()
		if req.OP == GETALL {
			for _, worker := range t.workers {
				go t.io.Send(worker, msg.ReplyTo, req, false)
			}
		} else {
			index := fnv1a.HashString32(req.Kv.Key) % uint32(len(t.workers))
			go t.io.Send(t.workers[index], msg.ReplyTo, req, false)
		}
	}
	logrus.Info(fmt.Sprintf("[timestamper %v] stopped", t.queue))
}
