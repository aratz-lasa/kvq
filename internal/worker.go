package internal

import (
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Worker struct {
	io    QueueIO
	queue string

	storage storageNode

	meta     *MetaManager
	metadata Metadata
}

func NewWorker(io QueueIO, queue string, meta *MetaManager) (*Worker, error) {
	storage, err := newMapNode(io)
	if err != nil {
		return nil, err
	}

	return &Worker{
		queue:    queue,
		io:       io,
		storage:  storage,
		meta:     meta,
		metadata: Metadata{},
	}, nil

}

func (w *Worker) Run() {
	// init metadata
	w.metadata.Scale = 1
	w.meta.addScale(w.metadata.Scale)

	// init queues
	msgs, err := w.io.Recv(w.queue, false)
	if err != nil {
		return
	}

	// initialize storage
	go w.storage.run()
	defer w.storage.close()

	logrus.Info(fmt.Sprintf("[worker %v] running", w.queue))

	// start handling requests
	for msg := range msgs {
		w.handleMsg(msg)
	}
	logrus.Info(fmt.Sprintf("[worker %v] stopped", w.queue))

}

func (w *Worker) handleMsg(msg amqp.Delivery) {

	req, err := decodeRequest(msg)
	if err != nil {
		logrus.Error(fmt.Sprintf("[storage %v] %v", w.queue, err.Error()))
		return
	}

	logrus.Debug(fmt.Sprintf("[storage %v] request: %+v", w.queue, req))

	switch req.OP {
	case SCALE:
		w.storage = w.storage.scale()
		w.meta.addScale(w.metadata.Scale)
		w.metadata.Scale = w.metadata.Scale * 2
	case ADD, GET, GETALL, DEL:
		w.storage.request(req)
	default:
		logrus.Error(fmt.Sprintf("invalid operation %v", req.OP))
		return
	}
}

func decodeRequest(msg amqp.Delivery) (Request, error) {
	req := Request{}
	err := json.Unmarshal(msg.Body, &req)
	req.ReplyTo = msg.ReplyTo
	return req, err
}
