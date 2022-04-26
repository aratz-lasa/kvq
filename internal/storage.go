package internal

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/segmentio/fasthash/fnv1a"
	"github.com/sirupsen/logrus"
)

type storageNode interface {
	run()
	close()

	request(req Request)
	scale() storageNode
}

type routerNode struct {
	name  string
	paths [2]storageNode
}

func (router *routerNode) run() {}

func (router *routerNode) close() {
	for _, path := range router.paths {
		path.close()
	}
}

func (router *routerNode) scale() storageNode {
	for i, path := range router.paths {
		router.paths[i] = path.scale()
	}
	return router
}

func (router *routerNode) request(req Request) {
	switch req.OP {
	case ADD, GET, DEL:
		index := fnv1a.HashString32(router.name+req.Kv.Key) % 2
		router.paths[index].request(req)
	case GETALL:
		for _, path := range router.paths {
			path.request(req)
		}
	}
}

type scaleRequest struct {
	response chan storageNode
}

type mapNode struct {
	storage map[string][]Value
	io      QueueIO

	adds    chan Request
	gets    chan Request
	getalls chan Request
	dels    chan Request

	scales chan scaleRequest

	stop chan struct{}
}

func newMapNode(io QueueIO) (*mapNode, error) {

	return &mapNode{
		storage: make(map[string][]Value),
		io:      io,

		adds:    make(chan Request),
		gets:    make(chan Request),
		getalls: make(chan Request),
		dels:    make(chan Request),

		scales: make(chan scaleRequest),

		stop: make(chan struct{}),
	}, nil
}

func (node *mapNode) run() {
	for {
		select {
		case <-node.stop:
			return
		case req := <-node.adds:
			node.handleAdd(req)
		case req := <-node.gets:
			node.handleGet(req)
		case req := <-node.getalls:
			node.handleGetAll(req)
		case req := <-node.dels:
			node.handleDel(req)
		case req := <-node.scales:
			node.handleScale(req)
		}
	}
}

func (node *mapNode) close() {
	close(node.stop)
}

func (node *mapNode) scale() storageNode {
	response := make(chan storageNode)
	node.scales <- scaleRequest{response: response}

	scaleNode, ok := <-response
	if ok {
		return scaleNode
	} else {
		return node
	}
}

func (node *mapNode) request(req Request) {
	switch req.OP {
	case ADD:
		node.adds <- req
	case GET:
		node.gets <- req
	case GETALL:
		node.getalls <- req
	case DEL:
		node.dels <- req
	default:
	}
}

func (node *mapNode) handleAdd(request Request) {
	if _, ok := node.storage[request.Kv.Key]; !ok {
		node.storage[request.Kv.Key] = []Value{request.Kv.Value}
		return
	}
	node.storage[request.Kv.Key] = append(node.storage[request.Kv.Key], request.Kv.Value)
	logrus.Debug(fmt.Sprintf("ADD Key=%v, Value=%v", request.Kv.Key, request.Kv.Value.Value))
}

func (node *mapNode) handleGet(request Request) {
	if values, ok := node.storage[request.Kv.Key]; ok {
		go node.io.Send(request.ReplyTo, "", values[:], true)
	} else {
		go node.io.Send(request.ReplyTo, "", []Value{}, true)
	}
	logrus.Debug(fmt.Sprintf("GET Key=%v", request.Kv.Key))
}

func (node *mapNode) handleGetAll(request Request) {
	allValues := make([]Value, 0, len(node.storage))

	for _, values := range node.storage {
		allValues = append(allValues, values...)
	}

	go node.io.Send(request.ReplyTo, "", allValues, true)
}

func (node *mapNode) handleDel(request Request) {
	delete(node.storage, request.Kv.Key)
}

func (node *mapNode) handleScale(req scaleRequest) {
	replica, err := newMapNode(node.io)
	if err != nil {
		logrus.Error(err.Error())
		close(req.response)
	}

	// partition storage data
	name := strconv.Itoa(rand.Intn(100))
	for key, values := range node.storage {
		if index := fnv1a.HashString32(name+key) % 2; index == 1 {
			replica.storage[key] = values
			delete(node.storage, key)
		}
	}
	// launch replica
	go replica.run()

	// create router to replicas
	req.response <- &routerNode{name: name, paths: [2]storageNode{node, replica}}
}
