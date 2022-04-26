package cmd

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/aratz-lasa/kvq/internal"
	"github.com/segmentio/fasthash/fnv1a"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/thanhpk/randstr"
	"github.com/urfave/cli/v2"
)

func Client() *cli.Command {
	return &cli.Command{
		Name:        "client",
		Usage:       "cli client for kv",
		Flags:       clientFlags,
		Subcommands: clientSubcommands,
	}
}

var clientFlags = []cli.Flag{
	&cli.StringFlag{
		Name:    "user",
		Aliases: []string{"u"},
		Usage:   "user id",
		Value:   "default",
		EnvVars: []string{"USER"},
	},
}

var clientSubcommands = []*cli.Command{
	Add(),
	Get(),
	GetAll(),
	Del(),
}

func Add() *cli.Command {
	return &cli.Command{
		Name:   "add",
		Usage:  "Add key-value pair",
		Action: add(),
	}
}

func add() cli.ActionFunc {
	return func(c *cli.Context) error {
		key, value := c.Args().Get(0), c.Args().Get(1)

		req := internal.Request{
			OP: internal.ADD,
			Kv: internal.NewKeyValue(key, value),
		}

		index := fnv1a.HashString32(c.String("user")) % uint32(len(conf.USER_QUEUES))
		queue := conf.USER_QUEUES[index]

		io := &internal.RabbitIO{Conn: conn}
		io.Send(queue, "", req, false)

		return nil
	}
}

func Get() *cli.Command {
	return &cli.Command{
		Name:   "get",
		Usage:  "Get value from key",
		Action: get(),
	}
}

func get() cli.ActionFunc {
	return func(c *cli.Context) error {
		key := c.Args().Get(0)

		// send request
		req := internal.Request{
			OP: internal.GET,
			Kv: internal.NewKey(key),
		}
		index := fnv1a.HashString32(c.String("user")) % uint32(len(conf.USER_QUEUES))
		queue := conf.USER_QUEUES[index]
		replyTo := randstr.Hex(10)

		io := &internal.RabbitIO{Conn: conn}
		io.Send(queue, replyTo, req, false)

		// recieve and decode response
		values, err := getValues(replyTo, 1)
		if err != nil {
			return err
		}

		// print response
		sort.Sort(internal.ByTimestamp(values))
		for _, value := range values {
			fmt.Printf("%v ", value.Value)
		}
		fmt.Printf("\n")

		return nil
	}
}

func GetAll() *cli.Command {
	return &cli.Command{
		Name:   "getall",
		Usage:  "Get all values from kv",
		Action: getAll(),
	}
}

func getAll() cli.ActionFunc {
	return func(c *cli.Context) error {
		req := internal.Request{
			OP: internal.GETALL,
		}
		replyTo := randstr.Hex(10)

		meta, err := getMetadata()
		if err != nil {
			return err
		}

		index := fnv1a.HashString32(c.String("user")) % uint32(len(conf.USER_QUEUES))
		queue := conf.USER_QUEUES[index]

		io := &internal.RabbitIO{Conn: conn}
		if err := io.Send(queue, replyTo, req, false); err != nil {
			return err
		}

		values, err := getValues(replyTo, int(meta.Scale))
		if err != nil {
			return err
		}

		sort.Sort(internal.ByTimestamp(values))
		for _, value := range values {
			fmt.Printf("%v ", value.Value)
		}
		fmt.Printf("\n")

		return nil
	}
}

func Del() *cli.Command {
	return &cli.Command{
		Name:   "del",
		Usage:  "Del key-value pair",
		Action: del(),
	}
}

func del() cli.ActionFunc {
	return func(c *cli.Context) error {
		key := c.Args().Get(0)

		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()

		req := internal.Request{
			OP: internal.DEL,
			Kv: internal.NewKey(key),
		}

		index := fnv1a.HashString32(c.String("user")) % uint32(len(conf.USER_QUEUES))
		queue := conf.USER_QUEUES[index]

		io := &internal.RabbitIO{Conn: conn}
		io.Send(queue, "", req, false)

		logrus.Info(fmt.Sprintf("[queue %v] published DEL Key=%v", queue, key))

		return nil
	}
}

func getValues(queue string, amount int) ([]internal.Value, error) {
	msgs, err := recvN(queue, amount, true)
	if err != nil {
		return nil, err
	}

	return decodeValues(msgs)
}

func getMetadata() (internal.Metadata, error) {
	replyTo := randstr.Hex(10)

	io := &internal.RabbitIO{Conn: conn}
	if err := io.Send(conf.META_QUEUE, replyTo, []int{}, false); err != nil {
		return internal.Metadata{}, err
	}

	msgs, err := recvN(replyTo, 1, true)
	if err != nil {
		return internal.Metadata{}, err
	}

	return decodeMetadata(msgs[0])
}

func recvN(queue string, n int, delete bool) ([]amqp.Delivery, error) {
	msgs := make([]amqp.Delivery, 0, 1)

	io := &internal.RabbitIO{Conn: conn}
	msg, err := io.Recv(queue, delete)
	if err != nil {
		return nil, err
	}

	for i := 0; i < n; i++ {
		delivery := <-msg
		msgs = append(msgs, delivery)
	}

	return msgs, nil
}

func decodeValues(msgs []amqp.Delivery) ([]internal.Value, error) {
	allValues := make([]internal.Value, 0, 1)
	values := make([]internal.Value, 0, 1)

	for _, msg := range msgs {
		if err := json.Unmarshal(msg.Body, &values); err != nil {
			return nil, err
		}
		allValues = append(allValues, values...)
		values = values[:0]
	}

	return allValues, nil
}

func decodeMetadata(msg amqp.Delivery) (meta internal.Metadata, err error) {
	err = json.Unmarshal(msg.Body, &meta)
	return
}
