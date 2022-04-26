package cmd

import (
	"github.com/aratz-lasa/kvq/internal"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

func Server() *cli.Command {
	return &cli.Command{
		Name:        "server",
		Usage:       "cli server for kv",
		Flags:       serverFlags,
		Subcommands: serverSubcommands,
	}
}

var serverFlags = []cli.Flag{}

var serverSubcommands = []*cli.Command{
	Start(),
	Scale(),
}


func Start() *cli.Command {
	return &cli.Command{
		Name:   "start",
		Usage:  "start kv server",
		Action: start(),
	}
}

func start() cli.ActionFunc {
	return func(c *cli.Context) error {
		io := &internal.RabbitIO{Conn: conn}

		meta, err := internal.NewMetaManager(io, conf.META_QUEUE)
		if err != nil {
			return err
		}

		for _, queue := range conf.USER_QUEUES {
			ts, err := internal.NewTimestamper(io, queue, conf.WORKER_QUEUES)
			if err != nil {
				return err
			}
			go ts.Run()
		}

		go meta.Run()

		for _, queue := range conf.WORKER_QUEUES {
			worker, err := internal.NewWorker(io, queue, meta)
			if err != nil {
				return err
			}
			go worker.Run()
		}

		logrus.Info("Server running")

		<-c.Context.Done()

		logrus.Info("Server stopping")

		return c.Context.Err()
	}
}

func Scale() *cli.Command {
	return &cli.Command{
		Name:   "scale",
		Usage:  "scale kv server",
		Action: scale(),
	}
}

func scale() cli.ActionFunc {
	return func(c *cli.Context) error {
		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()

		req := internal.Request{
			OP: internal.SCALE,
		}

		io := &internal.RabbitIO{Conn: conn}
		for _, queue := range conf.USER_QUEUES {
			if err := io.Send(queue, "", req, false); err != nil {
				return err
			}
		}
		return nil
	}
}
