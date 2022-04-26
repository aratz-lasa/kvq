package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/tkanos/gonfig"
	"github.com/urfave/cli/v2"
)

type Configuration struct {
	USER_QUEUES   []string
	WORKER_QUEUES []string
	META_QUEUE    string
	RABBITMQ      string
}

var (
	conf *Configuration
	conn *amqp.Connection
)

func Run() {
	run(&cli.App{
		Name:                 "Key-value store",
		EnableBashCompletion: true,
		Flags:                flags,
		Commands:             commands,
		Before:               setup(),
		After:                teardown(),
	})
}

var flags = []cli.Flag{
	&cli.StringFlag{
		Name:    "configuration",
		Aliases: []string{"c"},
		Usage:   "configuration file",
		Value:   "default_config.json",
		EnvVars: []string{"CONFIG"},
	},
	&cli.StringFlag{
		Name:    "loglvl",
		Usage:   "set logging `level` to trace, debug, info, warn, error or fatal",
		Value:   "info",
		EnvVars: []string{"LOGLVL"},
	},
}

var commands = []*cli.Command{
	Client(),
	Server(),
}

func run(app *cli.App) {
	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func setup() cli.BeforeFunc {
	return func(c *cli.Context) (err error) {
		level, err := parseLogLevel(c.String("loglvl"))
		if err != nil {
			return err
		}
		logrus.SetLevel(level)

		conf = &Configuration{}
		if err = gonfig.GetConf(c.String("configuration"), conf); err != nil {
			return err
		}

		conn, err = amqp.Dial(conf.RABBITMQ)
		return
	}
}

func teardown() cli.AfterFunc {
	return func(c *cli.Context) (err error) {
		if conn != nil {
			conn.Close()
		}
		return
	}
}

func parseLogLevel(level string) (logrus.Level, error) {
	switch level {
	case "debug":
		return logrus.DebugLevel, nil
	case "info":
		return logrus.InfoLevel, nil
	case "warn":
		return logrus.WarnLevel, nil
	case "error":
		return logrus.ErrorLevel, nil
	case "fatal":
		return logrus.FatalLevel, nil
	default:
		return 0, errors.New(fmt.Sprintf("invalid logging level %v", level))
	}
}
