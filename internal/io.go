package internal

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

type QueueIO interface {
	Send(queue string, replyTo string, data any, delete bool) error
	Recv(queue string, delete bool) (<-chan amqp.Delivery, error)
}

type RabbitIO struct {
	Conn *amqp.Connection
}

func (s *RabbitIO) Send(queue string, replyTo string, data any, delete bool) error {
	ch, err := s.Conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		queue,  // name
		false,  // durable
		delete, // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	if err != nil {
		return err
	}

	bdata, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			ReplyTo:     replyTo,
			Body:        bdata,
		})
}

func (s *RabbitIO) Recv(queue string, delete bool) (<-chan amqp.Delivery, error) {
	ch, err := s.Conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		queue,  // name
		false,  // durable
		delete, // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)

	if err != nil {
		return nil, err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, err
	}
	out := make(chan amqp.Delivery, 0)

	go func() {
		defer close(out)
		defer ch.Close()

		for msg := range msgs {
			out <- msg
		}
	}()
	return out, nil
}
