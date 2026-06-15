package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	routingKey string,
	queueType SimpleQueueType,
	args amqp.Table,
) (*amqp.Channel, amqp.Queue, error) {
	// Open a channel to RabbitMQ
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to open a channel: %w", err)
	}

	// Declare the queue
	newQueue, err := ch.QueueDeclare(
		queueName,              // name
		queueType == Durable,   // durable
		queueType == Transient, // autoDelete
		queueType == Transient, // exclusive
		false,                  // noWait
		args,                   // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to declare queue: %w", err)
	}
	// Bind the queue to the exchange
	err = ch.QueueBind(
		newQueue.Name, // queue name
		routingKey,    // routing key
		exchange,      // exchange
		false,         // noWait
		args,          // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to bind queue: %w", err)
	}

	return ch, newQueue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	routingKey string,
	queueType SimpleQueueType,
	args amqp.Table,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, routingKey, queueType, args)
	if err != nil {
		return fmt.Errorf("failed to declare and bind queue: %w", err)
	}

	msgCh, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		args,       // args
	)
	if err != nil {
		return fmt.Errorf("failed to register channel: %w", err)
	}

	go func() {
		for message := range msgCh {
			var msg T
			if err := json.Unmarshal(message.Body, &msg); err != nil {
				fmt.Printf("failed to unmarshal JSON: %v\n", err)
				continue
			}
			ack := handler(msg)
			switch ack {
			case Ack:
				message.Ack(false)
			case NackRequeue:
				message.Nack(false, true)
			case NackDiscard:
				message.Nack(false, false)
			}
		}
	}()

	return nil
}
