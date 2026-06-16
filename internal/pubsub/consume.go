package pubsub

import (
	"bytes"
	"encoding/gob"
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

// DeclareAndBind declares a queue with the given name and binds it to the specified exchange and routing key.
func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	routingKey string,
	queueType SimpleQueueType,
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
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		}, // args
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
		nil,           // args
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
	handler func(T) AckType,
) error {
	return subscribe(
		conn,
		exchange,
		queueName,
		routingKey,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			if err := json.Unmarshal(data, &target); err != nil {
				return target, fmt.Errorf("failed to unmarshal JSON: %w", err)
			}
			return target, nil
		},
	)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	routingKey string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(
		conn,
		exchange,
		queueName,
		routingKey,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&target); err != nil {
				return target, fmt.Errorf("failed to decode Gob: %w", err)
			}
			return target, nil
		},
	)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	routingKey string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, routingKey, queueType)
	if err != nil {
		return fmt.Errorf("failed to declare and bind queue: %w", err)
	}

	err = ch.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("failed to set prefetch: %w", err)
	}

	msgCh, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return fmt.Errorf("failed to register channel consumer: %w", err)
	}

	go func() {
		defer ch.Close()
		for message := range msgCh {
			target, err := unmarshaller(message.Body)
			if err != nil {
				fmt.Printf("failed to unmarshal message: %v\n", err)
				continue
			}
			switch handler(target) {
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
