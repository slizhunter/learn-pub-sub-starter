package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange string, routingKey string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange string, routingKey string, val T) error {
	body, err := encodeGob(val)
	if err != nil {
		return fmt.Errorf("failed to encode Gob: %w", err)
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

func PublishGameLog(ch *amqp.Channel, log routing.GameLog) error {
	exchange := routing.ExchangePerilTopic
	routingKey := fmt.Sprintf("%s.%s", routing.GameLogSlug, log.Username)
	err := PublishGob(ch, exchange, routingKey, log)
	if err != nil {
		return fmt.Errorf("failed to publish game log: %w", err)
	}
	return nil
}

func encodeGob[T any](val T) ([]byte, error) {
	var buf bytes.Buffer
	writer := gob.NewEncoder(&buf)
	err := writer.Encode(val)
	if err != nil {
		return nil, fmt.Errorf("failed to encode Gob: %w", err)
	}
	return buf.Bytes(), nil
}
