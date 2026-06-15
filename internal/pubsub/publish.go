package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

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
