package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connStr := "amqp://guest:guest@localhost:5672/"
	// Connect to RabbitMQ server using the connection string
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Open a channel to RabbitMQ
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	fmt.Println("Connected to RabbitMQ successfully!")

	// Get client username
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Failed to get client welcome: %v", err)
	}

	// Initialize game state for this client
	gs := gamelogic.NewGameState(username)

	// Subscribe to pause updates for this client
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.Transient,
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		},
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to pause updates: %v", err)
	}

	// Subscribe to move updates for this client
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		},
		handlerMove(gs, ch),
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to move updates: %v", err)
	}

	// Subscribe to war messages for this client
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		nil,
		handlerWarMessages(gs),
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to war messages: %v", err)
	}

	// Start the game loop to process user input
	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}
		cmd := inputs[0]
		switch cmd {
		case "spawn":
			err := gs.CommandSpawn(inputs)
			if err != nil {
				fmt.Printf("Error spawning unit: %v\n", err)
				continue
			}
		case "move":
			mv, err := gs.CommandMove(inputs)
			if err != nil {
				fmt.Printf("Error moving unit: %v\n", err)
				continue
			}
			// After a successful move command, publish the move to the server
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
				mv,
			)
			if err != nil {
				log.Printf("Failed to publish move: %v", err)
				continue
			}
			log.Printf("Published move to server.\n")
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			// Break out of loop
			return
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}
}
