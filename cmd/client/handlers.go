package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(msg routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(msg)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(mv gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		mvOutcome := gs.HandleMove(mv)
		switch mvOutcome {
		case gamelogic.MoveOutcomeSamePlayer:
			fmt.Println("This move is from you, so it does not affect your game state.")
			return pubsub.Ack
		case gamelogic.MoveOutcomeSafe:
			fmt.Println("This move does not put you at war, but keep an eye on it!")
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			fmt.Println("This move puts you at war! Watch out!")
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.Player.Username),
				gamelogic.RecognitionOfWar{
					Attacker: mv.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("Failed to publish war declaration: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("Unknown move outcome, discarding.")
			return pubsub.NackDiscard
		}
	}
}

func handlerWarMessages(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(msg gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		var outcomeMsg string
		var ackType pubsub.AckType
		outcome, winner, loser := gs.HandleWar(msg)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			fmt.Println("You are not in a war, but received a war message. Ignoring.")
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			fmt.Println("You have no more units left.")
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			outcomeMsg = fmt.Sprintf("%s won a war against %s", winner, loser)
			ackType = pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			outcomeMsg = fmt.Sprintf("%s won a war against %s", winner, loser)
			ackType = pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			outcomeMsg = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			ackType = pubsub.Ack
		default:
			fmt.Printf("Outcome not recognized: %v\n", outcome)
			return pubsub.NackDiscard
		}
		err := pubsub.PublishGameLog(ch, routing.GameLog{
			CurrentTime: time.Now(),
			Username:    msg.Attacker.Username,
			Message:     outcomeMsg,
		})
		if err != nil {
			fmt.Printf("Failed to publish game log: %v\n", err)
			return pubsub.NackRequeue
		}
		return ackType
	}
}
