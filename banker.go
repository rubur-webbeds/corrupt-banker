package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/streadway/amqp"
)

var amount int
var THIEF_MINIMUM int = 20

type Transaction struct {
	Action   string
	Amount   int
	ClientId string
}

type TransactionResult struct {
	Action   string
	Amount   int
	ClientId string
	Ok       bool
	Message  string
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func execTransaction(t Transaction) TransactionResult {
	result := TransactionResult{
		Action:   t.Action,
		ClientId: t.ClientId,
	}

	fmt.Printf("Client %s: ", t.ClientId)
	switch t.Action {
	case "add":
		fmt.Printf("Adding %d\n", t.Amount)
		result.Ok = addAmount(t.Amount)
	case "subs":
		fmt.Printf("Substracting %d\n", t.Amount)
		result.Ok = subsAmount(t.Amount)
	default:
		log.Printf("Action not defined: %s\n", t.Action)
	}
	if !result.Ok {
		result.Message = fmt.Sprintf("Cannot execute transaction %s %d", t.Action, t.Amount)
	}

	result.Amount = amount
	fmt.Printf("---> AMOUNT: %d\n", amount)

	return result
}

func addAmount(a int) bool {
	amount += a
	return true
}

func subsAmount(a int) bool {
	if amount-a < 0 {
		fmt.Printf("Can't substract %d\n", a)
		return false
	} else {
		amount -= a
		return true
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// TRANSACTIONS QUEUE
	transactions_q, err := ch.QueueDeclare(
		"transactions", // name
		false,          // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	failOnError(err, "Failed to declare a queue: transactions")

	// RESULTS QUEUE
	results_q, err := ch.QueueDeclare(
		"results", // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue: results")

	// THIEF QUEUE
	thief_q, err := ch.QueueDeclare(
		"thief", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	_ = thief_q
	failOnError(err, "Failed to declare a queue: transactions")

	msgs, err := ch.Consume(
		transactions_q.Name, // queue
		"",                  // consumer
		true,                // auto-ack
		false,               // exclusive
		false,               // no-local
		false,               // no-wait
		nil,                 // args
	)
	failOnError(err, "Failed to register a consumer")

	fmt.Println(" [*] Waiting for client transactions.\nYou can trust me :). To exit press CTRL+C")
	fmt.Printf("---> AMOUNT: %d\n", amount)

	forever := make(chan bool)

	go func() {

		// read transactions
		for d := range msgs {

			// notify the thief
			if amount >= THIEF_MINIMUM {
				err = ch.Publish(
					"",           // exchange
					thief_q.Name, // routing key
					false,        // mandatory
					false,        // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(strconv.Itoa(amount)),
					})
				failOnError(err, "Failed to publish a message")

				amount -= THIEF_MINIMUM

				fmt.Println("Ohh all the money disappeared! :O.\nBetter run.\nBye bye from banker ;)")
				os.Exit(0)
			}

			var t Transaction
			json.Unmarshal(d.Body, &t)

			result := execTransaction(t)

			bytes, err := json.Marshal(result)
			failOnError(err, "Failed to encode")

			// send the transaction result to the client
			err = ch.Publish(
				"",             // exchange
				results_q.Name, // routing key
				false,          // mandatory
				false,          // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(bytes),
				})
			failOnError(err, "Failed to publish a message")

		}

		forever <- false
	}()

	<-forever
}
