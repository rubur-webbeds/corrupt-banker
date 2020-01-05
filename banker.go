package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

var amount int

type Transaction struct {
	Action   string
	Amount   int
	ClientId string
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func execTransaction(t Transaction) {
	fmt.Printf("Client %s: ", t.ClientId)
	switch t.Action {
	case "add":
		fmt.Printf("Adding %d\n", t.Amount)
		addAmount(t.Amount)
	case "subs":
		fmt.Printf("Substracting %d\n", t.Amount)
		subsAmount(t.Amount)
	default:
		log.Printf("Action not defined: %s\n", t.Action)
	}
	fmt.Printf("AMOUNT: %d\n", amount)
}

func addAmount(a int) {
	amount += a
}

func subsAmount(a int) {
	if amount-a < 0 {
		fmt.Printf("Can't substract %d\n", a)
	} else {
		amount -= a
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

	/* // RESULTS QUEUE
	results_q, err := ch.QueueDeclare(
		"results", // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue: results") */

	err = ch.ExchangeDeclare(
		"results", // name
		"direct",  // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare an exchange")

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

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	fmt.Printf("AMOUNT: %d\n", amount)

	for d := range msgs {
		var t Transaction
		json.Unmarshal(d.Body, &t)
		execTransaction(t)

		body := "Bye World!"
		err = ch.Publish(
			"results",  // exchange
			t.ClientId, // routing key
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		//fmt.Printf(" [x] Sent %s", body)
		failOnError(err, "Failed to publish a message")
	}
}
