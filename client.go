package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/streadway/amqp"
)

var MAX_OPERATIONS int = 4
var MAX_AMOUNT int = 10

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

func getTransaction(number float64) string {
	if number <= 0.7 {
		return "add"
	}
	return "subs"
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

	msgs, err := ch.Consume(
		results_q.Name, // queue
		"",             // consumer
		false,          // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)
	failOnError(err, "Failed to register a consumer")

	// create the random number generator
	seed := rand.NewSource(time.Now().UnixNano())
	random := rand.New(seed)

	n_trans := 1 + random.Intn(MAX_OPERATIONS)
	fmt.Printf("\nI'm the client: %s and I'll do %d transactions!\n", os.Args[1], n_trans)

	for i := 0; i < n_trans; i++ {
		trans := getTransaction(random.Float64())
		amount := 1 + random.Intn(MAX_AMOUNT)
		fmt.Printf("Client %s -> Transaction %d: %s %d\n", os.Args[1], i+1, trans, amount)

		bytes, err := json.Marshal(Transaction{Action: trans, Amount: amount, ClientId: os.Args[1]})
		failOnError(err, "Failed to encode")

		err = ch.Publish(
			"",                  // exchange
			transactions_q.Name, // routing key
			false,               // mandatory
			false,               // immediate
			amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: os.Args[1],
				Body:          []byte(bytes),
			})

		failOnError(err, "Failed to publish a message")

		for d := range msgs {
			if os.Args[1] == d.CorrelationId {
				var t TransactionResult
				json.Unmarshal(d.Body, &t)

				if !t.Ok {
					fmt.Println(t.Message)
				}

				fmt.Printf("---> AMOUNT: %d\n", t.Amount)
				d.Ack(true)
				break
			}
		}
	}
}
