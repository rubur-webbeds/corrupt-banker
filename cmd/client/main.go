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

// MaxTransactions to perform by a client
const MaxTransactions int = 4

// MaxAmount to add or substract on each transaction
const MaxAmount int = 10

// Transaction type represents operations performed by the client
type Transaction struct {
	Action   string
	Amount   int
	ClientID string
}

// TransactionResult type represents the result of a transaction
type TransactionResult struct {
	Action   string
	Amount   int
	ClientID string
	Ok       bool
	Message  string
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func getAction(number float64) string {
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
	transactionsQueue, err := ch.QueueDeclare(
		"transactions", // name
		false,          // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	failOnError(err, "Failed to declare a queue: transactions")

	// RESULTS QUEUE
	resultsQueue, err := ch.QueueDeclare(
		"results", // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue: results")

	msgs, err := ch.Consume(
		resultsQueue.Name, // queue
		"",                // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	failOnError(err, "Failed to register a consumer")

	// create the random number generator
	seed := rand.NewSource(time.Now().UnixNano())
	random := rand.New(seed)

	nTrans := 1 + random.Intn(MaxTransactions)
	fmt.Printf("\nI'm the client: %s and I'll do %d transactions!\n", os.Args[1], nTrans)

	for i := 0; i < nTrans; i++ {
		action := getAction(random.Float64())
		amount := 1 + random.Intn(MaxAmount)
		fmt.Printf("Client %s -> Transaction %d: %s %d\n", os.Args[1], i+1, action, amount)

		bytes, err := json.Marshal(Transaction{Action: action, Amount: amount, ClientID: os.Args[1]})
		failOnError(err, "Failed to encode")

		// send transaction to banker
		err = ch.Publish(
			"",                     // exchange
			transactionsQueue.Name, // routing key
			false,                  // mandatory
			false,                  // immediate
			amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: os.Args[1],
				Body:          []byte(bytes),
			})

		failOnError(err, "Failed to publish a message")

		// read transaction result
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
