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
	Action string
	Amount int
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

	q, err := ch.QueueDeclare(
		"transactions", // name
		false,          // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// create the random number generator
	seed := rand.NewSource(time.Now().UnixNano())
	random := rand.New(seed)

	fmt.Printf("I'm the client: %s\n", os.Args[1])
	n_trans := 1 + random.Intn(MAX_OPERATIONS)
	fmt.Printf("I'll do %d transactions!\n", n_trans)

	for i := 0; i < n_trans; i++ {
		trans := getTransaction(random.Float64())
		amount := 1 + random.Intn(MAX_AMOUNT)
		fmt.Printf("Transaction %d: %s %d\n", i+1, trans, amount)

		bytes, err := json.Marshal(Transaction{Action: trans, Amount: amount})
		failOnError(err, "Failed to encode")

		//go publishTransaction(bytes, q.Name, ch)

		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(bytes),
			})

		log.Printf(" [x] Sent %s", bytes)
		failOnError(err, "Failed to publish a message")

	}
}
