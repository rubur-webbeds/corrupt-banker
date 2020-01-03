package main

import (
	"log"
	"fmt"
	"encoding/json"
	"github.com/streadway/amqp"
)

var amount int

type Transaction struct{
	Action string
	Amount int
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func execTransaction(t Transaction){
	switch t.Action{
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

func addAmount(a int){
	amount += a
}

func subsAmount(a int){
	if amount - a < 0 {
		fmt.Printf("Can't substract %d\n", a)
	}else{
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

	q, err := ch.QueueDeclare(
		"transactions", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	/*forever := make(chan bool)

	/go func() {
		for d := range msgs {
			var t Transaction
			json.Unmarshal(d.Body, &t)
			log.Printf("Received a message: %s:%d\n", t.Action, t.Amount)
		}
	}()*/

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	fmt.Printf("AMOUNT: %d\n", amount)

	for d := range msgs {
		var t Transaction
		json.Unmarshal(d.Body, &t)
		go execTransaction(t)
		//log.Printf("Received a message: %s:%d\n", t.Action, t.Amount)
	}

	//<-forever
}
