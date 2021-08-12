package main

import (
	"log"
	"runtime"
	"strconv"

	"math/rand"

	amqp "github.com/rabbitmq/amqp091-go"
)

func errorLoger(err error, msg string) {
	if err != nil {
		log.Fatalf("%s:%s", msg, err)
	}

}

func RandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func main() {
	runtime.GOMAXPROCS(2) //используем два ядра

	connect, err := amqp.Dial("amqp://user:user@!!!!!!!!!!!!!:5672")
	errorLoger(err, "Failed to connect to RabbitMQ")
	defer connect.Close()

	channel, err := connect.Channel()
	errorLoger(err, "Filed to open a channel")
	defer channel.Close()

	errorLoger(err, "Failed to declare a queue")
	// задаем кошличество очередей и генерим имя qwerty_номер очереди
	for i := 0; i < 5; i++ {

		queueName := "qwerty_" + strconv.Itoa(i)

		queue, err := channel.QueueDeclare(
			queueName, //Nаme
			true,      // durable
			false,     // delete when unused
			false,     // exclusive
			false,     // no-wait
			nil,       // arguments
		)

		for i := 0; i < 500000; i++ {
			body := RandomString(100)
			err = channel.Publish(
				"",         //exchange
				queue.Name, // routing key
				false,      // mandatory
				false,      // immedite
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(body),
				})

			errorLoger(err, "Failed to publish a message")
			log.Printf(" [x] Sent %s", body)
		}
	}
}
