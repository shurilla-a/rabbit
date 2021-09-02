package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
)

// функция для озаписи ошибок в лог
func errorLoger(errLogerFile error, msgtoErrorLogerFile string) {
	fileWrite, err := os.OpenFile("errorLog.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Error Open or Read errorLog.log File", err)
	}
	log.SetOutput(fileWrite)
	log.Fatalf("%s:%s", msgtoErrorLogerFile, errLogerFile)
}

// структура Конфига
type ConfigYmal struct {
	Host          string `yaml:"host"`
	Prot          string `yaml:"port"`
	Login         string `yaml:"login"`
	Password      string `yaml:"passwd"`
	QueueName     string `yaml:"queueName"`
	QueueMassages int    `yaml:"queueMessages"`
	QueueCount    int    `yaml:"queueCount"`
}

// функция парсинга Ymal Файла
func inConfigParsingYmal(configFile string) (*ConfigYmal, error) {
	configFileOpen, err := ioutil.ReadFile(configFile)
	fmt.Println(configFile)

	if err != nil {
		errorLoger(err, "НЕ МОГУ НАЙТИ ФАЙЛ КОНФИГА")
	}
	//return err

	c := &ConfigYmal{}
	err = yaml.Unmarshal(configFileOpen, c)
	if err != nil {
		errorLoger(err, "Cannot Parsing Ymal File")
	}
	return c, nil
}

// функция генерация строки
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
	// открываем Конфиг
	configReader, err := inConfigParsingYmal("config.yml")
	if err != nil {
		errorLoger(err, "Config not Found")
	}

	d := "amqp://" + configReader.Login + ":" + configReader.Password + "@" + configReader.Host + ":" + configReader.Prot
	fmt.Println(d)
	connect, err := amqp.Dial(d)

	if err != nil {
		errorLoger(err, "Failed to connect to RabbitMQ")
	}
	defer connect.Close()

	channel, err := connect.Channel()
	if err != nil {
		errorLoger(err, "Filed to open a channel")
	}
	defer channel.Close()

	////TODO Дописать много поточность
	// TODO Сделать распреденеие на количество сообщений в очередях и количество очередей из конфига

	// считаем количество сообщений в очереди
	// QueueMassages - колличество сообщений в очереди
	// QueueCount - количество очередей

	messageCountinQueueC := configReader.QueueMassages / configReader.QueueCount
	// задаем кошличество очередей и генерим имя и номер очереди
	for i := 1; i <= configReader.QueueCount; i++ {

		queueName := configReader.QueueName + strconv.Itoa(i)
		queue, err := channel.QueueDeclare(
			queueName, //Nаme
			true,      // durable
			false,     // delete when unused
			false,     // exclusive
			false,     // no-wait
			nil,       // arguments
		)
		if err != nil {
			errorLoger(err, "Errr Declare Channe")
		}
		// количество сообщений
		for i := 0; i < messageCountinQueueC; i++ {
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
			if err != nil {
				errorLoger(err, "Failed to publish a message")
			}
			log.Printf(" [x] Sent %s", body)
		}
	}
}
