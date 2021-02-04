package rabbit

import (
	"fmt"
	"log"
	"strconv"

	"github.com/streadway/amqp"
)

// Consumer is the interface that you must implement if you want to consume messages
type Consumer interface {
	Process(mesage Message) error
}

// Message ...
type Message struct {
	ID       string `json:"id"`
	Path     string `json:"path"`
	TraceID  string `json:"traceId"`
	Type     string `json:"type"`
	ImageURL string `json:"ImageURL"`
}

// Client ...
type Client struct {
	uri       string
	queue     string
	blacklist []string
}

// NewClient ...
func NewClient(host, user, password, queue string, blacklist ...string) *Client {
	client := MakeClient(host, user, password, queue, blacklist...)
	return &client
}

// MakeClient ...
func MakeClient(host, user, password, queue string, blacklist ...string) Client {
	uri := fmt.Sprintf("amqp://%s:%s@%s:5672/", user, password, host)
	if len(blacklist) > 1 {
		log.Printf("host:%s", host)
		log.Printf("user:%s", user)
		log.Printf("password:%s", password)
		log.Printf("queue:%s", queue)
		log.Printf("blacklist:%s", blacklist)
		panic("The only optional parameter is 'blacklist'")
	}
	return Client{uri, queue, blacklist}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func fib(n int) int {
	if n == 0 {
		return 0
	} else if n == 1 {
		return 1
	} else {
		return fib(n-1) + fib(n-2)
	}
}

// StartConsuming ...
func (c Client) StartConsuming(consumer Consumer) {
	conn, err := amqp.Dial(c.uri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		c.queue, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			n, err := strconv.Atoi(string(d.Body))
			failOnError(err, "Failed to convert body to integer")

			log.Printf(" [.] fib(%d)", n)
			response := fib(n)

			err = ch.Publish(
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(strconv.Itoa(response)),
				})
			failOnError(err, "Failed to publish a message")

			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting RPC requests")
	<-forever
}
