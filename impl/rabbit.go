package rabbit

import (
	"encoding/json"
	"fmt"
	"log"

	queuer "github.com/aamendola/go-rabbitmq-rpc"
	logutils "github.com/aamendola/go-utils/log"
	"github.com/streadway/amqp"
)

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
		panic("The only optional parameter is 'blacklist'")
	}
	return Client{uri, queue, blacklist}
}

// StartConsuming ...
func (c Client) StartConsuming(consumer queuer.Consumer) {
	conn, err := amqp.Dial(c.uri)
	logutils.Fatal(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	logutils.Fatal(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		c.queue, // name
		true,    // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	logutils.Fatal(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	logutils.Fatal(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	logutils.Fatal(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var dat map[string]interface{}
			err := json.Unmarshal(d.Body, &dat)
			logutils.Fatal(err)

			message := queuer.Message{}
			json.Unmarshal(d.Body, &message)

			err = consumer.Process(message)
			logutils.Fatal(err)

			err = ch.Publish(
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(fmt.Sprintf("The file %s is up for to be downloaded", message.Path)),
				})
			logutils.Fatal(err, "Failed to publish a message")

			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting RPC requests")
	<-forever
}
