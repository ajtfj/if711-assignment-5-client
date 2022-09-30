package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	SAMPLES_SIZE = 10000
)

var (
	clientUUID uuid.UUID
)

func createRequest() ([]byte, error) {
	requestPayload := RequestPayload{
		Ori:        "A",
		Dest:       "E",
		ClientUUID: clientUUID,
	}
	requestData, err := json.Marshal(&requestPayload)
	if err != nil {
		return nil, err
	}
	return requestData, nil
}

func parseResponse(responseData []byte) (*ResponsePayload, error) {
	responsePayload := ResponsePayload{}
	if err := json.Unmarshal(responseData, &responsePayload); err != nil {
		return nil, err
	}

	return &responsePayload, nil
}

func benchmark(ch *amqp.Channel, responseQueue *amqp.Queue, requestsQueue *amqp.Queue) error {
	response, err := ch.Consume(
		responseQueue.Name, // queue
		"",                 // consumer
		true,               // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)
	if err != nil {
		log.Panic(err)
	}

	var samples []time.Duration
	for i := 0; i < SAMPLES_SIZE; i++ {
		request, err := createRequest()
		if err != nil {
			return err
		}

		start := time.Now()

		if err := ch.PublishWithContext(context.Background(),
			"",                 // exchange
			requestsQueue.Name, // routing key
			false,              // mandatory
			false,              // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        request,
			},
		); err != nil {
			return err
		}

		d := <-response
		responsePayload, err := parseResponse(d.Body)
		if err != nil {
			return err
		}
		if responsePayload.Error != nil {
			i--
			continue
		}

		rtt := time.Since(start)
		samples = append(samples, rtt)

		log.Printf("shortest path received %v", responsePayload.Path)
	}

	var mean float64
	for _, sample := range samples {
		mean += float64(sample)
	}
	mean = mean / float64(len(samples))

	var sd float64
	for _, sample := range samples {
		sd += math.Pow((float64(sample) - mean), 2)
	}
	sd = math.Sqrt(sd / float64(len(samples)))

	log.Printf("average RTT is %.2f (+- %.2f)", mean, sd)

	return nil
}

func init() {
	clientUUID = uuid.New()
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Panic(err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"responses", // name
		"direct",    // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.Panic(err)
	}

	requestsQueue, err := ch.QueueDeclare(
		"requests", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		log.Panic(err)
	}

	responseQueue, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Panic(err)
	}

	err = ch.QueueBind(
		responseQueue.Name,  // queue name
		clientUUID.String(), // routing key
		"responses",         // exchange
		false,
		nil,
	)
	if err != nil {
		log.Panic(err)
	}

	if err := benchmark(ch, &responseQueue, &requestsQueue); err != nil {
		log.Fatal(err)
	}
}

type RequestPayload struct {
	Ori        string    `json:"ori"`
	Dest       string    `json:"dest"`
	ClientUUID uuid.UUID `json:"client_uuid"`
}

type ResponsePayload struct {
	Path  []string `json:"path"`
	Error error    `json:"error"`
}
