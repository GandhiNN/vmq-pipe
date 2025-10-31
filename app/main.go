package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func getEnv(key, defaultValue string) string {
	val := os.Getenv(key)
	if len(val) == 0 {
		return defaultValue
	}
	return val
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected to MQTT Broker")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connection lost: %v\n", err)
}

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	mqttMsgChan <- msg
}

func init() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
}

func main() {
	funcPtr := flag.String("func", "publish", "function to execute")
	numWorkerPtr := flag.Int("worker", 10, "number of workers")
	flag.Parse()

	broker := getEnv("BROKER_ADDRESS", "tcp://localhost:1883")
	publisherClientID := getEnv("PUBLISHER_CLIENT_ID", "go-mqtt-client-publisher")
	subscriberClientID := getEnv("SUBSCRIBER_CLIENT_ID", "go-mqtt-client-subscriber")
	topic := getEnv("TOPIC_NAME", "iot-messages")

	if *funcPtr == "publish" {
		var wg sync.WaitGroup
		for i := 0; i < *numWorkerPtr; i++ {
			s := strconv.Itoa(i)
			clientId := fmt.Sprintf("%s-%s", publisherClientID, s)
			wg.Go(func() {
				publishMessage(broker, topic, clientId)
			})
		}
		wg.Wait()
	} else if *funcPtr == "subscribe" {
		var wg sync.WaitGroup
		for i := 0; i < *numWorkerPtr; i++ {
			s := strconv.Itoa(i)
			clientId := fmt.Sprintf("%s-%s", subscriberClientID, s)
			wg.Go(func() {
				subscribeMessage(broker, topic, clientId)
			})
		}
		wg.Wait()
	}
}
