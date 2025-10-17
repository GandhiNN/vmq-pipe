package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	broker             = "tcp://localhost:1883"
	publisherClientID  = "go-mqtt-client-publisher"
	subscriberClientID = "go-mqtt-client-subscriber"
	topic              = "iot-messages"
)

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

	if *funcPtr == "publish" {
		var wg sync.WaitGroup
		for i := 0; i < *numWorkerPtr; i++ {
			s := strconv.Itoa(i)
			clientId := fmt.Sprintf("%s-%s", publisherClientID, s)
			wg.Go(func() {
				publishMessage(clientId)
			})
		}
		wg.Wait()
	} else if *funcPtr == "subscribe" {
		var wg sync.WaitGroup
		for i := 0; i < *numWorkerPtr; i++ {
			s := strconv.Itoa(i)
			clientId := fmt.Sprintf("%s-%s", subscriberClientID, s)
			wg.Go(func() {
				subscribeMessage(clientId)
			})
		}
		wg.Wait()
	}
}
