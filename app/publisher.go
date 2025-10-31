package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func generateRandomMessage() Message {
	msg := Message{
		Time:       time.Now(),
		DeviceId:   "043e5af81c",
		DeviceType: "TempRH",
		DeviceData: struct {
			Temp float32
			Rh   float32
		}{
			Temp: 76.3 + rand.Float32()*2.5,
			Rh:   52.9 + rand.Float32()*1.3,
		},
	}
	return msg
}

func publishMessage(broker, topic, clientId string) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(clientId)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	for {
		message := generateRandomMessage()
		payload, err := json.MarshalIndent(message, "", " ")
		if err != nil {
			panic(err)
		}
		token := client.Publish(topic, 0, false, payload)
		token.Wait()
		fmt.Printf("Client ID: %s | Published json message: %s\n", clientId, payload)
		time.Sleep(1 * time.Second)
	}

}
