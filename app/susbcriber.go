package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
)

var mqttMsgChan = make(chan mqtt.Message)

func processMsgV1(
	ctx context.Context,
	logger *zerolog.Logger,
	input <-chan mqtt.Message,
) chan Message {
	out := make(chan Message)
	go func() {
		defer close(out)
		for {
			select {
			case msg, ok := <-input:
				if !ok {
					return
				}
				fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
				var iotMsg Message
				err := json.Unmarshal(msg.Payload(), &iotMsg)
				if err != nil {
					logger.Error().Err(err).Msg("Error unmarshalling IoTDeviceMessage")
				} else {
					out <- iotMsg
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func processMsgV2(
	ctx context.Context,
	logger *zerolog.Logger,
	input <-chan mqtt.Message,
) chan IoTRawDeviceMessage {
	out := make(chan IoTRawDeviceMessage)
	go func() {
		defer close(out)
		for {
			select {
			case msg, ok := <-input:
				if !ok {
					return
				}
				fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
				var iotMsg IoTRawDeviceMessage
				err := json.Unmarshal(msg.Payload(), &iotMsg)
				if err != nil {
					logger.Error().Err(err).Msg("Error unmarshalling IoTDeviceMessage")
				} else {
					out <- iotMsg
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func subscribeMessage(broker, topic, clientID string) {
	appCtx := context.Background()
	logger := setupLogger(appCtx, "")

	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(clientID)
	opts.SetDefaultPublishHandler(messagePubHandler)

	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	ctx, cancel := context.WithCancel(appCtx)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		finalChan := processMsgV1(ctx, logger, mqttMsgChan)
		for iotMsg := range finalChan {
			logger.Info().
				Msg(fmt.Sprintf("Client ID: %s | Received iot msg: %+v", clientID, iotMsg))
		}
	}()

	// Subscribe to the topic
	token := client.Subscribe(topic, 1, nil)
	token.Wait()
	fmt.Printf("Subscribed to topic: %s\n", topic)

	// Wait for interrupt signal to gracefully shutdown the subscriber
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// Cancel the context to signal the goroutine to stop
	cancel()

	// Unsubscribe and disconnect
	fmt.Println("Unsubscribing and disconnecting...")
	client.Unsubscribe(topic)
	client.Disconnect(250)

	// Wait for the goroutine to finish
	wg.Wait()

	// Close
	fmt.Println("Goroutine terminated, exiting...")
}
