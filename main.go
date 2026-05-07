package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

/*
#include "sensor.h"
*/
import "C"

func mqttConnect() {
	opts := mqtt.NewClientOptions().AddBroker("127.0.0.1:1883")
	opts.ConnectTimeout = time.Second
	opts.SetConnectionNotificationHandler(func(client mqtt.Client, notification mqtt.ConnectionNotification) {
		fmt.Printf("\t[mqtt connection notification] %v\n", notification)
	})
	cli := mqtt.NewClient(opts)
	if tkn := cli.Connect(); tkn.Wait() && tkn.Error() != nil {
		os.Exit(1)
	}
	if tkn := cli.Subscribe("gateway/control/#", 0, mqttCmdHandler); tkn.Wait() && tkn.Error() != nil {
		os.Exit(2)
	}
}

func mqttCmdHandler(c mqtt.Client, msg mqtt.Message) {
	fmt.Println("cmd:", string(msg.Payload()))
}

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	ticks := time.Tick(3 * time.Second)

	mqttConnect()

	println("waiting for SIGTERM or SIGINT, just printing time and temperature sometimes")

mainLoop:
	for {
		select {
		case sig := <-signals:
			println("signal received:", sig.String())
			break mainLoop
		case <-ticks:
			t := time.Now().Unix()
			println(t, C.get_temperature_celsius(C.int(t&0xFFFF)))
		}
	}
	println("exiting")
}
