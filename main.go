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

type ThermalGatewayService struct {
	mqttClient mqtt.Client
	signals    chan os.Signal
}

func (s *ThermalGatewayService) AttachSignals() {
	s.signals = make(chan os.Signal, 1)
	signal.Notify(s.signals, syscall.SIGTERM, syscall.SIGINT)
}

func (s *ThermalGatewayService) InitMQ() {
	opts := mqtt.NewClientOptions().
		AddBroker(getEnvWithDefault("MQTT_BROKER", "127.0.0.1:1883")).
		SetClientID(getEnvWithDefault("MQTT_CLIENT_ID", "thermal-gateway")).
		SetConnectTimeout(time.Second).
		SetConnectRetry(true).
		SetConnectRetryInterval(3 * time.Second).
		SetKeepAlive(1).
		SetConnectionNotificationHandler(s.mqttConnStatus)

	s.mqttClient = mqtt.NewClient(opts)

	s.mqttClient.Connect()
}

func (s *ThermalGatewayService) cmdHandler(c mqtt.Client, msg mqtt.Message) {
	fmt.Println("cmd:", string(msg.Payload()))

}

func (s *ThermalGatewayService) mqttConnStatus(client mqtt.Client, notification mqtt.ConnectionNotification) {
	fmt.Printf("\t[mqtt connection notification] %v\n", notification)
	if notification.Type() == mqtt.ConnectionNotificationTypeConnected {
		if tkn := s.mqttClient.Subscribe("gateway/control/#", 0, s.cmdHandler); tkn.Wait() && tkn.Error() != nil {
			os.Exit(2)
		}
	}
}

func (s *ThermalGatewayService) Run() {
	ticks := time.Tick(3 * time.Second)

	println("waiting for SIGTERM or SIGINT, just printing time and temperature sometimes")

mainLoop:
	for {
		select {
		case sig := <-s.signals:
			println("signal received:", sig.String())
			break mainLoop
		case <-ticks:
			t := time.Now().Unix()
			println(t, C.get_temperature_celsius(C.int(t&0xFFFF)))
		}
	}
	println("exiting")
}

func getEnvWithDefault(name, defaultValue string) string {
	if val, ok := os.LookupEnv(name); ok {
		return val
	}
	return defaultValue
}

func main() {
	svc := &ThermalGatewayService{}
	svc.AttachSignals()
	svc.InitMQ()
	svc.Run()
}
