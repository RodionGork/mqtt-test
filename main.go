package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

/*
#include "sensor.h"
*/
import "C"

const (
	cmdTopicPattern = "gateway/control/#"
	dataTopicName   = "gateway/data"
	batchTopicName  = "gateway/batch"
)

type ThermalGatewayService struct {
	options    tgsOptions
	mqttClient mqtt.Client
	signals    chan os.Signal
}

type tgsOptions struct {
	mqttBroker         string
	clientId           string
	sensorReadInterval int
}

func (s *ThermalGatewayService) AttachSignals() {
	s.signals = make(chan os.Signal, 1)
	signal.Notify(s.signals, syscall.SIGTERM, syscall.SIGINT)
}

func (s *ThermalGatewayService) InitMQ() {
	opts := mqtt.NewClientOptions().
		AddBroker(s.options.mqttBroker).
		SetClientID(s.options.clientId).
		SetConnectTimeout(time.Second).
		SetConnectRetry(true).
		SetConnectRetryInterval(3 * time.Second).
		SetKeepAlive(1).
		SetConnectionNotificationHandler(s.mqttConnStatus)
	s.mqttClient = mqtt.NewClient(opts)
	s.mqttClient.Connect()
}

func (s *ThermalGatewayService) cmdHandler(c mqtt.Client, msg mqtt.Message) {
	type cmdJson struct {
		Action    string
		Sensor_id int
	}
	var cmd cmdJson
	if err := json.Unmarshal(msg.Payload(), &cmd); err != nil {
		slog.Debug("couldn't parse command, skipping it:", err.Error())
		return
	}
	if cmd.Action != "read" {
		slog.Debug("command received with 'action' missing or unsupported, skipped")
		return
	}
	slog.Debug("command 'read' received:", "sensor", cmd.Sensor_id)
	s.sendSingleSensor(cmd.Sensor_id)
}

func (s *ThermalGatewayService) sendSingleSensor(id int) {
	temp := int(C.get_temperature_celsius(C.int(id)))
	bytes, _ := json.Marshal(sensorReport(id, temp))
	s.publishAndMonitor(dataTopicName, bytes)
}

func (s *ThermalGatewayService) sendBatchSensors(num int) {
	slog.Debug("sending batch data from sensors")
	res := make([]any, num)
	for id := 1; id <= num; id++ {
		temp := int(C.get_temperature_celsius(C.int(id)))
		res[id-1] = sensorReport(id, temp)
	}
	bytes, _ := json.Marshal(res)
	go s.publishAndMonitor(batchTopicName, bytes)
}

func (s *ThermalGatewayService) publishAndMonitor(topic string, data []byte) {
	if !s.mqttClient.IsConnected() {
		slog.Info("publish skipped as mqtt currently not connected", "topic", topic)
		return
	}
	tkn := s.mqttClient.Publish(topic, 0, false, data)
	tkn.WaitTimeout(3 * time.Second)
	if tkn.Error() != nil {
		slog.Error("publish seemingly fails", "topic", topic)
	}
}

func sensorReport(id, value int) map[string]any {
	res := map[string]any{"sensor_id": id, "value": value, "timestamp": time.Now().Unix()}
	if value == -1 {
		res["error"] = "sensor malfunction"
	}
	return res
}

func (s *ThermalGatewayService) mqttConnStatus(client mqtt.Client, notification mqtt.ConnectionNotification) {
	switch notification.Type() {
	case mqtt.ConnectionNotificationTypeConnected:
		slog.Info("mqtt connected")
		if tkn := s.mqttClient.Subscribe(cmdTopicPattern, 0, s.cmdHandler); tkn.Wait() && tkn.Error() != nil {
			slog.Error("mqtt subscription failed, this needs investigation (so quit now):", tkn.Error())
			os.Exit(1)
		}
	case mqtt.ConnectionNotificationTypeFailed:
		slog.Info("mqtt connection failed (will retry)")
	case mqtt.ConnectionNotificationTypeLost:
		slog.Info("mqtt connection lost (will try to reconnect)")
	}
}

func (s *ThermalGatewayService) Run() {
	pollTicks := time.Tick(time.Duration(s.options.sensorReadInterval) * time.Second)

	slog.Warn("everything is all right, entering main processing loop until SIGTERM or SIGINT")

mainLoop:
	for {
		select {
		case sig := <-s.signals:
			slog.Warn("signal received:", "type", sig.String())
			break mainLoop
		case <-pollTicks:
			s.sendBatchSensors(5)
		}
	}
	slog.Warn("exiting, bye")
}

func (s *ThermalGatewayService) ConfigureFromEnv() {
	s.options = tgsOptions{
		mqttBroker:         getEnvWithDefault("MQTT_BROKER", "tcp://127.0.0.1:1883"),
		clientId:           getEnvWithDefault("MQTT_CLIENT_ID", "thermal-service"),
		sensorReadInterval: 30,
	}
	if sec, err := strconv.Atoi(os.Getenv("POLL_INTERVAL_SEC")); err == nil {
		s.options.sensorReadInterval = sec
	}
}

func getEnvWithDefault(name, defaultValue string) string {
	if val, ok := os.LookupEnv(name); ok {
		return val
	}
	return defaultValue
}

func setLogLevelFromEnv() {
	levels := map[string]slog.Level{"WARN": slog.LevelWarn, "INFO": slog.LevelInfo, "DEBUG": slog.LevelDebug}
	choice := slog.LevelInfo
	if val, ok := os.LookupEnv("LOG_LEVEL"); ok {
		if lvl, ok := levels[strings.ToUpper(val)]; ok {
			choice = lvl
		} else {
			slog.Error("unknown logger level suggested, so using default")
		}
	}
	slog.SetLogLoggerLevel(choice)
}

func main() {
	setLogLevelFromEnv()
	svc := &ThermalGatewayService{}
	svc.ConfigureFromEnv()
	svc.AttachSignals()
	svc.InitMQ()
	svc.Run()
}
