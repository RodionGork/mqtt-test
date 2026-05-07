package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"
)

/*
#include "sensor.h"
*/
import "C"

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	ticks := time.Tick(3 * time.Second)

	println("waiting for SIGTERM or SIGINT, just printing time and temperature sometimes")

mainLoop:
	for {
		select {
		case sig := <-signals:
			println("signal received:", sig.String())
			break mainLoop
		case <-ticks:
			t := time.Now().Unix()
			println(t, C.get_temperature_celsius(C.int(t & 0xFFFF)))
		}
	}
	println("exiting")
}
