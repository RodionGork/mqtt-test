package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	ticks := time.Tick(5 * time.Second)

	println("waiting for SIGTERM or SIGINT, just printing time sometimes")

mainLoop:
	for {
		select {
		case sig := <-signals:
			println("signal received:", sig.String())
			break mainLoop
		case <-ticks:
			println(time.Now().Unix())
		}
	}
	println("exiting")
}
