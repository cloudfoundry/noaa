package main

import (
	"crypto/tls"
	"fmt"
	"os"

	"github.com/cloudfoundry/noaa/v2/consumer"
)

var (
	dopplerAddress = os.Getenv("DOPPLER_ADDR")
	appGuid        = os.Getenv("APP_GUID")
	authToken      = os.Getenv("CF_ACCESS_TOKEN")
)

func main() {
	consumer := consumer.New(dopplerAddress, &tls.Config{InsecureSkipVerify: false, MinVersion: tls.VersionTLS12}, nil)
	consumer.SetDebugPrinter(ConsoleDebugPrinter{})

	fmt.Println("===== Streaming metrics")
	msgChan, errorChan := consumer.Stream(appGuid, authToken)

	go func() {
		for err := range errorChan {
			fmt.Fprintf(os.Stderr, "%v\n", err.Error())
		}
	}()

	for msg := range msgChan {
		fmt.Printf("%v \n", msg)
	}
}

type ConsoleDebugPrinter struct{}

func (c ConsoleDebugPrinter) Print(title, dump string) {
	println(title)
	println(dump)
}
