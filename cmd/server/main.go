package main

import (
	"os"
	"os/signal"
	"syscall"

	ueprofilesqlserver "github.com/maypril/ue-profile-sql-server"
)

func main() {

	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	server, err := ueprofilesqlserver.NewServer(wd)
	if err != nil {
		panic(err)
	}

	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.Start(); err != nil {
			panic(err)
		}
	}()

	<-quitChan

	if err := server.Close(); err != nil {
		panic(err)
	}

}
