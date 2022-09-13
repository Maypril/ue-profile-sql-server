package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	ueprofilesqlserver "github.com/maypril/ue-profile-sql-server"
)

func main() {
	var wd string
	var err error

	if len(os.Args) > 1 {
		wd = os.Args[len(os.Args)-1]

		if !filepath.IsAbs(wd) {
			wd, err = filepath.Abs(wd)
			if err != nil {
				fmt.Printf("Failed to get abspath to: %s. %v", wd, err)
				os.Exit(1)
			}
		}

		info, err := os.Stat(wd)
		if errors.Is(err, os.ErrNotExist) || !info.IsDir() {
			fmt.Printf("%s has to exist and be a directory", wd)
			os.Exit(1)
		}
	} else {
		wd, err = os.Getwd()
		if err != nil {
			fmt.Printf("failed to get workdir. %v", err)
			os.Exit(1)
		}
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
