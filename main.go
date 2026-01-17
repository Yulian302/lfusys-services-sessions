package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/joho/godotenv/autoload"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer stop()

	app, err := SetupApp()
	if err != nil {
		log.Fatalf("failed to initialize app: %v", err)
	}

	go func() {
		if err := app.Run(); err != nil {
			log.Printf("server stopped: %v", err)
		}
	}()

	log.Printf("Grpc server started at %v", app.Config.ServiceConfig.SessionGRPCAddr)

	<-ctx.Done()

	log.Println("shutdown signal received")
	shutDownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := app.Shutdown(shutDownContext); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}

	log.Println("server exited properly")

}
