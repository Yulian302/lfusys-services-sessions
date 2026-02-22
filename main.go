package main

import (
	"context"
	"os"
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
		app.Logger.Error("app initialization failed", "reason", err.Error())
		os.Exit(1)
	}

	go func() {
		if err := app.Run(); err != nil {
			app.Logger.Error("server stopped", "err", err.Error())
		}
	}()

	app.Logger.Info("gRPC server started at %v", app.Config.ServiceConfig.SessionGRPCAddr)

	<-ctx.Done()

	app.Logger.Info("shutdown signal received")
	shutDownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := app.Shutdown(shutDownContext); err != nil {
		app.Logger.Error("graceful shutdown failed", "err", err.Error())
	}

	app.Logger.Info("server exited properly")
}
