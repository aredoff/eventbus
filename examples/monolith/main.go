package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aredoff/eventbus"
	"github.com/aredoff/eventbus/middleware"
)

type OrderCreated struct {
	ID     int
	Amount float64
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	bus := eventbus.New(eventbus.Config{
		QueueSize: 512,
		Logger:    logger,
	})

	bus.Use(
		middleware.Recover(logger),
		middleware.Timeout(5*time.Second),
		// middleware.Logging(logger),
	)

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	demoCtx, demoCancel := context.WithTimeout(rootCtx, 12*time.Second)
	defer demoCancel()

	registerOrders(bus)
	registerBilling(bus)
	registerNotifications(bus, demoCtx)

	fmt.Println("eventbus monolith demo")
	fmt.Println("  publishing OrderCreated every 2s")
	fmt.Println("  stops after 12s or Ctrl+C")
	fmt.Println()

	go publishLoop(demoCtx, bus)

	<-demoCtx.Done()
	fmt.Println()
	fmt.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bus.Close(shutdownCtx)
	fmt.Println("done")
}

func publishLoop(ctx context.Context, bus *eventbus.Bus) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	id := 1000
	publish(ctx, bus, id, 99.90)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			id++
			publish(ctx, bus, id, float64(id)*10.99)
		}
	}
}

func publish(ctx context.Context, bus *eventbus.Bus, id int, amount float64) {
	order := &OrderCreated{ID: id, Amount: amount}
	if err := eventbus.Pub(bus, ctx, order); err != nil {
		slog.Error("publish failed", "order_id", id, "err", err)
		return
	}
	slog.Info("published", "order_id", id, "amount", amount)
}

func registerOrders(bus *eventbus.Bus) {
	eventbus.Sub(bus, func(_ context.Context, o *OrderCreated) {
		slog.Info("orders: persisted", "order_id", o.ID)
	})
}

func registerBilling(bus *eventbus.Bus) {
	eventbus.Sub(bus, func(_ context.Context, o *OrderCreated) {
		slog.Info("billing: charged", "order_id", o.ID, "amount", o.Amount)
	})
}

func registerNotifications(bus *eventbus.Bus, ctx context.Context) {
	ch := make(chan OrderCreated, 16)
	eventbus.Sub(bus, func(_ context.Context, o *OrderCreated) {
		select {
		case ch <- *o:
		default:
			slog.Warn("notifications: queue full, dropping", "order_id", o.ID)
		}
	})

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case o := <-ch:
				fmt.Printf("notifications: email sent for order %d\n", o.ID)
			}
		}
	}()
}
