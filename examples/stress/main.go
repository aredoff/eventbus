package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aredoff/eventbus"
	"github.com/aredoff/eventbus/middleware"
)

type PaymentCompleted struct {
	Seq int64
}

type OrderCreated struct {
	Seq int64
}

type AnalyticsHit struct {
	Seq int64
}

type WebhookJob struct {
	Seq int64
}

type AuditEntry struct {
	Seq int64
}

type InventoryChanged struct {
	Seq int64
}

type UserSignedUp struct {
	Seq int64
}

type EmailRequested struct {
	Seq int64
}

type CacheInvalidate struct {
	Seq int64
}

type MetricSample struct {
	Seq int64
}

type SearchIndexUpdate struct {
	Seq int64
}

type NotificationSent struct {
	Seq int64
}

type typeStats struct {
	name      string
	published atomic.Int64
	queueFull atomic.Int64
	processed atomic.Int64
}

type bench struct {
	bus        *eventbus.Bus
	types      []*typeStats
	publishers []func(context.Context, int64) error
}

func main() {
	var (
		duration       = flag.Duration("duration", 5*time.Second, "benchmark duration")
		publisherCount = flag.Int("publishers", runtime.NumCPU()*4, "concurrent publisher goroutines")
		queueSize      = flag.Int("queue", 512, "per-type queue size")
		webhookWorkers = flag.Int("webhook-workers", 8, "slow-path worker pool size")
		analyticsRatio = flag.Float64("analytics-ratio", 0.6, "fraction of publishes that are analytics flood")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	bus := eventbus.New(eventbus.Config{
		QueueSize: *queueSize,
		Logger:    logger,
	})
	bus.Use(
		middleware.Recover(logger),
		middleware.Timeout(2*time.Second),
	)

	b := newBench(bus)
	b.registerAll(*webhookWorkers)

	fmt.Printf("eventbus stress test\n")
	fmt.Printf("  duration=%s publishers=%d queue=%d/types webhook_workers=%d analytics_ratio=%.0f%%\n\n",
		*duration, *publisherCount, *queueSize, *webhookWorkers, *analyticsRatio*100)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(*publisherCount)

	start := time.Now()

	for i := range *publisherCount {
		go func(seed int) {
			defer wg.Done()
			var seq int64
			for ctx.Err() == nil {
				seq++
				if !publishMixed(ctx, b, seq, *analyticsRatio) {
					time.Sleep(time.Microsecond)
				}
			}
		}(i)
	}

	wg.Wait()

	// Drain in-flight events after publishers stop.
	drainDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(drainDeadline) {
		time.Sleep(50 * time.Millisecond)
	}

	elapsed := time.Since(start)

	printReport(b, elapsed)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	bus.Close(shutdownCtx)
}

func newBench(bus *eventbus.Bus) *bench {
	b := &bench{bus: bus}
	addType := func(name string, pub func(context.Context, int64) error) {
		st := &typeStats{name: name}
		b.types = append(b.types, st)
		b.publishers = append(b.publishers, pub)
	}

	addType("PaymentCompleted", func(ctx context.Context, seq int64) error {
		return eventbus.Pub(bus, ctx, &PaymentCompleted{Seq: seq})
	})
	addType("OrderCreated", func(ctx context.Context, seq int64) error {
		return eventbus.Pub(bus, ctx, &OrderCreated{Seq: seq})
	})
	addType("AnalyticsHit", func(ctx context.Context, seq int64) error {
		return eventbus.Pub(bus, ctx, &AnalyticsHit{Seq: seq})
	})
	addType("WebhookJob", func(ctx context.Context, seq int64) error {
		return eventbus.Pub(bus, ctx, &WebhookJob{Seq: seq})
	})
	addType("AuditEntry", func(ctx context.Context, seq int64) error {
		return eventbus.Pub(bus, ctx, &AuditEntry{Seq: seq})
	})
	addType("InventoryChanged", func(ctx context.Context, seq int64) error {
		return eventbus.Pub(bus, ctx, &InventoryChanged{Seq: seq})
	})
	addType("UserSignedUp", func(ctx context.Context, seq int64) error {
		return eventbus.Pub(bus, ctx, &UserSignedUp{Seq: seq})
	})
	addType("EmailRequested", func(ctx context.Context, seq int64) error {
		return eventbus.Pub(bus, ctx, &EmailRequested{Seq: seq})
	})
	addType("CacheInvalidate", func(ctx context.Context, seq int64) error {
		return eventbus.Pub(bus, ctx, &CacheInvalidate{Seq: seq})
	})
	addType("MetricSample", func(ctx context.Context, seq int64) error {
		return eventbus.Pub(bus, ctx, &MetricSample{Seq: seq})
	})
	addType("SearchIndexUpdate", func(ctx context.Context, seq int64) error {
		return eventbus.Pub(bus, ctx, &SearchIndexUpdate{Seq: seq})
	})
	addType("NotificationSent", func(ctx context.Context, seq int64) error {
		return eventbus.Pub(bus, ctx, &NotificationSent{Seq: seq})
	})

	return b
}

func (b *bench) stat(name string) *typeStats {
	for _, st := range b.types {
		if st.name == name {
			return st
		}
	}
	return nil
}

func (b *bench) registerAll(webhookWorkers int) {
	bus := b.bus

	pay := b.stat("PaymentCompleted")
	for range 3 {
		eventbus.Sub(bus, func(_ context.Context, _ *PaymentCompleted) {
			time.Sleep(200 * time.Microsecond)
			pay.processed.Add(1)
		})
	}

	ord := b.stat("OrderCreated")
	for range 2 {
		eventbus.Sub(bus, func(_ context.Context, _ *OrderCreated) {
			time.Sleep(100 * time.Microsecond)
			ord.processed.Add(1)
		})
	}

	analytics := b.stat("AnalyticsHit")
	for range 3 {
		eventbus.Sub(bus, func(_ context.Context, _ *AnalyticsHit) {
			analytics.processed.Add(1)
		})
	}

	ch := make(chan WebhookJob, 256)
	eventbus.Sub(bus, func(_ context.Context, job *WebhookJob) {
		ch <- *job
	})
	stWebhook := b.stat("WebhookJob")
	for range webhookWorkers {
		go func() {
			for job := range ch {
				time.Sleep(2 * time.Millisecond)
				stWebhook.processed.Add(1)
				_ = job
			}
		}()
	}

	audit := b.stat("AuditEntry")
	eventbus.Sub(bus, func(_ context.Context, _ *AuditEntry) {
		audit.processed.Add(1)
	})

	inv := b.stat("InventoryChanged")
	eventbus.Sub(bus, func(_ context.Context, _ *InventoryChanged) {
		inv.processed.Add(1)
	})

	user := b.stat("UserSignedUp")
	eventbus.Sub(bus, func(_ context.Context, _ *UserSignedUp) {
		time.Sleep(50 * time.Microsecond)
		user.processed.Add(1)
	})

	email := b.stat("EmailRequested")
	eventbus.Sub(bus, func(_ context.Context, _ *EmailRequested) {
		time.Sleep(50 * time.Microsecond)
		email.processed.Add(1)
	})

	cache := b.stat("CacheInvalidate")
	eventbus.Sub(bus, func(_ context.Context, _ *CacheInvalidate) {
		cache.processed.Add(1)
	})

	metric := b.stat("MetricSample")
	eventbus.Sub(bus, func(_ context.Context, _ *MetricSample) {
		metric.processed.Add(1)
	})

	search := b.stat("SearchIndexUpdate")
	eventbus.Sub(bus, func(_ context.Context, _ *SearchIndexUpdate) {
		time.Sleep(150 * time.Microsecond)
		search.processed.Add(1)
	})

	notify := b.stat("NotificationSent")
	eventbus.Sub(bus, func(_ context.Context, _ *NotificationSent) {
		notify.processed.Add(1)
	})
}

func publishMixed(ctx context.Context, b *bench, seq int64, analyticsRatio float64) bool {
	if float64(seq%100)/100 < analyticsRatio {
		return publishOne(ctx, b, 2, seq)
	}

	others := []int{0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	return publishOne(ctx, b, others[seq%int64(len(others))], seq)
}

func publishOne(ctx context.Context, b *bench, idx int, seq int64) bool {
	st := b.types[idx]
	err := b.publishers[idx](ctx, seq)
	if err != nil {
		if err == eventbus.ErrQueueFull {
			st.queueFull.Add(1)
		}
		return false
	}
	st.published.Add(1)
	return true
}

func printReport(b *bench, elapsed time.Duration) {
	var totalPub, totalFull, totalProc int64

	fmt.Printf("%-22s %12s %12s %12s %8s\n", "type", "published", "queue_full", "handler_calls", "loss%")
	fmt.Println(strings.Repeat("-", 72))

	for _, st := range b.types {
		pub := st.published.Load()
		full := st.queueFull.Load()
		proc := st.processed.Load()
		totalPub += pub
		totalFull += full
		totalProc += proc
		loss := 0.0
		if pub+full > 0 {
			loss = float64(full) / float64(pub+full) * 100
		}
		fmt.Printf("%-22s %12d %12d %12d %7.1f%%\n", st.name, pub, full, proc, loss)
	}

	fmt.Println(strings.Repeat("-", 72))
	fmt.Printf("%-22s %12d %12d %12d\n", "TOTAL", totalPub, totalFull, totalProc)
	fmt.Printf("\nelapsed: %s\n", elapsed.Round(time.Millisecond))
	if elapsed > 0 {
		fmt.Printf("publish throughput: %.0f events/s (enqueued)\n", float64(totalPub)/elapsed.Seconds())
		fmt.Printf("handler throughput: %.0f calls/s (includes duplicate subs per event)\n", float64(totalProc)/elapsed.Seconds())
	}

	pay := b.stat("PaymentCompleted")
	analytics := b.stat("AnalyticsHit")
	fmt.Printf("\nisolation check (critical vs noisy neighbor):\n")
	fmt.Printf("  PaymentCompleted enqueued=%d queue_full=%d handler_calls=%d\n",
		pay.published.Load(), pay.queueFull.Load(), pay.processed.Load())
	fmt.Printf("  AnalyticsHit       enqueued=%d queue_full=%d handler_calls=%d\n",
		analytics.published.Load(), analytics.queueFull.Load(), analytics.processed.Load())
	fmt.Printf("  subscribers on bus: %d (handler_calls > enqueued when multiple Sub per type)\n", b.bus.Subscribers())
}
