// Package clickhouse holds the batched async writer and analytical queries used
// by the writer and query services.
package clickhouse

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nickpatt/traceforge/internal/model"
)

// Connect opens a native ClickHouse connection.
func Connect(addr, database string) (driver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: database},
		Settings: clickhouse.Settings{
			"async_insert":          1,
			"wait_for_async_insert": 0,
		},
	})
}

// Writer batches spans and performs async bulk inserts with a bounded number of
// in-flight batches (backpressure).
type Writer struct {
	conn        driver.Conn
	batchMax    int
	batchWait   time.Duration
	maxInflight int

	buf    []*model.Span
	sem    chan struct{}
	lastAt time.Time
}

// NewWriter creates a Writer.
func NewWriter(conn driver.Conn, batchMax int, batchWait time.Duration, maxInflight int) *Writer {
	return &Writer{
		conn: conn, batchMax: batchMax, batchWait: batchWait, maxInflight: maxInflight,
		sem:    make(chan struct{}, maxInflight),
		lastAt: time.Now(),
	}
}

// Add buffers a span, flushing when the batch is full.
func (w *Writer) Add(ctx context.Context, s *model.Span) error {
	w.buf = append(w.buf, s)
	if len(w.buf) >= w.batchMax {
		return w.Flush(ctx)
	}
	return nil
}

// MaybeFlush flushes if the batch-wait window has elapsed.
func (w *Writer) MaybeFlush(ctx context.Context) error {
	if len(w.buf) > 0 && time.Since(w.lastAt) >= w.batchWait {
		return w.Flush(ctx)
	}
	return nil
}

// Flush writes the current buffer as one async batch. It acquires an in-flight
// slot, providing natural backpressure when ClickHouse is slow.
func (w *Writer) Flush(ctx context.Context) error {
	if len(w.buf) == 0 {
		return nil
	}
	rows := w.buf
	w.buf = nil
	w.lastAt = time.Now()

	w.sem <- struct{}{} // block if maxInflight reached => backpressure
	go func() {
		defer func() { <-w.sem }()
		_ = w.insert(context.Background(), rows)
	}()
	return nil
}

func (w *Writer) insert(ctx context.Context, rows []*model.Span) error {
	batch, err := w.conn.PrepareBatch(ctx, `INSERT INTO spans`)
	if err != nil {
		return err
	}
	for _, s := range rows {
		if err := batch.Append(
			s.TraceID, s.SpanID, s.ParentSpanID, s.ServiceName, s.SpanName,
			string(s.Kind), s.StartTime(), s.DurationNs(), string(s.StatusCode),
			s.StatusMsg, s.Attributes, time.Unix(0, int64(s.IngestedNs)),
		); err != nil {
			return err
		}
	}
	return batch.Send()
}
