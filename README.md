# fq

`fq` is a fast, persistent, disk-backed, partitioned FIFO queue for Go with **zero external dependencies**.

## Features

- **Zero dependencies** — built solely on the Go standard library.
- **Partitioned storage** — appends length-prefixed records into sequential partition files (`<name>.db<N>`, default 100 MiB) with automatic rollover and immediate deletion of consumed partitions.
- **Bounded resources** — keeps at most 2 open file descriptors per queue (`O(1)`) regardless of backlog size.
- **High throughput, minimal allocations** — `0 allocs/op` on `Push` (64 KiB reusable scratch buffer, zero-copy write path for larger payloads), 64 KiB read-ahead buffering on `Pop`, direct `[256]*queue` dispatch, and a dedicated metadata lock so background `fsync` never stalls `Push`/`Pop`.
- **Atomic metadata persistence** — queue cursors are persisted to `<name>.bin` via temp-file + `fsync` + atomic `rename` (`0o600` permissions).
- **Concurrency-safe** — safe for concurrent `Push`, `Pop`, `Stats`, and `Close` calls across multiple goroutines within a single process.
- **Prometheus-ready metrics** — `Stats() []QueueStats` snapshot API plus zero-dependency `WritePrometheus` v0.0.4 text exposition formatter.
- **Pluggable logging** — accepts any `fq.Logger` (the standard library `*slog.Logger` satisfies it directly); silent by default when `nil`.
- **Idiomatic constant errors** — exported sentinel errors are immutable `const` values of type `fq.Error` compatible with `errors.Is`.

## Installation

```bash
go get github.com/mustafaturan/fq
```

## Quick Start

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/mustafaturan/fq"
)

const queueEvents uint8 = 1

func main() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "fq-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	q, err := fq.New(fq.Params{
		Path:   dir,
		Logger: slog.Default(), // optional; nil disables logging
		Queues: []fq.QueueConfig{
			{ID: queueEvents, Name: "events"},
		},
	})
	if err != nil {
		panic(err)
	}
	defer q.Close(ctx)

	if err := q.Push(ctx, queueEvents, []byte("hello world")); err != nil {
		panic(err)
	}

	item, err := q.Pop(ctx, queueEvents)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(item)) // hello world

	if _, err := q.Pop(ctx, queueEvents); errors.Is(err, fq.ErrEmpty) {
		fmt.Println("queue is empty")
	}
}
```

### Concurrent Consumer Pool (`N` Workers)

Because `Pop` is goroutine-safe and delivers each item to exactly one caller, `N` workers can consume directly from the same queue without an extra dispatcher channel:

```go
func runConsumers(
	ctx context.Context,
	q fq.FileQueue,
	qID uint8,
	workers int,
	notify <-chan struct{}, // buffered chan notified after Push: select { case notify <- struct{}{}: default: }
	handle func(context.Context, []byte) error,
) {
	var wg sync.WaitGroup
	for workerID := range workers {
		wg.Go(func() {
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()

			for {
				data, err := q.Pop(ctx, qID)
				switch {
				case err == nil:
					if err := handle(ctx, data); err != nil {
						slog.Error("handle message failed", "worker", workerID, "err", err)
					}
				case errors.Is(err, fq.ErrEmpty):
					select {
					case <-ctx.Done():
						return
					case <-notify:
					case <-ticker.C:
					}
				case errors.Is(err, fq.ErrClosed), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
					return
				default:
					slog.Error("pop failed", "worker", workerID, "err", err)
					return
				}
			}
		})
	}
	wg.Wait()
}
```

> **Ordering note**: Items are dequeued in strict FIFO order, but with `workers > 1` handlers execute concurrently. If strict end-to-end ordering is required for a stream, use `1` worker per `qID` (or shard keys across multiple `qID`s with 1 worker each).

## Configuration

`fq.Params` supports camelCase YAML tags for config unmarshaling:

```yaml
path: /var/lib/myapp/queues
maxFileSize: 104857600   # optional; default 100 MiB (100 * 1024 * 1024)
persistInterval: 1s      # optional; default 1s
queues:
  - id: 1
    name: events
  - id: 2
    name: webhooks
```

| Field | YAML Tag | Default | Description |
| :--- | :--- | :--- | :--- |
| `Path` | `path` | *(required)* | Existing directory for partition and metadata files. |
| `Queues` | `queues` | `nil` | Queue definitions; each `ID` (`uint8`) and `Name` must be unique and non-empty (no path separators). |
| `MaxFileSize` | `maxFileSize` | `100 MiB` | Target partition file size in bytes (range: `1 MiB` to ~`2.13 GiB`). |
| `PersistInterval` | `persistInterval` | `1s` | Interval for background atomic metadata persistence. |
| `Logger` | `-` | no-op | Optional structured logger (`Info(msg string, args ...any)`, `Error(msg string, args ...any)`). `*slog.Logger` implements this directly and must be safe for concurrent use. |

### Using `zerolog` with `fq.Logger`

`*slog.Logger` satisfies `fq.Logger` out of the box. To wire [`github.com/rs/zerolog`](https://github.com/rs/zerolog) without adding a dependency to `fq`, wrap `zerolog.Logger` in a small adapter:

```go
import (
	"github.com/mustafaturan/fq"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type zerologAdapter struct {
	l zerolog.Logger
}

func (z zerologAdapter) Info(msg string, args ...any) {
	z.log(z.l.Info(), msg, args)
}

func (z zerologAdapter) Error(msg string, args ...any) {
	z.log(z.l.Error(), msg, args)
}

func (z zerologAdapter) log(ev *zerolog.Event, msg string, args []any) {
	if !ev.Enabled() {
		return
	}
	for i := 0; i+1 < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			continue
		}
		if err, isErr := args[i+1].(error); isErr && key == "err" {
			ev.Err(err)
			continue
		}
		ev.Any(key, args[i+1])
	}
	ev.Msg(msg)
}

// Usage:
q, err := fq.New(fq.Params{
	Path:   dir,
	Logger: zerologAdapter{l: log.Logger},
	Queues: []fq.QueueConfig{{ID: 1, Name: "events"}},
})
```

## On-Disk Layout

All files are created with mode `0o600` (owner read/write only):

- **Metadata file (`<name>.bin`)** — fixed 16 bytes (`4 × uint32`, big-endian):
  ```text
  [0..3]   AppendFileIndex (int32)
  [4..7]   ReadFileIndex   (int32)
  [8..11]  AppendOffset    (int32)
  [12..15] ReadOffset      (int32)
  ```
- **Partition files (`<name>.db<N>`)** — pre-allocated sparse files storing contiguous framed records:
  ```text
  [3-byte big-endian payload length][payload bytes]
  ```
  Maximum single payload size is `16,777,215` bytes (~16 MiB - 1). Empty payloads are rejected (`ErrEmptyData`).

## Sentinel Errors

All exported errors are constants of type `fq.Error` with the `"fq: "` prefix and work with `errors.Is`:

| Error | Description |
| :--- | :--- |
| `ErrEmpty` | Queue has no unread items (`Pop`). |
| `ErrNil` | Referenced `qID` is not configured. |
| `ErrClosed` | Operation called after `Close`. |
| `ErrEmptyData` | Zero-length payload passed to `Push`. |
| `ErrPayloadTooLarge` | Payload exceeds `16,777,215` bytes. |
| `ErrInvalidConfig` | Invalid `Params` passed to `New`. |
| `ErrInvalidMetadataSize` | Metadata file is not exactly 16 bytes. |
| `ErrFileRead` | Underlying partition file read error. |
| `ErrCorrupted` | Corrupt cursor metadata or partition framing detected. |
| `ErrEOF` | Logical end of partition file. |

## Metrics & Prometheus

`q.Stats()` returns a `[]fq.QueueStats` snapshot ordered by queue ID (zero external dependencies, safe for concurrent use and after `Close`).

### Built-In Prometheus Exposition (`net/http`)

Use `fq.WritePrometheus` to serve metrics in standard Prometheus text exposition format (`v0.0.4`) directly without pulling in `prometheus/client_golang`:

```go
http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", fq.PrometheusContentType)
	if err := fq.WritePrometheus(w, q.Stats()); err != nil {
		slog.Error("write metrics failed", "err", err)
	}
})
```

### Exposed Metrics (labeled with `queue="<name>"`)

| Metric | Type | Description |
| :--- | :--- | :--- |
| `fq_push_total` | Counter | Total records pushed |
| `fq_pop_total` | Counter | Total records popped |
| `fq_push_bytes_total` | Counter | Total payload bytes pushed |
| `fq_pop_bytes_total` | Counter | Total payload bytes popped |
| `fq_pop_empty_total` | Counter | `Pop` calls returning `ErrEmpty` |
| `fq_pending_bytes` | Gauge | Estimated unread bytes queued on disk |
| `fq_pending_partitions` | Gauge | Unconsumed partitions behind active append partition |
| `fq_append_file_index` / `fq_read_file_index` | Gauge | Active write and read partition indices |
| `fq_append_offset_bytes` / `fq_read_offset_bytes` | Gauge | Active write and read byte offsets |
| `fq_partition_rollovers_total` | Counter | New partitions created on write rollover |
| `fq_partitions_deleted_total` | Counter | Consumed/discarded partitions removed from disk |
| `fq_read_cache_hits_total` / `fq_read_cache_misses_total` | Counter | 64 KiB read-ahead buffer hits and misses |
| `fq_metadata_persists_total` / `fq_metadata_persist_errors_total` | Counter | Atomic metadata write successes and failures |
| `fq_push_errors_total` / `fq_pop_errors_total` | Counter | Failed `Push` and `Pop` calls (excluding `ErrEmpty`) |
| `fq_corruptions_total` | Counter | Corrupted partition records detected |

## Concurrency & Durability

- **In-process concurrency**: All `FileQueue` methods (`Push`, `Pop`, `Stats`, `Close`) are safe for concurrent goroutines within a single process. `Close` is idempotent.
- **Single-process ownership**: `fq` does not acquire cross-process file locks (`flock`); do not open the same `Path` from multiple OS processes simultaneously.
- **Durability model**: `Push` writes directly to the OS page cache without per-message `fsync`. Metadata cursors are flushed atomically every `PersistInterval` (default `1s`), and `Close` syncs open partition files and writes final metadata cleanly.

## Serialization & Performance Tips

`fq` stores raw `[]byte` payloads (`Push` allocates `0 B/op`, `Pop` serves sequential small reads from a 64 KiB read-ahead buffer). To avoid letting serialization dominate CPU and disk usage:

- Prefer compact binary codecs (`encoding/binary`, `tinylib/msgp`, Protobuf/`vtprotobuf`, FlatBuffers, or Cap'n Proto) over `encoding/gob` or reflection-heavy `encoding/json`.
- **Reuse encode buffers on `Push`**: `Push` writes synchronously and never retains the caller's slice, so reusing a per-worker or `sync.Pool` `[]byte` (`buf = buf[:0]`) keeps publishing at `0 allocs/op`.
- **Version your payloads**: Items persist across restarts and deployments—prefix custom binary records with a version byte so newer binaries can drain older records safely.

### Zero-Allocation `encoding/binary` Example

```go
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/mustafaturan/fq"
)

const (
	eventVersionV1  byte = 1
	eventHeaderSize      = 1 + 8 + 8 + 1 + 4 // version + ID + Timestamp + Kind + payload len
)

type Event struct {
	ID        int64
	Timestamp int64
	Kind      uint8
	Payload   []byte
}

// AppendBinary encodes e into dst without allocating when cap(dst) is sufficient.
// Implements encoding.BinaryAppender.
func (e Event) AppendBinary(dst []byte) ([]byte, error) {
	dst = slices.Grow(dst, eventHeaderSize+len(e.Payload))
	dst = append(dst, eventVersionV1)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.ID))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.Timestamp))
	dst = append(dst, e.Kind)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(e.Payload)))
	dst = append(dst, e.Payload...)
	return dst, nil
}

// UnmarshalBinary decodes b into e, slicing e.Payload directly from b (zero-copy).
// Safe with fq.Pop because Pop returns a freshly allocated slice owned by the caller.
func (e *Event) UnmarshalBinary(b []byte) error {
	if len(b) < eventHeaderSize {
		return errors.New("event: truncated header")
	}
	if b[0] != eventVersionV1 {
		return fmt.Errorf("event: unsupported version %d", b[0])
	}

	n := int(binary.LittleEndian.Uint32(b[18:22]))
	if len(b)-eventHeaderSize < n {
		return errors.New("event: truncated payload")
	}

	e.ID = int64(binary.LittleEndian.Uint64(b[1:9]))
	e.Timestamp = int64(binary.LittleEndian.Uint64(b[9:17]))
	e.Kind = b[17]
	e.Payload = b[eventHeaderSize : eventHeaderSize+n]
	return nil
}

func pushAndPopEvents(ctx context.Context, q fq.FileQueue, qID uint8) error {
	// Reuse one scratch buffer across Push calls for 0 allocs/op.
	buf := make([]byte, 0, 256)
	for i := range int64(10) {
		ev := Event{
			ID:        i,
			Timestamp: time.Now().UnixNano(),
			Kind:      1,
			Payload:   []byte("order-created"),
		}
		var err error
		buf, err = ev.AppendBinary(buf[:0])
		if err != nil {
			return err
		}
		if err := q.Push(ctx, qID, buf); err != nil {
			return err
		}
	}

	var out Event
	for {
		raw, err := q.Pop(ctx, qID)
		if errors.Is(err, fq.ErrEmpty) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := out.UnmarshalBinary(raw); err != nil {
			return err
		}
		fmt.Printf("id=%d kind=%d payload=%s\n", out.ID, out.Kind, out.Payload)
	}
}
```

## Benchmarks

Measured on Apple M2 (`darwin/arm64`, Go 1.27.4, `go test -run='^$' -bench=. -benchmem -benchtime=250ms -count=3 .`, medians):

| Benchmark | Latency | Throughput | Memory | Allocs |
| :--- | ---: | ---: | ---: | ---: |
| `Push/64B` | `1180 ns/op` | `54.3 MB/s` | `0 B/op` | `0` |
| `Push/1KiB` | `1681 ns/op` | `609 MB/s` | `0 B/op` | `0` |
| `Push/128KiB` | `79.7 µs/op` | `1.65 GB/s` | `0 B/op` | `0` |
| `Pop/64B` | `40.4 ns/op` | `1.58 GB/s` | `64 B/op` | `1` |
| `Pop/1KiB` | `214.0 ns/op` | `4.78 GB/s` | `1024 B/op` | `1` |
| `PushPop/64B` | `1596 ns/op` | `40.1 MB/s` | `64 B/op` | `1` |
| `PushPop/1KiB` | `2325 ns/op` | `440 MB/s` | `1024 B/op` | `1` |
| `PushPopParallel` | `2188 ns/op` | `29.3 MB/s` | `64 B/op` | `1` |

## Development

```bash
# Run tests with race detector and coverage
go test -race -cover ./...

# Run static checks
go vet ./...
golangci-lint run ./...

# Run benchmarks
go test -run='^$' -bench=. -benchmem ./...
```

## Agent Skills

Reusable AI coding agent skills (`fq-usage` and `fq-serialization`) are included under `.agents/skills/`. See [AGENTS.md](./AGENTS.md) for installation instructions across Antigravity, Claude Code, Codex, Cursor, and `npx skills`.
