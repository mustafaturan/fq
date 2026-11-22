---
name: fq-usage
description: >-
  Guides integration, configuration, operation, metrics export, and
  troubleshooting of the github.com/mustafaturan/fq disk-backed FIFO queue in
  Go applications. Use when adding fq to a Go project, configuring fq.Params or
  YAML settings, implementing producers/consumers with Push/PushN/Pop/PopN/Stats/Close,
  exposing Prometheus metrics via WritePrometheus, handling fq sentinel errors,
  tuning MaxFileSize/PersistInterval, or wiring slog/zerolog logging.
---

# Using `github.com/mustafaturan/fq`

`fq` is a zero-dependency, disk-backed, partitioned FIFO queue for Go. A single `fq.FileQueue` instance manages up to 256 independent queues (`uint8` IDs) within a single OS process.

For complete type definitions, error tables, and on-disk binary layouts, read [references/api.md](./references/api.md).

## Integration Checklist

1. **Ensure the storage directory exists before calling `fq.New`**:
   `fq.New` validates that `Params.Path` is an existing directory and does not create it automatically. Call `os.MkdirAll(dir, 0o700)` during startup.
2. **Define typed queue ID constants**:
   Use explicit `uint8` constants (e.g., `const queueEvents uint8 = 1`) with unique, filename-safe `Name` values (no `/`, `.`, or `..`).
3. **Wire structured logging directly with `log/slog` (or wrap `zerolog`)**:
   Pass `*slog.Logger` (e.g., `slog.Default()`) to `Params.Logger` — no adapter required. Leave `nil` to silence logs. Any custom `fq.Logger` implementation must be safe for concurrent use.

   To wire `github.com/rs/zerolog`:
   ```go
   type zerologAdapter struct{ l zerolog.Logger }

   func (z zerologAdapter) Info(msg string, args ...any)  { z.log(z.l.Info(), msg, args) }
   func (z zerologAdapter) Error(msg string, args ...any) { z.log(z.l.Error(), msg, args) }

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
   // Params{Path: dir, Logger: zerologAdapter{l: log.Logger}, Queues: ...}
   ```
4. **Always close cleanly on shutdown**:
   `defer q.Close(ctx)` (or call during graceful shutdown). `Close` is idempotent, stops the background metadata loop, syncs open partitions, and atomically persists final cursors.
5. **Handle sentinel errors with `errors.Is`**:
   All exported errors are immutable `const` values of type `fq.Error` prefixed with `"fq: "`. Never compare errors with `==` across wrapped call chains; use `errors.Is`.

## Producer Pattern (`Push` & `PushN`)

- `Push` validates `ctx.Err()`, rejects empty slices (`fq.ErrEmptyData`), and rejects payloads larger than `16,777,215` bytes (`fq.ErrPayloadTooLarge`).
- `Push` writes synchronously and **never retains** the caller's `data` slice, so producers can safely reuse a scratch buffer (`buf = buf[:0]`) across calls.
- **`PushN(ctx, qID, items)`** validates all elements upfront, batches contiguous records up to 64 KiB into single `WriteAt` syscalls under one lock (`0 allocs/op`), and returns `(pushed, err)`. If an I/O error occurs mid-batch, `items[:pushed]` are already committed so callers can retry `items[pushed:]`.
- For high-throughput struct encoding, also load the `fq-serialization` skill.

```go
if err := q.Push(ctx, queueEvents, payload); err != nil {
	if errors.Is(err, fq.ErrClosed) {
		return nil // shutting down
	}
	return fmt.Errorf("enqueue event: %w", err)
}
```

## Consumer Pattern (`Pop` & `PopN`)

`Pop` and `PopN` are **non-blocking**, goroutine-safe, and return `fq.ErrEmpty` when zero items remain. Every item is delivered to exactly one caller, so `N` concurrent workers can call `Pop` or `PopN` directly without a dispatcher channel. Drive idle workers with a buffered wake-up signal channel (notified non-blockingly after `Push`/`PushN`) plus a fallback ticker:

```go
// Producer side: wake an idle consumer without blocking Push.
func signalConsumer(notify chan<- struct{}) {
	select {
	case notify <- struct{}{}:
	default:
	}
}

// Consumer pool: N concurrent workers draining qID until ctx cancels or q closes.
func runConsumers(
	ctx context.Context,
	q fq.FileQueue,
	qID uint8,
	workers int,
	notify <-chan struct{},
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
					slog.Error("dequeue failed", "worker", workerID, "err", err)
					return
				}
			}
		})
	}
	wg.Wait()
}
```

- **Strict ordering vs. parallelism**: With `workers > 1`, items are dequeued in FIFO order while handlers run concurrently. For strict per-key ordering, shard across multiple `qID`s with 1 worker per `qID`.
- **Buffer `notify` to `workers` (or `1`)**: Sizing `notify := make(chan struct{}, workers)` wakes multiple idle workers immediately during bursts.
- **Batch popping with `PopN(ctx, qID, n)`**: Use `PopN` (e.g., `n = 32..256`) under heavy contention or for downstream bulk inserts/flushes. `PopN` returns partial batches with `err == nil` when fewer than `n` items exist, and returns any already-popped items alongside an error if a mid-batch I/O error occurs. Always process `len(batch) > 0` before handling `err`. Small payloads share 32 KiB slabs via append-safe 3-index slices (`cap == len`); copy an element if retaining it long-term outside the batch lifecycle.

## Configuration & Tuning

`fq.Params` uses **camelCase** YAML tags:

```yaml
path: /var/lib/myapp/queues
maxFileSize: 104857600 # optional; default 100 MiB
persistInterval: 1s    # optional; default 1s
queues:
  - id: 1
    name: events
```

- **`MaxFileSize`**: Defaults to `100 MiB`. Consumed partitions are deleted immediately on rollover, and open FDs stay bounded to `<= 2` per queue. Use smaller partitions (e.g., `16 MiB`–`64 MiB`) if you want disk space reclaimed more frequently on steady backlogs.
- **`PersistInterval`**: Defaults to `1s`. Background metadata `fsync` runs under an isolated `metaMu` lock and does **not** stall `Push` or `Pop`.

## Metrics & Prometheus

Call `q.Stats()` for a thread-safe `[]fq.QueueStats` snapshot, or write Prometheus v0.0.4 text format directly with `fq.WritePrometheus(w, q.Stats())`:

```go
http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", fq.PrometheusContentType)
	_ = fq.WritePrometheus(w, q.Stats())
})
```

Alert on `fq_corruptions_total`, `fq_metadata_persist_errors_total`, `fq_push_errors_total`, `fq_pop_errors_total`, and sustained growth in `fq_pending_bytes` / `fq_pending_partitions`.

## Pitfalls to Avoid

- **Do not share a `Path` across multiple OS processes**: `fq` is safe for concurrent goroutines within one process, but does not acquire `flock`.
- **Do not push empty payloads**: `len(data) == 0` returns `fq.ErrEmptyData` because `0x000000` is reserved as the unwritten/corrupt header sentinel.
- **Account for the unclean crash window**: `Push` writes to the OS page cache without per-message `fsync`; `Close` performs a clean sync and metadata persist, while an abrupt `SIGKILL`/power loss can lose up to `PersistInterval` of cursor updates.
- **Check file permissions**: `.bin` and `.db<N>` files are created with mode `0o600` (owner read/write only). Ensure the service runs under a consistent OS user across restarts.

## Verification

After integrating `fq`, verify the consumer project with:

```bash
go test -race ./...
```

Ensure tests cover:
- Empty queue (`errors.Is(err, fq.ErrEmpty)`)
- Persistence across `Close` and reopen with `fq.New`
- Graceful worker termination on `fq.ErrClosed` and canceled `context.Context`

