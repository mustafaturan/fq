# `fq` API & On-Disk Reference

## Exported Types

```go
type FileQueue interface {
	Push(ctx context.Context, qID uint8, data []byte) error
	PushN(ctx context.Context, qID uint8, items [][]byte) (int, error)
	Pop(ctx context.Context, qID uint8) ([]byte, error)
	PopN(ctx context.Context, qID uint8, n int) ([][]byte, error)
	Stats() []QueueStats
	Close(ctx context.Context)
}

const PrometheusContentType = "text/plain; version=0.0.4; charset=utf-8"

func WritePrometheus(w io.Writer, stats []QueueStats) error

type QueueStats struct {
	ID                         uint8
	Name                       string
	PushTotal                  uint64
	PopTotal                   uint64
	PushBytesTotal             uint64
	PopBytesTotal              uint64
	PopEmptyTotal              uint64
	PendingBytes               int64
	PendingPartitions          int32
	AppendFileIndex            int32
	ReadFileIndex              int32
	AppendOffset               int32
	ReadOffset                 int32
	PartitionRolloversTotal    uint64
	PartitionsDeletedTotal     uint64
	ReadCacheHitsTotal         uint64
	ReadCacheMissesTotal       uint64
	MetadataPersistsTotal      uint64
	PushErrorsTotal            uint64
	PopErrorsTotal             uint64
	CorruptionsTotal           uint64
	MetadataPersistErrorsTotal uint64
}

type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type Params struct {
	Path            string        `yaml:"path"`
	Queues          []QueueConfig `yaml:"queues"`
	MaxFileSize     int64         `yaml:"maxFileSize"`     // default: 100 MiB; range [1 MiB, ~2.13 GiB]
	PersistInterval time.Duration `yaml:"persistInterval"` // default: 1s; must be >= 0
	Logger          Logger        `yaml:"-"`               // optional; *slog.Logger satisfies directly
}

type QueueConfig struct {
	ID   uint8  `yaml:"id"`
	Name string `yaml:"name"`
}

func New(p Params) (FileQueue, error)
```

## Constant Sentinel Errors (`fq.Error`)

| Constant | String Value | Trigger |
| :--- | :--- | :--- |
| `ErrEmpty` | `"fq: queue is empty"` | `Pop` (or `PopN` when zero items remain) |
| `ErrNil` | `"fq: queue is nil"` | `Push`/`PushN`/`Pop`/`PopN` with unconfigured `qID` |
| `ErrClosed` | `"fq: queue is closed"` | `Push`/`PushN`/`Pop`/`PopN` after `Close` |
| `ErrEmptyData` | `"fq: payload cannot be empty"` | `Push`/`PushN` with `len(data) == 0` |
| `ErrPayloadTooLarge` | `"fq: payload exceeds maximum size"` | `Push`/`PushN` with `len(data) > 16,777,215` |
| `ErrInvalidConfig` | `"fq: invalid configuration"` | Invalid `Path`, `MaxFileSize`, `PersistInterval`, or duplicate/unsafe queue `ID`/`Name` |
| `ErrInvalidMetadataSize` | `"fq: invalid metadata size"` | `<name>.bin` is not exactly 16 bytes |
| `ErrFileRead` | `"fq: failed reading from file"` | Partition `ReadAt` failure |
| `ErrCorrupted` | `"fq: corrupted queue data"` | Negative/inverted metadata cursors, zero-length header, or record length exceeding `AppendOffset` |
| `ErrEOF` | `"fq: end of file"` | Partition logical EOF sentinel |

## On-Disk Binary Format (`0o600` Permissions)

### Metadata (`<path>/<name>.bin`, 16 bytes, big-endian)

| Byte Range | Field | Type |
| :--- | :--- | :--- |
| `0..3` | `AppendFileIndex` | `int32` (`uint32` wire) |
| `4..7` | `ReadFileIndex` | `int32` (`uint32` wire) |
| `8..11` | `AppendOffset` | `int32` (`uint32` wire) |
| `12..15` | `ReadOffset` | `int32` (`uint32` wire) |

Written atomically via temp file (`.fq-meta-*.tmp`) → `Write` → `Sync` → `Chmod(0o600)` → `Close` → `os.Rename`.

### Partition (`<path>/<name>.db<N>`)

Pre-allocated sparse file (`Truncate(MaxFileSize)`). Contiguous records:

```text
[3-byte big-endian payload length (1..16,777,215)][payload bytes]
```

- Rollover condition (symmetric on writer and reader): `nextOffset + 3 > MaxFileSize`.
- Consumed partitions are closed and removed immediately upon reader rollover.
- At most 2 file descriptors (`appendFM` and `readFM`) remain open per queue.

