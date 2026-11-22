// Package fq implements a persistent, disk-backed, partitioned FIFO queue.
//
// Each configured queue stores length-prefixed records across sequential partition
// files (<name>.db<N>) and persists its read/write cursors atomically to a 16-byte
// metadata file (<name>.bin). All exported operations on FileQueue are safe for
// concurrent use by multiple goroutines within a single process.
package fq

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mustafaturan/fq/internal/fm"
)

const (
	// initialFileSize is the default target size for each partition file (100 MiB).
	initialFileSize = 100 * 1024 * 1024

	// cfgSizeHeaderBytes is the byte width of the big-endian per-record length header.
	cfgSizeHeaderBytes = 3

	// maxPayloadSize is the largest single payload representable by cfgSizeHeaderBytes (~16 MiB - 1).
	maxPayloadSize = (1 << (cfgSizeHeaderBytes * 8)) - 1

	// minFileSize is the smallest permitted partition target size (1 MiB).
	minFileSize = 1024 * 1024

	// maxFileSizeLimit ensures AppendOffset never overflows int32 after writing a max-size payload.
	maxFileSizeLimit = math.MaxInt32 - maxPayloadSize - cfgSizeHeaderBytes

	// maxWriteBufSize caps the per-queue reusable scratch buffer used for single-write framing (64 KiB).
	// Larger payloads write payload and header directly to avoid copying large buffers.
	maxWriteBufSize = 64 * 1024

	// readAheadBufSize is the size of the per-queue read-ahead buffer (64 KiB).
	readAheadBufSize = 64 * 1024

	// maxPopSlabSize caps per-slab allocations in PopN at 32 KiB (Go's per-P mcache
	// small-object ceiling) to avoid global mheap span locking on batch pops.
	maxPopSlabSize = 32 * 1024

	// popBatchInitCap bounds initial slice pre-allocation in PopN so very large n
	// values do not over-allocate when fewer items exist.
	popBatchInitCap = 256

	// metadataSize is the fixed byte length of the serialized queue metadata (4 x uint32).
	metadataSize = 16

	// defaultPersistInterval controls how often background metadata persistence runs by default.
	defaultPersistInterval = time.Second

	// metadataFileFormat is the filename template for queue cursor metadata files.
	metadataFileFormat = "%s.bin"

	// partitionFileFormat is the filename template for queue partition data files.
	partitionFileFormat = "%s.db%d"

	// tempMetadataPattern is the os.CreateTemp pattern for atomic metadata writes.
	tempMetadataPattern = ".fq-meta-*.tmp"

	fileMode os.FileMode = 0o600
)

// Error represents a constant sentinel error returned by package fq.
type Error string

// Error implements the error interface.
func (e Error) Error() string { return string(e) }

const (
	// ErrEOF indicates that a partition file has reached its logical end.
	ErrEOF Error = "fq: end of file"
	// ErrEmpty is returned by Pop when a queue has no unread items.
	ErrEmpty Error = "fq: queue is empty"
	// ErrNil is returned when an operation references an unconfigured queue ID.
	ErrNil Error = "fq: queue is nil"
	// ErrClosed is returned when Push or Pop is called on a closed queue.
	ErrClosed Error = "fq: queue is closed"
	// ErrInvalidMetadataSize is returned when a metadata file does not match the expected size.
	ErrInvalidMetadataSize Error = "fq: invalid metadata size"
	// ErrFileRead is returned when reading from a partition file fails.
	ErrFileRead Error = "fq: failed reading from file"
	// ErrInvalidConfig is returned by New when Params contains invalid settings.
	ErrInvalidConfig Error = "fq: invalid configuration"
	// ErrEmptyData is returned by Push when a zero-length payload is provided.
	ErrEmptyData Error = "fq: payload cannot be empty"
	// ErrPayloadTooLarge is returned by Push when a payload exceeds maxPayloadSize.
	ErrPayloadTooLarge Error = "fq: payload exceeds maximum size"
	// ErrCorrupted is returned when corrupt metadata or partition framing is detected.
	ErrCorrupted Error = "fq: corrupted queue data"
)

// Logger defines the structured logging interface used by package fq.
// The standard library *slog.Logger satisfies this interface directly.
// Implementations must be safe for concurrent use.
type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

// FileQueue defines the operations for pushing to, popping from, inspecting, and closing queues.
type FileQueue interface {
	Push(ctx context.Context, qID uint8, data []byte) error
	PushN(ctx context.Context, qID uint8, items [][]byte) (int, error)
	Pop(ctx context.Context, qID uint8) ([]byte, error)
	PopN(ctx context.Context, qID uint8, n int) ([][]byte, error)
	Stats() []QueueStats
	Close(ctx context.Context)
}

// Params configures a new FileQueue instance.
type Params struct {
	// Path is the existing directory where partition and metadata files are stored.
	Path string `yaml:"path"`
	// Queues lists the queues to initialize. IDs and names must be unique and non-empty.
	Queues []QueueConfig `yaml:"queues"`
	// MaxFileSize optionally overrides the target partition file size in bytes.
	// When zero, initialFileSize (100 MiB) is used.
	MaxFileSize int64 `yaml:"maxFileSize"`
	// PersistInterval optionally overrides how frequently background metadata persistence runs.
	// When zero, defaultPersistInterval (1s) is used.
	PersistInterval time.Duration `yaml:"persistInterval"`
	// Logger optionally receives structured operational and error logs.
	// When nil, logging is discarded.
	Logger Logger `yaml:"-"`
}

// QueueConfig defines the identity and file prefix of a single queue.
type QueueConfig struct {
	ID   uint8  `yaml:"id"`
	Name string `yaml:"name"`
}

type nopLogger struct{}

func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

type fileStore interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
	Path() string
	Sync() error
}

type tempFile interface {
	io.WriteCloser
	Name() string
	Sync() error
	Chmod(mode os.FileMode) error
}

type fileQueue struct {
	path            string
	maxFileSize     int64
	persistInterval time.Duration
	logger          Logger
	queues          [256]*queue
	queueIDs        []uint8

	stopCh    chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
}

type queue struct {
	AppendFileIndex int32
	ReadFileIndex   int32

	AppendOffset int32
	ReadOffset   int32

	closed bool
	name   string

	// appendFM and readFM always correspond to AppendFileIndex and ReadFileIndex
	// respectively; they reference the same fileStore when both indices are equal.
	appendFM fileStore
	readFM   fileStore

	// Per-queue scratch and read-ahead buffers guarded by mu.
	writeBuf   []byte
	hdrBuf     [cfgSizeHeaderBytes]byte
	readBuf    []byte
	readBufOff int32
	readBufLen int32

	// Hot-path metrics guarded by mu.
	pushTotal               uint64
	popTotal                uint64
	pushBytesTotal          uint64
	popBytesTotal           uint64
	popEmptyTotal           uint64
	partitionRolloversTotal uint64
	partitionsDeletedTotal  uint64
	readCacheHitsTotal      uint64
	readCacheMissesTotal    uint64
	corruptionsTotal        uint64

	// Metrics that may be incremented outside mu.
	pushErrorsTotal            atomic.Uint64
	popErrorsTotal             atomic.Uint64
	metadataPersistsTotal      atomic.Uint64
	metadataPersistErrorsTotal atomic.Uint64

	// Lock ordering: metaMu (outer, serializes metadata disk writes) -> mu (inner, hot path).
	metaMu sync.Mutex
	mu     sync.Mutex
}

// New validates p, initializes or restores all configured queues from disk,
// starts the background metadata persistence loop, and returns a ready FileQueue.
func New(p Params) (FileQueue, error) {
	if p.Path == "" {
		return nil, fmt.Errorf("%w: path is required", ErrInvalidConfig)
	}

	pathInfo, err := os.Stat(p.Path)
	if err != nil {
		return nil, fmt.Errorf("fq: failed to stat path %q: %w", p.Path, err)
	}
	if !pathInfo.IsDir() {
		return nil, fmt.Errorf("%w: path %q is not a directory", ErrInvalidConfig, p.Path)
	}

	maxFileSize := p.MaxFileSize
	if maxFileSize == 0 {
		maxFileSize = initialFileSize
	} else if maxFileSize < minFileSize || maxFileSize > maxFileSizeLimit {
		return nil, fmt.Errorf("%w: maxFileSize %d out of range [%d, %d]", ErrInvalidConfig, maxFileSize, minFileSize, maxFileSizeLimit)
	}

	persistInterval := p.PersistInterval
	if persistInterval == 0 {
		persistInterval = defaultPersistInterval
	} else if persistInterval < 0 {
		return nil, fmt.Errorf("%w: persistInterval must be non-negative (%s)", ErrInvalidConfig, persistInterval)
	}

	logger := p.Logger
	if logger == nil {
		logger = nopLogger{}
	}

	var queues [256]*queue
	queueIDs := make([]uint8, 0, len(p.Queues))
	seenNames := make(map[string]struct{}, len(p.Queues))

	cleanupAll := func() {
		for _, id := range queueIDs {
			q := queues[id]
			if q.appendFM != nil {
				_ = q.appendFM.Close()
			}
			if q.readFM != nil && q.readFM != q.appendFM {
				_ = q.readFM.Close()
			}
		}
	}

	for _, qc := range p.Queues {
		if qc.Name == "" || filepath.Base(qc.Name) != qc.Name || qc.Name == "." || qc.Name == ".." {
			cleanupAll()
			return nil, fmt.Errorf("%w: invalid queue name %q", ErrInvalidConfig, qc.Name)
		}
		if queues[qc.ID] != nil {
			cleanupAll()
			return nil, fmt.Errorf("%w: duplicate queue ID %d", ErrInvalidConfig, qc.ID)
		}
		if _, exists := seenNames[qc.Name]; exists {
			cleanupAll()
			return nil, fmt.Errorf("%w: duplicate queue name %q", ErrInvalidConfig, qc.Name)
		}
		seenNames[qc.Name] = struct{}{}

		q, err := initQueue(p.Path, qc.Name, maxFileSize, logger)
		if err != nil {
			cleanupAll()
			return nil, err
		}

		queues[qc.ID] = q
		queueIDs = append(queueIDs, qc.ID)
	}

	slices.Sort(queueIDs)

	f := &fileQueue{
		path:            p.Path,
		maxFileSize:     maxFileSize,
		persistInterval: persistInterval,
		logger:          logger,
		queues:          queues,
		queueIDs:        queueIDs,
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
	}

	go f.metadataLoop()

	return f, nil
}

func initQueue(basePath, name string, maxFileSize int64, logger Logger) (*queue, error) {
	metaPath := metadataFilePath(basePath, name)
	eq := &queue{name: name}

	data, err := os.ReadFile(metaPath)
	switch {
	case err == nil:
		if err := eq.unmarshal(data); err != nil {
			logger.Error("failed to unmarshal metadata", "queue", name, "fpath", metaPath, "err", err)
			return nil, fmt.Errorf("%w: metadata %q", err, metaPath)
		}
	case errors.Is(err, os.ErrNotExist):
		meta := eq.marshal()
		if err := writeFileAtomic(metaPath, meta[:]); err != nil {
			return nil, err
		}
	default:
		logger.Error("failed to read metadata file", "queue", name, "fpath", metaPath, "err", err)
		return nil, fmt.Errorf("fq: failed to read metadata file %q: %w", metaPath, err)
	}

	appendPath := partitionFilePath(basePath, name, eq.AppendFileIndex)
	appendFM, err := fm.New(appendPath, maxFileSize)
	if err != nil {
		return nil, fmt.Errorf("fq: failed creating file manager for %q: %w", appendPath, err)
	}
	eq.appendFM = appendFM

	if eq.ReadFileIndex == eq.AppendFileIndex {
		eq.readFM = appendFM
		return eq, nil
	}

	readPath := partitionFilePath(basePath, name, eq.ReadFileIndex)
	readFM, err := fm.New(readPath, maxFileSize)
	if err != nil {
		_ = appendFM.Close()
		return nil, fmt.Errorf("fq: failed creating file manager for %q: %w", readPath, err)
	}
	eq.readFM = readFM

	return eq, nil
}

// Push appends data as a framed record to the queue identified by qID.
// It returns ErrNil for unknown queues, ErrClosed after Close, ErrEmptyData for
// empty payloads, and ErrPayloadTooLarge if len(data) exceeds maxPayloadSize.
func (f *fileQueue) Push(ctx context.Context, qID uint8, data []byte) error {
	q := f.queues[qID]
	if err := ctx.Err(); err != nil {
		if q != nil {
			q.pushErrorsTotal.Add(1)
		}
		return fmt.Errorf("fq: push canceled: %w", err)
	}
	if q == nil {
		return ErrNil
	}
	if len(data) == 0 {
		q.pushErrorsTotal.Add(1)
		return ErrEmptyData
	}
	if len(data) > maxPayloadSize {
		q.pushErrorsTotal.Add(1)
		return fmt.Errorf("%w: got %d bytes, max %d", ErrPayloadTooLarge, len(data), maxPayloadSize)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.pushErrorsTotal.Add(1)
		return ErrClosed
	}

	appendFM, err := f.ensureAppendFM(q)
	if err != nil {
		q.pushErrorsTotal.Add(1)
		return err
	}

	itemLen := cfgSizeHeaderBytes + len(data)
	nextOffset := int64(q.AppendOffset) + int64(itemLen)
	willRollover := nextOffset+cfgSizeHeaderBytes > f.maxFileSize

	var (
		nextIndex = q.AppendFileIndex + 1
		nextFM    fileStore
	)
	if willRollover {
		nextPath := partitionFilePath(f.path, q.name, nextIndex)
		nextFM, err = fm.New(nextPath, f.maxFileSize)
		if err != nil {
			q.pushErrorsTotal.Add(1)
			return fmt.Errorf("fq: failed creating partition %q: %w", nextPath, err)
		}
	}

	if err := q.writeRecord(appendFM, int64(q.AppendOffset), data); err != nil {
		if nextFM != nil {
			_ = nextFM.Close()
			_ = os.Remove(nextFM.Path())
		}
		q.pushErrorsTotal.Add(1)
		return fmt.Errorf("fq: failed appending to file: %w", err)
	}

	q.pushTotal++
	q.pushBytesTotal += uint64(len(data))

	if !willRollover {
		q.AppendOffset = int32(nextOffset)
		return nil
	}

	f.commitAppendRollover(qID, q, appendFM, nextFM, nextIndex, nextOffset)
	return nil
}

// PushN appends items in FIFO order to the queue identified by qID, batching
// contiguous records up to maxWriteBufSize into single partition writes under
// one lock acquisition. All items are validated upfront before writing.
//
// PushN returns the number of items written and any error encountered. When an
// I/O error occurs mid-batch across chunks or partition rollovers, the returned
// count reflects items already committed to the queue. An empty items slice on
// an open, valid queue returns (0, nil).
func (f *fileQueue) PushN(ctx context.Context, qID uint8, items [][]byte) (int, error) {
	q := f.queues[qID]
	if err := ctx.Err(); err != nil {
		if q != nil {
			q.pushErrorsTotal.Add(1)
		}
		return 0, fmt.Errorf("fq: push canceled: %w", err)
	}
	if q == nil {
		return 0, ErrNil
	}
	for i, data := range items {
		if len(data) == 0 {
			q.pushErrorsTotal.Add(1)
			return 0, fmt.Errorf("%w: item %d", ErrEmptyData, i)
		}
		if len(data) > maxPayloadSize {
			q.pushErrorsTotal.Add(1)
			return 0, fmt.Errorf("%w: item %d got %d bytes, max %d", ErrPayloadTooLarge, i, len(data), maxPayloadSize)
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.pushErrorsTotal.Add(1)
		return 0, ErrClosed
	}
	if len(items) == 0 {
		return 0, nil
	}

	if _, err := f.ensureAppendFM(q); err != nil {
		q.pushErrorsTotal.Add(1)
		return 0, err
	}

	pushed := 0
	for pushed < len(items) {
		appendFM := q.appendFM
		start := pushed
		endOffset := int64(q.AppendOffset)
		chunkBytes := 0
		chunkPayloadBytes := uint64(0)
		willRollover := false

		for pushed < len(items) {
			itemLen := cfgSizeHeaderBytes + len(items[pushed])
			if chunkBytes == 0 && itemLen > maxWriteBufSize {
				endOffset += int64(itemLen)
				chunkBytes = itemLen
				chunkPayloadBytes = uint64(len(items[pushed]))
				pushed++
				willRollover = endOffset+cfgSizeHeaderBytes > f.maxFileSize
				break
			}
			if chunkBytes > 0 && chunkBytes+itemLen > maxWriteBufSize {
				break
			}
			chunkBytes += itemLen
			endOffset += int64(itemLen)
			chunkPayloadBytes += uint64(len(items[pushed]))
			pushed++
			if endOffset+cfgSizeHeaderBytes > f.maxFileSize {
				willRollover = true
				break
			}
		}

		var (
			nextIndex = q.AppendFileIndex + 1
			nextFM    fileStore
			err       error
		)
		if willRollover {
			nextPath := partitionFilePath(f.path, q.name, nextIndex)
			nextFM, err = fm.New(nextPath, f.maxFileSize)
			if err != nil {
				q.pushErrorsTotal.Add(1)
				return start, fmt.Errorf("fq: failed creating partition %q: %w", nextPath, err)
			}
		}

		batch := items[start:pushed]
		if len(batch) == 1 && chunkBytes > maxWriteBufSize {
			err = q.writeRecord(appendFM, int64(q.AppendOffset), batch[0])
		} else {
			err = q.writeBatch(appendFM, int64(q.AppendOffset), batch, chunkBytes)
		}
		if err != nil {
			if nextFM != nil {
				_ = nextFM.Close()
				_ = os.Remove(nextFM.Path())
			}
			q.pushErrorsTotal.Add(1)
			return start, fmt.Errorf("fq: failed appending to file: %w", err)
		}

		q.pushTotal += uint64(len(batch))
		q.pushBytesTotal += chunkPayloadBytes

		if !willRollover {
			q.AppendOffset = int32(endOffset)
			continue
		}

		f.commitAppendRollover(qID, q, appendFM, nextFM, nextIndex, endOffset)
	}

	return pushed, nil
}

// commitAppendRollover advances q to nextFM at nextIndex and closes oldFM when
// it is no longer needed for reading. Caller must hold q.mu.
func (f *fileQueue) commitAppendRollover(qID uint8, q *queue, oldFM, nextFM fileStore, nextIndex int32, writtenEnd int64) {
	oldIndex := q.AppendFileIndex
	q.AppendFileIndex = nextIndex
	q.AppendOffset = 0
	q.appendFM = nextFM
	q.partitionRolloversTotal++

	f.logger.Info("created new queue partition",
		"qID", qID,
		"name", q.name,
		"rfi", q.ReadFileIndex,
		"ro", q.ReadOffset,
		"afi", q.AppendFileIndex,
		"ao", q.AppendOffset,
		"total", writtenEnd,
		"max", f.maxFileSize,
	)

	// Close intermediate partitions that are neither being written nor read to keep FDs O(1).
	if oldIndex != q.ReadFileIndex {
		if err := oldFM.Close(); err != nil {
			f.logger.Error("failed closing rolled partition", "queue", q.name, "index", oldIndex, "err", err)
		}
	}
}

// Pop removes and returns the next item in FIFO order from the queue identified by qID.
// It returns ErrNil for unknown queues, ErrClosed after Close, and ErrEmpty when no items remain.
func (f *fileQueue) Pop(ctx context.Context, qID uint8) ([]byte, error) {
	q := f.queues[qID]
	if err := ctx.Err(); err != nil {
		if q != nil {
			q.popErrorsTotal.Add(1)
		}
		return nil, fmt.Errorf("fq: pop canceled: %w", err)
	}
	if q == nil {
		f.logger.Error(ErrNil.Error(), "qID", qID)
		return nil, ErrNil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.popErrorsTotal.Add(1)
		f.logger.Error(ErrClosed.Error(), "qID", qID)
		return nil, ErrClosed
	}

	data, _, err := f.popOne(qID, q, nil, 1)
	if errors.Is(err, ErrEmpty) {
		q.popEmptyTotal++
		return nil, ErrEmpty
	}
	return data, err
}

// PopN removes and returns up to n items in FIFO order from the queue identified
// by qID under a single lock acquisition. Small payloads are packed into shared
// backing slabs using 3-index sub-slices (append-safe, reducing GC allocations).
//
// If fewer than n items are available (and at least one is popped), PopN returns
// those items with a nil error. ErrEmpty is returned only when zero items are
// available. If an I/O or corruption error occurs after one or more items have
// been popped, PopN returns the popped items alongside the error. If n <= 0 on
// an open, valid queue, PopN returns (nil, nil).
func (f *fileQueue) PopN(ctx context.Context, qID uint8, n int) ([][]byte, error) {
	q := f.queues[qID]
	if err := ctx.Err(); err != nil {
		if q != nil {
			q.popErrorsTotal.Add(1)
		}
		return nil, fmt.Errorf("fq: pop canceled: %w", err)
	}
	if q == nil {
		f.logger.Error(ErrNil.Error(), "qID", qID)
		return nil, ErrNil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.popErrorsTotal.Add(1)
		f.logger.Error(ErrClosed.Error(), "qID", qID)
		return nil, ErrClosed
	}
	if n <= 0 {
		return nil, nil
	}

	var (
		items [][]byte
		slab  []byte
	)
	for len(items) < n {
		var (
			data []byte
			err  error
		)
		data, slab, err = f.popOne(qID, q, slab, n-len(items))
		if err != nil {
			if errors.Is(err, ErrEmpty) {
				if len(items) == 0 {
					q.popEmptyTotal++
					return nil, ErrEmpty
				}
				return items, nil
			}
			return items, err
		}
		if items == nil {
			initCap := min(n, popBatchInitCap)
			if q.ReadFileIndex == q.AppendFileIndex {
				if avail := int(q.AppendOffset-q.ReadOffset)/(cfgSizeHeaderBytes+1) + 1; avail < initCap {
					initCap = avail
				}
			}
			items = make([][]byte, 0, initCap)
		}
		items = append(items, data)
	}
	return items, nil
}

// popOne removes and returns a single item from q. Caller must hold q.mu.
// It does not increment q.popEmptyTotal on ErrEmpty so PopN can distinguish
// an empty queue from a partial batch drain.
func (f *fileQueue) popOne(qID uint8, q *queue, slab []byte, remaining int) ([]byte, []byte, error) {
	for {
		if q.ReadFileIndex > q.AppendFileIndex || (q.ReadFileIndex == q.AppendFileIndex && q.ReadOffset >= q.AppendOffset) {
			return nil, slab, ErrEmpty
		}

		// Advance to the next partition when no header can fit in the remaining tail.
		if int64(q.ReadOffset)+cfgSizeHeaderBytes > f.maxFileSize {
			if err := f.advanceReadPartition(qID, q); err != nil {
				q.popErrorsTotal.Add(1)
				return nil, slab, err
			}
			continue
		}

		readFM, err := f.ensureReadFM(q)
		if err != nil {
			q.popErrorsTotal.Add(1)
			return nil, slab, err
		}

		if err := q.readBytes(readFM, q.ReadOffset, q.hdrBuf[:]); err != nil {
			q.popErrorsTotal.Add(1)
			return nil, slab, fmt.Errorf("%w: reading size header: %w", ErrFileRead, err)
		}

		size := uint24(q.hdrBuf[:])
		if size == 0 {
			q.corruptionsTotal++
			q.popErrorsTotal.Add(1)
			return nil, slab, f.selfHealCorruptedPartition(qID, q)
		}

		nextOffset := int64(q.ReadOffset) + cfgSizeHeaderBytes + int64(size)
		if q.ReadFileIndex == q.AppendFileIndex && nextOffset > int64(q.AppendOffset) {
			q.corruptionsTotal++
			q.popErrorsTotal.Add(1)
			return nil, slab, fmt.Errorf("%w: record size %d at offset %d exceeds append offset %d", ErrCorrupted, size, q.ReadOffset, q.AppendOffset)
		}

		var data []byte
		data, slab = q.allocPopPayload(slab, int(size), remaining)
		if err := q.readBytes(readFM, q.ReadOffset+cfgSizeHeaderBytes, data); err != nil {
			q.popErrorsTotal.Add(1)
			return nil, slab, fmt.Errorf("%w: reading payload: %w", ErrFileRead, err)
		}

		q.ReadOffset = int32(nextOffset)
		q.popTotal++
		q.popBytesTotal += uint64(size)
		return data, slab, nil
	}
}

func (q *queue) allocPopPayload(slab []byte, size, remaining int) ([]byte, []byte) {
	if len(slab) >= size {
		return slab[:size:size], slab[size:]
	}
	if remaining == 1 || size >= maxPopSlabSize {
		return make([]byte, size), slab
	}

	slabSize := min(remaining, maxPopSlabSize/size) * size
	if q.ReadFileIndex == q.AppendFileIndex {
		if avail := int(q.AppendOffset-q.ReadOffset) - cfgSizeHeaderBytes; avail < slabSize {
			slabSize = avail
		}
	}

	slab = make([]byte, slabSize)
	return slab[:size:size], slab[size:]
}

// Stats returns a point-in-time metrics snapshot for every configured queue,
// ordered by queue ID. Safe for concurrent use and after Close.
func (f *fileQueue) Stats() []QueueStats {
	if f == nil {
		return nil
	}
	out := make([]QueueStats, len(f.queueIDs))
	for i, qID := range f.queueIDs {
		out[i] = f.queueStats(qID, f.queues[qID])
	}
	return out
}

func (f *fileQueue) queueStats(qID uint8, q *queue) QueueStats {
	q.mu.Lock()
	defer q.mu.Unlock()

	pendingPartitions := q.AppendFileIndex - q.ReadFileIndex
	pendingBytes := max(int64(pendingPartitions)*f.maxFileSize+int64(q.AppendOffset)-int64(q.ReadOffset), 0)

	return QueueStats{
		ID:                         qID,
		Name:                       q.name,
		PushTotal:                  q.pushTotal,
		PopTotal:                   q.popTotal,
		PushBytesTotal:             q.pushBytesTotal,
		PopBytesTotal:              q.popBytesTotal,
		PopEmptyTotal:              q.popEmptyTotal,
		PendingBytes:               pendingBytes,
		PendingPartitions:          pendingPartitions,
		AppendFileIndex:            q.AppendFileIndex,
		ReadFileIndex:              q.ReadFileIndex,
		AppendOffset:               q.AppendOffset,
		ReadOffset:                 q.ReadOffset,
		PartitionRolloversTotal:    q.partitionRolloversTotal,
		PartitionsDeletedTotal:     q.partitionsDeletedTotal,
		ReadCacheHitsTotal:         q.readCacheHitsTotal,
		ReadCacheMissesTotal:       q.readCacheMissesTotal,
		MetadataPersistsTotal:      q.metadataPersistsTotal.Load(),
		PushErrorsTotal:            q.pushErrorsTotal.Load(),
		PopErrorsTotal:             q.popErrorsTotal.Load(),
		CorruptionsTotal:           q.corruptionsTotal,
		MetadataPersistErrorsTotal: q.metadataPersistErrorsTotal.Load(),
	}
}

// Close stops background metadata persistence, closes all open partition files,
// and atomically persists final metadata for every queue. Close is idempotent.
func (f *fileQueue) Close(_ context.Context) {
	if f == nil {
		return
	}
	f.closeOnce.Do(func() {
		close(f.stopCh)
		<-f.doneCh

		for _, qID := range f.queueIDs {
			f.closeQueue(f.queues[qID])
		}
	})
}

func (f *fileQueue) closeQueue(q *queue) {
	q.metaMu.Lock()
	defer q.metaMu.Unlock()

	q.mu.Lock()
	q.closed = true
	appendFM, readFM := q.appendFM, q.readFM
	q.appendFM, q.readFM = nil, nil
	q.writeBuf = nil
	q.readBuf = nil
	q.resetReadBuf()
	finalMeta := q.marshal()
	afi, rfi := q.AppendFileIndex, q.ReadFileIndex
	q.mu.Unlock()

	if appendFM != nil {
		f.syncAndCloseStore(q.name, afi, appendFM)
	}
	if readFM != nil && readFM != appendFM {
		f.syncAndCloseStore(q.name, rfi, readFM)
	}

	metaPath := metadataFilePath(f.path, q.name)
	if err := writeFileAtomic(metaPath, finalMeta[:]); err != nil {
		q.metadataPersistErrorsTotal.Add(1)
		f.logger.Error("failed to write metadata", "queue", q.name, "fpath", metaPath, "err", err)
		return
	}
	q.metadataPersistsTotal.Add(1)
}

func (f *fileQueue) syncAndCloseStore(queueName string, index int32, store fileStore) {
	if err := store.Sync(); err != nil {
		f.logger.Error("failed syncing partition file", "queue", queueName, "index", index, "err", err)
	}
	if err := store.Close(); err != nil {
		f.logger.Error("failed closing partition file", "queue", queueName, "index", index, "err", err)
	}
}

func (f *fileQueue) ensureAppendFM(q *queue) (fileStore, error) {
	if q.appendFM != nil {
		return q.appendFM, nil
	}
	fPath := partitionFilePath(f.path, q.name, q.AppendFileIndex)
	store, err := fm.New(fPath, f.maxFileSize)
	if err != nil {
		return nil, fmt.Errorf("fq: failed opening partition %q: %w", fPath, err)
	}
	q.appendFM = store
	if q.AppendFileIndex == q.ReadFileIndex {
		q.readFM = store
	}
	return store, nil
}

func (f *fileQueue) ensureReadFM(q *queue) (fileStore, error) {
	if q.readFM != nil {
		return q.readFM, nil
	}
	fPath := partitionFilePath(f.path, q.name, q.ReadFileIndex)
	store, err := fm.New(fPath, f.maxFileSize)
	if err != nil {
		return nil, fmt.Errorf("fq: failed opening partition %q: %w", fPath, err)
	}
	q.readFM = store
	if q.ReadFileIndex == q.AppendFileIndex {
		q.appendFM = store
	}
	return store, nil
}

func (f *fileQueue) openNextReadFM(q *queue, nextIndex int32) (fileStore, error) {
	if nextIndex == q.AppendFileIndex && q.appendFM != nil {
		return q.appendFM, nil
	}
	fPath := partitionFilePath(f.path, q.name, nextIndex)
	store, err := fm.New(fPath, f.maxFileSize)
	if err != nil {
		return nil, fmt.Errorf("fq: failed opening partition %q: %w", fPath, err)
	}
	return store, nil
}

// advanceReadPartition moves q to ReadFileIndex+1 and deletes the consumed partition.
// Caller must hold q.mu.
func (f *fileQueue) advanceReadPartition(qID uint8, q *queue) error {
	oldIndex := q.ReadFileIndex
	nextIndex := oldIndex + 1

	nextFM, err := f.openNextReadFM(q, nextIndex)
	if err != nil {
		return err
	}

	f.logger.Info("switching to next partition file for reader",
		"qID", qID,
		"name", q.name,
		"rfi", q.ReadFileIndex,
		"ro", q.ReadOffset,
		"afi", q.AppendFileIndex,
		"ao", q.AppendOffset,
	)

	oldFM := q.readFM
	q.ReadFileIndex = nextIndex
	q.ReadOffset = 0
	q.readFM = nextFM
	q.resetReadBuf()

	if oldIndex != q.AppendFileIndex {
		f.removePartition(qID, q, oldIndex, oldFM)
	}

	return nil
}

// selfHealCorruptedPartition skips past a zero-length record header and cleans up
// the discarded partition if it is behind AppendFileIndex. Caller must hold q.mu.
func (f *fileQueue) selfHealCorruptedPartition(qID uint8, q *queue) error {
	oldIndex := q.ReadFileIndex
	oldOffset := q.ReadOffset
	fPath := partitionFilePath(f.path, q.name, oldIndex)

	f.logger.Error("corrupted partition file; attempting self-heal",
		"qID", qID,
		"path", fPath,
		"rfi", q.ReadFileIndex,
		"ro", q.ReadOffset,
		"afi", q.AppendFileIndex,
		"ao", q.AppendOffset,
	)

	if q.ReadFileIndex >= q.AppendFileIndex {
		q.ReadFileIndex = q.AppendFileIndex
		q.ReadOffset = q.AppendOffset
		q.resetReadBuf()
		return fmt.Errorf("%w: zero-length header in %q at offset %d", ErrCorrupted, fPath, oldOffset)
	}

	nextIndex := oldIndex + 1
	nextFM, err := f.openNextReadFM(q, nextIndex)
	if err != nil {
		return err
	}

	oldFM := q.readFM
	q.ReadFileIndex = nextIndex
	q.ReadOffset = 0
	q.readFM = nextFM
	q.resetReadBuf()
	f.removePartition(qID, q, oldIndex, oldFM)

	return fmt.Errorf("%w: zero-length header in %q at offset %d", ErrCorrupted, fPath, oldOffset)
}

func (f *fileQueue) removePartition(qID uint8, q *queue, index int32, store fileStore) {
	fPath := partitionFilePath(f.path, q.name, index)
	f.logger.Info("deleting consumed partition",
		"qID", qID,
		"name", q.name,
		"fpath", fPath,
		"index", index,
	)

	if store != nil {
		if err := store.Close(); err != nil {
			f.logger.Error("failed closing consumed partition", "path", fPath, "err", err)
		}
	}

	err := os.Remove(fPath)
	if err == nil {
		q.partitionsDeletedTotal++
		return
	}
	if !errors.Is(err, os.ErrNotExist) {
		f.logger.Error("failed removing consumed partition", "path", fPath, "err", err)
	}
}

func (f *fileQueue) metadataLoop() {
	defer close(f.doneCh)

	cache := make(map[uint8][metadataSize]byte, len(f.queueIDs))
	ticker := time.NewTicker(f.persistInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.stopCh:
			return
		case <-ticker.C:
			f.persistMetadata(cache)
		}
	}
}

func (f *fileQueue) persistMetadata(cache map[uint8][metadataSize]byte) {
	for _, qID := range f.queueIDs {
		f.persistQueueMetadata(qID, f.queues[qID], cache)
	}
}

func (f *fileQueue) persistQueueMetadata(qID uint8, q *queue, cache map[uint8][metadataSize]byte) {
	q.metaMu.Lock()
	defer q.metaMu.Unlock()

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	current := q.marshal()
	rfi, ro := q.ReadFileIndex, q.ReadOffset
	afi, ao := q.AppendFileIndex, q.AppendOffset
	q.mu.Unlock()

	if existing, ok := cache[qID]; ok && existing == current {
		return
	}

	fPath := metadataFilePath(f.path, q.name)
	f.logger.Info("persisting metadata",
		"qID", qID,
		"name", q.name,
		"fpath", fPath,
		"rfi", rfi,
		"ro", ro,
		"afi", afi,
		"ao", ao,
	)

	if err := writeFileAtomic(fPath, current[:]); err != nil {
		q.metadataPersistErrorsTotal.Add(1)
		f.logger.Error("failed to write metadata", "queue", q.name, "fpath", fPath, "err", err)
		return
	}

	q.metadataPersistsTotal.Add(1)
	cache[qID] = current
}

// writeRecord frames and writes data at offset using q's reusable scratch buffer
// for payloads up to maxWriteBufSize, or zero-copy payload-then-header writes for
// larger payloads. Caller must hold q.mu.
func (q *queue) writeRecord(store fileStore, offset int64, data []byte) error {
	itemLen := cfgSizeHeaderBytes + len(data)
	if itemLen <= maxWriteBufSize {
		if cap(q.writeBuf) < itemLen {
			q.writeBuf = make([]byte, itemLen)
		}
		buf := q.writeBuf[:itemLen]
		putUint24(buf[:cfgSizeHeaderBytes], int32(len(data)))
		copy(buf[cfgSizeHeaderBytes:], data)
		_, err := store.WriteAt(buf, offset)
		return err
	}

	// Write payload before header so an interrupted write leaves a 0x000000 header.
	if _, err := store.WriteAt(data, offset+cfgSizeHeaderBytes); err != nil {
		return err
	}
	putUint24(q.hdrBuf[:], int32(len(data)))
	_, err := store.WriteAt(q.hdrBuf[:], offset)
	return err
}

// writeBatch frames items contiguously into q.writeBuf and writes them in a
// single WriteAt call. Caller must hold q.mu and ensure totalBytes <= maxWriteBufSize.
func (q *queue) writeBatch(store fileStore, offset int64, items [][]byte, totalBytes int) error {
	if cap(q.writeBuf) < totalBytes {
		q.writeBuf = make([]byte, totalBytes)
	}
	buf := q.writeBuf[:totalBytes]
	pos := 0
	for _, data := range items {
		putUint24(buf[pos:pos+cfgSizeHeaderBytes], int32(len(data)))
		pos += cfgSizeHeaderBytes
		pos += copy(buf[pos:], data)
	}
	_, err := store.WriteAt(buf, offset)
	return err
}

// readBytes reads len(dst) bytes from store at offset, serving sequential small
// reads from q's read-ahead buffer. Caller must hold q.mu.
func (q *queue) readBytes(store fileStore, offset int32, dst []byte) error {
	need := int32(len(dst))
	if offset >= q.readBufOff && offset+need <= q.readBufOff+q.readBufLen {
		q.readCacheHitsTotal++
		start := offset - q.readBufOff
		copy(dst, q.readBuf[start:start+need])
		return nil
	}

	q.readCacheMissesTotal++

	// Bypass read-ahead buffer for large reads to avoid an extra copy.
	if len(dst) >= readAheadBufSize {
		_, err := store.ReadAt(dst, int64(offset))
		return err
	}

	if q.readBuf == nil {
		q.readBuf = make([]byte, readAheadBufSize)
	}

	toRead := int32(readAheadBufSize)
	if q.ReadFileIndex == q.AppendFileIndex {
		if avail := q.AppendOffset - offset; avail < toRead {
			toRead = max(avail, need)
		}
	}

	n, err := store.ReadAt(q.readBuf[:toRead], int64(offset))
	if err != nil && (!errors.Is(err, io.EOF) || int32(n) < need) {
		return err
	}

	q.readBufOff = offset
	q.readBufLen = int32(n)
	copy(dst, q.readBuf[:need])
	return nil
}

func (q *queue) resetReadBuf() {
	q.readBufOff = 0
	q.readBufLen = 0
}

func (q *queue) marshal() [metadataSize]byte {
	var buff [metadataSize]byte
	binary.BigEndian.PutUint32(buff[0:4], uint32(q.AppendFileIndex))
	binary.BigEndian.PutUint32(buff[4:8], uint32(q.ReadFileIndex))
	binary.BigEndian.PutUint32(buff[8:12], uint32(q.AppendOffset))
	binary.BigEndian.PutUint32(buff[12:16], uint32(q.ReadOffset))
	return buff
}

func (q *queue) unmarshal(data []byte) error {
	if len(data) != metadataSize {
		return fmt.Errorf("%w: got %d bytes, want %d", ErrInvalidMetadataSize, len(data), metadataSize)
	}

	afi := int32(binary.BigEndian.Uint32(data[0:4]))
	rfi := int32(binary.BigEndian.Uint32(data[4:8]))
	ao := int32(binary.BigEndian.Uint32(data[8:12]))
	ro := int32(binary.BigEndian.Uint32(data[12:16]))

	if afi < 0 || rfi < 0 || ao < 0 || ro < 0 {
		return fmt.Errorf("%w: negative cursor values (afi=%d rfi=%d ao=%d ro=%d)", ErrCorrupted, afi, rfi, ao, ro)
	}
	if rfi > afi || (rfi == afi && ro > ao) {
		return fmt.Errorf("%w: read cursor (%d:%d) ahead of append cursor (%d:%d)", ErrCorrupted, rfi, ro, afi, ao)
	}

	q.AppendFileIndex = afi
	q.ReadFileIndex = rfi
	q.AppendOffset = ao
	q.ReadOffset = ro
	return nil
}

func metadataFilePath(basePath, name string) string {
	return filepath.Join(basePath, fmt.Sprintf(metadataFileFormat, name))
}

func partitionFilePath(basePath, name string, index int32) string {
	return filepath.Join(basePath, fmt.Sprintf(partitionFileFormat, name, index))
}

// writeFileAtomic writes data to a temporary file in path's directory, syncs it,
// and atomically renames it over path so crashes cannot leave a truncated file.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, tempMetadataPattern)
	if err != nil {
		return fmt.Errorf("fq: failed to create temp file: %w", err)
	}
	return writeTempAndRename(tmp, path, data)
}

func writeTempAndRename(tmp tempFile, path string, data []byte) error {
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fq: failed to write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fq: failed to sync temp file: %w", err)
	}
	if err := tmp.Chmod(fileMode); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fq: failed to chmod temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("fq: failed to close temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("fq: failed to rename temp file: %w", err)
	}

	cleanup = false
	return nil
}

func putUint24(dst []byte, n int32) {
	dst[0] = byte(n >> 16)
	dst[1] = byte(n >> 8)
	dst[2] = byte(n)
}

func uint24(src []byte) int32 {
	return int32(src[0])<<16 | int32(src[1])<<8 | int32(src[2])
}
