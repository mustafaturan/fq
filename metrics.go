package fq

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

// PrometheusContentType is the standard MIME content type for Prometheus text exposition format v0.0.4.
const PrometheusContentType = "text/plain; version=0.0.4; charset=utf-8"

// QueueStats holds a point-in-time snapshot of counters and cursor positions
// for a single queue, suitable for exporting to Prometheus or other metrics systems.
type QueueStats struct {
	ID   uint8
	Name string

	// Traffic counters (monotonic since New).
	PushTotal      uint64
	PopTotal       uint64
	PushBytesTotal uint64
	PopBytesTotal  uint64
	PopEmptyTotal  uint64

	// Backlog and cursor gauges.
	PendingBytes      int64
	PendingPartitions int32
	AppendFileIndex   int32
	ReadFileIndex     int32
	AppendOffset      int32
	ReadOffset        int32

	// Storage lifecycle and read-ahead buffer counters.
	PartitionRolloversTotal uint64
	PartitionsDeletedTotal  uint64
	ReadCacheHitsTotal      uint64
	ReadCacheMissesTotal    uint64
	MetadataPersistsTotal   uint64

	// Error and health counters.
	PushErrorsTotal            uint64
	PopErrorsTotal             uint64
	CorruptionsTotal           uint64
	MetadataPersistErrorsTotal uint64
}

type promMetric struct {
	name      string
	kind      string
	help      string
	appendVal func([]byte, QueueStats) []byte
}

var promMetrics = []promMetric{
	{
		name:      "fq_push_total",
		kind:      "counter",
		help:      "Total number of records successfully pushed to the queue.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.PushTotal, 10) },
	},
	{
		name:      "fq_pop_total",
		kind:      "counter",
		help:      "Total number of records successfully popped from the queue.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.PopTotal, 10) },
	},
	{
		name:      "fq_push_bytes_total",
		kind:      "counter",
		help:      "Total payload bytes successfully pushed to the queue.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.PushBytesTotal, 10) },
	},
	{
		name:      "fq_pop_bytes_total",
		kind:      "counter",
		help:      "Total payload bytes successfully popped from the queue.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.PopBytesTotal, 10) },
	},
	{
		name:      "fq_pop_empty_total",
		kind:      "counter",
		help:      "Total number of Pop or PopN calls that returned ErrEmpty.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.PopEmptyTotal, 10) },
	},
	{
		name:      "fq_pending_bytes",
		kind:      "gauge",
		help:      "Estimated unread bytes currently queued on disk.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendInt(b, s.PendingBytes, 10) },
	},
	{
		name:      "fq_pending_partitions",
		kind:      "gauge",
		help:      "Number of unconsumed partitions behind the active append partition.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendInt(b, int64(s.PendingPartitions), 10) },
	},
	{
		name:      "fq_append_file_index",
		kind:      "gauge",
		help:      "Active write partition file index.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendInt(b, int64(s.AppendFileIndex), 10) },
	},
	{
		name:      "fq_read_file_index",
		kind:      "gauge",
		help:      "Active read partition file index.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendInt(b, int64(s.ReadFileIndex), 10) },
	},
	{
		name:      "fq_append_offset_bytes",
		kind:      "gauge",
		help:      "Current write byte offset within the active append partition.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendInt(b, int64(s.AppendOffset), 10) },
	},
	{
		name:      "fq_read_offset_bytes",
		kind:      "gauge",
		help:      "Current read byte offset within the active read partition.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendInt(b, int64(s.ReadOffset), 10) },
	},
	{
		name:      "fq_partition_rollovers_total",
		kind:      "counter",
		help:      "Total number of new partitions created on write rollover.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.PartitionRolloversTotal, 10) },
	},
	{
		name:      "fq_partitions_deleted_total",
		kind:      "counter",
		help:      "Total number of consumed or discarded partition files deleted from disk.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.PartitionsDeletedTotal, 10) },
	},
	{
		name:      "fq_read_cache_hits_total",
		kind:      "counter",
		help:      "Total reads served from the in-memory read-ahead buffer.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.ReadCacheHitsTotal, 10) },
	},
	{
		name:      "fq_read_cache_misses_total",
		kind:      "counter",
		help:      "Total reads that refilled the read-ahead buffer or read directly from disk.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.ReadCacheMissesTotal, 10) },
	},
	{
		name:      "fq_metadata_persists_total",
		kind:      "counter",
		help:      "Total number of successful atomic metadata writes.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.MetadataPersistsTotal, 10) },
	},
	{
		name:      "fq_push_errors_total",
		kind:      "counter",
		help:      "Total number of failed Push or PushN calls.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.PushErrorsTotal, 10) },
	},
	{
		name:      "fq_pop_errors_total",
		kind:      "counter",
		help:      "Total number of failed Pop or PopN calls (excluding ErrEmpty).",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.PopErrorsTotal, 10) },
	},
	{
		name:      "fq_corruptions_total",
		kind:      "counter",
		help:      "Total number of corrupted partition records encountered.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.CorruptionsTotal, 10) },
	},
	{
		name:      "fq_metadata_persist_errors_total",
		kind:      "counter",
		help:      "Total number of failed atomic metadata writes.",
		appendVal: func(b []byte, s QueueStats) []byte { return strconv.AppendUint(b, s.MetadataPersistErrorsTotal, 10) },
	},
}

var promLabelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

// WritePrometheus formats stats in Prometheus text exposition format (v0.0.4)
// and writes the result to w.
func WritePrometheus(w io.Writer, stats []QueueStats) error {
	if w == nil {
		return fmt.Errorf("%w: prometheus writer is nil", ErrInvalidConfig)
	}
	if len(stats) == 0 {
		return nil
	}

	escapedNames := make([]string, len(stats))
	for i, s := range stats {
		escapedNames[i] = promLabelEscaper.Replace(s.Name)
	}

	buf := make([]byte, 0, len(promMetrics)*(96+len(stats)*48))
	for _, m := range promMetrics {
		buf = append(buf, "# HELP "...)
		buf = append(buf, m.name...)
		buf = append(buf, ' ')
		buf = append(buf, m.help...)
		buf = append(buf, '\n')

		buf = append(buf, "# TYPE "...)
		buf = append(buf, m.name...)
		buf = append(buf, ' ')
		buf = append(buf, m.kind...)
		buf = append(buf, '\n')

		for i, s := range stats {
			buf = append(buf, m.name...)
			buf = append(buf, `{queue="`...)
			buf = append(buf, escapedNames[i]...)
			buf = append(buf, `"} `...)
			buf = m.appendVal(buf, s)
			buf = append(buf, '\n')
		}
	}

	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("fq: failed writing prometheus metrics: %w", err)
	}
	return nil
}
