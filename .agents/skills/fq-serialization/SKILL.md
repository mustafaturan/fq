---
name: fq-serialization
description: >-
  Guides designing high-throughput, crash-safe, versioned binary codecs for
  Go structs persisted in github.com/mustafaturan/fq. Use when serializing or
  deserializing structs for fq Push/Pop, optimizing allocations or disk size
  for fq payloads, implementing encoding/binary codecs, or choosing between
  encoding/binary, msgp, Protobuf, FlatBuffers, and Cap'n Proto.
---

# High-Performance Serialization for `fq`

`fq.Push` runs at `0 allocs/op` and `fq.Pop` serves sequential small reads from a 64 KiB read-ahead buffer (~37 ns/op for 64 B payloads). Reflection-heavy encoders (`encoding/json`, `encoding/gob`) easily become 10×–100× slower than `fq` itself and bloat partition files.

## Choosing a Codec

| Format | When to Choose |
| :--- | :--- |
| **Hand-written `encoding/binary`** | Simple/flat structs, zero extra dependencies, smallest wire size, `0 allocs/op` |
| **`tinylib/msgp`** | Pure Go structs via `//go:generate` without maintaining a separate schema DSL |
| **Protobuf + `vtprotobuf`** | Production services needing strict schema evolution across teams and upgrades |
| **FlatBuffers / Cap'n Proto (`capnp/v3`)** | Medium/large nested structs (1 KiB+) where consumers inspect only a subset of fields zero-copy |

**Avoid:**
- `encoding/gob` — repeats type descriptors on standalone messages; stateful stream encoders break across restarts.
- `encoding/json` on hot paths — slow reflection, high allocations, and 2–4× larger payloads (fewer items per 64 KiB `fq` read-ahead window).

## Design Rules for `fq` Codecs

1. **Always version payloads**: Persisted `fq` records survive binary deployments. Start custom binary codecs with a `uint8` version byte so new binaries can drain older records safely.
2. **Implement `encoding.BinaryAppender` (`AppendBinary`) for buffer reuse**: Because `fq.Push` writes synchronously and never retains `data`, reuse a per-worker or `sync.Pool` slice (`buf = buf[:0]`) for zero-allocation publishing.
3. **Bounds-check every slice index on decode**: Validate minimum header length, version byte, and variable-length bounds before slicing so corrupt payloads return an error instead of panicking.
4. **Slice variable `[]byte` fields directly from `Pop`'s output**: `fq.Pop` returns a freshly allocated slice owned exclusively by the caller, so zero-copy slicing inside `UnmarshalBinary` is safe.
5. **Stay within `fq`'s payload limit**: Single payloads must be non-empty and `<= 16,777,215` bytes (`(1 << 24) - 1`).

## Reference `encoding/binary` Pattern

```go
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
```

## Cap'n Proto Tips (`capnproto.org/go/capnp/v3`)

- Build messages with `capnp.SingleSegment(buf[:0])` on the producer side to reuse backing buffers across `Push` calls.
- Decode with `capnp.Unmarshal(data)` (never unsafe casts) so segment bounds and traversal limits are enforced on disk data.
- Use `MarshalPacked`/`UnmarshalPacked` if compact disk footprint matters more than zero-copy reads on small structs.

## Verification

Write round-trip and truncation unit tests plus an allocation benchmark for every custom codec:

```bash
go test -race -run TestEventCodec ./...
go test -run='^$' -bench=BenchmarkEventCodec -benchmem ./...
```

Confirm `AppendBinary` reports `0 allocs/op` when given a pre-sized buffer.

