package fq

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func assertFQErrPrefix(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected non-nil error")
	}
	if !strings.HasPrefix(err.Error(), "fq: ") {
		t.Errorf("error %q does not start with %q", err.Error(), "fq: ")
	}
	if strings.Count(err.Error(), "fq: ") > 1 {
		t.Errorf("error %q contains duplicate %q prefix", err.Error(), "fq: ")
	}
}

func newTestFileQueue(t *testing.T, dir string, queues ...QueueConfig) FileQueue {
	t.Helper()
	s, err := New(Params{
		Path:   dir,
		Queues: queues,
	})
	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}
	return s
}

func TestLogger_SlogCompatibility(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	s, err := New(Params{
		Path:        t.TempDir(),
		MaxFileSize: minFileSize,
		Logger:      logger,
		Queues:      []QueueConfig{{ID: 1, Name: "logged"}},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer s.Close(ctx)

	if err := s.Push(ctx, 1, bytes.Repeat([]byte("r"), minFileSize)); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	if _, err := s.Pop(ctx, 99); !errors.Is(err, ErrNil) {
		t.Fatalf("Pop unknown queue err = %v, want %v", err, ErrNil)
	}

	out := buf.String()
	if !strings.Contains(out, "created new queue partition") {
		t.Errorf("expected Info log output, got: %s", out)
	}
	if !strings.Contains(out, ErrNil.Error()) {
		t.Errorf("expected Error log output containing %q, got: %s", ErrNil.Error(), out)
	}
}

func TestNew_ValidationAndErrors(t *testing.T) {
	t.Run("empty path", func(t *testing.T) {
		s, err := New(Params{
			Path:   "",
			Queues: []QueueConfig{{ID: 1, Name: "events"}},
		})
		if !errors.Is(err, ErrInvalidConfig) {
			if s != nil {
				s.Close(context.Background())
			}
			t.Fatalf("New with empty Path err = %v, want %v", err, ErrInvalidConfig)
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("path is a regular file", func(t *testing.T) {
		filePath := filepath.Join(t.TempDir(), "not-a-dir")
		if err := os.WriteFile(filePath, []byte("x"), 0644); err != nil {
			t.Fatalf("os.WriteFile failed: %v", err)
		}
		s, err := New(Params{
			Path:   filePath,
			Queues: []QueueConfig{{ID: 1, Name: "events"}},
		})
		if !errors.Is(err, ErrInvalidConfig) {
			if s != nil {
				s.Close(context.Background())
			}
			t.Fatalf("New with file Path err = %v, want %v", err, ErrInvalidConfig)
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("nonexistent directory", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "does-not-exist")
		s, err := New(Params{
			Path:   dir,
			Queues: []QueueConfig{{ID: 1, Name: "events"}},
		})
		if err == nil {
			s.Close(context.Background())
			t.Fatalf("New with nonexistent directory expected error, got nil")
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("invalid max file size", func(t *testing.T) {
		for _, badSize := range []int64{-1, 1, cfgSizeHeaderBytes, minFileSize - 1, maxFileSizeLimit + 1} {
			s, err := New(Params{
				Path:        t.TempDir(),
				MaxFileSize: badSize,
				Queues:      []QueueConfig{{ID: 1, Name: "events"}},
			})
			if !errors.Is(err, ErrInvalidConfig) {
				if s != nil {
					s.Close(context.Background())
				}
				t.Fatalf("New with MaxFileSize=%d err = %v, want %v", badSize, err, ErrInvalidConfig)
			}
			assertFQErrPrefix(t, err)
		}
	})

	t.Run("invalid queue names", func(t *testing.T) {
		for _, badName := range []string{"", ".", "..", "../escape", "sub/dir"} {
			s, err := New(Params{
				Path:   t.TempDir(),
				Queues: []QueueConfig{{ID: 1, Name: badName}},
			})
			if !errors.Is(err, ErrInvalidConfig) {
				if s != nil {
					s.Close(context.Background())
				}
				t.Fatalf("New with name %q err = %v, want %v", badName, err, ErrInvalidConfig)
			}
			assertFQErrPrefix(t, err)
		}
	})

	t.Run("duplicate queue IDs", func(t *testing.T) {
		s, err := New(Params{
			Path: t.TempDir(),
			Queues: []QueueConfig{
				{ID: 1, Name: "q1"},
				{ID: 1, Name: "q2"},
			},
		})
		if !errors.Is(err, ErrInvalidConfig) {
			if s != nil {
				s.Close(context.Background())
			}
			t.Fatalf("New with duplicate queue IDs err = %v, want %v", err, ErrInvalidConfig)
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("duplicate queue names", func(t *testing.T) {
		s, err := New(Params{
			Path: t.TempDir(),
			Queues: []QueueConfig{
				{ID: 1, Name: "shared"},
				{ID: 2, Name: "shared"},
			},
		})
		if !errors.Is(err, ErrInvalidConfig) {
			if s != nil {
				s.Close(context.Background())
			}
			t.Fatalf("New with duplicate queue names err = %v, want %v", err, ErrInvalidConfig)
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("corrupted metadata file too small", func(t *testing.T) {
		dir := t.TempDir()
		metaPath := filepath.Join(dir, "events.bin")
		if err := os.WriteFile(metaPath, []byte{1, 2, 3, 4, 5}, 0644); err != nil {
			t.Fatalf("os.WriteFile failed: %v", err)
		}

		s, err := New(Params{
			Path:   dir,
			Queues: []QueueConfig{{ID: 1, Name: "events"}},
		})
		if !errors.Is(err, ErrInvalidMetadataSize) {
			if s != nil {
				s.Close(context.Background())
			}
			t.Fatalf("New with short metadata err = %v, want %v", err, ErrInvalidMetadataSize)
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("invalid persist interval", func(t *testing.T) {
		s, err := New(Params{
			Path:            t.TempDir(),
			PersistInterval: -time.Second,
			Queues:          []QueueConfig{{ID: 1, Name: "events"}},
		})
		if !errors.Is(err, ErrInvalidConfig) {
			if s != nil {
				s.Close(context.Background())
			}
			t.Fatalf("New with negative PersistInterval err = %v, want %v", err, ErrInvalidConfig)
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("read-only directory fails initial metadata write", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("skipping permission test when running as root")
		}
		dir := t.TempDir()
		if err := os.Chmod(dir, 0555); err != nil {
			t.Fatalf("chmod 0555 failed: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0755) })

		s, err := New(Params{
			Path:   dir,
			Queues: []QueueConfig{{ID: 1, Name: "ro"}},
		})
		if err == nil {
			s.Close(context.Background())
			t.Fatalf("expected error initializing queue in read-only directory")
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("append partition blocked by directory cleans up earlier queues with distinct read and append FMs", func(t *testing.T) {
		dir := t.TempDir()
		q1Meta := queue{AppendFileIndex: 1, ReadFileIndex: 0, AppendOffset: 10, ReadOffset: 0}
		raw := q1Meta.marshal()
		if err := os.WriteFile(filepath.Join(dir, "q1.bin"), raw[:], 0644); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}
		if err := os.Mkdir(filepath.Join(dir, "q2.db0"), 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		s, err := New(Params{
			Path: dir,
			Queues: []QueueConfig{
				{ID: 1, Name: "q1"},
				{ID: 2, Name: "q2"},
			},
		})
		if err == nil {
			s.Close(context.Background())
			t.Fatalf("expected error when partition path is a directory")
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("read partition blocked by directory closes opened append partition", func(t *testing.T) {
		dir := t.TempDir()
		qMeta := queue{AppendFileIndex: 1, ReadFileIndex: 0, AppendOffset: 10, ReadOffset: 0}
		raw := qMeta.marshal()
		if err := os.WriteFile(filepath.Join(dir, "q.bin"), raw[:], 0644); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}
		if err := os.Mkdir(filepath.Join(dir, "q.db0"), 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		s, err := New(Params{
			Path:   dir,
			Queues: []QueueConfig{{ID: 1, Name: "q"}},
		})
		if err == nil {
			s.Close(context.Background())
			t.Fatalf("expected error when read partition path is a directory")
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("unreadable metadata file", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("skipping permission test when running as root")
		}
		dir := t.TempDir()
		metaPath := filepath.Join(dir, "locked.bin")
		if err := os.WriteFile(metaPath, make([]byte, metadataSize), 0000); err != nil {
			t.Fatalf("os.WriteFile failed: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(metaPath, 0644) })

		s, err := New(Params{
			Path:   dir,
			Queues: []QueueConfig{{ID: 1, Name: "locked"}},
		})
		if err == nil {
			s.Close(context.Background())
			t.Fatalf("New with unreadable metadata file expected error, got nil")
		}
		assertFQErrPrefix(t, err)
	})
}

func TestPushPop_FIFOAndInterleaved(t *testing.T) {
	ctx := context.Background()
	s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "fifo"})
	defer s.Close(ctx)

	if _, err := s.Pop(ctx, 1); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Pop on empty queue err = %v, want %v", err, ErrEmpty)
	}

	const total = 100
	expected := make([][]byte, total)
	for i := range total {
		msg := bytes.Repeat(fmt.Appendf(nil, "msg-%03d:", i), (i%7)+1)
		expected[i] = msg
		if err := s.Push(ctx, 1, msg); err != nil {
			t.Fatalf("Push(%d) failed: %v", i, err)
		}
	}

	for i := range 40 {
		got, err := s.Pop(ctx, 1)
		if err != nil {
			t.Fatalf("Pop(%d) failed: %v", i, err)
		}
		if !bytes.Equal(got, expected[i]) {
			t.Fatalf("Pop(%d) = %q, want %q", i, got, expected[i])
		}
	}

	for i := total; i < total+20; i++ {
		msg := fmt.Appendf(nil, "interleaved-%d", i)
		expected = append(expected, msg)
		if err := s.Push(ctx, 1, msg); err != nil {
			t.Fatalf("Push(%d) failed: %v", i, err)
		}
	}

	for i := 40; i < len(expected); i++ {
		got, err := s.Pop(ctx, 1)
		if err != nil {
			t.Fatalf("Pop(%d) failed: %v", i, err)
		}
		if !bytes.Equal(got, expected[i]) {
			t.Fatalf("Pop(%d) = %q, want %q", i, got, expected[i])
		}
	}

	if _, err := s.Pop(ctx, 1); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Pop after draining err = %v, want %v", err, ErrEmpty)
	}
}

func TestPushPop_LargePayloadBypassBuffers(t *testing.T) {
	ctx := context.Background()
	s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "large"})
	defer s.Close(ctx)

	small1 := []byte("small-before")
	large := bytes.Repeat([]byte("L"), maxWriteBufSize+1024) // > 64 KiB
	large[0] = 'S'
	large[len(large)-1] = 'E'
	small2 := []byte("small-after")

	for _, p := range [][]byte{small1, large, small2} {
		if err := s.Push(ctx, 1, p); err != nil {
			t.Fatalf("Push(len=%d) failed: %v", len(p), err)
		}
	}

	for _, want := range [][]byte{small1, large, small2} {
		got, err := s.Pop(ctx, 1)
		if err != nil {
			t.Fatalf("Pop(want len=%d) failed: %v", len(want), err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Pop mismatch for len=%d", len(want))
		}
	}
}

func TestMultipleQueuesIsolation(t *testing.T) {
	ctx := context.Background()
	s := newTestFileQueue(t, t.TempDir(),
		QueueConfig{ID: 1, Name: "alpha"},
		QueueConfig{ID: 2, Name: "beta"},
	)
	defer s.Close(ctx)

	if err := s.Push(ctx, 1, []byte("a1")); err != nil {
		t.Fatalf("Push q1 failed: %v", err)
	}
	if err := s.Push(ctx, 2, []byte("b1")); err != nil {
		t.Fatalf("Push q2 failed: %v", err)
	}
	if err := s.Push(ctx, 1, []byte("a2")); err != nil {
		t.Fatalf("Push q1 failed: %v", err)
	}

	gotB, err := s.Pop(ctx, 2)
	if err != nil || string(gotB) != "b1" {
		t.Fatalf("Pop q2 = (%q, %v), want (%q, nil)", gotB, err, "b1")
	}
	if _, err := s.Pop(ctx, 2); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Pop empty q2 err = %v, want %v", err, ErrEmpty)
	}

	for _, want := range []string{"a1", "a2"} {
		got, err := s.Pop(ctx, 1)
		if err != nil || string(got) != want {
			t.Fatalf("Pop q1 = (%q, %v), want (%q, nil)", got, err, want)
		}
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	qCfg := QueueConfig{ID: 1, Name: "durable"}

	s := newTestFileQueue(t, dir, qCfg)
	for i := range 10 {
		if err := s.Push(ctx, 1, fmt.Appendf(nil, "item-%d", i)); err != nil {
			t.Fatalf("Push(%d) failed: %v", i, err)
		}
	}
	for i := range 4 {
		got, err := s.Pop(ctx, 1)
		if err != nil || string(got) != fmt.Sprintf("item-%d", i) {
			t.Fatalf("Pop(%d) = (%q, %v)", i, got, err)
		}
	}
	s.Close(ctx)

	for _, name := range []string{"durable.bin", "durable.db0"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("os.Stat(%s) failed: %v", name, err)
		}
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Fatalf("%s perm = %04o, want 0600", name, perm)
		}
	}

	s2 := newTestFileQueue(t, dir, qCfg)
	defer s2.Close(ctx)

	for i := 4; i < 10; i++ {
		got, err := s2.Pop(ctx, 1)
		if err != nil || string(got) != fmt.Sprintf("item-%d", i) {
			t.Fatalf("after restart Pop(%d) = (%q, %v), want item-%d", i, got, err, i)
		}
	}
	if _, err := s2.Pop(ctx, 1); !errors.Is(err, ErrEmpty) {
		t.Fatalf("after restart Pop empty err = %v, want %v", err, ErrEmpty)
	}
}

func TestPersistence_OffsetsAbove16MiB(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	qCfg := QueueConfig{ID: 1, Name: "large_offset"}

	s := newTestFileQueue(t, dir, qCfg)

	const chunkSize = 1024 * 1024 // 1 MiB
	const chunks = 18             // 18 MiB > 16 MiB (2^24 - 1)
	for i := range chunks {
		payload := bytes.Repeat([]byte{byte('A' + i)}, chunkSize)
		if err := s.Push(ctx, 1, payload); err != nil {
			t.Fatalf("Push(%d) failed: %v", i, err)
		}
	}

	for i := range chunks - 1 {
		got, err := s.Pop(ctx, 1)
		if err != nil {
			t.Fatalf("Pop(%d) failed: %v", i, err)
		}
		if len(got) != chunkSize || got[0] != byte('A'+i) || got[len(got)-1] != byte('A'+i) {
			t.Fatalf("Pop(%d) returned unexpected data (len=%d)", i, len(got))
		}
	}
	s.Close(ctx)

	s2 := newTestFileQueue(t, dir, qCfg)
	defer s2.Close(ctx)

	got, err := s2.Pop(ctx, 1)
	if err != nil {
		t.Fatalf("after restart Pop(17) failed: %v", err)
	}
	wantByte := byte('A' + chunks - 1)
	if len(got) != chunkSize || got[0] != wantByte || got[len(got)-1] != wantByte {
		t.Fatalf("after restart Pop(17) returned corrupted data (len=%d, first=%q, want=%q)", len(got), got[0], wantByte)
	}
	if _, err := s2.Pop(ctx, 1); !errors.Is(err, ErrEmpty) {
		t.Fatalf("after restart Pop empty err = %v, want %v", err, ErrEmpty)
	}
}

func TestPartitionRollover_Default100MiB(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	qCfg := QueueConfig{ID: 1, Name: "rollover"}

	s := newTestFileQueue(t, dir, qCfg)
	defer s.Close(ctx)

	const chunkSize = 10 * 1024 * 1024 // 10 MiB
	const chunks = 12                  // 120 MiB > 100 MiB initialFileSize
	for i := range chunks {
		payload := bytes.Repeat([]byte{byte('a' + i)}, chunkSize)
		if err := s.Push(ctx, 1, payload); err != nil {
			t.Fatalf("Push(%d) failed: %v", i, err)
		}
	}

	db0Path := filepath.Join(dir, "rollover.db0")
	db1Path := filepath.Join(dir, "rollover.db1")
	if _, err := os.Stat(db1Path); err != nil {
		t.Fatalf("expected partition file %s to exist: %v", db1Path, err)
	}

	for i := range chunks {
		got, err := s.Pop(ctx, 1)
		if err != nil {
			t.Fatalf("Pop(%d) across rollover failed: %v", i, err)
		}
		wantByte := byte('a' + i)
		if len(got) != chunkSize || got[0] != wantByte || got[len(got)-1] != wantByte {
			t.Fatalf("Pop(%d) corrupted: len=%d first=%q want=%q", i, len(got), got[0], wantByte)
		}
	}

	if _, err := os.Stat(db0Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected consumed partition %s to be deleted, stat err = %v", db0Path, err)
	}

	if _, err := s.Pop(ctx, 1); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Pop after draining rolled-over partitions err = %v, want %v", err, ErrEmpty)
	}
}

func TestMultiPartitionRollover_BoundedFDsAndRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	qCfg := QueueConfig{ID: 1, Name: "multi"}

	s, err := New(Params{
		Path:        dir,
		MaxFileSize: minFileSize,
		Queues:      []QueueConfig{qCfg},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	const (
		total     = 12
		chunkSize = 600 * 1024 // 600 KiB -> rolls over every 2 items on 1 MiB partitions
	)
	makeRecord := func(i int) []byte {
		return bytes.Repeat([]byte{byte('A' + i)}, chunkSize)
	}
	for i := range total {
		if err := s.Push(ctx, 1, makeRecord(i)); err != nil {
			t.Fatalf("Push(%d) failed: %v", i, err)
		}
	}

	srv := s.(*fileQueue)
	q := srv.queues[1]
	q.mu.Lock()
	afi := q.AppendFileIndex
	rfi := q.ReadFileIndex
	hasAppend := q.appendFM != nil
	hasRead := q.readFM != nil
	q.mu.Unlock()

	if afi < 5 {
		t.Fatalf("expected >= 5 partition rollovers, got afi=%d", afi)
	}
	if rfi != 0 {
		t.Fatalf("expected rfi=0 before any pop, got %d", rfi)
	}
	if !hasAppend || !hasRead {
		t.Fatalf("expected both appendFM and readFM open (append=%v, read=%v)", hasAppend, hasRead)
	}

	for i := range 4 {
		got, err := s.Pop(ctx, 1)
		if err != nil {
			t.Fatalf("Pop(%d) failed: %v", i, err)
		}
		if !bytes.Equal(got, makeRecord(i)) {
			t.Fatalf("Pop(%d) data mismatch (len=%d)", i, len(got))
		}
	}
	s.Close(ctx)

	s2, err := New(Params{
		Path:        dir,
		MaxFileSize: minFileSize,
		Queues:      []QueueConfig{qCfg},
	})
	if err != nil {
		t.Fatalf("reopen New failed: %v", err)
	}
	defer s2.Close(ctx)

	for i := 4; i < total; i++ {
		got, err := s2.Pop(ctx, 1)
		if err != nil {
			t.Fatalf("after restart Pop(%d) failed: %v", i, err)
		}
		if !bytes.Equal(got, makeRecord(i)) {
			t.Fatalf("after restart Pop(%d) data mismatch (len=%d)", i, len(got))
		}
	}

	if _, err := s2.Pop(ctx, 1); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Pop after drain err = %v, want %v", err, ErrEmpty)
	}
}

func TestCorruptedPartition_SelfHeal(t *testing.T) {
	ctx := context.Background()

	t.Run("same partition resets read offset to append offset", func(t *testing.T) {
		dir := t.TempDir()
		s, err := New(Params{
			Path:        dir,
			MaxFileSize: minFileSize,
			Queues:      []QueueConfig{{ID: 1, Name: "corrupt_same"}},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer s.Close(ctx)

		if err := s.Push(ctx, 1, []byte("hello")); err != nil {
			t.Fatalf("Push failed: %v", err)
		}

		srv := s.(*fileQueue)
		if _, err := srv.queues[1].appendFM.WriteAt([]byte{0, 0, 0}, 0); err != nil {
			t.Fatalf("corrupting header failed: %v", err)
		}

		_, err = s.Pop(ctx, 1)
		if !errors.Is(err, ErrCorrupted) {
			t.Fatalf("Pop on corrupted header err = %v, want %v", err, ErrCorrupted)
		}
		assertFQErrPrefix(t, err)

		if _, err := s.Pop(ctx, 1); !errors.Is(err, ErrEmpty) {
			t.Fatalf("Pop after self-heal err = %v, want %v", err, ErrEmpty)
		}
		if err := s.Push(ctx, 1, []byte("recovered")); err != nil {
			t.Fatalf("Push after self-heal failed: %v", err)
		}
		got, err := s.Pop(ctx, 1)
		if err != nil || string(got) != "recovered" {
			t.Fatalf("Pop after self-heal = (%q, %v), want (%q, nil)", got, err, "recovered")
		}
	})

	t.Run("older partition skips to next partition and deletes corrupt file", func(t *testing.T) {
		dir := t.TempDir()
		s, err := New(Params{
			Path:        dir,
			MaxFileSize: minFileSize,
			Queues:      []QueueConfig{{ID: 1, Name: "corrupt_old"}},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer s.Close(ctx)

		if err := s.Push(ctx, 1, bytes.Repeat([]byte("a"), minFileSize)); err != nil {
			t.Fatalf("Push 1 failed: %v", err)
		}
		if err := s.Push(ctx, 1, []byte("second-in-db1")); err != nil {
			t.Fatalf("Push 2 failed: %v", err)
		}

		srv := s.(*fileQueue)
		if _, err := srv.queues[1].readFM.WriteAt([]byte{0, 0, 0}, 0); err != nil {
			t.Fatalf("corrupting .db0 failed: %v", err)
		}

		if _, err := s.Pop(ctx, 1); !errors.Is(err, ErrCorrupted) {
			t.Fatalf("Pop on corrupted .db0 err = %v, want %v", err, ErrCorrupted)
		}

		db0Path := filepath.Join(dir, "corrupt_old.db0")
		if _, err := os.Stat(db0Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected corrupted %s to be removed, stat err = %v", db0Path, err)
		}

		got, err := s.Pop(ctx, 1)
		if err != nil || string(got) != "second-in-db1" {
			t.Fatalf("Pop after partition skip = (%q, %v), want (%q, nil)", got, err, "second-in-db1")
		}
	})

	t.Run("oversized corrupted header exceeds append offset", func(t *testing.T) {
		dir := t.TempDir()
		s, err := New(Params{
			Path:        dir,
			MaxFileSize: minFileSize,
			Queues:      []QueueConfig{{ID: 1, Name: "corrupt_len"}},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer s.Close(ctx)

		if err := s.Push(ctx, 1, []byte("hi")); err != nil {
			t.Fatalf("Push failed: %v", err)
		}

		srv := s.(*fileQueue)
		if _, err := srv.queues[1].appendFM.WriteAt([]byte{0x00, 0x0F, 0xFF}, 0); err != nil {
			t.Fatalf("corrupting header length failed: %v", err)
		}

		_, err = s.Pop(ctx, 1)
		if !errors.Is(err, ErrCorrupted) {
			t.Fatalf("Pop on oversized header err = %v, want %v", err, ErrCorrupted)
		}
		assertFQErrPrefix(t, err)
	})
}

func TestPop_UnknownQueue(t *testing.T) {
	ctx := context.Background()
	s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "q1"})
	defer s.Close(ctx)

	_, err := s.Pop(ctx, 99)
	if !errors.Is(err, ErrNil) {
		t.Fatalf("Pop on unknown qID err = %v, want %v", err, ErrNil)
	}
	assertFQErrPrefix(t, err)
}

func TestPush_UnknownQueue(t *testing.T) {
	ctx := context.Background()
	s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "q1"})
	defer s.Close(ctx)

	err := s.Push(ctx, 99, []byte("hello"))
	if !errors.Is(err, ErrNil) {
		t.Fatalf("Push on unknown qID err = %v, want %v", err, ErrNil)
	}
	assertFQErrPrefix(t, err)
}

func TestPushAndPop_AfterClose(t *testing.T) {
	ctx := context.Background()
	s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "closed"})

	if err := s.Push(ctx, 1, []byte("before-close")); err != nil {
		t.Fatalf("Push before Close failed: %v", err)
	}

	s.Close(ctx)
	s.Close(ctx)
	(*fileQueue)(nil).Close(ctx)

	if err := s.Push(ctx, 1, []byte("after-close")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Push after Close err = %v, want %v", err, ErrClosed)
	} else {
		assertFQErrPrefix(t, err)
	}
	if _, err := s.Pop(ctx, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Pop after Close err = %v, want %v", err, ErrClosed)
	} else {
		assertFQErrPrefix(t, err)
	}
}

func TestPush_EmptyPayloadDoesNotCorruptQueue(t *testing.T) {
	ctx := context.Background()
	s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "empty_payload"})
	defer s.Close(ctx)

	if err := s.Push(ctx, 1, []byte("before")); err != nil {
		t.Fatalf("Push(before) failed: %v", err)
	}

	emptyErr := s.Push(ctx, 1, []byte{})
	if !errors.Is(emptyErr, ErrEmptyData) {
		t.Fatalf("Push(empty) err = %v, want %v", emptyErr, ErrEmptyData)
	}
	assertFQErrPrefix(t, emptyErr)

	if err := s.Push(ctx, 1, []byte("after")); err != nil {
		t.Fatalf("Push(after) failed: %v", err)
	}

	for _, want := range []string{"before", "after"} {
		got, err := s.Pop(ctx, 1)
		if err != nil || string(got) != want {
			t.Fatalf("Pop = (%q, %v), want (%q, nil)", got, err, want)
		}
	}
}

func TestPush_OversizedPayloadDoesNotCorruptQueue(t *testing.T) {
	ctx := context.Background()
	s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "oversized"})
	defer s.Close(ctx)

	if err := s.Push(ctx, 1, []byte("before")); err != nil {
		t.Fatalf("Push(before) failed: %v", err)
	}

	oversized := make([]byte, maxPayloadSize+1)
	oversizedErr := s.Push(ctx, 1, oversized)
	if !errors.Is(oversizedErr, ErrPayloadTooLarge) {
		t.Fatalf("Push(oversized) err = %v, want %v", oversizedErr, ErrPayloadTooLarge)
	}
	assertFQErrPrefix(t, oversizedErr)

	if err := s.Push(ctx, 1, []byte("after")); err != nil {
		t.Fatalf("Push(after) failed: %v", err)
	}

	for _, want := range []string{"before", "after"} {
		got, err := s.Pop(ctx, 1)
		if err != nil || string(got) != want {
			t.Fatalf("Pop = (%q, %v), want (%q, nil)", got, err, want)
		}
	}
}

func TestContextCancellation(t *testing.T) {
	s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "ctx"})
	defer s.Close(context.Background())

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Push(canceledCtx, 1, []byte("hello")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Push with canceled ctx err = %v, want %v", err, context.Canceled)
	} else {
		assertFQErrPrefix(t, err)
	}
	if _, err := s.Pop(canceledCtx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Pop with canceled ctx err = %v, want %v", err, context.Canceled)
	} else {
		assertFQErrPrefix(t, err)
	}
}

func TestPersistMetadata_RetriesAfterWriteError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	ctx := context.Background()
	dir := t.TempDir()
	s := newTestFileQueue(t, dir, QueueConfig{ID: 1, Name: "retry"}).(*fileQueue)
	defer s.Close(ctx)

	if err := s.Push(ctx, 1, []byte("payload")); err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	if err := os.Chmod(dir, 0555); err != nil {
		t.Fatalf("chmod 0555 failed: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0755) })

	cache := map[uint8][metadataSize]byte{}
	s.persistMetadata(cache)

	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatalf("chmod 0755 failed: %v", err)
	}
	s.persistMetadata(cache)
	s.persistMetadata(cache)

	metaPath := filepath.Join(dir, "retry.bin")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("os.ReadFile failed: %v", err)
	}
	var got queue
	if err := got.unmarshal(data); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got.AppendOffset == 0 {
		t.Fatalf("metadata AppendOffset = 0 after retry; failed write was incorrectly cached")
	}
}

func TestCloseAndPersistMetadata_NoDeadlock(t *testing.T) {
	ctx := context.Background()
	queues := []QueueConfig{
		{ID: 1, Name: "q1"},
		{ID: 2, Name: "q2"},
		{ID: 3, Name: "q3"},
		{ID: 4, Name: "q4"},
		{ID: 5, Name: "q5"},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 50 {
			s := newTestFileQueue(t, t.TempDir(), queues...).(*fileQueue)
			for _, qc := range queues {
				_ = s.Push(ctx, qc.ID, []byte("x"))
			}

			var wg sync.WaitGroup
			wg.Go(func() {
				s.persistMetadata(map[uint8][metadataSize]byte{})
			})
			wg.Go(func() {
				s.Close(ctx)
			})
			wg.Wait()
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("deadlock detected between Close and persistMetadata with multiple queues")
	}
}

func TestConcurrentPushAndPop(t *testing.T) {
	ctx := context.Background()
	s, err := New(Params{
		Path:        t.TempDir(),
		MaxFileSize: minFileSize,
		Queues:      []QueueConfig{{ID: 1, Name: "concurrent"}},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer s.Close(ctx)

	const (
		producers = 4
		consumers = 4
		perProd   = 100
		total     = producers * perProd
	)
	pad := bytes.Repeat([]byte("x"), 8*1024) // 8 KiB -> ~3.2 MiB total, triggering concurrent rollovers

	var prodWG sync.WaitGroup
	for p := range producers {
		prodWG.Go(func() {
			for i := range perProd {
				msg := append(fmt.Appendf(nil, "p%d-m%d:", p, i), pad...)
				if err := s.Push(ctx, 1, msg); err != nil {
					t.Errorf("Push failed: %v", err)
					return
				}
			}
		})
	}

	seen := make(map[string]int, total)
	var seenMu sync.Mutex
	var consWG sync.WaitGroup
	doneProducing := make(chan struct{})
	go func() {
		prodWG.Wait()
		close(doneProducing)
	}()

	for range consumers {
		consWG.Go(func() {
			for {
				data, err := s.Pop(ctx, 1)
				if errors.Is(err, ErrEmpty) {
					select {
					case <-doneProducing:
						data, err = s.Pop(ctx, 1)
						if errors.Is(err, ErrEmpty) {
							return
						}
					default:
						time.Sleep(time.Millisecond)
						continue
					}
				}
				if err != nil {
					t.Errorf("Pop failed: %v", err)
					return
				}
				seenMu.Lock()
				seen[string(data)]++
				seenMu.Unlock()
			}
		})
	}

	consWG.Wait()

	if len(seen) != total {
		t.Fatalf("consumed %d unique messages, want %d", len(seen), total)
	}
	for k, count := range seen {
		if count != 1 {
			t.Errorf("message %q consumed %d times, want 1", k, count)
		}
	}
}

func TestConcurrentStress_PushPopPersistCloseAndRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	queues := []QueueConfig{
		{ID: 1, Name: "stress1"},
		{ID: 2, Name: "stress2"},
		{ID: 3, Name: "stress3"},
	}

	s, err := New(Params{
		Path:            dir,
		MaxFileSize:     minFileSize,
		PersistInterval: 5 * time.Millisecond,
		Queues:          queues,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	pad := bytes.Repeat([]byte("s"), 8*1024)
	pushed := make(map[uint8]map[string]struct{}, len(queues))
	popped := make(map[uint8]map[string]int, len(queues))
	for _, qc := range queues {
		pushed[qc.ID] = make(map[string]struct{})
		popped[qc.ID] = make(map[string]int)
	}
	var (
		mu          sync.Mutex
		totalPushed atomic.Int64
		workersWG   sync.WaitGroup
	)

	for _, qc := range queues {
		qID := qc.ID
		for pid := range 3 {
			workersWG.Go(func() {
				for seq := 0; ; seq++ {
					msg := append(fmt.Appendf(nil, "q%d-p%d-m%d:", qID, pid, seq), pad...)
					err := s.Push(ctx, qID, msg)
					if errors.Is(err, ErrClosed) {
						return
					}
					if err != nil {
						t.Errorf("q%d Push unexpected error: %v", qID, err)
						return
					}
					mu.Lock()
					pushed[qID][string(msg)] = struct{}{}
					mu.Unlock()
					totalPushed.Add(1)
				}
			})
		}

		for range 2 {
			workersWG.Go(func() {
				for {
					data, err := s.Pop(ctx, qID)
					if errors.Is(err, ErrClosed) {
						return
					}
					if errors.Is(err, ErrEmpty) {
						time.Sleep(time.Millisecond)
						continue
					}
					if err != nil {
						t.Errorf("q%d Pop unexpected error: %v", qID, err)
						return
					}
					mu.Lock()
					popped[qID][string(data)]++
					mu.Unlock()
				}
			})
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for totalPushed.Load() < 300 && time.Now().Before(deadline) {
		_ = s.Stats()
		time.Sleep(5 * time.Millisecond)
	}

	var closeWG sync.WaitGroup
	for range 4 {
		closeWG.Go(func() {
			s.Close(ctx)
		})
	}
	closeWG.Wait()
	workersWG.Wait()

	s2, err := New(Params{
		Path:        dir,
		MaxFileSize: minFileSize,
		Queues:      queues,
	})
	if err != nil {
		t.Fatalf("reopen New failed: %v", err)
	}
	defer s2.Close(ctx)

	for _, qc := range queues {
		qID := qc.ID
		for {
			data, err := s2.Pop(ctx, qID)
			if errors.Is(err, ErrEmpty) {
				break
			}
			if err != nil {
				t.Fatalf("q%d Pop after restart failed: %v", qID, err)
			}
			popped[qID][string(data)]++
		}

		if len(pushed[qID]) == 0 {
			t.Errorf("q%d had 0 pushed items during stress test", qID)
		}
		if len(popped[qID]) != len(pushed[qID]) {
			t.Errorf("q%d consumed %d unique messages, want %d", qID, len(popped[qID]), len(pushed[qID]))
		}
		for msg := range pushed[qID] {
			if count := popped[qID][msg]; count != 1 {
				t.Errorf("q%d message %q consumed %d times, want 1", qID, msg, count)
			}
		}
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	t.Run("round trip including offsets above 16 MiB", func(t *testing.T) {
		want := queue{
			AppendFileIndex: 12,
			ReadFileIndex:   10,
			AppendOffset:    80 * 1024 * 1024,
			ReadOffset:      50 * 1024 * 1024,
		}

		raw := want.marshal()
		var got queue
		if err := got.unmarshal(raw[:]); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}

		if got.AppendFileIndex != want.AppendFileIndex ||
			got.ReadFileIndex != want.ReadFileIndex ||
			got.AppendOffset != want.AppendOffset ||
			got.ReadOffset != want.ReadOffset {
			t.Fatalf("marshal/unmarshal mismatch:\ngot:  afi=%d rfi=%d ao=%d ro=%d\nwant: afi=%d rfi=%d ao=%d ro=%d",
				got.AppendFileIndex, got.ReadFileIndex, got.AppendOffset, got.ReadOffset,
				want.AppendFileIndex, want.ReadFileIndex, want.AppendOffset, want.ReadOffset)
		}
	})

	t.Run("invalid metadata size", func(t *testing.T) {
		var q queue
		for _, bad := range [][]byte{{1, 2, 3}, make([]byte, 12), make([]byte, 20)} {
			err := q.unmarshal(bad)
			if !errors.Is(err, ErrInvalidMetadataSize) {
				t.Fatalf("unmarshal(len=%d) err = %v, want %v", len(bad), err, ErrInvalidMetadataSize)
			}
			assertFQErrPrefix(t, err)
		}
	})

	t.Run("corrupted negative or inverted cursors", func(t *testing.T) {
		cases := []struct {
			name             string
			afi, rfi, ao, ro uint32
		}{
			{name: "negative append offset", afi: 0, rfi: 0, ao: 0x80000000, ro: 0},
			{name: "read file ahead of append file", afi: 1, rfi: 2, ao: 0, ro: 0},
			{name: "read offset ahead of append offset", afi: 1, rfi: 1, ao: 10, ro: 20},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				buf := make([]byte, metadataSize)
				binary.BigEndian.PutUint32(buf[0:4], tc.afi)
				binary.BigEndian.PutUint32(buf[4:8], tc.rfi)
				binary.BigEndian.PutUint32(buf[8:12], tc.ao)
				binary.BigEndian.PutUint32(buf[12:16], tc.ro)

				var q queue
				err := q.unmarshal(buf)
				if !errors.Is(err, ErrCorrupted) {
					t.Fatalf("unmarshal(%s) err = %v, want %v", tc.name, err, ErrCorrupted)
				}
				assertFQErrPrefix(t, err)
			})
		}
	})
}

func TestUint24Encoding(t *testing.T) {
	var buf [cfgSizeHeaderBytes]byte
	for _, val := range []int32{0, 1, 255, 256, 65535, 1 << 16, maxPayloadSize} {
		putUint24(buf[:], val)
		if got := uint24(buf[:]); got != val {
			t.Fatalf("uint24(putUint24(%d)) = %d", val, got)
		}
	}
}

type stubFileStore struct {
	fileStore
	writeErr error
	writeSeq []error
	writeIdx int
	readErr  error
	syncErr  error
	closeErr error
	readSeq  []error
	readIdx  int
}

func (s *stubFileStore) WriteAt(p []byte, off int64) (int, error) {
	if s.writeIdx < len(s.writeSeq) {
		err := s.writeSeq[s.writeIdx]
		s.writeIdx++
		if err != nil {
			return 0, err
		}
		return s.fileStore.WriteAt(p, off)
	}
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	return s.fileStore.WriteAt(p, off)
}

func (s *stubFileStore) ReadAt(p []byte, off int64) (int, error) {
	if s.readIdx < len(s.readSeq) {
		err := s.readSeq[s.readIdx]
		s.readIdx++
		if err != nil {
			return 0, err
		}
		return s.fileStore.ReadAt(p, off)
	}
	if s.readErr != nil {
		return 0, s.readErr
	}
	return s.fileStore.ReadAt(p, off)
}

func (s *stubFileStore) Sync() error {
	if s.syncErr != nil {
		return s.syncErr
	}
	return s.fileStore.Sync()
}

func (s *stubFileStore) Close() error {
	if s.fileStore != nil {
		_ = s.fileStore.Close()
	}
	return s.closeErr
}

type stubTempFile struct {
	name     string
	writeErr error
	syncErr  error
	chmodErr error
	closeErr error
	closed   bool
}

func (s *stubTempFile) Name() string                { return s.name }
func (s *stubTempFile) Write(p []byte) (int, error) { return len(p), s.writeErr }
func (s *stubTempFile) Sync() error                 { return s.syncErr }
func (s *stubTempFile) Chmod(os.FileMode) error     { return s.chmodErr }
func (s *stubTempFile) Close() error                { s.closed = true; return s.closeErr }

func TestMetadataLoop_PeriodicPersist(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := New(Params{
		Path:            dir,
		MaxFileSize:     minFileSize,
		PersistInterval: 10 * time.Millisecond,
		Queues:          []QueueConfig{{ID: 1, Name: "periodic"}},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer s.Close(ctx)

	if err := s.Push(ctx, 1, []byte("hello")); err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	metaPath := filepath.Join(dir, "periodic.bin")
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(metaPath)
		if err == nil {
			var q queue
			if q.unmarshal(data) == nil && q.AppendOffset > 0 {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("background metadataLoop did not persist updated AppendOffset before deadline")
}

func TestPush_IOErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("ensureAppendFM lazy open and error", func(t *testing.T) {
		dir := t.TempDir()
		s := newTestFileQueue(t, dir, QueueConfig{ID: 1, Name: "q"}).(*fileQueue)
		defer s.Close(ctx)

		// Closing & nilling appendFM on an intact file succeeds via lazy reopen.
		q := s.queues[1]
		q.mu.Lock()
		_ = q.appendFM.Close()
		q.appendFM = nil
		q.readFM = nil
		q.mu.Unlock()

		if err := s.Push(ctx, 1, []byte("reopened")); err != nil {
			t.Fatalf("Push lazy reopen failed: %v", err)
		}

		// Blocking .db0 with a directory causes ensureAppendFM to fail.
		q.mu.Lock()
		_ = q.appendFM.Close()
		q.appendFM = nil
		q.readFM = nil
		q.mu.Unlock()

		db0 := filepath.Join(dir, "q.db0")
		_ = os.Remove(db0)
		if err := os.Mkdir(db0, 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		err := s.Push(ctx, 1, []byte("hello"))
		if err == nil {
			t.Fatalf("expected error when append partition cannot be opened")
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("rollover partition creation error preserves offsets", func(t *testing.T) {
		dir := t.TempDir()
		s, err := New(Params{
			Path:        dir,
			MaxFileSize: minFileSize,
			Queues:      []QueueConfig{{ID: 1, Name: "q"}},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer s.Close(ctx)

		if err := os.Mkdir(filepath.Join(dir, "q.db1"), 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		err = s.Push(ctx, 1, bytes.Repeat([]byte("r"), minFileSize))
		if err == nil {
			t.Fatalf("expected error when rollover partition cannot be created")
		}
		assertFQErrPrefix(t, err)

		q := s.(*fileQueue).queues[1]
		q.mu.Lock()
		afi, ao := q.AppendFileIndex, q.AppendOffset
		q.mu.Unlock()
		if afi != 0 || ao != 0 {
			t.Fatalf("offsets mutated on failed rollover: afi=%d ao=%d, want 0, 0", afi, ao)
		}
	})

	t.Run("write failure during rollover cleans up created partition", func(t *testing.T) {
		dir := t.TempDir()
		s, err := New(Params{
			Path:        dir,
			MaxFileSize: minFileSize,
			Queues:      []QueueConfig{{ID: 1, Name: "q"}},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer s.Close(ctx)

		q := s.(*fileQueue).queues[1]
		q.mu.Lock()
		stub := &stubFileStore{fileStore: q.appendFM, writeErr: errors.New("disk full")}
		q.appendFM = stub
		q.readFM = stub
		q.mu.Unlock()

		err = s.Push(ctx, 1, bytes.Repeat([]byte("r"), minFileSize))
		if err == nil {
			t.Fatalf("expected write error during rollover")
		}
		assertFQErrPrefix(t, err)

		if _, statErr := os.Stat(filepath.Join(dir, "q.db1")); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("expected newly created rollover partition q.db1 to be cleaned up, stat err = %v", statErr)
		}
	})

	t.Run("large payload write failure on payload and on header", func(t *testing.T) {
		s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "q"}).(*fileQueue)
		defer s.Close(ctx)

		large := bytes.Repeat([]byte("L"), maxWriteBufSize+100)
		q := s.queues[1]
		q.mu.Lock()
		orig := q.appendFM
		stub := &stubFileStore{
			fileStore: orig,
			writeSeq:  []error{errors.New("payload write err"), nil, errors.New("header write err")},
		}
		q.appendFM = stub
		q.readFM = stub
		q.mu.Unlock()

		if err := s.Push(ctx, 1, large); err == nil {
			t.Fatalf("expected payload write error on large push")
		} else {
			assertFQErrPrefix(t, err)
		}

		if err := s.Push(ctx, 1, large); err == nil {
			t.Fatalf("expected header write error on large push")
		} else {
			assertFQErrPrefix(t, err)
		}
	})

	t.Run("close error on rolled intermediate partition is handled gracefully", func(t *testing.T) {
		dir := t.TempDir()
		s, err := New(Params{
			Path:        dir,
			MaxFileSize: minFileSize,
			Queues:      []QueueConfig{{ID: 1, Name: "q"}},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer s.Close(ctx)

		fill := bytes.Repeat([]byte("f"), minFileSize)
		if err := s.Push(ctx, 1, fill); err != nil {
			t.Fatalf("Push 0 failed: %v", err)
		}

		q := s.(*fileQueue).queues[1]
		q.mu.Lock()
		q.appendFM = &stubFileStore{fileStore: q.appendFM, closeErr: errors.New("close failed")}
		q.mu.Unlock()

		if err := s.Push(ctx, 1, fill); err != nil {
			t.Fatalf("Push 1 failed: %v", err)
		}
	})
}

func TestPop_IOErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("ensureReadFM lazy open and error", func(t *testing.T) {
		dir := t.TempDir()
		s := newTestFileQueue(t, dir, QueueConfig{ID: 1, Name: "q"}).(*fileQueue)
		defer s.Close(ctx)

		if err := s.Push(ctx, 1, []byte("one")); err != nil {
			t.Fatalf("Push 1 failed: %v", err)
		}
		if err := s.Push(ctx, 1, []byte("two")); err != nil {
			t.Fatalf("Push 2 failed: %v", err)
		}

		// Force lazy reopen of readFM when ReadFileIndex == AppendFileIndex.
		q := s.queues[1]
		q.mu.Lock()
		_ = q.readFM.Close()
		q.readFM = nil
		q.appendFM = nil
		q.mu.Unlock()

		got, err := s.Pop(ctx, 1)
		if err != nil || string(got) != "one" {
			t.Fatalf("Pop lazy reopen = (%q, %v), want (%q, nil)", got, err, "one")
		}

		// Block .db0 with a directory so ensureReadFM fails.
		q.mu.Lock()
		_ = q.readFM.Close()
		q.readFM = nil
		q.appendFM = nil
		q.resetReadBuf()
		q.mu.Unlock()

		db0 := filepath.Join(dir, "q.db0")
		_ = os.Remove(db0)
		if err := os.Mkdir(db0, 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		_, err = s.Pop(ctx, 1)
		if err == nil {
			t.Fatalf("expected error when read partition cannot be opened")
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("advanceReadPartition error when next partition cannot open", func(t *testing.T) {
		dir := t.TempDir()
		s, err := New(Params{
			Path:        dir,
			MaxFileSize: minFileSize,
			Queues:      []QueueConfig{{ID: 1, Name: "q"}},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer s.Close(ctx)

		if err := s.Push(ctx, 1, bytes.Repeat([]byte("a"), minFileSize)); err != nil {
			t.Fatalf("Push 0 failed: %v", err)
		}
		if err := s.Push(ctx, 1, []byte("ok")); err != nil {
			t.Fatalf("Push 1 failed: %v", err)
		}
		if _, err := s.Pop(ctx, 1); err != nil {
			t.Fatalf("Pop 0 failed: %v", err)
		}

		q := s.(*fileQueue).queues[1]
		q.mu.Lock()
		if q.appendFM != nil {
			_ = q.appendFM.Close()
			q.appendFM = nil
		}
		q.mu.Unlock()

		db1 := filepath.Join(dir, "q.db1")
		_ = os.Remove(db1)
		if err := os.Mkdir(db1, 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		_, err = s.Pop(ctx, 1)
		if err == nil {
			t.Fatalf("expected error when advancing to unreadable partition")
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("header read error preserves ReadOffset", func(t *testing.T) {
		s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "q"}).(*fileQueue)
		defer s.Close(ctx)

		if err := s.Push(ctx, 1, []byte("hello")); err != nil {
			t.Fatalf("Push failed: %v", err)
		}

		q := s.queues[1]
		q.mu.Lock()
		stub := &stubFileStore{fileStore: q.readFM, readErr: errors.New("io error")}
		q.readFM = stub
		q.appendFM = stub
		q.mu.Unlock()

		_, err := s.Pop(ctx, 1)
		if !errors.Is(err, ErrFileRead) {
			t.Fatalf("Pop header read err = %v, want %v", err, ErrFileRead)
		}
		assertFQErrPrefix(t, err)

		q.mu.Lock()
		ro := q.ReadOffset
		q.mu.Unlock()
		if ro != 0 {
			t.Fatalf("ReadOffset = %d after failed header read, want 0", ro)
		}
	})

	t.Run("payload read error preserves ReadOffset", func(t *testing.T) {
		s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "q"}).(*fileQueue)
		defer s.Close(ctx)

		// Use a large payload (> readAheadBufSize) so header and payload issue distinct ReadAt calls.
		large := bytes.Repeat([]byte("P"), readAheadBufSize+100)
		if err := s.Push(ctx, 1, large); err != nil {
			t.Fatalf("Push failed: %v", err)
		}

		q := s.queues[1]
		q.mu.Lock()
		stub := &stubFileStore{
			fileStore: q.readFM,
			readSeq:   []error{nil, errors.New("payload io error")},
		}
		q.readFM = stub
		q.appendFM = stub
		q.mu.Unlock()

		_, err := s.Pop(ctx, 1)
		if !errors.Is(err, ErrFileRead) {
			t.Fatalf("Pop payload read err = %v, want %v", err, ErrFileRead)
		}
		assertFQErrPrefix(t, err)

		q.mu.Lock()
		ro := q.ReadOffset
		q.mu.Unlock()
		if ro != 0 {
			t.Fatalf("ReadOffset = %d after failed payload read, want 0", ro)
		}
	})

	t.Run("selfHealCorruptedPartition returns error if next partition cannot open", func(t *testing.T) {
		dir := t.TempDir()
		s, err := New(Params{
			Path:        dir,
			MaxFileSize: minFileSize,
			Queues:      []QueueConfig{{ID: 1, Name: "q"}},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer s.Close(ctx)

		if err := s.Push(ctx, 1, bytes.Repeat([]byte("a"), minFileSize)); err != nil {
			t.Fatalf("Push 0 failed: %v", err)
		}

		q := s.(*fileQueue).queues[1]
		q.mu.Lock()
		_, _ = q.readFM.WriteAt([]byte{0, 0, 0}, 0)
		if q.appendFM != nil {
			_ = q.appendFM.Close()
			q.appendFM = nil
		}
		q.mu.Unlock()

		db1 := filepath.Join(dir, "q.db1")
		_ = os.Remove(db1)
		if err := os.Mkdir(db1, 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		_, err = s.Pop(ctx, 1)
		if err == nil {
			t.Fatalf("expected error when self-heal cannot open next partition")
		}
		assertFQErrPrefix(t, err)
	})
}

func TestCloseAndRemovePartition_ErrorHandling(t *testing.T) {
	ctx := context.Background()

	t.Run("closeQueue handles distinct read and append sync, close, and metadata write errors gracefully", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("skipping permission test when running as root")
		}
		dir := t.TempDir()
		s, err := New(Params{
			Path:        dir,
			MaxFileSize: minFileSize,
			Queues:      []QueueConfig{{ID: 1, Name: "q"}},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}

		// Force distinct readFM (.db0) and appendFM (.db1).
		if err := s.Push(ctx, 1, bytes.Repeat([]byte("a"), minFileSize)); err != nil {
			t.Fatalf("Push failed: %v", err)
		}

		q := s.(*fileQueue).queues[1]
		q.mu.Lock()
		q.appendFM = &stubFileStore{
			fileStore: q.appendFM,
			syncErr:   errors.New("sync err"),
			closeErr:  errors.New("close err"),
		}
		q.readFM = &stubFileStore{
			fileStore: q.readFM,
			syncErr:   errors.New("read sync err"),
			closeErr:  errors.New("read close err"),
		}
		q.mu.Unlock()

		if err := os.Chmod(dir, 0555); err != nil {
			t.Fatalf("chmod 0555 failed: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0755) })

		s.Close(ctx)
	})

	t.Run("removePartition handles close and remove errors gracefully", func(t *testing.T) {
		dir := t.TempDir()
		s := newTestFileQueue(t, dir, QueueConfig{ID: 1, Name: "q"}).(*fileQueue)
		defer s.Close(ctx)

		nonEmptyDir := filepath.Join(dir, "q.db5")
		if err := os.Mkdir(nonEmptyDir, 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}
		if err := os.WriteFile(filepath.Join(nonEmptyDir, "child"), []byte("x"), 0644); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		s.removePartition(1, s.queues[1], 5, &stubFileStore{closeErr: errors.New("close failed")})
	})
}

type errWriter struct{ err error }

func (e errWriter) Write([]byte) (int, error) { return 0, e.err }

func TestStatsAndWritePrometheus(t *testing.T) {
	ctx := context.Background()

	// Nil receiver returns nil safely.
	if got := (*fileQueue)(nil).Stats(); got != nil {
		t.Fatalf("nil Stats() = %v, want nil", got)
	}

	dir := t.TempDir()
	s, err := New(Params{
		Path:        dir,
		MaxFileSize: minFileSize,
		Queues: []QueueConfig{
			{ID: 2, Name: "beta"},
			{ID: 1, Name: "alpha"},
		},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Verify deterministic sorted qID order.
	initial := s.Stats()
	if len(initial) != 2 || initial[0].ID != 1 || initial[0].Name != "alpha" || initial[1].ID != 2 || initial[1].Name != "beta" {
		t.Fatalf("unexpected initial stats order: %+v", initial)
	}

	// Trigger empty pop, push errors, and valid pushes/pops on alpha (ID 1).
	if _, err := s.Pop(ctx, 1); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected ErrEmpty, got %v", err)
	}
	_ = s.Push(ctx, 1, nil) // ErrEmptyData -> PushErrorsTotal++

	if err := s.Push(ctx, 1, []byte("hello")); err != nil {
		t.Fatalf("Push 1 failed: %v", err)
	}
	if err := s.Push(ctx, 1, []byte("world!")); err != nil {
		t.Fatalf("Push 2 failed: %v", err)
	}
	if _, err := s.Pop(ctx, 1); err != nil {
		t.Fatalf("Pop 1 failed: %v", err)
	}

	// Trigger rollover + clamped PendingBytes on beta (ID 2): push 1 MiB (rolls to .db1) then pop it
	// so ReadOffset (1 MiB + 3) > AppendFileIndex(1)*1 MiB + AppendOffset(0), exercising clamp to 0.
	big := bytes.Repeat([]byte("z"), minFileSize)
	if err := s.Push(ctx, 2, big); err != nil {
		t.Fatalf("Push big failed: %v", err)
	}
	if _, err := s.Pop(ctx, 2); err != nil {
		t.Fatalf("Pop big failed: %v", err)
	}

	st := s.Stats()
	alpha, beta := st[0], st[1]
	if alpha.PushTotal != 2 || alpha.PopTotal != 1 || alpha.PushBytesTotal != 11 || alpha.PopBytesTotal != 5 {
		t.Fatalf("unexpected alpha traffic stats: %+v", alpha)
	}
	if alpha.PopEmptyTotal != 1 || alpha.PushErrorsTotal != 1 {
		t.Fatalf("unexpected alpha error/empty stats: %+v", alpha)
	}
	if alpha.PendingBytes != int64(cfgSizeHeaderBytes+6) || alpha.PendingPartitions != 0 {
		t.Fatalf("unexpected alpha pending stats: bytes=%d partitions=%d", alpha.PendingBytes, alpha.PendingPartitions)
	}
	if alpha.ReadCacheMissesTotal != 1 || alpha.ReadCacheHitsTotal != 1 {
		t.Fatalf("unexpected alpha read cache stats: hits=%d misses=%d", alpha.ReadCacheHitsTotal, alpha.ReadCacheMissesTotal)
	}
	if beta.PartitionRolloversTotal != 1 || beta.PendingBytes != 0 || beta.PendingPartitions != 1 {
		t.Fatalf("unexpected beta rollover/clamped stats: %+v", beta)
	}

	// Next Pop on beta advances across partition boundary, deletes .db0, and returns ErrEmpty.
	if _, err := s.Pop(ctx, 2); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected ErrEmpty after rollover, got %v", err)
	}
	st = s.Stats()
	if st[1].PartitionsDeletedTotal != 1 || st[1].PendingPartitions != 0 {
		t.Fatalf("expected beta deleted=1 pendingPartitions=0, got %+v", st[1])
	}

	// Trigger corruption counter on alpha.
	srv := s.(*fileQueue)
	srv.queues[1].mu.Lock()
	_, _ = srv.queues[1].readFM.WriteAt([]byte{0, 0, 0}, int64(srv.queues[1].ReadOffset))
	srv.queues[1].resetReadBuf()
	srv.queues[1].mu.Unlock()
	if _, err := s.Pop(ctx, 1); !errors.Is(err, ErrCorrupted) {
		t.Fatalf("expected ErrCorrupted, got %v", err)
	}

	// Trigger metadata persist counter via Close.
	s.Close(ctx)
	finalStats := s.Stats()
	if finalStats[0].CorruptionsTotal != 1 || finalStats[0].PopErrorsTotal != 1 {
		t.Fatalf("unexpected alpha corruption/error stats: %+v", finalStats[0])
	}
	if finalStats[0].MetadataPersistsTotal == 0 {
		t.Fatalf("expected MetadataPersistsTotal > 0 after Close, got %+v", finalStats[0])
	}

	// Verify WritePrometheus exposition output.
	var buf bytes.Buffer
	if err := WritePrometheus(&buf, finalStats); err != nil {
		t.Fatalf("WritePrometheus failed: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"# HELP fq_push_total ",
		"# TYPE fq_push_total counter",
		`fq_push_total{queue="alpha"} 2`,
		`fq_pop_total{queue="alpha"} 1`,
		`fq_push_bytes_total{queue="alpha"} 11`,
		`fq_pop_bytes_total{queue="alpha"} 5`,
		`fq_pop_empty_total{queue="alpha"} 1`,
		`fq_corruptions_total{queue="alpha"} 1`,
		`fq_partition_rollovers_total{queue="beta"} 1`,
		`fq_partitions_deleted_total{queue="beta"} 1`,
		"# TYPE fq_pending_bytes gauge",
		`fq_metadata_persists_total{queue="alpha"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("WritePrometheus output missing %q\ngot:\n%s", want, out)
		}
	}

	// Empty stats slice is a valid no-op.
	buf.Reset()
	if err := WritePrometheus(&buf, nil); err != nil || buf.Len() != 0 {
		t.Fatalf("WritePrometheus(nil stats) = (%v, len=%d), want (nil, 0)", err, buf.Len())
	}

	// Nil writer returns prefixed error.
	if err := WritePrometheus(nil, finalStats); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("WritePrometheus(nil writer) err = %v, want %v", err, ErrInvalidConfig)
	} else {
		assertFQErrPrefix(t, err)
	}

	// Underlying io.Writer error is wrapped with fq: prefix.
	wantErr := errors.New("broken pipe")
	if err := WritePrometheus(errWriter{err: wantErr}, finalStats); !errors.Is(err, wantErr) {
		t.Fatalf("WritePrometheus(errWriter) err = %v, want %v", err, wantErr)
	} else {
		assertFQErrPrefix(t, err)
	}

	// Label escaping for special characters in QueueStats.Name.
	buf.Reset()
	if err := WritePrometheus(&buf, []QueueStats{{Name: "a\"b\\c\nd", PushTotal: 7}}); err != nil {
		t.Fatalf("WritePrometheus escaped label failed: %v", err)
	}
	if !strings.Contains(buf.String(), `fq_push_total{queue="a\"b\\c\nd"} 7`) {
		t.Errorf("label escaping failed:\n%s", buf.String())
	}
}

func TestWriteFileAtomic_ErrorPaths(t *testing.T) {
	t.Run("rename over existing directory cleans up temp file", func(t *testing.T) {
		dir := t.TempDir()
		targetDir := filepath.Join(dir, "existing_dir")
		if err := os.Mkdir(targetDir, 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		err := writeFileAtomic(targetDir, []byte("metadata"))
		if err == nil {
			t.Fatalf("expected rename error over existing directory")
		}
		assertFQErrPrefix(t, err)

		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir failed: %v", err)
		}
		if len(entries) != 1 || entries[0].Name() != "existing_dir" {
			t.Fatalf("expected temp file to be cleaned up, found entries: %v", entries)
		}
	})

	for _, tc := range []struct {
		name string
		stub *stubTempFile
	}{
		{name: "write error", stub: &stubTempFile{name: filepath.Join(t.TempDir(), "t1"), writeErr: errors.New("write err")}},
		{name: "sync error", stub: &stubTempFile{name: filepath.Join(t.TempDir(), "t2"), syncErr: errors.New("sync err")}},
		{name: "chmod error", stub: &stubTempFile{name: filepath.Join(t.TempDir(), "t3"), chmodErr: errors.New("chmod err")}},
		{name: "close error", stub: &stubTempFile{name: filepath.Join(t.TempDir(), "t4"), closeErr: errors.New("close err")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := filepath.Join(t.TempDir(), "meta.bin")
			err := writeTempAndRename(tc.stub, dst, []byte("data"))
			if err == nil {
				t.Fatalf("writeTempAndRename(%s) expected error, got nil", tc.name)
			}
			assertFQErrPrefix(t, err)
			if !tc.stub.closed {
				t.Fatalf("writeTempAndRename(%s) expected temp file to be closed", tc.name)
			}
		})
	}
}

func TestPushNPopN_BasicPartialAndSlabIsolation(t *testing.T) {
	ctx := context.Background()
	s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "batch"})
	defer s.Close(ctx)

	// Empty PushN and non-positive PopN are clean no-ops on an open queue.
	if n, err := s.PushN(ctx, 1, nil); err != nil || n != 0 {
		t.Fatalf("PushN(nil) = (%d, %v), want (0, nil)", n, err)
	}
	if items, err := s.PopN(ctx, 1, 0); err != nil || items != nil {
		t.Fatalf("PopN(0) = (%v, %v), want (nil, nil)", items, err)
	}
	if items, err := s.PopN(ctx, 1, -5); err != nil || items != nil {
		t.Fatalf("PopN(-5) = (%v, %v), want (nil, nil)", items, err)
	}

	// PopN on empty queue returns ErrEmpty and increments PopEmptyTotal.
	if items, err := s.PopN(ctx, 1, 10); !errors.Is(err, ErrEmpty) || items != nil {
		t.Fatalf("PopN on empty = (%v, %v), want (nil, %v)", items, err, ErrEmpty)
	} else {
		assertFQErrPrefix(t, err)
	}

	const total = 100
	input := make([][]byte, total)
	for i := range total {
		input[i] = fmt.Appendf(nil, "msg-%04d", i)
	}

	if n, err := s.PushN(ctx, 1, input); err != nil || n != total {
		t.Fatalf("PushN(%d) = (%d, %v), want (%d, nil)", total, n, err, total)
	}

	// PopN(1) exercises single-item allocation path.
	single, err := s.PopN(ctx, 1, 1)
	if err != nil || len(single) != 1 || string(single[0]) != "msg-0000" {
		t.Fatalf("PopN(1) = (%q, %v), want ([msg-0000], nil)", single, err)
	}

	// PopN(64) returns exact FIFO order and 3-index append-isolated slices.
	batch, err := s.PopN(ctx, 1, 64)
	if err != nil || len(batch) != 64 {
		t.Fatalf("PopN(64) = (len=%d, %v), want (64, nil)", len(batch), err)
	}
	for i, got := range batch {
		want := fmt.Sprintf("msg-%04d", i+1)
		if string(got) != want {
			t.Fatalf("batch[%d] = %q, want %q", i, got, want)
		}
		if cap(got) != len(got) {
			t.Fatalf("batch[%d] cap=%d, want len=%d (3-index isolation)", i, cap(got), len(got))
		}
	}
	// Appending to batch[0] must allocate a new backing array and never mutate batch[1].
	batch[0] = append(batch[0], []byte("MUTATED")...)
	if string(batch[1]) != "msg-0002" {
		t.Fatalf("appending to batch[0] mutated batch[1]: got %q, want %q", batch[1], "msg-0002")
	}

	// Partial PopN with huge n returns remaining 35 items with nil error and no OOM.
	rest, err := s.PopN(ctx, 1, 1_000_000)
	if err != nil || len(rest) != 35 {
		t.Fatalf("PopN(1_000_000) = (len=%d, %v), want (35, nil)", len(rest), err)
	}
	for i, got := range rest {
		want := fmt.Sprintf("msg-%04d", i+65)
		if string(got) != want {
			t.Fatalf("rest[%d] = %q, want %q", i, got, want)
		}
	}

	// Queue is now empty again; PopEmptyTotal was only incremented by empty calls (1 before, 1 now).
	if _, err := s.PopN(ctx, 1, 8); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected ErrEmpty after drain, got %v", err)
	}
	st := s.Stats()[0]
	if st.PushTotal != total || st.PopTotal != total || st.PopEmptyTotal != 2 {
		t.Fatalf("unexpected stats after batch drain: %+v", st)
	}
}

func TestPushNPopN_MultiChunkLargePayloadsAndRollover(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	s, err := New(Params{
		Path:        dir,
		MaxFileSize: minFileSize,
		Queues:      []QueueConfig{{ID: 1, Name: "roll"}},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Build a batch mixing small items (spanning > maxWriteBufSize chunks),
	// large items (> maxWriteBufSize zero-copy path), and enough data to cross
	// multiple 1 MiB partition rollovers.
	var want [][]byte
	for i := range 120 {
		want = append(want, bytes.Repeat([]byte{byte('a' + i%26)}, 1024))
	}
	// Large payload (> maxWriteBufSize) between small items.
	largeA := bytes.Repeat([]byte("L"), maxWriteBufSize+4096)
	largeB := bytes.Repeat([]byte("M"), minFileSize) // triggers immediate rollover on a single large item
	want = append(want, []byte("before-large"), largeA, []byte("after-large"), largeB)
	// Add > popBatchInitCap (300) small items spanning another rollover.
	for i := range 300 {
		p := bytes.Repeat([]byte{byte('0' + i%10)}, 4096)
		want = append(want, p)
	}

	pushed, err := s.PushN(ctx, 1, want)
	if err != nil || pushed != len(want) {
		t.Fatalf("PushN(%d) = (%d, %v)", len(want), pushed, err)
	}

	// Close and reopen to verify persistence of batch-written partitions.
	s.Close(ctx)

	s, err = New(Params{
		Path:        dir,
		MaxFileSize: minFileSize,
		Queues:      []QueueConfig{{ID: 1, Name: "roll"}},
	})
	if err != nil {
		t.Fatalf("reopen New failed: %v", err)
	}
	defer s.Close(ctx)

	// Drain everything in one large PopN (exceeding popBatchInitCap and crossing multiple partitions).
	got, err := s.PopN(ctx, 1, len(want)+50)
	if err != nil {
		t.Fatalf("PopN across rollovers failed: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("PopN returned %d items, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("item %d mismatch: got len %d, want len %d", i, len(got[i]), len(want[i]))
		}
	}

	st := s.Stats()[0]
	if st.PartitionsDeletedTotal == 0 {
		t.Fatalf("expected consumed partitions to be deleted, stats=%+v", st)
	}
}

func TestPushNPopN_ValidationAndIOErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation, unknown queue, canceled context, and closed queue", func(t *testing.T) {
		s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "q"})

		// Canceled context on known and unknown queues.
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if n, err := s.PushN(canceled, 1, [][]byte{[]byte("x")}); !errors.Is(err, context.Canceled) || n != 0 {
			t.Fatalf("PushN canceled = (%d, %v), want (0, context.Canceled)", n, err)
		} else {
			assertFQErrPrefix(t, err)
		}
		if _, err := s.PushN(canceled, 99, [][]byte{[]byte("x")}); !errors.Is(err, context.Canceled) {
			t.Fatalf("PushN canceled unknown = %v, want context.Canceled", err)
		}
		if items, err := s.PopN(canceled, 1, 5); !errors.Is(err, context.Canceled) || items != nil {
			t.Fatalf("PopN canceled = (%v, %v), want (nil, context.Canceled)", items, err)
		} else {
			assertFQErrPrefix(t, err)
		}
		if _, err := s.PopN(canceled, 99, 5); !errors.Is(err, context.Canceled) {
			t.Fatalf("PopN canceled unknown = %v, want context.Canceled", err)
		}

		// Unknown queue ID.
		if n, err := s.PushN(ctx, 99, [][]byte{[]byte("x")}); !errors.Is(err, ErrNil) || n != 0 {
			t.Fatalf("PushN unknown = (%d, %v), want (0, ErrNil)", n, err)
		} else {
			assertFQErrPrefix(t, err)
		}
		if items, err := s.PopN(ctx, 99, 5); !errors.Is(err, ErrNil) || items != nil {
			t.Fatalf("PopN unknown = (%v, %v), want (nil, ErrNil)", items, err)
		} else {
			assertFQErrPrefix(t, err)
		}

		// All-or-nothing upfront validation: invalid item at index 1 must NOT write item 0.
		if n, err := s.PushN(ctx, 1, [][]byte{[]byte("ok"), {}}); !errors.Is(err, ErrEmptyData) || n != 0 {
			t.Fatalf("PushN empty item = (%d, %v), want (0, ErrEmptyData)", n, err)
		} else {
			assertFQErrPrefix(t, err)
		}
		huge := make([]byte, maxPayloadSize+1)
		if n, err := s.PushN(ctx, 1, [][]byte{[]byte("ok"), huge}); !errors.Is(err, ErrPayloadTooLarge) || n != 0 {
			t.Fatalf("PushN huge item = (%d, %v), want (0, ErrPayloadTooLarge)", n, err)
		} else {
			assertFQErrPrefix(t, err)
		}
		if _, err := s.Pop(ctx, 1); !errors.Is(err, ErrEmpty) {
			t.Fatalf("expected queue to remain empty after rejected PushN, got %v", err)
		}

		// Closed queue rejects PushN/PopN (even with empty items or n <= 0).
		s.Close(ctx)
		if n, err := s.PushN(ctx, 1, nil); !errors.Is(err, ErrClosed) || n != 0 {
			t.Fatalf("PushN after Close = (%d, %v), want (0, ErrClosed)", n, err)
		} else {
			assertFQErrPrefix(t, err)
		}
		if items, err := s.PopN(ctx, 1, 0); !errors.Is(err, ErrClosed) || items != nil {
			t.Fatalf("PopN after Close = (%v, %v), want (nil, ErrClosed)", items, err)
		} else {
			assertFQErrPrefix(t, err)
		}
	})

	t.Run("PushN ensureAppendFM error", func(t *testing.T) {
		dir := t.TempDir()
		s := newTestFileQueue(t, dir, QueueConfig{ID: 1, Name: "q"}).(*fileQueue)
		defer s.Close(ctx)

		q := s.queues[1]
		q.mu.Lock()
		_ = q.appendFM.Close()
		q.appendFM = nil
		q.readFM = nil
		q.mu.Unlock()

		db0 := filepath.Join(dir, "q.db0")
		_ = os.Remove(db0)
		if err := os.Mkdir(db0, 0o755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		n, err := s.PushN(ctx, 1, [][]byte{[]byte("a")})
		if err == nil || n != 0 {
			t.Fatalf("PushN ensureAppendFM = (%d, %v), want (0, error)", n, err)
		}
		assertFQErrPrefix(t, err)
	})

	t.Run("PushN partial commit on rollover partition creation error", func(t *testing.T) {
		dir := t.TempDir()
		s, err := New(Params{
			Path:        dir,
			MaxFileSize: minFileSize,
			Queues:      []QueueConfig{{ID: 1, Name: "q"}},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer s.Close(ctx)

		// Block q.db1 so the rollover chunk fails while the earlier chunk succeeds.
		if err := os.Mkdir(filepath.Join(dir, "q.db1"), 0o755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		items := [][]byte{
			[]byte("first-committed"),
			bytes.Repeat([]byte("r"), minFileSize), // separate chunk triggering rollover to blocked q.db1
		}
		n, err := s.PushN(ctx, 1, items)
		if err == nil || n != 1 {
			t.Fatalf("PushN partial rollover error = (%d, %v), want (1, error)", n, err)
		}
		assertFQErrPrefix(t, err)

		got, err := s.Pop(ctx, 1)
		if err != nil || string(got) != "first-committed" {
			t.Fatalf("Pop committed item = (%q, %v), want (%q, nil)", got, err, "first-committed")
		}
	})

	t.Run("PushN write failure during rollover cleans up created partition", func(t *testing.T) {
		dir := t.TempDir()
		s, err := New(Params{
			Path:        dir,
			MaxFileSize: minFileSize,
			Queues:      []QueueConfig{{ID: 1, Name: "q"}},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		defer s.Close(ctx)

		q := s.(*fileQueue).queues[1]
		q.mu.Lock()
		stub := &stubFileStore{fileStore: q.appendFM, writeErr: errors.New("disk full")}
		q.appendFM = stub
		q.readFM = stub
		q.mu.Unlock()

		n, err := s.PushN(ctx, 1, [][]byte{bytes.Repeat([]byte("r"), minFileSize)})
		if err == nil || n != 0 {
			t.Fatalf("PushN write error = (%d, %v), want (0, error)", n, err)
		}
		assertFQErrPrefix(t, err)

		if _, statErr := os.Stat(filepath.Join(dir, "q.db1")); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("expected rollover partition q.db1 cleanup, stat err = %v", statErr)
		}
	})

	t.Run("PopN returns already-popped items alongside mid-batch I/O error", func(t *testing.T) {
		s := newTestFileQueue(t, t.TempDir(), QueueConfig{ID: 1, Name: "q"}).(*fileQueue)
		defer s.Close(ctx)

		large := bytes.Repeat([]byte("P"), readAheadBufSize+100)
		if _, err := s.PushN(ctx, 1, [][]byte{[]byte("ok-first"), large}); err != nil {
			t.Fatalf("PushN failed: %v", err)
		}

		// ReadAt #1 fills readBuf (serves item 0 header+payload AND item 1 header);
		// ReadAt #2 reads item 1's large payload directly and fails.
		q := s.queues[1]
		q.mu.Lock()
		stub := &stubFileStore{
			fileStore: q.readFM,
			readSeq:   []error{nil, errors.New("payload io error")},
		}
		q.readFM = stub
		q.appendFM = stub
		q.mu.Unlock()

		got, err := s.PopN(ctx, 1, 2)
		if !errors.Is(err, ErrFileRead) {
			t.Fatalf("PopN mid-batch err = %v, want %v", err, ErrFileRead)
		}
		assertFQErrPrefix(t, err)
		if len(got) != 1 || string(got[0]) != "ok-first" {
			t.Fatalf("PopN partial items = %q, want [%q]", got, "ok-first")
		}
	})
}

func TestPushNPopN_ConcurrentStress(t *testing.T) {
	ctx := context.Background()
	s, err := New(Params{
		Path:            t.TempDir(),
		MaxFileSize:     minFileSize,
		PersistInterval: 10 * time.Millisecond,
		Queues:          []QueueConfig{{ID: 1, Name: "stress-batch"}},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer s.Close(ctx)

	const (
		producers   = 4
		consumers   = 4
		batchesEach = 30
		batchSize   = 25
		totalMsgs   = producers * batchesEach * batchSize
	)

	var (
		prodWG   sync.WaitGroup
		consWG   sync.WaitGroup
		seenMu   sync.Mutex
		seen     = make(map[string]int, totalMsgs)
		consumed atomic.Int64
	)

	for p := range producers {
		prodWG.Go(func() {
			for b := range batchesEach {
				items := make([][]byte, batchSize)
				for i := range batchSize {
					items[i] = fmt.Appendf(nil, "p%d-b%d-i%d", p, b, i)
				}
				if n, err := s.PushN(ctx, 1, items); err != nil || n != batchSize {
					t.Errorf("PushN failed: n=%d err=%v", n, err)
					return
				}
			}
		})
	}

	doneProducing := make(chan struct{})
	go func() {
		prodWG.Wait()
		close(doneProducing)
	}()

	for range consumers {
		consWG.Go(func() {
			for {
				items, err := s.PopN(ctx, 1, 16)
				if err == nil {
					seenMu.Lock()
					for _, data := range items {
						seen[string(data)]++
					}
					seenMu.Unlock()
					consumed.Add(int64(len(items)))
					continue
				}
				if !errors.Is(err, ErrEmpty) {
					t.Errorf("PopN error: %v", err)
					return
				}
				select {
				case <-doneProducing:
					if consumed.Load() >= totalMsgs {
						return
					}
				default:
					runtime.Gosched()
				}
			}
		})
	}

	consWG.Wait()

	if len(seen) != totalMsgs {
		t.Fatalf("consumed %d unique messages, want %d", len(seen), totalMsgs)
	}
	for msg, count := range seen {
		if count != 1 {
			t.Fatalf("message %q delivered %d times, want 1", msg, count)
		}
	}
}
