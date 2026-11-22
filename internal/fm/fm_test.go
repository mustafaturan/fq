package fm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func assertErrPrefix(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected non-nil error")
	}
	if !strings.HasPrefix(err.Error(), "fq/fm: ") {
		t.Errorf("error %q does not start with %q", err.Error(), "fq/fm: ")
	}
	if strings.Count(err.Error(), "fq/fm: ") > 1 {
		t.Errorf("error %q contains duplicate %q prefix", err.Error(), "fq/fm: ")
	}
}

func TestNew_CreateAndPreallocate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db0")
	const size int64 = 1024

	m, err := New(path, size)
	if err != nil {
		t.Fatalf("New(%q, %d) unexpected error: %v", path, size, err)
	}
	defer func() {
		if err := m.Close(); err != nil {
			t.Errorf("Close() unexpected error: %v", err)
		}
	}()

	if got := m.Path(); got != path {
		t.Fatalf("Path() = %q, want %q", got, path)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("os.Stat(%q) unexpected error: %v", path, err)
	}
	if info.Size() != size {
		t.Fatalf("file size = %d, want %d", info.Size(), size)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("file perm = %04o, want 0600", perm)
	}

	got := make([]byte, size)
	n, err := m.ReadAt(got, 0)
	if err != nil {
		t.Fatalf("ReadAt(0) unexpected error: %v", err)
	}
	if n != int(size) {
		t.Fatalf("ReadAt(0) n = %d, want %d", n, size)
	}
	if !bytes.Equal(got, make([]byte, size)) {
		t.Fatalf("newly preallocated file is not zero-filled")
	}
}

func TestNew_EmptyPath(t *testing.T) {
	m, err := New("", 1024)
	if !errors.Is(err, ErrInvalidPath) {
		if m != nil {
			_ = m.Close()
		}
		t.Fatalf("New(\"\", 1024) err = %v, want %v", err, ErrInvalidPath)
	}
	assertErrPrefix(t, err)
	if m != nil {
		_ = m.Close()
		t.Fatalf("expected nil FileManager on empty path")
	}
}

func TestNew_ZeroSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.db0")

	m, err := New(path, 0)
	if err != nil {
		t.Fatalf("New(%q, 0) unexpected error: %v", path, err)
	}
	defer m.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("os.Stat(%q) unexpected error: %v", path, err)
	}
	if info.Size() != 0 {
		t.Fatalf("file size = %d, want 0", info.Size())
	}
}

func TestNew_NegativeSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "neg.db0")

	m, err := New(path, -1)
	if !errors.Is(err, ErrInvalidSize) {
		if m != nil {
			_ = m.Close()
		}
		t.Fatalf("New(%q, -1) err = %v, want %v", path, err, ErrInvalidSize)
	}
	assertErrPrefix(t, err)
	if m != nil {
		_ = m.Close()
		t.Fatalf("New(%q, -1) expected nil FileManager on error", path)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected file not to be created on negative size, stat err = %v", statErr)
	}
}

func TestNew_ExistingFilePreservesData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.db0")
	content := []byte("hello world")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatalf("os.WriteFile failed: %v", err)
	}

	// Reopen with both a larger and smaller requested size; existing data and size must remain intact.
	for _, reqSize := range []int64{1024, 4} {
		m, err := New(path, reqSize)
		if err != nil {
			t.Fatalf("New(%q, %d) unexpected error: %v", path, reqSize, err)
		}

		info, err := os.Stat(path)
		if err != nil {
			_ = m.Close()
			t.Fatalf("os.Stat failed: %v", err)
		}
		if info.Size() != int64(len(content)) {
			_ = m.Close()
			t.Fatalf("reopening existing file with size %d changed file size to %d, want %d", reqSize, info.Size(), len(content))
		}

		got := make([]byte, len(content))
		if _, err := m.ReadAt(got, 0); err != nil {
			_ = m.Close()
			t.Fatalf("ReadAt failed: %v", err)
		}
		if !bytes.Equal(got, content) {
			_ = m.Close()
			t.Fatalf("ReadAt = %q, want %q", got, content)
		}

		if err := m.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}
}

func TestNew_ExistingZeroByteFilePreallocates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "zero.db0")
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatalf("os.WriteFile failed: %v", err)
	}

	const size int64 = 512
	m, err := New(path, size)
	if err != nil {
		t.Fatalf("New(%q, %d) unexpected error: %v", path, size, err)
	}
	defer m.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("os.Stat failed: %v", err)
	}
	if info.Size() != size {
		t.Fatalf("file size = %d, want %d", info.Size(), size)
	}
}

func TestNew_ExistingReadOnlyFileReturnsError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	path := filepath.Join(t.TempDir(), "readonly.db0")
	if err := os.WriteFile(path, []byte("locked"), 0444); err != nil {
		t.Fatalf("os.WriteFile failed: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(path, 0644)
	})

	m, err := New(path, 1024)
	if err == nil {
		if m != nil {
			_ = m.Close()
		}
		t.Fatalf("New on read-only file expected error, got nil (fm=%+v)", m)
	}
	assertErrPrefix(t, err)
	if m != nil {
		_ = m.Close()
		t.Fatalf("New on read-only file expected nil FileManager, got %+v", m)
	}
}

func TestNew_ExistingDirectoryReturnsError(t *testing.T) {
	dir := t.TempDir()

	m, err := New(dir, 1024)
	if err == nil {
		if m != nil {
			_ = m.Close()
		}
		t.Fatalf("New on directory path expected error, got nil (fm=%+v)", m)
	}
	assertErrPrefix(t, err)
	if m != nil {
		_ = m.Close()
		t.Fatalf("New on directory path expected nil FileManager, got %+v", m)
	}
}

func TestNew_MissingParentDirReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nonexistent", "test.db0")

	m, err := New(path, 1024)
	if err == nil {
		if m != nil {
			_ = m.Close()
		}
		t.Fatalf("New with missing parent dir expected error, got nil")
	}
	assertErrPrefix(t, err)
	if m != nil {
		_ = m.Close()
		t.Fatalf("New with missing parent dir expected nil FileManager")
	}
}

func TestNew_TruncateFailureCleansUpNewFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "huge.db0")

	m, err := New(path, math.MaxInt64)
	if err == nil {
		if m != nil {
			_ = m.Close()
		}
		t.Skip("filesystem unexpectedly accepted math.MaxInt64 truncate size")
	}
	assertErrPrefix(t, err)
	if m != nil {
		_ = m.Close()
		t.Fatalf("expected nil FileManager on truncate error")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected newly created file to be removed after truncate failure, stat err = %v", statErr)
	}
}

func TestNew_ConcurrentOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent_open.db0")
	const (
		workers       = 16
		size    int64 = 4096
	)

	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for range workers {
		wg.Go(func() {
			m, err := New(path, size)
			if err != nil {
				errCh <- err
				return
			}
			if err := m.Close(); err != nil {
				errCh <- err
			}
		})
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent New failed: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("os.Stat failed: %v", err)
	}
	if info.Size() != size {
		t.Fatalf("file size = %d, want %d", info.Size(), size)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("file perm = %04o, want 0600", perm)
	}
}

func TestWriteAtAndReadAt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rw.db0")
	m, err := New(path, 64)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	first := []byte("hello")
	if n, err := m.WriteAt(first, 0); err != nil || n != len(first) {
		t.Fatalf("WriteAt(0) = (%d, %v), want (%d, nil)", n, err, len(first))
	}

	mid := []byte("world!")
	if n, err := m.WriteAt(mid, 20); err != nil || n != len(mid) {
		t.Fatalf("WriteAt(20) = (%d, %v), want (%d, nil)", n, err, len(mid))
	}

	tail := []byte("beyond-initial-size")
	if n, err := m.WriteAt(tail, 60); err != nil || n != len(tail) {
		t.Fatalf("WriteAt(60) = (%d, %v), want (%d, nil)", n, err, len(tail))
	}

	if err := m.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	gotFirst := make([]byte, len(first))
	if _, err := m.ReadAt(gotFirst, 0); err != nil {
		t.Fatalf("ReadAt(0) failed: %v", err)
	}
	if !bytes.Equal(gotFirst, first) {
		t.Fatalf("ReadAt(0) = %q, want %q", gotFirst, first)
	}

	gap := make([]byte, 20-len(first))
	if _, err := m.ReadAt(gap, int64(len(first))); err != nil {
		t.Fatalf("ReadAt gap failed: %v", err)
	}
	if !bytes.Equal(gap, make([]byte, 20-len(first))) {
		t.Fatalf("gap bytes were unexpectedly modified: %x", gap)
	}

	gotMid := make([]byte, len(mid))
	if _, err := m.ReadAt(gotMid, 20); err != nil {
		t.Fatalf("ReadAt(20) failed: %v", err)
	}
	if !bytes.Equal(gotMid, mid) {
		t.Fatalf("ReadAt(20) = %q, want %q", gotMid, mid)
	}

	gotTail := make([]byte, len(tail))
	if _, err := m.ReadAt(gotTail, 60); err != nil {
		t.Fatalf("ReadAt(60) failed: %v", err)
	}
	if !bytes.Equal(gotTail, tail) {
		t.Fatalf("ReadAt(60) = %q, want %q", gotTail, tail)
	}
}

func TestWriteAt_NegativeOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "neg_write.db0")
	m, err := New(path, 64)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	_, err = m.WriteAt([]byte("abc"), -1)
	if !errors.Is(err, ErrInvalidOffset) {
		t.Fatalf("WriteAt with negative offset err = %v, want %v", err, ErrInvalidOffset)
	}
	assertErrPrefix(t, err)
}

func TestReadAt_ValidationAndErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "read_err.db0")
	m, err := New(path, 16)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	t.Run("negative offset", func(t *testing.T) {
		buf := make([]byte, 4)
		_, err := m.ReadAt(buf, -1)
		if !errors.Is(err, ErrInvalidOffset) {
			t.Fatalf("ReadAt(-1) err = %v, want %v", err, ErrInvalidOffset)
		}
		assertErrPrefix(t, err)
	})

	t.Run("empty buffer", func(t *testing.T) {
		n, err := m.ReadAt(nil, 0)
		if err != nil || n != 0 {
			t.Fatalf("ReadAt(nil, 0) = (%d, %v), want (0, nil)", n, err)
		}
	})

	t.Run("past EOF", func(t *testing.T) {
		buf := make([]byte, 4)
		n, err := m.ReadAt(buf, 16)
		if !errors.Is(err, io.EOF) || n != 0 {
			t.Fatalf("ReadAt(16) = (%d, %v), want (0, io.EOF)", n, err)
		}
		assertErrPrefix(t, err)
	})

	t.Run("straddling EOF returns partial count and EOF", func(t *testing.T) {
		buf := make([]byte, 4)
		n, err := m.ReadAt(buf, 14)
		if !errors.Is(err, io.EOF) || n != 2 {
			t.Fatalf("ReadAt(14) = (%d, %v), want (2, io.EOF)", n, err)
		}
		assertErrPrefix(t, err)
	})
}

func TestOperationsAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "closed.db0")
	m, err := New(path, 64)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := m.Close(); err != nil {
		t.Fatalf("first Close() unexpected error: %v", err)
	}

	if _, err := m.WriteAt([]byte("data"), 0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("WriteAt after Close err = %v, want os.ErrClosed", err)
	} else {
		assertErrPrefix(t, err)
	}

	if _, err := m.ReadAt(make([]byte, 4), 0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("ReadAt after Close err = %v, want os.ErrClosed", err)
	} else {
		assertErrPrefix(t, err)
	}

	if err := m.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Sync after Close err = %v, want os.ErrClosed", err)
	} else {
		assertErrPrefix(t, err)
	}

	if err := m.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("second Close() err = %v, want os.ErrClosed", err)
	} else {
		assertErrPrefix(t, err)
	}
}

func TestNilAndZeroValueFileManager(t *testing.T) {
	for _, tc := range []struct {
		name string
		fm   *FileManager
	}{
		{name: "nil receiver", fm: nil},
		{name: "zero value", fm: &FileManager{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.fm.Path(); got != "" {
				t.Fatalf("Path() = %q, want empty", got)
			}
			if _, err := tc.fm.WriteAt([]byte("a"), 0); !errors.Is(err, os.ErrInvalid) {
				t.Fatalf("WriteAt err = %v, want os.ErrInvalid", err)
			} else {
				assertErrPrefix(t, err)
			}
			if _, err := tc.fm.ReadAt(make([]byte, 1), 0); !errors.Is(err, os.ErrInvalid) {
				t.Fatalf("ReadAt err = %v, want os.ErrInvalid", err)
			} else {
				assertErrPrefix(t, err)
			}
			if err := tc.fm.Sync(); !errors.Is(err, os.ErrInvalid) {
				t.Fatalf("Sync err = %v, want os.ErrInvalid", err)
			} else {
				assertErrPrefix(t, err)
			}
			if err := tc.fm.Close(); !errors.Is(err, os.ErrInvalid) {
				t.Fatalf("Close err = %v, want os.ErrInvalid", err)
			} else {
				assertErrPrefix(t, err)
			}
		})
	}
}

func TestConcurrentWriteAtAndReadAt(t *testing.T) {
	const (
		workers   = 16
		chunkSize = 32
	)

	path := filepath.Join(t.TempDir(), "concurrent.db0")
	m, err := New(path, workers*chunkSize)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			offset := int64(id * chunkSize)
			payload := bytes.Repeat([]byte{byte('A' + id)}, chunkSize)

			if _, err := m.WriteAt(payload, offset); err != nil {
				errCh <- fmt.Errorf("worker %d write: %w", id, err)
				return
			}

			got := make([]byte, chunkSize)
			if _, err := m.ReadAt(got, offset); err != nil {
				errCh <- fmt.Errorf("worker %d read: %w", id, err)
				return
			}
			if !bytes.Equal(got, payload) {
				errCh <- fmt.Errorf("worker %d data mismatch: got %q, want %q", id, got, payload)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}

type stubPreallocFile struct {
	statErr     error
	truncateErr error
	closed      bool
}

func (s *stubPreallocFile) Stat() (os.FileInfo, error) { return nil, s.statErr }
func (s *stubPreallocFile) Truncate(int64) error       { return s.truncateErr }
func (s *stubPreallocFile) Close() error               { s.closed = true; return nil }

func TestPreallocate_StatFailureClosesFile(t *testing.T) {
	wantErr := errors.New("stat boom")
	stub := &stubPreallocFile{statErr: wantErr}

	err := preallocate(stub, "/tmp/test.db0", 1024, false)
	if !errors.Is(err, wantErr) {
		t.Fatalf("preallocate err = %v, want %v", err, wantErr)
	}
	assertErrPrefix(t, err)
	if !stub.closed {
		t.Fatalf("expected file to be closed on Stat failure")
	}
}
