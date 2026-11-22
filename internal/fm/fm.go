// Package fm provides positional file I/O with optional initial size pre-allocation.
package fm

import (
	"errors"
	"fmt"
	"io"
	"os"
)

const fileMode os.FileMode = 0o600

// Error represents a constant sentinel error returned by package fm.
type Error string

// Error implements the error interface.
func (e Error) Error() string { return string(e) }

const (
	// ErrInvalidPath is returned when an empty file path is supplied to New.
	ErrInvalidPath Error = "fq/fm: file path is required"
	// ErrInvalidSize is returned when a negative size is supplied to New.
	ErrInvalidSize Error = "fq/fm: size must be non-negative"
	// ErrInvalidOffset is returned when a negative byte offset is supplied to ReadAt or WriteAt.
	ErrInvalidOffset Error = "fq/fm: offset must be non-negative"
)

var (
	_ io.ReaderAt = (*FileManager)(nil)
	_ io.WriterAt = (*FileManager)(nil)
	_ io.Closer   = (*FileManager)(nil)
)

type preallocFile interface {
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
	Close() error
}

// FileManager manages positional reads and writes against a single *os.File.
// It implements io.ReaderAt, io.WriterAt, and io.Closer. Concurrent ReadAt and
// WriteAt calls on non-overlapping regions are safe because they use pread/pwrite
// semantics without mutating a shared file offset.
type FileManager struct {
	file *os.File
}

// New opens the file at filePath for reading and writing, creating it if it does not exist.
// If the file is newly created (or has size 0) and size > 0, it is pre-allocated to size bytes
// via Truncate. Existing non-empty files are opened without modifying their size or contents.
func New(filePath string, size int64) (*FileManager, error) {
	if filePath == "" {
		return nil, ErrInvalidPath
	}
	if size < 0 {
		return nil, fmt.Errorf("%w: %d", ErrInvalidSize, size)
	}

	// Attempt atomic exclusive creation first to avoid a Stat-then-Open TOCTOU race
	// and to know whether cleanup is safe if pre-allocation fails.
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, fileMode)
	created := err == nil
	switch {
	case err == nil:
	case errors.Is(err, os.ErrExist):
		file, err = os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, fileMode)
		if err != nil {
			return nil, fmt.Errorf("fq/fm: failed to open file %q: %w", filePath, err)
		}
	default:
		return nil, fmt.Errorf("fq/fm: failed to create file %q: %w", filePath, err)
	}

	if err := preallocate(file, filePath, size, created); err != nil {
		return nil, err
	}

	return &FileManager{file: file}, nil
}

func preallocate(file preallocFile, filePath string, size int64, created bool) error {
	if size == 0 {
		return nil
	}

	needsTruncate := created
	if !needsTruncate {
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return fmt.Errorf("fq/fm: failed to stat file %q: %w", filePath, err)
		}
		needsTruncate = info.Size() == 0
	}

	if !needsTruncate {
		return nil
	}

	if err := file.Truncate(size); err != nil {
		_ = file.Close()
		if created {
			_ = os.Remove(filePath)
		}
		return fmt.Errorf("fq/fm: failed to set file size for %q: %w", filePath, err)
	}
	return nil
}

// Path returns the path of the underlying file, or an empty string if m is uninitialized.
func (m *FileManager) Path() string {
	if m == nil || m.file == nil {
		return ""
	}
	return m.file.Name()
}

// WriteAt writes len(p) bytes from p to the underlying file starting at byte offset off.
// It implements io.WriterAt.
func (m *FileManager) WriteAt(p []byte, off int64) (int, error) {
	if m == nil || m.file == nil {
		return 0, fmt.Errorf("fq/fm: failed writing to file: %w", os.ErrInvalid)
	}
	if off < 0 {
		return 0, fmt.Errorf("%w: write offset %d", ErrInvalidOffset, off)
	}
	n, err := m.file.WriteAt(p, off)
	if err != nil {
		return n, fmt.Errorf("fq/fm: failed writing to file: %w", err)
	}
	return n, nil
}

// ReadAt reads len(p) bytes into p starting at byte offset off in the underlying file.
// It implements io.ReaderAt.
func (m *FileManager) ReadAt(p []byte, off int64) (int, error) {
	if m == nil || m.file == nil {
		return 0, fmt.Errorf("fq/fm: failed reading from file: %w", os.ErrInvalid)
	}
	if off < 0 {
		return 0, fmt.Errorf("%w: read offset %d", ErrInvalidOffset, off)
	}
	n, err := m.file.ReadAt(p, off)
	if err != nil {
		return n, fmt.Errorf("fq/fm: failed reading from file: %w", err)
	}
	return n, nil
}

// Sync commits the current contents of the file to stable storage.
func (m *FileManager) Sync() error {
	if m == nil || m.file == nil {
		return fmt.Errorf("fq/fm: failed syncing file: %w", os.ErrInvalid)
	}
	if err := m.file.Sync(); err != nil {
		return fmt.Errorf("fq/fm: failed syncing file: %w", err)
	}
	return nil
}

// Close closes the underlying file associated with the FileManager.
// It implements io.Closer.
func (m *FileManager) Close() error {
	if m == nil || m.file == nil {
		return fmt.Errorf("fq/fm: failed closing file: %w", os.ErrInvalid)
	}
	if err := m.file.Close(); err != nil {
		return fmt.Errorf("fq/fm: failed closing file: %w", err)
	}
	return nil
}
