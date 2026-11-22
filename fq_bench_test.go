package fq

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func BenchmarkPush(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{name: "64B", size: 64},
		{name: "1KiB", size: 1024},
		{name: "128KiB", size: 128 * 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			s, err := New(Params{
				Path:   b.TempDir(),
				Queues: []QueueConfig{{ID: 1, Name: "bench"}},
			})
			if err != nil {
				b.Fatalf("New failed: %v", err)
			}
			defer s.Close(ctx)

			payload := bytes.Repeat([]byte("a"), tc.size)
			b.SetBytes(int64(tc.size))
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				if err := s.Push(ctx, 1, payload); err != nil {
					b.Fatalf("Push failed: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkPop(b *testing.B) {
	const batchSize = 4096

	for _, tc := range []struct {
		name string
		size int
	}{
		{name: "64B", size: 64},
		{name: "1KiB", size: 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			s, err := New(Params{
				Path:   b.TempDir(),
				Queues: []QueueConfig{{ID: 1, Name: "bench"}},
			})
			if err != nil {
				b.Fatalf("New failed: %v", err)
			}
			defer s.Close(ctx)

			payload := bytes.Repeat([]byte("b"), tc.size)
			refill := func() {
				for range batchSize {
					if err := s.Push(ctx, 1, payload); err != nil {
						b.Fatalf("refill Push failed: %v", err)
					}
				}
			}

			refill()
			b.SetBytes(int64(tc.size))
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				_, err := s.Pop(ctx, 1)
				if errors.Is(err, ErrEmpty) {
					b.StopTimer()
					refill()
					b.StartTimer()
					if _, err = s.Pop(ctx, 1); err != nil {
						b.Fatalf("Pop after refill failed: %v", err)
					}
					continue
				}
				if err != nil {
					b.Fatalf("Pop failed: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkPushPop(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{name: "64B", size: 64},
		{name: "1KiB", size: 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			s, err := New(Params{
				Path:   b.TempDir(),
				Queues: []QueueConfig{{ID: 1, Name: "bench"}},
			})
			if err != nil {
				b.Fatalf("New failed: %v", err)
			}
			defer s.Close(ctx)

			payload := bytes.Repeat([]byte("c"), tc.size)
			b.SetBytes(int64(tc.size))
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				if err := s.Push(ctx, 1, payload); err != nil {
					b.Fatalf("Push failed: %v", err)
				}
				if _, err := s.Pop(ctx, 1); err != nil {
					b.Fatalf("Pop failed: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkPushPopParallel(b *testing.B) {
	ctx := context.Background()
	s, err := New(Params{
		Path:   b.TempDir(),
		Queues: []QueueConfig{{ID: 1, Name: "bench"}},
	})
	if err != nil {
		b.Fatalf("New failed: %v", err)
	}
	defer s.Close(ctx)

	payload := bytes.Repeat([]byte("p"), 64)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.Push(ctx, 1, payload); err != nil {
				b.Errorf("Push failed: %v", err)
				return
			}
			if _, err := s.Pop(ctx, 1); err != nil && !errors.Is(err, ErrEmpty) {
				b.Errorf("Pop failed: %v", err)
				return
			}
		}
	})
	b.StopTimer()
}

func BenchmarkPushN(b *testing.B) {
	const batchSize = 64

	for _, tc := range []struct {
		name string
		size int
	}{
		{name: "64B_x64", size: 64},
		{name: "1KiB_x64", size: 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			s, err := New(Params{
				Path:   b.TempDir(),
				Queues: []QueueConfig{{ID: 1, Name: "bench"}},
			})
			if err != nil {
				b.Fatalf("New failed: %v", err)
			}
			defer s.Close(ctx)

			payload := bytes.Repeat([]byte("a"), tc.size)
			batch := make([][]byte, batchSize)
			for i := range batch {
				batch[i] = payload
			}

			b.SetBytes(int64(tc.size * batchSize))
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				if _, err := s.PushN(ctx, 1, batch); err != nil {
					b.Fatalf("PushN failed: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkPopN(b *testing.B) {
	const (
		batchSize  = 64
		refillSets = 64
	)

	for _, tc := range []struct {
		name string
		size int
	}{
		{name: "64B_x64", size: 64},
		{name: "1KiB_x64", size: 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			s, err := New(Params{
				Path:   b.TempDir(),
				Queues: []QueueConfig{{ID: 1, Name: "bench"}},
			})
			if err != nil {
				b.Fatalf("New failed: %v", err)
			}
			defer s.Close(ctx)

			payload := bytes.Repeat([]byte("b"), tc.size)
			batch := make([][]byte, batchSize)
			for i := range batch {
				batch[i] = payload
			}

			refill := func() {
				for range refillSets {
					if _, err := s.PushN(ctx, 1, batch); err != nil {
						b.Fatalf("refill PushN failed: %v", err)
					}
				}
			}

			refill()
			b.SetBytes(int64(tc.size * batchSize))
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				_, err := s.PopN(ctx, 1, batchSize)
				if errors.Is(err, ErrEmpty) {
					b.StopTimer()
					refill()
					b.StartTimer()
					if _, err = s.PopN(ctx, 1, batchSize); err != nil {
						b.Fatalf("PopN after refill failed: %v", err)
					}
					continue
				}
				if err != nil {
					b.Fatalf("PopN failed: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkPushNPopNParallel(b *testing.B) {
	const batchSize = 64

	ctx := context.Background()
	s, err := New(Params{
		Path:   b.TempDir(),
		Queues: []QueueConfig{{ID: 1, Name: "bench"}},
	})
	if err != nil {
		b.Fatalf("New failed: %v", err)
	}
	defer s.Close(ctx)

	payload := bytes.Repeat([]byte("p"), 64)
	batch := make([][]byte, batchSize)
	for i := range batch {
		batch[i] = payload
	}

	b.SetBytes(int64(len(payload) * batchSize))
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := s.PushN(ctx, 1, batch); err != nil {
				b.Errorf("PushN failed: %v", err)
				return
			}
			if _, err := s.PopN(ctx, 1, batchSize); err != nil && !errors.Is(err, ErrEmpty) {
				b.Errorf("PopN failed: %v", err)
				return
			}
		}
	})
	b.StopTimer()
}
