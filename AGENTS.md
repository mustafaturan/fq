# AGENTS.md

Guidelines for coding agents working in `github.com/mustafaturan/fq` and instructions for installing this repository's Agent Skills in consumer projects.

## Repository Architecture

- [fq.go](./fq.go) — Core `FileQueue` implementation (`New`, `Push`, `Pop`, `Stats`, `Close`), atomic metadata persistence (`<name>.bin`), partition rollover (`<name>.db<N>`), 64 KiB scratch/read-ahead buffers, and `Logger` interface.
- [metrics.go](./metrics.go) — `QueueStats`, `PrometheusContentType`, and zero-dependency `WritePrometheus` text exporter.
- [internal/fm/fm.go](./internal/fm/fm.go) — Internal `FileManager` implementing `io.ReaderAt`, `io.WriterAt`, `io.Closer`, `Sync`, and atomic `O_CREATE|O_EXCL` pre-allocation.
- [fq_test.go](./fq_test.go), [fq_bench_test.go](./fq_bench_test.go), [internal/fm/fm_test.go](./internal/fm/fm_test.go) — Unit, concurrency/stress, and benchmark suites.
- [.golangci.yml](./.golangci.yml) — `golangci-lint` v2 configuration.

## Coding Conventions

- **Zero external dependencies**: Keep `go.mod` pure Go standard library (`go mod tidy` must add no external `require` entries).
- **Effective Go & modern stdlib**: Use canonical stdlib interfaces (`io.ReaderAt`, `io.WriterAt`, `io.Closer`, `*slog.Logger`-compatible `Logger`), early returns, `for range N`, `wg.Go`, and `slices.Sort`.
- **Constant sentinel errors**: Exported sentinel errors must be `const` values backed by `type Error string` implementing `Error() string`.
  - All `fq` errors must start with `"fq: "` (no duplicate prefix).
  - All `internal/fm` errors must start with `"fq/fm: "` (no duplicate prefix).
- **YAML tags**: Use `camelCase` (`path`, `queues`, `maxFileSize`, `persistInterval`).
- **File permissions**: Create persisted data and metadata files with `0o600`.
- **Concurrency invariants**: Lock ordering is strictly `q.metaMu` (outer, disk metadata writes) → `q.mu` (inner, hot path). Never hold two queue mutexes simultaneously, and never perform `fsync` while holding `q.mu`.
- **100% statement coverage**: Maintain `100.0%` statement coverage and zero race warnings across both `fq` and `internal/fm`.

## Verification Commands

Run after any code change:

```bash
gofmt -l .
go vet ./...
go fix -diff ./...
go test -race -timeout 30s -cover -count=1 ./...
```

Optional linters & benchmarks:

```bash
golangci-lint run ./...
go test -run='^$' -bench=. -benchmem -benchtime=250ms -count=3 .
```

## Agent Skills in This Repository

This repo ships reusable Agent Skills under `.agents/skills/`:

| Skill | Path | Purpose |
| :--- | :--- | :--- |
| `fq-usage` | [.agents/skills/fq-usage/SKILL.md](./.agents/skills/fq-usage/SKILL.md) | Configuring, integrating, operating, and troubleshooting `fq` in Go applications (with [references/api.md](./.agents/skills/fq-usage/references/api.md)). |
| `fq-serialization` | [.agents/skills/fq-serialization/SKILL.md](./.agents/skills/fq-serialization/SKILL.md) | Zero-allocation, versioned binary codecs (`encoding/binary`, `msgp`, Protobuf, FlatBuffers, Cap'n Proto) for `fq` payloads. |

### How to Install These Skills in Your Project

#### Option 1: From the Go module cache (recommended when `fq` is in your `go.mod`)

```bash
# Project-scoped (Antigravity, Codex, Cursor, and open Agent Skills tools)
FQ_DIR="$(go list -m -f '{{.Dir}}' github.com/mustafaturan/fq)"
mkdir -p .agents/skills
cp -R "$FQ_DIR/.agents/skills/fq-usage" "$FQ_DIR/.agents/skills/fq-serialization" .agents/skills/
```

For **Claude Code** (project-scoped):

```bash
FQ_DIR="$(go list -m -f '{{.Dir}}' github.com/mustafaturan/fq)"
mkdir -p .claude/skills
cp -R "$FQ_DIR/.agents/skills/fq-usage" "$FQ_DIR/.agents/skills/fq-serialization" .claude/skills/
```

#### Option 2: Global installation (available in all workspaces on your machine)

- **Antigravity**:
  ```bash
  FQ_DIR="$(go list -m -f '{{.Dir}}' github.com/mustafaturan/fq)"
  mkdir -p ~/.gemini/config/skills
  cp -R "$FQ_DIR/.agents/skills/fq-usage" "$FQ_DIR/.agents/skills/fq-serialization" ~/.gemini/config/skills/
  ```
- **Claude Code**:
  ```bash
  FQ_DIR="$(go list -m -f '{{.Dir}}' github.com/mustafaturan/fq)"
  mkdir -p ~/.claude/skills
  cp -R "$FQ_DIR/.agents/skills/fq-usage" "$FQ_DIR/.agents/skills/fq-serialization" ~/.claude/skills/
  ```

#### Option 3: Using the Agent Skills CLI (`npx skills`)

```bash
npx skills add mustafaturan/fq
```

To install a specific skill only:

```bash
npx skills add mustafaturan/fq --skill fq-usage
```

#### Option 4: Clone directly from GitHub

```bash
git clone --depth 1 https://github.com/mustafaturan/fq.git /tmp/fq
mkdir -p .agents/skills
cp -R /tmp/fq/.agents/skills/fq-* .agents/skills/
rm -rf /tmp/fq
```

