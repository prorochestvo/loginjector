package loginjector

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func TelegramHandler(botToken, chatID, fileName string, labels ...string) io.Writer {
	const telegramAPI = "https://api.telegram.org/bot"
	url := fmt.Sprintf("%s%s/sendDocument", telegramAPI, botToken)
	// redactToken masks botToken in an error string before it is surfaced: a failed
	// request build or client.Do returns a *url.Error whose text embeds the full URL,
	// token included, which would otherwise land in logs and log files.
	redactToken := func(s string) string {
		// guard: an empty or very short token would over-match unrelated substrings, and
		// no real Telegram token is that short, so skip redaction below the threshold.
		if len(botToken) < 8 {
			return s
		}
		return strings.ReplaceAll(s, botToken, "***")
	}
	w := &writer{
		h: func(msg []byte) (int, error) {
			payload := &bytes.Buffer{}
			parts := multipart.NewWriter(payload)

			if filePart, err := parts.CreateFormFile("document", fileName); err != nil {
				return 0, fmt.Errorf("could not create request form file, details: %w", err)
			} else if _, err = filePart.Write(msg); err != nil {
				return 0, fmt.Errorf("could not write file part to request, details: %w", err)
			}

			if err := parts.WriteField("chat_id", chatID); err != nil {
				return 0, fmt.Errorf("could not write chat_id field to request: %w", err)
			}

			caption := strings.Join(labels, "\n")
			caption = fmt.Sprintf("%s %s", time.Now().UTC().Format("2006-01-02 15:04:05"), caption)
			caption = strings.TrimSpace(caption)
			if err := parts.WriteField("caption", caption); err != nil {
				return 0, fmt.Errorf("could not write caption field to request: %w", err)
			}

			if err := parts.WriteField("parse_mode", "HTML"); err != nil {
				return 0, fmt.Errorf("could not write parse_mode field to request: %w", err)
			}

			contentType := parts.FormDataContentType()

			if err := parts.Close(); err != nil {
				return 0, fmt.Errorf("could not close request: %w", err)
			}

			request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, payload)
			if err != nil {
				return 0, fmt.Errorf("could not create HTTP request: %v", redactToken(err.Error()))
			}
			request.Header.Set("Content-Type", contentType)

			r, err := (&http.Client{Timeout: time.Second * 20}).Do(request)
			if err != nil {
				return 0, fmt.Errorf("could not send HTTP request: %v", redactToken(err.Error()))
			}
			// close error is discarded; logging here would re-enter the logger.
			defer func() { _ = r.Body.Close() }()

			if r.StatusCode != http.StatusOK {
				return 0, fmt.Errorf("could not to deliver message, status code: %v", r.StatusCode)
			}

			var response struct {
				Ok bool `json:"ok"`
			}
			rawResponse := bytes.NewBufferString("")
			err = json.NewDecoder(io.TeeReader(r.Body, rawResponse)).Decode(&response)
			if err != nil || !response.Ok {
				//nolint:errorlint // err may be nil when response.Ok is false; %w would render %!w(<nil>)
				return 0, fmt.Errorf("could not decode response: %v\n%s", err, rawResponse.String())
			}

			return len(msg), nil
		},
	}
	return w
}

// RotatingFileOption configures RotatingFileHandler.
type RotatingFileOption func(*rotatingFileConfig)

// WithMaxFileSize overrides the maximum size of a single log file in bytes before
// the handler rotates to the next index. The default is 5 MiB (5 << 20).
func WithMaxFileSize(n uint32) RotatingFileOption {
	return func(c *rotatingFileConfig) { c.maxFileSize = n }
}

// WithMaxFiles overrides the maximum number of rotated log files retained in the
// folder. The bound is enforced on rotation; the backup just rotated out survives until
// a newer live file replaces it. Values below 1 are treated as 1. The default is 7.
func WithMaxFiles(n int) RotatingFileOption {
	return func(c *rotatingFileConfig) { c.maxFiles = n }
}

// WithFreshStart makes the handler remove every existing prefix.<8 hex>.log file in
// folder once at construction, before the first write, so each process run begins with a
// genuinely empty ring at index 1. Existing rotated content is permanently destroyed —
// this is the intended behaviour; omit the option to resume and preserve prior content
// across restarts. Files in folder that do not match the prefix.<8 hex>.log shape are
// left untouched. When combined with WithCompress the compressed prefix.<8 hex>.log.gz
// backups and any interrupted prefix.*.log.gz.tmp temps are removed too; with
// WithStableCurrentName the fixed prefix.log live file is removed as well — so the ring
// is genuinely empty under every option combination.
//
// A missing file or folder is a no-op: no file is created at construction, and the first
// write creates index-1. Any removal error is stashed and surfaced on the first Write's
// return value, then cleared. WithFreshStart forces the starting index to 1 and the
// seeded size to 0, overriding the resume-at-highest-index behaviour.
func WithFreshStart() RotatingFileOption {
	return func(c *rotatingFileConfig) { c.freshStart = true }
}

// WithStableCurrentName keeps the live file at a fixed path, prefix.log, instead of
// naming it after the current rotation index. On rotation the live file is renamed to
// the next indexed backup (prefix.<8 hex>.log) and a fresh prefix.log is opened, so
// external tooling can follow the log at a stable path with tail -F (not tail -f: the
// inode changes on every rotation). Rotated backups stay indexed exactly as in the
// default scheme.
//
// On resume the handler seeds its size counter from prefix.log and appends to it; the
// next backup index is one past the highest existing prefix.<8 hex>.log[.gz]. Legacy
// indexed files already in the folder are adopted as backups and never chosen as the
// live target. Switching a folder between the indexed and stable schemes between runs is
// unsupported without WithFreshStart or a clean folder; a stale prefix.log left by a
// prior stable run must be drained manually when switching back to the indexed scheme.
//
// Single-writer invariant: exactly one process may own a (folder, prefix) pair. Two
// processes renaming prefix.log concurrently race and lose backups; this is not defended
// in code (lumberjack has the same limitation).
func WithStableCurrentName() RotatingFileOption {
	return func(c *rotatingFileConfig) { c.stableName = true }
}

// WithMaxAge prunes rotated backups whose file modification time is older than d on each
// rotation, on top of the WithMaxFiles count bound (a backup is removed when either bound
// is exceeded). The live file is never pruned. A d of zero or negative disables age
// pruning; that is the default.
//
// The unit is a time.Duration, so pass the full span — WithMaxAge(14 * 24 * time.Hour)
// for two weeks. WithMaxAge(14) is fourteen nanoseconds, so it wipes every older backup
// at each rotation; the backup produced by that rotation becomes eligible on the next one.
//
// Age is measured by mtime, which is unreliable across NFS clock skew, a cp without -p,
// or a restore that resets timestamps. When WithCompress is also set the compressed
// backup keeps the source file's mtime (via os.Chtimes), so a genuinely old log
// compressed today still ages out.
func WithMaxAge(d time.Duration) RotatingFileOption {
	return func(c *rotatingFileConfig) { c.maxAge = d }
}

// WithMaxAgeDays is WithMaxAge(time.Duration(days) * 24 * time.Hour) with an
// unmistakable unit — it prunes rotated backups older than the given number of days.
// See WithMaxAge for the full pruning semantics, mtime caveats, and the zero-disables
// rule; this is purely a units-safe wrapper over it.
func WithMaxAgeDays(days int) RotatingFileOption {
	return WithMaxAge(time.Duration(days) * 24 * time.Hour)
}

// WithFileMode overrides the permission bits used when the handler CREATES a log
// file — the live prefix.<8 hex>.log (or prefix.log under WithStableCurrentName) and
// the gzip .log.gz temp under WithCompress. The default is 0640.
//
// It applies only to files this handler creates; the process umask still masks the
// bits (the effective mode is only ever more restrictive than requested). It does NOT
// re-chmod a file that already exists on disk — a live file left at 0640 by a prior
// run keeps 0640 because it is opened O_APPEND, not created. Pass WithFreshStart if you
// need the mode to apply to a from-scratch ring.
func WithFileMode(mode os.FileMode) RotatingFileOption {
	return func(c *rotatingFileConfig) { c.fileMode = mode }
}

// WithCompress gzips each rotated backup to prefix.<8 hex>.log.gz and removes the
// plaintext, synchronously, on the Write that triggers the rotation. It is OFF by
// default; with no options the handler never compresses and a pre-existing foreign
// *.log.gz in the folder is ignored.
//
// Compression is crash-safe: the gzip is written to a temp file, fsync'd, and renamed
// into place before the plaintext is removed, so a reader of *.log.gz never sees a
// partial file. If a crash leaves both prefix.<idx>.log and prefix.<idx>.log.gz for one
// index, the .gz is authoritative and the plaintext is discarded on the next
// construction. A .log/.log.gz pair counts as a single file for the WithMaxFiles bound.
//
// Because it runs under the handler mutex, compression adds a bounded latency spike to
// the one Write per rotation; the async background variant is deferred. The handler never
// appends into a .gz file.
func WithCompress() RotatingFileOption {
	return func(c *rotatingFileConfig) { c.compress = true }
}

// RotatingFileHandler saves messages to files by number. The file name is
// generated from prefix and an incrementing index (e.g. prefix.00000001.log).
// When a file exceeds the maximum size the handler moves to the next index and
// prunes the oldest files so that no more than the configured maximum remain in
// folder. The handler appends to files and ring-prunes the oldest ones; it never
// overwrites a file's existing contents unless WithFreshStart is passed.
//
// On construction the handler resumes at the highest existing index for prefix in
// folder (index 1 when none exist or WithFreshStart is set) and seeds the
// in-memory size counter from that file's on-disk size, so the first write after
// a restart accounts for existing content instead of writing into the oldest file
// and letting pruning delete the newest data. Filenames in folder that do not
// match the exact prefix.<8 hex>.log shape are ignored. Any listing or stat error
// encountered while resolving the resume point is stashed and joined into the
// first Write's return value.
//
// Defaults: max file size 5 MiB, max files 7, no age bound, no compression, index-named
// live file. Override with WithMaxFileSize and WithMaxFiles; use WithFreshStart to begin
// each run from a truncated index-1 file. The opt-in options WithStableCurrentName (fixed
// prefix.log live path for tail -F), WithMaxAge (mtime-based retention), and WithCompress
// (gzip rotated backups to prefix.<8 hex>.log.gz) layer on additively; with none of them
// the on-disk output is byte-identical to a plain rotating handler. FileByFormatHandler is
// the sibling for filename-generator-driven rotation and has no fresh-start variant (its
// target name is not known until the first write).
//
// The returned writer is mutex-guarded; the logger never calls its Write concurrently,
// and compression (when enabled) runs synchronously inside that lock. A single process
// must own a given (folder, prefix) pair; concurrent writers from multiple processes are
// unsupported under WithStableCurrentName and WithCompress.
//
// Write returns len(p) when the line was written in full, 0 with a non-nil error when it
// was not; a close, rotation or prune error after a full write is returned with n == len(p).
func RotatingFileHandler(folder, prefix string, opts ...RotatingFileOption) io.Writer {
	cfg := rotatingFileConfig{
		maxFileSize: 5 << 20,
		maxFiles:    7,
		fileMode:    defaultFilePermissions, // zero-value os.FileMode is 0 (invalid); seed it
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.maxFiles < 1 {
		cfg.maxFiles = 1
	}

	// reject a prefix that contains path separators — it would escape the folder.
	if filepath.Base(prefix) != prefix {
		return &writer{
			h: func([]byte) (int, error) {
				return 0, fmt.Errorf("loginjector: file name prefix %q must not contain path separators", prefix)
			},
		}
	}

	index := 1
	var fileSize uint64
	var seedErr error
	if cfg.freshStart {
		// overwrite-on-start forces a clean start regardless of what is on disk, so the
		// resume scan is skipped and every existing prefix.<8 hex>.log[.gz] file (plus the
		// stable live file and stale .log.gz.tmp temps) is removed — not just index-1
		// truncated — so the ring truly begins empty. Leaving stale higher-index files
		// behind would let pruning remove the fresh files ahead of them and let a later
		// rotation append onto old content. Index/size stay 1/0; a missing file or folder
		// is a no-op. Any remove error is surfaced on first Write.
		seedErr = resetRotation(folder, prefix, cfg)
	} else {
		// resume at the highest existing index so the handler keeps appending to the
		// newest file across a process restart, instead of restarting at index 1 —
		// which is the lexicographically-oldest file that pruning removes first,
		// destroying the newest data.
		index, fileSize, seedErr = resumeRotation(folder, prefix, cfg)
	}

	// with compression on, reconcile a crash-interrupted gzip: a stale plaintext left
	// beside a complete .gz is discarded (the .gz wins) and orphaned .log.gz.tmp files are
	// removed. resumeRotation already refuses to append into a .gz-topped index, so this
	// is order-independent cleanup; it is skipped entirely with compression off, keeping
	// seedErr byte-identical to the plain handler.
	if cfg.compress && !cfg.freshStart {
		seedErr = errors.Join(reconcileCompressed(folder, prefix), seedErr)
	}

	fileName := liveFileName(prefix, index, cfg.stableName)
	var buf []byte

	// rotate advances the ring one step and prunes; it mutates index/fileName/fileSize in
	// place under the handler mutex. Split out of the Write closure only for readability.
	rotate := func() error {
		var err error
		protectIdx := 0
		if cfg.stableName {
			// never clobber a pre-existing backup left by a crash or an earlier run.
			for backupExists(folder, prefix, index, cfg.compress) {
				index++
			}
			backupBase := rotatingFileName(prefix, index)
			src := filepath.Join(folder, fileName)
			if _, e := os.Stat(src); e == nil {
				if renameErr := os.Rename(src, filepath.Join(folder, backupBase)); renameErr != nil {
					err = errors.Join(err, renameErr)
				} else {
					protectIdx = index
					if cfg.compress {
						err = errors.Join(err, compressFile(
							folder,
							backupBase,
							cfg.fileMode,
						))
					}
				}
			} else if !os.IsNotExist(e) {
				err = errors.Join(err, e)
			}
			index++ // advance to the next free backup slot for the following rotation.
			fileSize = 0
			// fileName stays the fixed prefix.log live path.
		} else {
			oldIndex := index
			protectIdx = oldIndex
			index++
			if cfg.compress {
				err = errors.Join(err, compressFile(folder, rotatingFileName(prefix, oldIndex), cfg.fileMode))
			}
			fileName = rotatingFileName(prefix, index)
			// seed the new target's size from disk rather than assuming 0: when the
			// handler rotates into a pre-existing higher-index file (from a prior run) an
			// assumed-0 counter would let that file grow to ~2× the cap before rotating
			// again.
			fileSize = 0
			if fi, e := os.Stat(filepath.Join(folder, fileName)); e == nil {
				fileSize = uint64(fi.Size())
			} else if !os.IsNotExist(e) {
				err = errors.Join(err, e)
			}
		}

		// zero-option handlers keep the original count-based pruning, scoped to prefix's
		// own files; any opt-in (age, compression, stable name) uses the richer prune
		// that understands .gz pairs, the age cutoff, and the live-file exclusion.
		if !cfg.stableName && cfg.maxAge <= 0 && !cfg.compress {
			err = errors.Join(err, verifyRotationFiles(folder, prefix, cfg.maxFiles))
		} else {
			err = errors.Join(err, pruneRotation(
				folder,
				prefix,
				cfg,
				filepath.Base(fileName),
				protectIdx,
				time.Now(),
			))
		}
		return err
	}

	w := &writer{
		h: func(msg []byte) (int, error) {
			// surface any construction-time stat/truncate error on the first Write,
			// then clear it.
			err := seedErr
			seedErr = nil

			written, complete, ioErr := appendLine(
				filepath.Join(folder, fileName),
				cfg.fileMode,
				&buf,
				msg,
			)
			if !complete && written == 0 {
				// nothing reached the file (open or write failure): no size change, no rotation.
				return 0, errors.Join(err, ioErr)
			}

			fileSize += uint64(written)

			var rotateErr error
			if fileSize > uint64(cfg.maxFileSize) {
				rotateErr = rotate()
			}

			// a short write still counts its bytes above so rotation accounting stays right,
			// but reports 0; after a full write a close or rotation error keeps n == len(p).
			if !complete {
				return 0, errors.Join(err, ioErr, rotateErr)
			}
			return len(msg), errors.Join(err, ioErr, rotateErr)
		},
	}
	return w
}

// FileByFormatHandler save messages to files by format.
// The file name is generated by fileNameGenerator.
// The generated name must be a plain file name: a Write whose generated name is empty,
// ".", "..", or contains a path separator returns an error and creates nothing.
// The folder is the directory where the files are saved.
// The maxFilesInFolder is the maximum number of *.log files in the folder: pruning counts
// and removes every *.log file there, whatever its name, so give this handler a folder
// of its own. Values below 1 are treated as 1. The file just written is never pruned.
// No fresh-start variant exists for this handler; see RotatingFileHandler's
// WithFreshStart for an analogous feature on the index-based handler (there the
// construction-time target is known, whereas here the name is generated per-write
// by fileNameGenerator).
//
// Write returns len(p) when the line was written in full, 0 with a non-nil error when it
// was not; a close, rotation or prune error after a full write is returned with n == len(p).
func FileByFormatHandler(folder string, maxFilesInFolder int, fileNameGenerator func() string) io.Writer {
	if maxFilesInFolder < 1 {
		maxFilesInFolder = 1
	}
	lastFileName := ""
	var buf []byte
	w := &writer{
		h: func(msg []byte) (int, error) {
			generatedName := fileNameGenerator()
			fileName := generatedName + "." + defaultFileExtension
			if generatedName == "" || generatedName == "." || generatedName == ".." ||
				filepath.Base(generatedName) != generatedName || !filepath.IsLocal(fileName) {
				return 0, fmt.Errorf("loginjector: generated file name %q must be a plain file name", generatedName)
			}

			written, complete, ioErr := appendLine(
				filepath.Join(folder, fileName),
				defaultFilePermissions,
				&buf,
				msg,
			)
			if !complete && written == 0 {
				// nothing reached the file (open or write failure): nothing to prune for.
				return 0, ioErr
			}

			var pruneErr error
			if lastFileName != fileName {
				lastFileName = fileName
				pruneErr = verifyFiles(
					folder,
					maxFilesInFolder,
					filepath.Join(folder, fileName),
				)
			}

			if !complete {
				return 0, errors.Join(ioErr, pruneErr)
			}
			return len(msg), errors.Join(ioErr, pruneErr)
		},
	}
	return w
}

// PrintOption configures TimestampedPrintHandler.
type PrintOption func(*printConfig)

// WithTimeLayout overrides the timestamp layout used by TimestampedPrintHandler.
// The default layout is "2006/01/02 15:04:05".
func WithTimeLayout(layout string) PrintOption {
	return func(c *printConfig) { c.layout = layout }
}

// WithOutput overrides the sink used by TimestampedPrintHandler. The default is
// os.Stdout. This is primarily intended to redirect output for testing, but it is a
// legitimate consumer need (e.g. writing timestamped messages to a file instead of
// stdout).
func WithOutput(w io.Writer) PrintOption {
	return func(c *printConfig) { c.out = w }
}

// withClock is an unexported test seam that injects a fixed clock into
// TimestampedPrintHandler to make timestamp-sensitive assertions deterministic.
func withClock(fn func() time.Time) PrintOption {
	return func(c *printConfig) { c.clock = fn }
}

// printConfig holds the resolved configuration for TimestampedPrintHandler.
type printConfig struct {
	layout string
	out    io.Writer
	clock  func() time.Time
}

// TimestampedHandler wraps inner so each message is prefixed with a leading
// timestamp before being forwarded to inner, indenting the continuation lines of a
// multi-line message so the body reads as a block. It is the composable form of
// TimestampedPrintHandler: any io.Writer (a file handler, a buffer, another sink)
// can be given sink-side timestamps. The returned writer is mutex-guarded; the
// logger never calls its Write concurrently.
//
// The timestamp layout defaults to "2006/01/02 15:04:05". The first line of a
// message is prefixed with "<timestamp> "; subsequent lines are indented by the
// same width so multi-line payloads read as a visual block. Trailing whitespace and
// newlines are trimmed from the input before formatting; the output always ends
// with exactly one newline. An empty or whitespace-only input produces a single
// "<timestamp>\n" line.
//
// WithTimeLayout and withClock apply; WithOutput is ignored because the sink is the
// explicit inner argument. Use TimestampedPrintHandler when you want the os.Stdout
// instantiation with WithOutput support.
func TimestampedHandler(inner io.Writer, opts ...PrintOption) io.Writer {
	cfg := printConfig{
		layout: "2006/01/02 15:04:05",
		out:    inner,
		clock:  time.Now,
	}
	for _, o := range opts {
		o(&cfg)
	}
	// the sink is the explicit inner argument; WithOutput must not redirect it.
	cfg.out = inner
	return newTimestampedWriter(cfg)
}

// TimestampedPrintHandler prints each message to stdout with a leading timestamp,
// indenting the continuation lines of a multi-line message so the body reads as a
// block. Unlike PrintHandler, which writes to stdout via a plain writer, this
// handler adds a timestamp; the sink is os.Stdout by default and overridable with
// WithOutput. The returned writer is mutex-guarded; the logger never calls its
// Write concurrently. It is the os.Stdout instantiation of TimestampedHandler.
//
// The timestamp layout defaults to "2006/01/02 15:04:05". The first line of a message
// is prefixed with "<timestamp> "; subsequent lines are indented by the same width so
// multi-line payloads read as a visual block. Trailing whitespace and newlines are
// trimmed from the input before formatting; the output always ends with exactly one
// newline. An empty or whitespace-only input produces a single "<timestamp>\n" line.
func TimestampedPrintHandler(opts ...PrintOption) io.Writer {
	cfg := printConfig{
		layout: "2006/01/02 15:04:05",
		out:    os.Stdout,
		clock:  time.Now,
	}
	for _, o := range opts {
		o(&cfg)
	}
	return newTimestampedWriter(cfg)
}

// newTimestampedWriter builds the mutex-guarded writer shared by TimestampedHandler
// and TimestampedPrintHandler: it renders each message with a leading timestamp and
// continuation-line indent, then forwards the rendered bytes to cfg.out.
func newTimestampedWriter(cfg printConfig) io.Writer {
	// precompute the continuation indent once — the rendered width of a fixed-width
	// layout is constant, so formatting a reference time gives the exact width.
	const sep = " "
	indent := strings.Repeat(" ", len(time.Time{}.Format(cfg.layout))+len(sep))

	return &writer{
		h: func(msg []byte) (int, error) {
			ts := cfg.clock().Format(cfg.layout)

			trimmed := bytes.TrimSpace(msg)

			var b strings.Builder

			// fast-path: single-line message (no '\n' in trimmed body).
			if bytes.IndexByte(trimmed, '\n') < 0 {
				b.WriteString(ts)
				if len(trimmed) > 0 {
					b.WriteString(sep)
					b.Write(trimmed)
				}
				b.WriteByte('\n')
			} else {
				body := string(trimmed)
				lines := strings.Split(body, "\n")
				b.WriteString(ts)
				if lines[0] != "" {
					b.WriteString(sep)
					b.WriteString(lines[0])
				}
				b.WriteByte('\n')
				for _, ln := range lines[1:] {
					b.WriteString(indent)
					b.WriteString(ln)
					b.WriteByte('\n')
				}
			}

			if _, err := io.WriteString(cfg.out, b.String()); err != nil {
				return 0, err
			}
			return len(msg), nil
		},
	}
}

// PrintHandler writes each message to os.Stdout as a plain line, trimming
// surrounding whitespace and appending a single newline. It adds no timestamp; for
// the timestamped console sink (which is the default when NewLogger is given no
// handlers) use TimestampedPrintHandler. The returned writer is mutex-guarded; the
// logger never calls its Write concurrently.
func PrintHandler() io.Writer {
	w := &writer{
		h: func(msg []byte) (int, error) {
			if _, err := fmt.Fprintln(os.Stdout, string(bytes.TrimSpace(msg))); err != nil {
				return 0, err
			}
			return len(msg), nil
		},
	}
	return w
}

// rotatingFileConfig holds the resolved configuration for RotatingFileHandler.
type rotatingFileConfig struct {
	maxFileSize uint32
	maxFiles    int
	freshStart  bool
	stableName  bool          // WithStableCurrentName: live file is the fixed prefix.log.
	maxAge      time.Duration // WithMaxAge: prune backups older than this; <= 0 disables.
	compress    bool          // WithCompress: gzip rotated backups to prefix.<idx>.log.gz.
	fileMode    os.FileMode   // WithFileMode: perms for files this handler CREATES; seeded to defaultFilePermissions.
}

// rotatingFileName renders the on-disk name for a given rotation index, e.g.
// "prefix.00000001.log". The index is formatted as fixed-width uppercase hex so
// names sort lexicographically in index order.
func rotatingFileName(prefix string, index int) string {
	return fmt.Sprintf("%s.%08X.%s", prefix, index, defaultFileExtension)
}

// resumeRotation resolves the starting rotation index and seed size for prefix in folder
// by scanning existing prefix.<8 hex>.log[.gz] backups and picking the highest index.
// .gz files are considered only when cfg.compress is set. It returns (1, 0, nil) when no
// backups exist.
//
// Indexed mode: when the highest index is uncompressed the handler resumes appending to
// it and seeds size from its on-disk size; when the highest index is a .gz (the handler
// never appends into a compressed file) it resumes at highest+1 with size 0, ignoring any
// stale plaintext left beside the .gz.
//
// Stable-name mode: the live file is the fixed prefix.log, so size is seeded from it (not
// from any backup) and the next backup index is highest+1. Filenames not matching the
// exact shape are ignored. A listing or unexpected stat error is returned as seedErr for the
// caller to surface on the first Write.
func resumeRotation(folder, prefix string, cfg rotatingFileConfig) (index int, size uint64, seedErr error) {
	// list folder entries directly so metacharacters in folder or prefix cannot corrupt
	// the match set, then filter to prefix.<8 hex>.log[.gz] via parseRotationIndex. The prefix
	// guard is load-bearing: without it a foreign "XXXXXXXX.log" would be misparsed as a
	// valid index.
	matches, err := listRotation(
		folder,
		rotationSuffixes(cfg.compress)...,
	)
	if err != nil {
		return 1, 0, err
	}

	highest := 0
	found := false
	highestGz := false // whether the highest index has a compressed (.gz) form.
	for _, p := range matches {
		idx, gz, ok := parseRotationIndex(filepath.Base(p), prefix)
		if !ok {
			continue
		}
		if !found || idx > highest {
			highest = idx
			found = true
			highestGz = gz
		} else if idx == highest && gz {
			// a .gz for the current top index wins over a co-existing stale plaintext.
			highestGz = true
		}
	}

	if cfg.stableName {
		// the live file is prefix.log; the next backup slot is one past the highest.
		index = highest + 1
		if !found {
			index = 1
		}
		if fi, e := os.Stat(filepath.Join(folder, stableLiveName(prefix))); e == nil {
			size = uint64(fi.Size())
		} else if !os.IsNotExist(e) {
			seedErr = e
		}
		return index, size, seedErr
	}

	if !found {
		return 1, 0, nil
	}

	if highestGz {
		// the top index is compressed; never append into a .gz — resume one past it.
		return highest + 1, 0, nil
	}

	// the top index is plain; resume appending to it, seeding size from disk.
	index = highest
	if fi, e := os.Stat(filepath.Join(folder, rotatingFileName(prefix, index))); e == nil {
		size = uint64(fi.Size())
	} else if !os.IsNotExist(e) {
		seedErr = e
	}
	return index, size, seedErr
}

// resetRotation removes every prefix.<8 hex>.log[.gz] file in folder so a WithFreshStart
// handler begins with a genuinely empty ring instead of only truncating index-1 — which
// would leave stale higher-index files for pruning to remove ahead of the fresh ones and
// for a later rotation to append onto. It lists folder entries directly (so metacharacters
// in folder or prefix cannot corrupt the match set) and filters to the strict indexed shape,
// mirroring resumeRotation. Under WithCompress the .gz backups and any interrupted
// .log.gz.tmp temps are removed too; under WithStableCurrentName the fixed prefix.log live
// file is removed as well. Foreign filenames are left untouched. Remove errors are joined
// and returned for the caller to surface on the first Write; a missing file is not an error.
func resetRotation(folder, prefix string, cfg rotatingFileConfig) error {
	matches, err := listRotation(
		folder,
		rotationSuffixes(cfg.compress)...,
	)
	if err != nil {
		return err
	}
	for _, p := range matches {
		if _, _, ok := parseRotationIndex(filepath.Base(p), prefix); !ok {
			continue
		}
		err = errors.Join(err, removeIfExists(p))
	}
	if cfg.stableName {
		// the stable live file has no index and would survive the loop above.
		err = errors.Join(err, removeIfExists(filepath.Join(folder, stableLiveName(prefix))))
	}
	if cfg.compress {
		err = errors.Join(err, removeCompressTemps(folder, prefix))
	}
	return err
}

// verifyFiles removes older files if the number of files exceeds limit. It is the blunt
// count prune behind FileByFormatHandler, whose generated names carry no parseable
// prefix: it sorts every *.log lexically and removes the oldest past the bound, without
// parsing indices (so date-named and foreign files are pruned too). current is the file
// just written: it is excluded by name or by file identity (os.SameFile, which also covers
// case-insensitive filesystems) and then counts toward limit. The richer pruneRotation
// handles the opt-in age/compress/stable paths. .gz files are never in scope here — it
// always lists plaintext only.
func verifyFiles(folder string, limit int, current string) error {
	files, err := listRotation(
		folder,
		rotationSuffixes(false)...,
	)
	if err != nil || len(files) == 0 {
		return err
	}
	sort.Strings(files)
	currentInfo, statErr := os.Stat(current)
	otherFiles := files[:0]
	excluded := false
	for _, file := range files {
		if file == current {
			excluded = true
			continue
		}
		if statErr == nil {
			fileInfo, fileErr := os.Stat(file)
			if fileErr == nil && os.SameFile(fileInfo, currentInfo) {
				excluded = true
				continue
			}
		}
		otherFiles = append(otherFiles, file)
	}
	files = otherFiles
	if excluded {
		limit--
	}
	// remove older files
	for f, lFiles := 0, len(files); f < lFiles && (lFiles-f) > limit; f++ {
		err = errors.Join(err, os.Remove(files[f]))
	}
	return err
}

// verifyRotationFiles is verifyFiles restricted to prefix's own prefix.<8 hex>.log files:
// it removes the oldest of them beyond limit and never touches other names in folder,
// so handlers with different prefixes, and foreign .log files, can share one folder.
func verifyRotationFiles(folder, prefix string, limit int) error {
	files, err := listRotation(
		folder,
		rotationSuffixes(false)...,
	)
	if err != nil || len(files) == 0 {
		return err
	}
	own := files[:0]
	for _, f := range files {
		if _, _, ok := parseRotationIndex(filepath.Base(f), prefix); ok {
			own = append(own, f)
		}
	}
	sort.Strings(own)
	for f, lFiles := 0, len(own); f < lFiles && (lFiles-f) > limit; f++ {
		err = errors.Join(err, os.Remove(own[f]))
	}
	return err
}

const defaultFilePermissions = 0o640
const defaultFileExtension = "log"
const gzipExtension = "gz"
const tmpExtension = "tmp"

// stableLiveName is the fixed live-file name under WithStableCurrentName: prefix.log.
func stableLiveName(prefix string) string {
	return prefix + "." + defaultFileExtension
}

// liveFileName is the current write target: the fixed prefix.log under stable-name mode,
// otherwise the index-named prefix.<8 hex>.log.
func liveFileName(prefix string, index int, stableName bool) string {
	if stableName {
		return stableLiveName(prefix)
	}
	return rotatingFileName(prefix, index)
}

// parseRotationIndex parses a rotated backup's index from its base name. It strips an
// optional trailing .gz first (reporting gz), then applies the strict
// prefix.<8 hex>.log shape check. Stripping .gz before the check is load-bearing: without
// it prefix.<8 hex>.log.gz has a 15-character middle and is silently dropped. The live
// prefix.log (middle "log", length 3) is intentionally not an index and returns ok=false;
// it is resolved by liveFileName, not this scan.
func parseRotationIndex(base, prefix string) (index int, gz, ok bool) {
	if strings.HasSuffix(base, "."+gzipExtension) {
		gz = true
		base = strings.TrimSuffix(base, "."+gzipExtension)
	}
	if !strings.HasPrefix(base, prefix+".") {
		return 0, gz, false
	}
	mid := strings.TrimSuffix(strings.TrimPrefix(base, prefix+"."), "."+defaultFileExtension)
	if len(mid) != 8 {
		return 0, gz, false
	}
	n, err := strconv.ParseUint(mid, 16, 32)
	if err != nil {
		return 0, gz, false
	}
	return int(n), gz, true
}

// backupExists reports whether a rotated backup already occupies index for prefix in
// folder, as either the plaintext or (when includeGz) the compressed form. The rotation
// collision guard uses it so a crash/resume leftover is never clobbered by a rename.
func backupExists(folder, prefix string, index int, includeGz bool) bool {
	if _, err := os.Stat(filepath.Join(folder, rotatingFileName(prefix, index))); err == nil {
		return true
	}
	if includeGz {
		if _, err := os.Stat(filepath.Join(folder, rotatingFileName(prefix, index)+"."+gzipExtension)); err == nil {
			return true
		}
	}
	return false
}

// backupEntry groups the on-disk files that make up one rotated backup index: the
// plaintext prefix.<idx>.log and/or its compressed prefix.<idx>.log.gz sibling. mtime is
// the newest modification time among them, used by the age prune so a pair is aged as a
// unit. The index is the map key in listBackups, so it is not repeated here.
type backupEntry struct {
	paths []string
	mtime time.Time
}

// pruneRotation enforces the retention bounds on prefix's rotated backups in folder: first
// age (when cfg.maxAge > 0, any backup whose mtime precedes now-maxAge is removed), then
// count (keep the newest cfg.maxFiles-1 backups, since the live file is the +1). The
// backup at protectIdx (the one just rotated out; 0 protects nothing) survives both phases
// when listed: it is never aged out by the rotation that produced it, and it occupies one
// retained slot, raised to 1 when cfg.maxFiles-1 is 0, so it stays until a newer live file
// replaces it. The live file, identified by liveBase, is excluded by name so a quiet log's
// live file is never pruned even when it is the oldest on disk. A .log/.log.gz pair for
// one index counts as a single backup and is removed together. .gz files are considered
// only when cfg.compress is set.
func pruneRotation(folder, prefix string, cfg rotatingFileConfig, liveBase string, protectIdx int, now time.Time) error {
	groups, err := listBackups(folder, prefix, cfg.compress, liveBase)
	if err != nil {
		return err
	}

	indices := make([]int, 0, len(groups))
	for i := range groups {
		if protectIdx <= 0 || i != protectIdx {
			indices = append(indices, i)
		}
	}
	sort.Ints(indices) // ascending index == oldest data first.

	remove := func(idx int) {
		for _, p := range groups[idx].paths {
			err = errors.Join(err, removeIfExists(p))
		}
	}

	// age phase: drop everything older than the cutoff, keeping the survivors in order.
	survivors := make([]int, 0, len(indices))
	if cfg.maxAge > 0 {
		cutoff := now.Add(-cfg.maxAge)
		for _, i := range indices {
			if groups[i].mtime.Before(cutoff) {
				remove(i)
				continue
			}
			survivors = append(survivors, i)
		}
	} else {
		survivors = append(survivors, indices...)
	}

	// count phase: keep the newest maxFiles-1 backups (the live file is the +1).
	keep := cfg.maxFiles - 1
	if keep < 0 {
		keep = 0
	}
	if protectIdx > 0 && groups[protectIdx] != nil {
		if keep < 1 {
			keep = 1
		}
		keep-- // the protected backup occupies one retained slot.
	}
	for i := range len(survivors) - keep {
		remove(survivors[i])
	}
	return err
}

// listBackups groups prefix's rotated backup files in folder by rotation index, skipping
// the live file (excludeBase) and any non-matching name. .gz siblings are included only
// when includeGz is set, preserving the byte-identical zero-option contract. Files that
// vanish between the listing and the stat are ignored (nothing left to prune).
func listBackups(folder, prefix string, includeGz bool, excludeBase string) (map[int]*backupEntry, error) {
	matches, err := listRotation(
		folder,
		rotationSuffixes(includeGz)...,
	)
	if err != nil {
		return nil, err
	}
	groups := make(map[int]*backupEntry)
	for _, p := range matches {
		base := filepath.Base(p)
		if base == excludeBase {
			continue
		}
		idx, _, ok := parseRotationIndex(base, prefix)
		if !ok {
			continue
		}
		fi, e := os.Stat(p)
		if e != nil {
			continue
		}
		g := groups[idx]
		if g == nil {
			g = &backupEntry{}
			groups[idx] = g
		}
		g.paths = append(g.paths, p)
		if fi.ModTime().After(g.mtime) {
			g.mtime = fi.ModTime()
		}
	}
	return groups, nil
}

// compressFile gzips folder/base (a rotated plaintext prefix.<idx>.log) to
// folder/base.gz and removes the plaintext, crash-safely: it writes to an O_EXCL temp
// created with mode (never os.Create, which is world-readable, and O_EXCL also refuses a
// pre-planted symlink), flushes and fsyncs, then renames the temp into place before
// removing the plaintext — so a reader of *.log.gz never sees a partial file. mode is the
// handler's configured file mode (default 0640, overridable via WithFileMode); the process
// umask still masks it. The .gz keeps the source file's mtime (os.Chtimes) so age pruning
// stays honest. A missing source is a no-op.
func compressFile(folder, base string, mode os.FileMode) error {
	src := filepath.Join(folder, base)
	fi, err := os.Stat(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	dst := src + "." + gzipExtension
	tmp := dst + "." + tmpExtension
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}

	zw := gzip.NewWriter(out)
	if _, e := io.Copy(zw, in); e != nil {
		_ = zw.Close()
		_ = out.Close()
		return errors.Join(e, removeIfExists(tmp))
	}
	if e := zw.Close(); e != nil {
		_ = out.Close()
		return errors.Join(e, removeIfExists(tmp))
	}
	if e := out.Sync(); e != nil {
		_ = out.Close()
		return errors.Join(e, removeIfExists(tmp))
	}
	if e := out.Close(); e != nil {
		return errors.Join(e, removeIfExists(tmp))
	}
	if e := os.Rename(tmp, dst); e != nil {
		return errors.Join(e, removeIfExists(tmp))
	}

	// preserve the source mtime on the .gz so WithMaxAge still ages it out correctly,
	// then drop the plaintext now that the compressed copy is durable.
	err = errors.Join(err, os.Chtimes(dst, time.Now(), fi.ModTime()))
	err = errors.Join(err, removeIfExists(src))
	return err
}

// reconcileCompressed heals a crash-interrupted compression at construction: it removes
// orphaned prefix.*.log.gz.tmp temps and, wherever both prefix.<idx>.log and its .gz exist,
// discards the plaintext (the durable .gz is authoritative). It only runs under
// WithCompress, so a plain handler never touches .gz files.
func reconcileCompressed(folder, prefix string) error {
	err := removeCompressTemps(folder, prefix)

	// a leftover plaintext beside a complete .gz is a crash remnant; the .gz wins.
	logs, e := listRotation(
		folder,
		rotationSuffixes(false)...,
	)
	if e != nil {
		return errors.Join(err, e)
	}
	for _, p := range logs {
		idx, gz, ok := parseRotationIndex(filepath.Base(p), prefix)
		if !ok || gz {
			continue
		}
		gzPath := filepath.Join(folder, rotatingFileName(prefix, idx)+"."+gzipExtension)
		if _, statErr := os.Stat(gzPath); statErr == nil {
			err = errors.Join(err, removeIfExists(p))
		}
	}
	return err
}

// removeCompressTemps deletes stale prefix.*.log.gz.tmp files left by an interrupted
// compression. It lists matching temp suffixes directly and filters by prefix, so
// metacharacters in folder or prefix cannot corrupt the match set.
func removeCompressTemps(folder, prefix string) error {
	tmps, err := listRotation(
		folder,
		"."+defaultFileExtension+"."+gzipExtension+"."+tmpExtension,
	)
	if err != nil {
		return err
	}
	for _, p := range tmps {
		if !strings.HasPrefix(filepath.Base(p), prefix+".") {
			continue
		}
		err = errors.Join(err, removeIfExists(p))
	}
	return err
}

// removeIfExists removes path, treating an already-absent file as success.
func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// WithMinLevel wraps handler so that Logger treats it as a LeveledHandler with
// the given minimum level. The returned writer forwards Write calls to handler
// unchanged; the only added behaviour is the MinLevel() method that Logger.WriteLog
// reads via interface assertion. Use this to give one handler a threshold
// distinct from the parent Logger's minimumLogLevel — for example, to send only
// high-severity messages to the console while sending everything to a file:
//
//	file    := loginjector.RotatingFileHandler("./logs", "app")
//	console := loginjector.WithMinLevel(levels.Warning, loginjector.TimestampedPrintHandler())
//	logger  := loginjector.NewLogger(levels.Info, file, console)
//
// The returned writer is itself thread-safe via Logger's existing ensureThreadSafe
// wrapping at registration. Calling WithMinLevel on an already-leveled writer
// replaces the inner MinLevel().
func WithMinLevel(level LogLevel, handler io.Writer) io.Writer {
	return &leveledWriter{inner: handler, level: level}
}

// leveledWriter is the concrete LeveledHandler implementation behind WithMinLevel.
type leveledWriter struct {
	inner io.Writer
	level LogLevel
}

func (lw *leveledWriter) Write(p []byte) (int, error) { return lw.inner.Write(p) }

// WriteLevel forwards p with its level to the inner writer when it is a
// LevelWriter, and to its Write otherwise.
func (lw *leveledWriter) WriteLevel(level LogLevel, p []byte) (int, error) {
	if inner, ok := lw.inner.(LevelWriter); ok {
		return inner.WriteLevel(level, p)
	}
	return lw.inner.Write(p)
}

func (lw *leveledWriter) MinLevel() LogLevel { return lw.level }

var _ LeveledHandler = (*leveledWriter)(nil)

// writer is a thread-safe writer.
type writer struct {
	m        sync.Mutex
	shared   *sync.Mutex // when set, the mutex to use instead of &w.m (shared per-identity registry)
	h        func(msg []byte) (n int, err error)
	original io.Writer // the unwrapped sink; set by ensureThreadSafe for unwrapLeveled.
}

// Write writes the message to the handler.
func (w *writer) Write(p []byte) (n int, err error) {
	mu := w.mutex()
	mu.Lock()
	defer mu.Unlock()
	return w.h(p)
}

// WriteLevel writes p to the handler with the given level while holding its sink mutex.
func (w *writer) WriteLevel(level LogLevel, p []byte) (n int, err error) {
	mu := w.mutex()
	mu.Lock()
	defer mu.Unlock()
	if original, ok := w.original.(LevelWriter); ok {
		return original.WriteLevel(level, p)
	}
	return w.h(p)
}

// mutex returns the shared per-sink mutex when one is set, else the writer's own.
func (w *writer) mutex() *sync.Mutex {
	if w.shared != nil {
		return w.shared
	}
	return &w.m
}

var _ LevelWriter = (*writer)(nil)

// listRotation lists non-directory entries whose names end with one of suffixes. It
// reads the directory directly so metacharacters in folder names are treated literally.
// Missing folders are a no-op; other listing errors are returned. Callers filter the
// result to a specific prefix with parseRotationIndex.
func listRotation(folder string, suffixes ...string) ([]string, error) {
	// an empty folder means the working directory, as filepath.Join("", name) does.
	dir := folder
	if dir == "" {
		dir = "."
	}

	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		for _, suffix := range suffixes {
			if strings.HasSuffix(name, suffix) {
				files = append(files, filepath.Join(folder, name))
				break
			}
		}
	}
	return files, nil
}

// rotationSuffixes returns the listing suffixes for rotation scans: ".log", plus ".log.gz"
// when includeGz is set.
func rotationSuffixes(includeGz bool) []string {
	if includeGz {
		return []string{"." + defaultFileExtension, "." + defaultFileExtension + "." + gzipExtension}
	}
	return []string{"." + defaultFileExtension}
}

// appendLine appends bytes.TrimSpace(msg) plus one '\n' to path with a single write. The
// line is built in *buf (reused across calls; callers serialize under their writer mutex)
// rather than by appending to the trimmed slice, which aliases the caller's p. written is
// the byte count actually written; complete reports that the whole line was written; err
// joins the open, write (io.ErrShortWrite on a short write) and close errors. A buffer
// grown past 64 KiB is dropped so one large message does not pin memory.
func appendLine(path string, perm os.FileMode, buf *[]byte, msg []byte) (written int, complete bool, err error) {
	defer func() {
		if cap(*buf) > 64<<10 {
			*buf = nil
		}
	}()
	*buf = append(
		(*buf)[:0],
		bytes.TrimSpace(msg)...,
	)
	*buf = append(*buf, '\n')
	line := *buf

	f, err := os.OpenFile(
		path,
		os.O_WRONLY|os.O_CREATE|os.O_APPEND,
		perm,
	)
	if err != nil {
		return 0, false, err
	}

	written, writeErr := f.Write(line)
	complete = written == len(line) && writeErr == nil
	if !complete && writeErr == nil {
		writeErr = io.ErrShortWrite
	}
	closeErr := f.Close()
	return written, complete, errors.Join(writeErr, closeErr)
}
