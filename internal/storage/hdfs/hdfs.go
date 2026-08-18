// Package hdfs is a storage.Backend over the Hadoop Distributed File System.
package hdfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	osuser "os/user"
	"path"
	"strings"
	"sync"
	"time"

	hdfsclient "github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
	"github.com/google/uuid"

	"github.com/phrocker/shoal/internal/storage"
)

// Client is the subset of the colinmarc/hdfs client used by Backend.
// It is exported so tests and callers with custom connection management can
// provide an adapter without requiring a live HDFS cluster.
type Client interface {
	Open(name string) (Reader, error)
	Create(name string) (storage.Writer, error)
	MkdirAll(dirname string, perm os.FileMode) error
	ReadDir(dirname string) ([]os.FileInfo, error)
	Remove(name string) error
	Rename(oldpath, newpath string) error
	Close() error
}

// Reader is the read-only HDFS file contract used by Backend.
type Reader interface {
	io.ReaderAt
	io.Closer
	Stat() os.FileInfo
}

// Option customizes Backend construction.
type Option func(*config)

type config struct {
	client             Client
	clientLeaseFactory func(context.Context) (*leasedClient, error)
	clientOptions      *hdfsclient.ClientOptions
}

// WithClient supplies a pre-built HDFS client adapter. New otherwise creates a
// github.com/colinmarc/hdfs/v2 client for address.
func WithClient(client Client) Option {
	return func(c *config) { c.client = client }
}

// WithClientOptions supplies explicit colinmarc/hdfs connection and
// authentication options. If Addresses is empty, New's address is used.
func WithClientOptions(options hdfsclient.ClientOptions) Option {
	return func(c *config) { c.clientOptions = &options }
}

// Backend opens and manages files in one HDFS cluster.
type Backend struct {
	client       Client
	newOperation func(context.Context) (*leasedClient, error)
	authority    string
	closeClient  func() error
}

// New constructs a Backend for a namenode address such as "namenode:8020" or
// "hdfs://namenode:8020". The colinmarc/hdfs default configuration and
// authentication behavior apply when New creates the client. Prefer
// NewContext when the backend lifetime is already scoped by a context.
func New(address string, opts ...Option) (*Backend, error) {
	return NewContext(context.Background(), address, opts...)
}

// NewContext constructs a Backend like New. The contexts later passed to
// Open/Create/List/Remove are bound to short-lived operation clients so their
// namenode and datanode dials, handshakes, and blocked RPCs can be interrupted.
// The cleanup client remains usable until Backend.Close, even after ctx ends.
func NewContext(ctx context.Context, address string, opts ...Option) (*Backend, error) {
	authority, clientAddress, err := parseAddress(address)
	if err != nil {
		return nil, err
	}

	backgroundCtx := contextOrBackground(ctx)
	if err := backgroundCtx.Err(); err != nil {
		return nil, err
	}
	cleanupCtx, cleanupCancel := context.WithCancel(context.WithoutCancel(backgroundCtx))
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.clientLeaseFactory != nil && cfg.client == nil {
		lease, leaseErr := cfg.clientLeaseFactory(cleanupCtx)
		if leaseErr != nil {
			cleanupCancel()
			return nil, leaseErr
		}
		cfg.client = lease.client
		closeFn := lease.release
		if closeFn == nil {
			closeFn = func() error { return nil }
		}
		return &Backend{
			client:       cfg.client,
			newOperation: cfg.clientLeaseFactory,
			authority:    authority,
			closeClient:  cancelThenClose(cleanupCancel, closeFn),
		}, nil
	}
	if cfg.client != nil {
		cleanupCancel()
		opFactory := cfg.clientLeaseFactory
		if opFactory == nil {
			opFactory = func(context.Context) (*leasedClient, error) {
				return newSharedLease(cfg.client), nil
			}
		}
		return &Backend{
			client:       cfg.client,
			newOperation: opFactory,
			authority:    authority,
			closeClient:  onceCloser(cfg.client.Close),
		}, nil
	}

	options, err := loadClientOptions(clientAddress, cfg)
	if err != nil {
		cleanupCancel()
		return nil, err
	}
	baseClient, err := newHDFSClient(cleanupCtx, clientAddress, options)
	if err != nil {
		cleanupCancel()
		return nil, err
	}
	return &Backend{
		client: baseClient.client,
		newOperation: func(opCtx context.Context) (*leasedClient, error) {
			return newHDFSClient(opCtx, clientAddress, options)
		},
		authority:   authority,
		closeClient: cancelThenClose(cleanupCancel, baseClient.release),
	}, nil
}

// Close releases the underlying HDFS client and its open leases.
func (b *Backend) Close() error {
	if err := b.closeClient(); err != nil {
		return fmt.Errorf("hdfs: close client: %w", err)
	}
	return nil
}

// Open opens path read-only.
func (b *Backend) Open(ctx context.Context, objectPath string) (storage.File, error) {
	if err := contextOrBackground(ctx).Err(); err != nil {
		return nil, err
	}
	lease, err := b.newOperation(ctx)
	if err != nil {
		return nil, err
	}
	resolved, _, err := b.resolve(objectPath)
	if err != nil {
		_ = lease.release()
		return nil, err
	}
	reader, err := lease.client.Open(resolved)
	if err != nil {
		_ = lease.release()
		if isNotFound(err) {
			return nil, fmt.Errorf("%w: %s", storage.ErrNotFound, objectPath)
		}
		return nil, fmt.Errorf("hdfs: open %s: %w", objectPath, err)
	}
	if err := applyDeadline(ctx, reader); err != nil {
		_ = reader.Close()
		_ = lease.release()
		return nil, fmt.Errorf("hdfs: open %s: %w", objectPath, err)
	}
	return &file{reader: reader, size: reader.Stat().Size(), release: lease.release}, nil
}

// Create creates or replaces path. Parent directories are created as needed.
func (b *Backend) Create(ctx context.Context, objectPath string) (storage.Writer, error) {
	if err := contextOrBackground(ctx).Err(); err != nil {
		return nil, err
	}
	lease, err := b.newOperation(ctx)
	if err != nil {
		return nil, err
	}
	resolved, _, err := b.resolve(objectPath)
	if err != nil {
		_ = lease.release()
		return nil, err
	}
	if parent := path.Dir(resolved); parent != "." && parent != "/" {
		if err := lease.client.MkdirAll(parent, 0o755); err != nil {
			_ = lease.release()
			return nil, fmt.Errorf("hdfs: mkdir %s: %w", parent, err)
		}
	}

	tempPath := resolved + ".shoal-tmp-" + uuid.NewString()
	writer, err := lease.client.Create(tempPath)
	if err != nil {
		_ = lease.release()
		return nil, fmt.Errorf("hdfs: create temporary file %s: %w", tempPath, err)
	}
	if err := applyDeadline(ctx, writer); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		_ = closeAfterReplication(cleanupCtx, writer)
		_ = b.cleanupRemove(cleanupCtx, tempPath)
		cancel()
		_ = lease.release()
		return nil, fmt.Errorf("hdfs: create temporary file %s: %w", tempPath, err)
	}
	return &replaceWriter{
		client:        lease.client,
		cleanupClient: b.client,
		release:       lease.release,
		writer:        writer,
		ctx:           ctx,
		temp:          tempPath,
		target:        resolved,
	}, nil
}

// List returns regular files directly under prefix.
func (b *Backend) List(ctx context.Context, prefix string) ([]string, error) {
	if err := contextOrBackground(ctx).Err(); err != nil {
		return nil, err
	}
	lease, err := b.newOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer lease.release()
	resolved, qualifier, err := b.resolve(prefix)
	if err != nil {
		return nil, err
	}
	entries, err := lease.client.ReadDir(resolved)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("hdfs: list %s: %w", prefix, err)
	}

	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		child := path.Join(resolved, entry.Name())
		if qualifier != "" {
			child = qualifier + ensureLeadingSlash(child)
		}
		out = append(out, child)
	}
	return out, nil
}

// Remove deletes path. A missing path is not an error.
func (b *Backend) Remove(ctx context.Context, objectPath string) error {
	if err := contextOrBackground(ctx).Err(); err != nil {
		return err
	}
	lease, err := b.newOperation(ctx)
	if err != nil {
		return err
	}
	defer lease.release()
	resolved, _, err := b.resolve(objectPath)
	if err != nil {
		return err
	}
	if err := lease.client.Remove(resolved); err != nil && !isNotFound(err) {
		return fmt.Errorf("hdfs: remove %s: %w", objectPath, err)
	}
	return nil
}

func (b *Backend) cleanupRemove(ctx context.Context, resolved string) error {
	if err := removeWithContext(ctx, b.client, resolved); err != nil && !isNotFound(err) {
		return fmt.Errorf("hdfs: remove temporary file %s: %w", resolved, err)
	}
	return nil
}

func (b *Backend) resolve(objectPath string) (resolved, qualifier string, err error) {
	u, err := url.Parse(objectPath)
	if err != nil {
		return "", "", fmt.Errorf("hdfs: parse path %q: %w", objectPath, err)
	}
	if u.Scheme == "" {
		return objectPath, "", nil
	}
	if u.Scheme != "hdfs" {
		return "", "", fmt.Errorf("hdfs: unsupported path scheme %q", u.Scheme)
	}
	if u.Opaque != "" {
		return "", "", fmt.Errorf("hdfs: opaque path %q is not supported", objectPath)
	}
	if u.Host != "" && b.authority == "" {
		return "", "", fmt.Errorf("hdfs: qualified path authority %q requires a configured namenode", u.Host)
	}
	if u.Host != "" && b.authority != "" && !strings.EqualFold(u.Host, b.authority) {
		return "", "", fmt.Errorf("hdfs: path authority %q does not match backend authority %q", u.Host, b.authority)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", "", fmt.Errorf("hdfs: path %q must not contain a query or fragment", objectPath)
	}
	if u.Host != "" {
		qualifier = "hdfs://" + u.Host
	} else {
		qualifier = "hdfs:"
	}
	resolved = u.Path
	if resolved == "" {
		resolved = "/"
	}
	return resolved, qualifier, nil
}

// AddressFromPath returns the namenode authority from a qualified HDFS path.
// Authority-less HDFS paths return an empty address so Hadoop configuration can
// select the cluster.
func AddressFromPath(objectPath string) (string, error) {
	u, err := url.Parse(objectPath)
	if err != nil {
		return "", fmt.Errorf("hdfs: parse path %q: %w", objectPath, err)
	}
	if u.Scheme != "hdfs" {
		return "", fmt.Errorf("hdfs: path %q does not use the hdfs scheme", objectPath)
	}
	if u.Opaque != "" {
		return "", fmt.Errorf("hdfs: opaque path %q is not supported", objectPath)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("hdfs: path %q must not contain a query or fragment", objectPath)
	}
	return u.Host, nil
}

func parseAddress(address string) (authority, clientAddress string, err error) {
	if address == "" {
		return "", "", nil
	}
	if !strings.Contains(address, "://") {
		return address, address, nil
	}
	u, err := url.Parse(address)
	if err != nil {
		return "", "", fmt.Errorf("hdfs: parse address %q: %w", address, err)
	}
	if u.Scheme != "hdfs" || u.Host == "" || (u.Path != "" && u.Path != "/") {
		return "", "", fmt.Errorf("hdfs: invalid namenode address %q", address)
	}
	return u.Host, u.Host, nil
}

func bindClientOptions(ctx context.Context, options hdfsclient.ClientOptions) hdfsclient.ClientOptions {
	options.NamenodeDialFunc = bindDialContext(ctx, options.NamenodeDialFunc)
	options.DatanodeDialFunc = bindDialContext(ctx, options.DatanodeDialFunc)
	return options
}

func bindDialContext(ctx context.Context, dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	ctx = contextOrBackground(ctx)
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	return func(_ context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		conn = bindConnContext(ctx, conn)
		if deadline, ok := ctx.Deadline(); ok {
			if err := conn.SetDeadline(deadline); err != nil {
				_ = conn.Close()
				return nil, err
			}
		}
		return conn, nil
	}
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func ensureLeadingSlash(value string) string {
	if strings.HasPrefix(value, "/") {
		return value
	}
	return "/" + value
}

func isNotFound(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || os.IsNotExist(err)
}

type deadlineSetter interface {
	SetDeadline(time.Time) error
}

type contextCloser interface {
	CloseContext(context.Context) error
}

type contextRemover interface {
	RemoveContext(context.Context, string) error
}

func removeWithContext(ctx context.Context, client Client, name string) error {
	if remover, ok := client.(contextRemover); ok {
		return remover.RemoveContext(contextOrBackground(ctx), name)
	}
	return client.Remove(name)
}

func clearDeadline(target any) error {
	setter, ok := target.(deadlineSetter)
	if !ok {
		return nil
	}
	return setter.SetDeadline(time.Time{})
}

func applyDeadline(ctx context.Context, target any) error {
	deadline, ok := contextOrBackground(ctx).Deadline()
	if !ok {
		return nil
	}
	setter, ok := target.(deadlineSetter)
	if !ok {
		return nil
	}
	return setter.SetDeadline(deadline)
}

type file struct {
	mu      sync.Mutex
	reader  Reader
	release func() error
	size    int64
	closed  bool
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	// colinmarc/hdfs implements ReadAt as Seek+Read and mutates reader state.
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reader.ReadAt(p, off)
}

func (f *file) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var err error
	if !f.closed {
		err = f.reader.Close()
		f.closed = true
	}
	if f.release != nil {
		err = errors.Join(err, f.release())
	}
	return err
}

func (f *file) Size() int64 { return f.size }

type replaceWriter struct {
	client        Client
	cleanupClient Client
	release       func() error
	writer        storage.Writer
	ctx           context.Context
	temp          string
	target        string
	closed        bool
	aborted       bool
	writerClosed  bool
	state         replacementState
}

type replacementState uint8

const (
	replacementStaged replacementState = iota
	replacementPublished
	replacementCommitted
	replacementUnabortable
)

func (w *replaceWriter) Write(p []byte) (int, error) {
	if w.closed || w.aborted {
		return 0, errors.New("hdfs: write after close")
	}
	return w.writer.Write(p)
}

func (w *replaceWriter) Close() error {
	defer w.releaseOperationClient()
	if w.aborted {
		return errors.New("hdfs: writer already aborted")
	}
	if w.closed {
		return nil
	}
	if err := closeAfterReplication(w.ctx, w.writer); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		return errors.Join(
			fmt.Errorf("hdfs: close temporary file %s: %w", w.temp, err),
			w.removeTemp(cleanupCtx),
		)
	}
	w.writerClosed = true

	if err := w.commitReplacement(); err != nil {
		return err
	}
	w.closed = true
	return nil
}

func (w *replaceWriter) commitReplacement() error {
	backup := w.target + ".shoal-backup-" + uuid.NewString()
	hadOld := true
	if err := w.client.Rename(w.target, backup); err != nil {
		if !isNotFound(err) {
			return fmt.Errorf("hdfs: preserve existing file %s: %w", w.target, err)
		}
		hadOld = false
	}

	if err := w.client.Rename(w.temp, w.target); err != nil {
		publishErr := fmt.Errorf("hdfs: publish %s: %w", w.target, err)
		if hadOld {
			if restoreErr := w.restoreBackup(backup); restoreErr != nil {
				w.state = replacementUnabortable
				publishErr = errors.Join(publishErr, restoreErr)
			}
		}
		return publishErr
	}
	w.state = replacementPublished
	if hadOld {
		if err := w.client.Remove(backup); err != nil && !isNotFound(err) {
			cleanupErr := fmt.Errorf("hdfs: remove replacement backup %s: %w", backup, err)
			if rollbackErr := w.withCleanupClient(func(client Client) error {
				return w.rollbackPublishedReplacement(client, backup)
			}); rollbackErr != nil {
				return errors.Join(cleanupErr, rollbackErr)
			}
			return cleanupErr
		}
	}
	w.state = replacementCommitted
	return nil
}

func (w *replaceWriter) rollbackPublishedReplacement(client Client, backup string) error {
	if err := client.Rename(w.target, w.temp); err != nil {
		return fmt.Errorf("hdfs: roll back published file %s: %w", w.target, err)
	}
	if err := client.Rename(backup, w.target); err != nil {
		restoreErr := fmt.Errorf("hdfs: restore existing file %s from %s: %w", w.target, backup, err)
		if republishErr := client.Rename(w.temp, w.target); republishErr != nil {
			w.state = replacementUnabortable
			return errors.Join(
				restoreErr,
				fmt.Errorf("hdfs: restore published file %s after rollback failure: %w", w.target, republishErr),
			)
		}
		w.state = replacementPublished
		return restoreErr
	}
	w.state = replacementStaged
	return nil
}

func (w *replaceWriter) Abort() error {
	defer w.releaseOperationClient()
	if w.aborted {
		return nil
	}
	if w.closed {
		return errors.New("hdfs: writer already closed")
	}
	if w.state != replacementStaged {
		return fmt.Errorf("hdfs: replacement for %s cannot be safely aborted in state %d", w.target, w.state)
	}
	w.aborted = true

	var abortErr error
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	if !w.writerClosed {
		if err := closeAfterReplication(cleanupCtx, w.writer); err != nil {
			abortErr = fmt.Errorf("hdfs: close temporary file %s: %w", w.temp, err)
		} else {
			w.writerClosed = true
		}
	}
	if cleanupErr := w.removeTemp(cleanupCtx); cleanupErr != nil {
		abortErr = errors.Join(abortErr, cleanupErr)
	}
	return abortErr
}

const cleanupTimeout = 10 * time.Second

func closeAfterReplication(ctx context.Context, writer storage.Writer) (retErr error) {
	const (
		initialDelay = 100 * time.Millisecond
		maxDelay     = time.Second
	)

	ctx = contextOrBackground(ctx)
	retryCtx, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	if err := applyDeadline(retryCtx, writer); err != nil {
		return err
	}
	defer func() {
		if retErr == nil {
			return
		}
		var restoreErr error
		if _, ok := ctx.Deadline(); ok {
			restoreErr = applyDeadline(ctx, writer)
		} else {
			restoreErr = clearDeadline(writer)
		}
		if restoreErr != nil {
			retErr = errors.Join(retErr, restoreErr)
		}
	}()
	delay := initialDelay
	for {
		var closeErr error
		if closer, ok := writer.(contextCloser); ok {
			closeErr = closer.CloseContext(retryCtx)
		} else {
			closeErr = writer.Close()
		}
		if ctxErr := retryCtx.Err(); ctxErr != nil {
			return errors.Join(closeErr, ctxErr)
		}
		if !errors.Is(closeErr, hdfsclient.ErrReplicating) {
			return closeErr
		}
		timer := time.NewTimer(delay)
		select {
		case <-retryCtx.Done():
			timer.Stop()
			return retryCtx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, maxDelay)
	}
}

type clientAdapter struct {
	*hdfsclient.Client
}

func (c clientAdapter) Open(name string) (Reader, error) {
	return c.Client.Open(name)
}

func (c clientAdapter) Create(name string) (storage.Writer, error) {
	return c.Client.Create(name)
}

type leasedClient struct {
	client  Client
	release func() error
}

func newSharedLease(client Client) *leasedClient {
	return &leasedClient{
		client: client,
		release: func() error {
			return nil
		},
	}
}

func newOwnedLease(client Client, closeFn func() error) *leasedClient {
	return &leasedClient{
		client:  client,
		release: onceCloser(closeFn),
	}
}

func onceCloser(closeFn func() error) func() error {
	var once sync.Once
	var closeErr error
	return func() error {
		if closeFn == nil {
			return nil
		}
		once.Do(func() {
			closeErr = closeFn()
		})
		return closeErr
	}
}

func cancelThenClose(cancel context.CancelFunc, closeFn func() error) func() error {
	return onceCloser(func() error {
		cancel()
		if closeFn == nil {
			return nil
		}
		return closeFn()
	})
}

func loadClientOptions(clientAddress string, cfg *config) (hdfsclient.ClientOptions, error) {
	if cfg.clientOptions != nil {
		options := *cfg.clientOptions
		if len(options.Addresses) == 0 && clientAddress != "" {
			options.Addresses = []string{clientAddress}
		}
		return options, nil
	}
	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil {
		return hdfsclient.ClientOptions{}, fmt.Errorf("hdfs: load Hadoop configuration: %w", err)
	}
	options := hdfsclient.ClientOptionsFromConf(conf)
	if clientAddress != "" {
		options.Addresses = strings.Split(clientAddress, ",")
	}
	if userName := os.Getenv("HADOOP_USER_NAME"); userName != "" {
		options.User = userName
		return options, nil
	}
	currentUser, err := osuser.Current()
	if err != nil {
		return hdfsclient.ClientOptions{}, fmt.Errorf("hdfs: resolve current user: %w", err)
	}
	options.User = currentUser.Username
	return options, nil
}

func newHDFSClient(ctx context.Context, clientAddress string, options hdfsclient.ClientOptions) (*leasedClient, error) {
	client, err := hdfsclient.NewClient(bindClientOptions(ctx, options))
	if err != nil {
		return nil, fmt.Errorf("hdfs: connect %s: %w", clientAddress, err)
	}
	return newOwnedLease(clientAdapter{client}, client.Close), nil
}

type contextBoundConn struct {
	net.Conn
	stop func() bool
}

func (c *contextBoundConn) Close() error {
	if c.stop != nil {
		c.stop()
	}
	return c.Conn.Close()
}

func bindConnContext(ctx context.Context, conn net.Conn) net.Conn {
	if ctx.Done() == nil {
		return conn
	}
	return &contextBoundConn{
		Conn: conn,
		stop: context.AfterFunc(ctx, func() {
			_ = conn.Close()
		}),
	}
}

func (w *replaceWriter) releaseOperationClient() {
	if w.release == nil {
		return
	}
	_ = w.release()
}

func (w *replaceWriter) removeTemp(ctx context.Context) error {
	if err := removeWithContext(ctx, w.cleanupClient, w.temp); err != nil && !isNotFound(err) {
		return fmt.Errorf("hdfs: remove temporary file %s: %w", w.temp, err)
	}
	return nil
}

func (w *replaceWriter) restoreBackup(backup string) error {
	return w.withCleanupClient(func(client Client) error {
		if err := client.Rename(backup, w.target); err != nil {
			return fmt.Errorf("hdfs: restore existing file %s from %s: %w", w.target, backup, err)
		}
		return nil
	})
}

func (w *replaceWriter) withCleanupClient(fn func(Client) error) error {
	if w.cleanupClient == nil {
		return errors.New("hdfs: cleanup client is not configured")
	}
	return fn(w.cleanupClient)
}

var (
	_ storage.Backend         = (*Backend)(nil)
	_ storage.WritableBackend = (*Backend)(nil)
	_ storage.Lister          = (*Backend)(nil)
	_ storage.Remover         = (*Backend)(nil)
	_ storage.Aborter         = (*replaceWriter)(nil)
	_ contextCloser           = (*hdfsclient.FileWriter)(nil)
	_ contextRemover          = clientAdapter{}
)
