package bucket

import (
	"errors"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// KV stores key-value pairs, and supports transactional updates.
// All keys and values must be smaller than 1 GiB.
// Empty keys and values are not allowed.
type KV interface {
	// View the current committed state.
	// This may be called concurrently.
	// A view operation may block edits.
	// The view must not be accessed after the function returns.
	View(func(View) error) error

	// Edit the key-value pairs.
	// This operation is atomic and durable.
	// An Edit may observe state from other not-yet-committed edits, but will not be committed unless the observed edits are also committed.
	// This may be called concurrently.
	// The provided function may be executed on another goroutine.
	// The transaction must not be accessed after the function returns.
	Edit(func(Transaction) error) error

	io.Closer
}

// View is a consistent view of the state of a KV.
// It is not concurrency-safe.
type View interface {
	// Get the value currently associated with the key.
	// If there is no value, this returns an empty string.
	Get(key string) (string, error)

	// Each runs the specified function for all key-value pairs that are currently present.
	Each(func(key, value string) error) error

	io.WriterTo
}

// Transaction is an interactive edit operation.
// It is not concurrency-safe.
type Transaction interface {
	View

	// Set updates a key-value pair.
	// It is not safe to run during a call to Each.
	Set(key, value string) error

	// Delete removes a pair with the specified key.
	// It is not safe to run during a call to Each.
	Delete(key string) error
}

// Open a key-value file at the given path.
// If it does not exist, it is created with default permissions and initialized as empty.
func Open(path string) (KV, error) {
	return open(path)
}

func open(path string) (*kv, error) {
	kv := &kv{path: path}
	err := kv.init()
	kv.writeSema = make(chan struct{}, 1)
	if err != nil {
		return nil, err
	}

	return kv, nil
}

// TODO: flock

// kv is an implementation of KV backed by a file.
// Updates are atomically appended to the file.
// The file is rewritten as a single transaction when the estimated amount of overwritten data exceeds 20% of the file size.
type kv struct {
	// path is the path to the file.
	// When rewriting, the temporary version will be stored at path+".tmp".
	path string

	// file is the backing file being accessed.
	file *os.File

	// cur is the current committed state.
	cur Map

	// size is the total size of the file.
	size int64

	// live is an estimate of the total live data in the file.
	// It is used to determine when to rewrite the file.
	live int64

	// mu is a mutex used to control access to file/cur/size/live.
	// A read lock is held when Viewing the kv.
	// A write lock is held when committing Edits.
	mu sync.RWMutex

	// batches and transactions are some counters used for benchmarking.
	batches, transactions uint64

	// writeSema is a semaphore used to control Edits.
	writeSema chan struct{}

	// txQueue is a queue of transactions waiting to be processed.
	// This is used to implement batching without extra goroutine wakeups.
	txQueue txQueue
}

// View the kv's committed state by observing the "cur" Map.
func (kv *kv) View(fn func(View) error) error {
	// Acquire a read lock.
acquire:
	kv.mu.RLock()
	if kv.cur == nil {
		// The kv is in an inconsistent state and needs to be re-initialized.
		kv.mu.RUnlock()
		err := kv.maybeInit()
		if err != nil {
			return err
		}
		goto acquire
	}
	defer kv.mu.RUnlock()

	// Run user function under read lock.
	return fn(kv.cur)
}

// Edit the kv's state.
// This batches transactions transparently using txQueue.
func (kv *kv) Edit(fn func(Transaction) error) error {
	// Push the transaction onto the queue.
	txElem := kv.txQueue.push(fn)
	select {
	case kv.writeSema <- struct{}{}:
		// The write lock was acquired.
		defer func() { <-kv.writeSema }()
		select {
		case err := <-txElem.commitNotify:
			// We acquired the write lock after our transaction was processed.
			// This does not happen frequently but is possible.
			// Release it and exit.
			txElem.done()
			return err
		default:
			// We acquired the write lock and our transaction has not been processed.
			// Remove it from the queue so we can process it first.
			kv.txQueue.remove(txElem)
			txElem.done()
		}

		// Set a deadline for batching transactions.
		// A millisecond is high enough that we can generally get hundreds of small updates in, but low enough to keep latency under control.
		batchDeadline := time.Now().Add(time.Millisecond)

		// Run a batch of trnasactions.
		var notifyChans []chan<- error
		var err error
		var ok bool
		defer func() {
			if !ok {
				err = errors.New("tx executor crashed")
			}
			for _, ch := range notifyChans {
				ch <- err
			}
		}()
		err = kv.edit(func(tx *tx) error {
			// Run the transaction directly.
			kv.transactions++
			err := fn(tx)
			if err != nil {
				return err
			}

			// Try to batch the transaction.
			var batch *batchTx
			for time.Now().Before(batchDeadline) {
				// Pop a tx off the queue.
				txElem := kv.txQueue.pop()
				if txElem == nil {
					// There are no more waiting transactions.
					break
				}

				// Add the commit notification for the tx.
				notifyChans = append(notifyChans, txElem.commitNotify)

				if batch == nil {
					// Create a chain transaction.
					batch = &batchTx{
						base:    tx,
						overlay: make(map[string]string),
					}
					defer func() { batch.base = nil }()
				} else {
					// Wipe the overlay from a previously chained transaction so that we can re-use it.
					overlay := batch.overlay
					for k := range overlay {
						delete(overlay, k)
					}
				}

				// Run the transaction in the chain.
				kv.transactions++
				if err := txElem.fn(batch); err != nil {
					// The transaction aborted.
					// Forward the error and continue.
					notifyChans = notifyChans[:len(notifyChans)-1]
					txElem.commitNotify <- err
					continue
				}

				// Merge the chain overlay.
				for k, v := range batch.overlay {
					var err error
					if v != "" {
						err = tx.Set(k, v)
					} else {
						err = tx.Delete(k)
					}
					if err != nil {
						// If this happens, its probbably a bug.
						return err
					}
				}
			}

			return nil
		})
		ok = true
		return err

	case err := <-txElem.commitNotify:
		// This transaction was processed as part of a larger batch.
		txElem.done()
		return err
	}
}

// edit executes an ACID transaction on the KV.
// It does not batch or anything.
// It must not be called concurrently.
func (kv *kv) edit(fn func(*tx) error) (err error) {
	// Init the KV if necessary.
	if func() bool {
		kv.mu.RLock()
		defer kv.mu.RUnlock()
		return kv.cur == nil
	}() {
		err := kv.maybeInit()
		if err != nil {
			return err
		}
	}

	kv.batches++

	// Create a transaction.
	overlay := make(map[string]string)
	tx := tx{
		kv:      kv,
		overlay: overlay,

		// Transaction size starts at 1 to account for the terminator.
		txSize: 1,
	}

	// Run the transaction.
	err = fn(&tx)
	tx.overlay = nil
	if err != nil {
		return err
	}
	if len(overlay) == 0 {
		// Nothing to commit.
		return nil
	}

	// Acquire a write lock (wait for all Views to finish).
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if tx.liveDiff+kv.live < (tx.txSize+kv.size)/5 {
		// Rewrite the file completely.
		return kv.rewrite(overlay)
	}

	// Apply the update.
	return kv.doUpdate(overlay, tx.liveDiff)
}

// doUpdate atomically applies an update by appending it to the file.
func (kv *kv) doUpdate(overlay map[string]string, liveDiff int64) error {
	// Write the overlay data to the file.
	txw := startTx(kv.file)
	var consistent bool
	defer func() {
		if !consistent {
			// The KV is stuck in an inconsistent state.
			// Reset so that the next access will attempt to re-initialize.
			kv.reset()
		}
	}()
	var err error
	for k, v := range overlay {
		err = txw.writePair(k, v)
		if err != nil {
			break
		}
	}
	if err != nil {
		txw.abort()

		// Try to restore the KV to a consitent state by truncating.
		terr := kv.file.Truncate(kv.size)
		if terr != nil {
			return err
		}
		terr = kv.file.Sync()
		if terr != nil {
			return err
		}
		consistent = true

		return err
	}
	n, err := txw.finish()
	if err != nil {
		return err
	}

	// Merge the overlay.
	if len(overlay) < len(kv.cur) {
		// Copy pairs from the overlay into the current map.
		for k, v := range overlay {
			if v != "" {
				kv.cur[k] = v
			} else {
				delete(kv.cur, k)
			}
		}
	} else {
		// Copy unchanged pairs from the current map into the overlay, then swap.
		// This reduces processing time by about 40% when doing an initial bulk-insert.
		for k, v := range kv.cur {
			if v, ok := overlay[k]; ok {
				if v == "" {
					delete(overlay, k)
				}
				continue
			}

			overlay[k] = v
		}
		kv.cur = overlay
	}
	kv.size += int64(n)
	kv.live += liveDiff

	// Sync the file to persistent storage.
	err = kv.file.Sync()
	if err != nil {
		return err
	}

	consistent = true

	return nil
}

func encodingSize(str string) int64 {
	return 1 + int64(bits.Len32(uint32(len(str))<<2)/8) + int64(len(str))
}

// rewrite atomically applies an update by rewriting the file and atomically moving it into place.
func (kv *kv) rewrite(overlay map[string]string) error {
	// Create a new file to write into.
	tmpPath := kv.path + ".tmp"
	newFile, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	var moved bool
	defer func() {
		if kv.file == newFile {
			return
		}

		newFile.Close()
		if !moved {
			// Try to delete the file (best-effort).
			os.Remove(tmpPath)
		}
	}()

	// Write the data into the file.
	txw := startTx(newFile)
	m := make(map[string]string)
	for k, v := range kv.cur {
		if _, ok := overlay[k]; ok {
			continue
		}

		m[k] = v
		err = txw.writePair(k, v)
		if err != nil {
			break
		}
	}
	if err == nil {
		for k, v := range overlay {
			if v == "" {
				continue
			}

			m[k] = v
			err = txw.writePair(k, v)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		txw.abort()
		return err
	}
	n, err := txw.finish()
	if err != nil {
		return err
	}
	err = newFile.Sync()
	if err != nil {
		return err
	}

	// Reset the KV state.
	kv.reset()

	// Move the new file into place.
	err = os.Rename(tmpPath, kv.path)
	if err != nil {
		return err
	}
	moved = true

	// Sync the directory.
	err = kv.syncDir()
	if err != nil {
		return err
	}

	// Reload the state.
	if n > 0 {
		kv.live = int64(n - 1)
	} else {
		kv.live = 0
	}
	kv.size = int64(n)
	kv.file, kv.cur = newFile, m
	return nil
}

func (kv *kv) reset() {
	kv.cur = nil
	kv.file.Close()
	kv.file = nil
}

func (kv *kv) maybeInit() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.cur != nil {
		return nil
	}

	return kv.init()
}

func (kv *kv) init() (err error) {
	// Open the data file.
	f, err := os.OpenFile(kv.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	// Parse the data into a map.
	m := make(Map)
	size, err := m.ReadFrom(f)
	switch e := err.(type) {
	case nil:
	case ErrIncompleteTxn:
		// Truncate the incomplete transaction away.
		err = f.Truncate(int64(e.ValidLen))
		if err != nil {
			return err
		}
		_, err = f.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		size = int64(e.ValidLen)
	default:
		return err
	}
	aliveSize, err := m.WriteTo(io.Discard)
	if err != nil {
		panic(err)
	}
	if aliveSize > size {
		panic("live size is greater than disk size")
	}

	// Sync the file with persistent storage.
	err = f.Sync()
	if err != nil {
		return err
	}

	// Sync the directory with persistent storage.
	// This is necessary if the KV died while compacting.
	err = kv.syncDir()
	if err != nil {
		return err
	}

	// Save the state.
	kv.file = f
	kv.cur = m
	kv.size, kv.live = size, aliveSize
	return nil
}

func (kv *kv) syncDir() (err error) {
	d, err := os.Open(filepath.Dir(kv.path))
	if err != nil {
		return err
	}
	defer d.Close()

	err = d.Sync()
	if err != nil {
		return err
	}

	return d.Close()
}

func (kv *kv) Close() error {
	return kv.file.Close()
}
