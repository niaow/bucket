package bucket

import (
	"io"
)

// tx is a simple edit transaction on a bucket.
// It uses a map to overlay modified keys.
type tx struct {
	// kv is the store modified by this transaction.
	kv *kv

	// overlay holds updated key-value pairs.
	// An empty value represents a delete.
	overlay map[string]string

	// version is a version number used for sanity checking.
	version uint64

	// txSize is an estimate of the encoded size of the transaction.
	// This is computed during Update & Delete for efficiency reasons.
	txSize int64

	// liveDiff is an estimate of the change in size of live data.
	// Updated pair sizes are added, and overwritten pair sizes are subtracted.
	// This is computed during Update & Delete for efficiency reasons.
	liveDiff int64
}

func (tx *tx) Get(key string) (string, error) {
	switch {
	case tx.overlay == nil:
		// This transaction is being used after it has been committed/aborted.
		panic("access to finished transaction")
	case key == "":
		// An empty key is not representable, and can be assumed to not be present.
		return "", nil
	case len(key) >= 1<<30:
		// An 1 GiB+ key is not representable, and can be assumed to not be present.
		return "", nil
	}

	// Look in the overlay first.
	if v, ok := tx.overlay[key]; ok {
		return v, nil
	}

	// Look in the KV's committed state.
	return tx.kv.cur[key], nil
}

func (tx *tx) Each(fn func(key, value string) error) error {
	if tx.overlay == nil {
		// This transaction is being used after it has been committed/aborted.
		panic("access to finished transaction")
	}

	// Save the starting version for sanity checking.
	startVer := tx.version

	// Start by processing the unmodified data.
	err := tx.kv.cur.Each(func(key, value string) error {
		if tx.version != startVer {
			// The tx was modified.
			panic("tx modified during each")
		}

		if _, ok := tx.overlay[key]; ok {
			// The key is modified in the overlay.
			// It will be processed later.
			return nil
		}

		// Process the pair.
		return fn(key, value)
	})
	if err != nil {
		return err
	}

	// Process the modified data.
	for k, v := range tx.overlay {
		if tx.version != startVer {
			// The tx was modified.
			panic("tx modified during each")
		}

		if v == "" {
			// This is a deletion marker.
			continue
		}

		// Process the modified pair.
		err := fn(k, v)
		if err != nil {
			return err
		}
	}

	if tx.version != startVer {
		// The tx was modified.
		panic("tx modified during each")
	}

	return nil
}

func (tx *tx) Set(key, value string) error {
	// Update the version number.
	tx.version++

	switch {
	case tx.overlay == nil:
		// This transaction is being used after it has been committed/aborted.
		panic("access to finished transaction")
	case key == "":
		// An empty key is not representable.
		return errEmptyKey
	case value == "":
		// An empty value is not representable.
		return errEmptyValue
	case len(key) >= 1<<30:
		// The key is too large to be represented.
		return errKeyTooLarge
	case len(value) >= 1<<30:
		// The value is too large to be represented.
		return errValueTooLarge
	}

	// Calculate the size of the new pair.
	keySize := encodingSize(key)
	valSize := encodingSize(value)

	// Look up the previous value.
	prev := tx.kv.cur[key]

	if old, ok := tx.overlay[key]; ok {
		// A previous edit to this key exists in the overlay.
		oldSize := encodingSize(old)
		if prev == value {
			// This cancels the edit.
			prevSize := encodingSize(prev)
			delete(tx.overlay, key)
			tx.txSize -= keySize + oldSize
			if old == "" {
				tx.liveDiff += keySize + prevSize
			} else {
				tx.liveDiff += oldSize - prevSize
			}
			return nil
		}

		// Replace the update.
		tx.overlay[key] = value
		tx.txSize += valSize - oldSize
		tx.liveDiff += valSize - oldSize
		return nil
	}

	// If it is not different from committed state, do nothing.
	if prev == value {
		return nil
	}

	// Compute the size of the replaced pair (if it existed).
	var prevSize int64
	if prev != "" {
		prevSize = encodingSize(prev)
	}

	// Add the pair to the overlay.
	tx.overlay[key] = value

	// Update the size diffs.
	tx.txSize += keySize + valSize
	tx.liveDiff += valSize - prevSize

	return nil
}

func (tx *tx) Delete(key string) error {
	// Update the version number.
	tx.version++

	switch {
	case tx.overlay == nil:
		// This transaction is being used after it has been committed/aborted.
		panic("access to finished transaction")
	case key == "":
		// An empty key is not representable.
		return nil
	case len(key) >= 1<<30:
		// The key is too large to be represented.
		return nil
	}

	keySize := encodingSize(key)

	if old, ok := tx.overlay[key]; ok {
		// The key has been modified in the overlay.
		if old == "" {
			// The key has already been deleted already.
			return nil
		}
		oldSize := encodingSize(old)
		if _, ok := tx.kv.cur[key]; ok {
			// The key has been updated in the overlay.
			// Convert it to a deletion.
			tx.overlay[key] = ""
		} else {
			// The key has been inserted in the overlay.
			// Discard it.
			delete(tx.overlay, key)
		}
		tx.txSize -= keySize + oldSize
		tx.liveDiff -= keySize + oldSize
		return nil
	}

	prev, ok := tx.kv.cur[key]
	if !ok {
		// It does not exist in committed state.
		// No changes are necessary.
		return nil
	}

	// Add a deletion marker to the overlay.
	prevSize := encodingSize(prev)
	tx.overlay[key] = ""
	tx.txSize += keySize + 1
	tx.liveDiff -= keySize + prevSize

	return nil
}

func (tx *tx) WriteTo(w io.Writer) (int64, error) {
	txw := startTx(w)
	err := tx.Each(txw.writePair)
	if err != nil {
		return int64(txw.abort()), err
	}

	n, err := txw.finish()
	return int64(n), err
}
