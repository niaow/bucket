package bucket

import "io"

// batchTx is a transaction implementation used to implement batching.
// It adds an extra layer of overlay over a normal transaction.
type batchTx struct {
	// base is the base transaction.
	base *tx

	// version is a version number used for sanity checking.
	version uint64

	// overlay holds updated key-value pairs.
	// An empty value represents a delete.
	overlay map[string]string
}

var _ Transaction = (*batchTx)(nil)

func (tx *batchTx) Get(key string) (string, error) {
	switch {
	case tx.base == nil:
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

	// Look in the base transaction.
	return tx.base.Get(key)
}

func (tx *batchTx) Each(fn func(key, value string) error) error {
	if tx.base == nil {
		// This transaction is being used after it has been committed/aborted.
		panic("access to finished transaction")
	}

	// Save the starting version for sanity checking.
	startVer := tx.version

	// Start by processing the unmodified data.
	err := tx.base.Each(func(key, value string) error {
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

func (tx *batchTx) WriteTo(w io.Writer) (int64, error) {
	txw := startTx(w)
	err := tx.Each(txw.writePair)
	if err != nil {
		return int64(txw.abort()), err
	}

	n, err := txw.finish()
	return int64(n), err
}

func (tx *batchTx) Set(key, value string) error {
	// Update the version number.
	tx.version++

	switch {
	case tx.base == nil:
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

	// Update the overlay.
	tx.overlay[key] = value
	return nil
}

func (tx *batchTx) Delete(key string) error {
	// Update the version number.
	tx.version++

	switch {
	case tx.base == nil:
		// This transaction is being used after it has been committed/aborted.
		panic("access to finished transaction")
	case key == "":
		// An empty key is not representable.
		return nil
	case len(key) >= 1<<30:
		// The key is too large to be represented.
		return nil
	}

	// Insert a deletion marker in the overlay.
	tx.overlay[key] = ""
	return nil
}
