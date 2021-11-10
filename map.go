package bucket

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Map is an in-memory implementation of View.
type Map map[string]string

var _ View = Map{}

func (m Map) Get(key string) (string, error) {
	return m[key], nil
}

func (m Map) Each(fn func(key, value string) error) error {
	for k, v := range m {
		err := fn(k, v)
		if err != nil {
			return err
		}
	}

	return nil
}

var errKeyTooLarge = errors.New("key too large")

var errValueTooLarge = errors.New("value too large")

var errEmptyKey = errors.New("empty key")

var errEmptyValue = errors.New("empty value")

// ErrIncompleteTxn is an error indicating that the data contain an incomplete transaction.
type ErrIncompleteTxn struct {
	ValidLen uint64
}

func (err ErrIncompleteTxn) Error() string {
	return fmt.Sprintf("incomplete transaction (%d valid bytes)", err.ValidLen)
}

func (m Map) ReadFrom(r io.Reader) (read int64, err error) {
	if len(m) != 0 {
		panic(errors.New("map is not empty"))
	}

	br := bufio.NewReader(r)

	var failed bool
	defer func() {
		if failed {
			for k := range m {
				delete(m, k)
			}
		}
	}()

	txData := make(map[string]string)
	for {
		// Scan the pairs within a transaction.
		var txRead int64
		for {
			// Peek at the first 8 bytes (max combined size of key + value lengths).
			data, err := br.Peek(8)
			switch err {
			case nil:
			case io.EOF:
				switch {
				case len(data) == 0 && len(txData) == 0:
					return read, nil
				case len(data) == 0:
					return read + txRead, ErrIncompleteTxn{ValidLen: uint64(read)}
				}
			default:
				failed = true
				return read + txRead + int64(br.Buffered()), err
			}

			// Read the key size.
			var toSkip int
			var kSize uint32
			if len(data) >= 4 {
				tmp := binary.LittleEndian.Uint32(data)
				width := (tmp & 0b11) + 1
				tmp &^= ^uint32(0) << (8 * width)
				tmp >>= 2
				toSkip += int(width)
				kSize = tmp
			} else {
				width := data[0]&2 + 1
				if len(data) < int(width) {
					return read + txRead + int64(br.Buffered()), ErrIncompleteTxn{ValidLen: uint64(read)}
				}
				var tmp uint32
				for i, v := range data[:width] {
					tmp |= uint32(v) << (8 * i)
				}
				tmp >>= 2
				toSkip += int(width)
				kSize = tmp
			}
			if kSize == 0 {
				// This terminates the transaction.
				br.Discard(toSkip)
				txRead += int64(toSkip)
				break
			}
			data = data[toSkip:]

			// Read the value size.
			if len(data) == 0 {
				return read + txRead + int64(br.Buffered()), ErrIncompleteTxn{ValidLen: uint64(read)}
			}
			var vSize uint32
			if len(data) >= 4 {
				tmp := binary.LittleEndian.Uint32(data)
				width := (tmp & 0b11) + 1
				tmp &^= ^uint32(0) << (8 * width)
				tmp >>= 2
				toSkip += int(width)
				vSize = tmp
			} else {
				width := data[0]&2 + 1
				if len(data) < int(width) {
					return read + txRead + int64(br.Buffered()), ErrIncompleteTxn{ValidLen: uint64(read)}
				}
				var tmp uint32
				for i, v := range data[:width] {
					tmp |= uint32(v) << (8 * i)
				}
				tmp >>= 2
				toSkip += int(width)
				vSize = tmp
			}

			br.Discard(toSkip)
			txRead += int64(toSkip)

			// Read the key.
			n, key, err := readString(br, kSize)
			txRead += int64(n)
			switch err {
			case nil:
			case io.EOF:
				return read + txRead + int64(br.Buffered()), ErrIncompleteTxn{ValidLen: uint64(read)}
			default:
				failed = true
				return read + txRead + int64(br.Buffered()), err
			}

			// Read the value.
			var value string
			if vSize != 0 {
				n, value, err = readString(br, vSize)
				txRead += int64(n)
				switch err {
				case nil:
				case io.EOF:
					return read + txRead + int64(br.Buffered()), ErrIncompleteTxn{ValidLen: uint64(read)}
				default:
					failed = true
					return read + txRead + int64(br.Buffered()), err
				}
			}

			// Add the pair to the transaction.
			txData[key] = value
		}

		// Apply the transaction.
		for k, v := range txData {
			if v == "" {
				delete(m, k)
			} else {
				m[k] = v
			}
		}

		// Clear the temporary map.
		for k := range txData {
			delete(txData, k)
		}

		read += txRead
	}
}

func readString(r *bufio.Reader, len uint32) (int, string, error) {
	if int(len) < r.Size() {
		data, err := r.Peek(int(len))
		if err != nil {
			return 0, "", err
		}

		r.Discard(int(len))

		return int(len), string(data), nil
	} else {
		buf := make([]byte, len)
		for i := 0; i < int(len); {
			n, err := r.Read(buf[i:])
			i += n
			if err != nil {
				return i, "", err
			}
		}

		return int(len), string(buf), nil
	}
}

// WriteTo encodes the map in bucket format.
// The resulting data is not encoded in a deterministic format.
func (m Map) WriteTo(w io.Writer) (int64, error) {
	// Check that the map is valid before trying to write it.
	for k, v := range m {
		if k == "" {
			return 0, errEmptyKey
		}
		if v == "" {
			return 0, errEmptyValue
		}
		if len(k) >= 1<<30 {
			return 0, errKeyTooLarge
		}
		if len(v) >= 1<<30 {
			return 0, errValueTooLarge
		}
	}

	// Create a transaction.
	tx := startTx(w)
	var err error
	for k, v := range m {
		err = tx.writePair(k, v)
		if err != nil {
			break
		}
	}
	if err != nil {
		return int64(tx.abort()), err
	}

	n, err := tx.finish()
	return int64(n), err
}
