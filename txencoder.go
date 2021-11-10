package bucket

import (
	"encoding/binary"
	"io"
	"math/bits"
	"sync"
)

// startTx starts encoding a transaction.
func startTx(w io.Writer) txEncoder {
	buf := txBufPool.Get().(*txbuf)
	return txEncoder{
		w:   w,
		buf: buf,
	}
}

type txEncoder struct {
	// w is the destination to write the transaction.
	w io.Writer

	// n is the number of bytes written to w.
	n int64

	// buffered is the number of bytes currently stored in the buffer.
	buffered uint

	// buf is a buffer used to hold bytes before writing them.
	buf *txbuf
}

// writePair encodes a raw pair update.
// An empty value is equivalent to a deletion.
func (e *txEncoder) writePair(key, value string) error {
	switch {
	case key == "":
		// An empty key is not representable.
		return errEmptyKey
	case len(key) >= 1<<30:
		// The key is too large to encode.
		return errKeyTooLarge
	case len(value) >= 1<<30:
		// The value is too large to encode.
		return errValueTooLarge
	}

	// Encode the lengths.
	err := e.writeLengths(uint32(len(key)), uint32(len(value)))
	if err != nil {
		return err
	}

	// Encode the key.
	err = e.writeString(key)
	if err != nil {
		return err
	}

	// Encode the value.
	err = e.writeString(value)
	if err != nil {
		return err
	}

	return nil
}

// writeString encodes raw string data.
func (e *txEncoder) writeString(str string) error {
	var notStringWriter bool
	for {
		if len(str) > len(e.buf) && e.buffered == 0 && !notStringWriter {
			if sw, ok := e.w.(io.StringWriter); ok {
				// Write the string directly.
				n, err := sw.WriteString(str)
				e.n += int64(n)
				if err != nil {
					return err
				}
				str = str[n:]
				continue
			} else {
				// Set this flag to avoid repeated useless type assertions.
				notStringWriter = true
			}
		}

		// Copy (part) of the string into the buffer.
		n := copy(e.buf[e.buffered:], str)
		e.buffered += uint(n)
		str = str[n:]
		if len(str) == 0 {
			// The string has been copied completely into the buffer.
			break
		}

		// Flush the buffer to make space for more string data.
		err := e.flush()
		if err != nil {
			return err
		}
	}

	return nil
}

// writeLengths encodes lengths for a key-value pair.
func (e *txEncoder) writeLengths(keyLen, valueLen uint32) error {
	// Carve out a chunk of buffer to store the lengths.
	if e.buffered > uint(len(e.buf))-8 {
		err := e.flush()
		if err != nil {
			return err
		}
	}
	lenBuf := e.buf[e.buffered:][:8]

	// Encode the key.
	keyLenWidth := (bits.Len32(keyLen) + 1) / 8
	binary.LittleEndian.PutUint32(lenBuf[:], (keyLen<<2)|uint32(keyLenWidth))

	// Encode the value.
	valueLenWidth := (bits.Len32(valueLen) + 1) / 8
	binary.LittleEndian.PutUint32(lenBuf[keyLenWidth+1:][:4], (valueLen<<2)|uint32(valueLenWidth))

	// Update the buffer state.
	e.buffered += uint(keyLenWidth) + uint(valueLenWidth) + 2

	return nil
}

// finish terminates the transaction by encoding a transaction commit marker and flushing the data.
// The txEncoder must not be used after this call returns.
func (e *txEncoder) finish() (int64, error) {
	err := e.doFinish()
	n := e.abort()
	return n, err
}

func (e *txEncoder) doFinish() error {
	if e.n == 0 && e.buffered == 0 {
		// This is a no-op transaction.
		// There is nothing to do.
		return nil
	}

	// Encode a deletion marker.
	if int(e.buffered) >= len(e.buf) {
		// Flush the buffer to make room for the deletion marker.
		err := e.flush()
		if err != nil {
			return err
		}
	}
	e.buf[e.buffered] = 0
	e.buffered++

	// Flush the buffer.
	return e.flush()
}

// abort the transaction.
// The txEncoder must not be used after this call returns.
func (e *txEncoder) abort() int64 {
	e.w = nil
	txBufPool.Put(e.buf)

	return e.n
}

func (e *txEncoder) flush() error {
	if e.buffered == 0 {
		// There is nothing to flush.
		return nil
	}

	// Write all buffered data.
	n, err := e.w.Write(e.buf[:e.buffered])
	e.n += int64(n)
	if err != nil {
		e.w = nil
		return err
	}
	if n != int(e.buffered) {
		panic("short write without error")
	}

	// There is no remaining buffered data.
	e.buffered = 0

	return nil
}

type txbuf = [1 << 16]byte

var txBufPool = sync.Pool{
	New: func() interface{} {
		return &txbuf{}
	},
}
