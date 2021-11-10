package bucket

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"golang.org/x/exp/rand"
)

func TestTxEncode(t *testing.T) {
	t.Parallel()

	// Create a transaction to test with.
	var buf bytes.Buffer
	tx := startTx(&buf)

	// Write some pairs.
	err := tx.writePair("x", "y")
	if err != nil {
		t.Errorf("failed to write pair: %s", err.Error())
	}
	err = tx.writePair("xyzzy", "")
	if err != nil {
		t.Errorf("failed to write pair: %s", err.Error())
	}

	// Finish the transaction.
	n, err := tx.finish()
	if err != nil {
		t.Errorf("failed to finish transaction: %s", err.Error())
	}
	expect := "\x04\x04xy\x14\x00xyzzy\x00"
	data := buf.String()
	if data != expect {
		t.Errorf("expected encoding %q but got %q", expect, data)
	}
	if n != int64(len(data)) {
		t.Errorf("reported %d written bytes but actually wrote %d", n, len(data))
	}
}

func TestWriteLengths(t *testing.T) {
	t.Parallel()

	cases := []struct {
		keyLen, valueLen uint32
		data             string
	}{
		{1, 0, "\x04\x00"},
		{1, 1, "\x04\x04"},
		{(1 << 6) - 1, 1, "\xFC\x04"},
		{1, (1 << 6) - 1, "\x04\xFC"},
		{(1 << 6) - 1, (1 << 6) - 1, "\xFC\xFC"},
		{1 << 6, (1 << 6) - 1, "\x01\x01\xFC"},
		{(1 << 6) - 1, 1 << 6, "\xFC\x01\x01"},
		{1 << 6, 1 << 6, "\x01\x01\x01\x01"},
		{3<<6 + 5<<14, 3<<6 + 5<<14, "\x02\x03\x05\x02\x03\x05"},
		{3<<6 + 5<<14 + 7<<22, 3<<6 + 5<<14 + 7<<22, "\x03\x03\x05\x07\x03\x03\x05\x07"},
	}

	for _, c := range cases {
		c := c
		t.Run(fmt.Sprintf("%d,%d", c.keyLen, c.valueLen), func(t *testing.T) {
			t.Parallel()

			// Create a transaction to test with.
			var buf bytes.Buffer
			tx := startTx(&buf)
			defer tx.abort()

			// Write the lengths and flush.
			err := tx.writeLengths(c.keyLen, c.valueLen)
			if err != nil {
				t.Errorf("failed to write lengths: %s", err.Error())
				return
			}
			err = tx.flush()
			if err != nil {
				t.Errorf("failed to flush: %s", err.Error())
				return
			}

			// Compare the result.
			if buf.String() != c.data {
				t.Errorf("expected %x but got %x", c.data, buf.Bytes())
			}
		})
	}
}

func TestWriteString(t *testing.T) {
	t.Parallel()

	// Test writing a small string directly into the buffer.
	t.Run("Buffered", func(t *testing.T) {
		t.Parallel()

		// Create a transaction to test with.
		tx := startTx(io.Discard)
		defer tx.abort()

		// Write a tiny string.
		str := "hello"
		err := tx.writeString(str)
		if err != nil {
			t.Errorf("failed to write string: %s", err.Error())
			return
		}

		// Confirm that the string is buffered.
		if tx.n != 0 {
			t.Errorf("transaction flushed %d bytes", tx.n)
		}
		if buffered := string(tx.buf[:tx.buffered]); buffered != str {
			t.Errorf("expected buffered string %q but got %q", str, buffered)
		}
	})

	// Test writing a small string into a nearly-full buffer.
	t.Run("Overflow", func(t *testing.T) {
		t.Parallel()

		// Create a transaction to test with.
		var buf bytes.Buffer
		tx := startTx(&buf)
		defer tx.abort()

		// Populate the transaction with filler garbage so that there is exactly one byte left.
		tx.buffered = uint(len(tx.buf)) - 1
		*tx.buf = txbuf{}

		// Write a tiny string.
		str := "hello"
		err := tx.writeString(str)
		if err != nil {
			t.Errorf("failed to write string: %s", err.Error())
			return
		}

		// Check that the first character was flushed and the rest is buffered.
		switch written := buf.Bytes(); true {
		case len(written) != len(tx.buf):
			t.Errorf("entire tx buf not filled: found %d bytes written", len(written))
		case written[len(written)-1] != str[0]:
			t.Errorf("expected final byte %q but found %q", str[0], written[len(written)-1])
		}
		if buffered := string(tx.buf[:tx.buffered]); buffered != str[1:] {
			t.Errorf("expected buffered string %q but got %q", str[1:], buffered)
		}
	})

	// Test writing a giant string that does not fit in the buffer.
	t.Run("Gigantic", func(t *testing.T) {
		t.Parallel()

		// Create a transaction to test with.
		// For this purpose, we must hide the io.StringWriter implementation by wrapping the buffer.
		var buf bytes.Buffer
		tx := startTx(struct{ io.Writer }{&buf})
		defer tx.abort()

		// Write a huge string.
		baseStr := "hello"
		str := strings.Repeat(baseStr, len(tx.buf))
		err := tx.writeString(str)
		if err != nil {
			t.Errorf("failed to write string: %s", err.Error())
			return
		}
		err = tx.flush()
		if err != nil {
			t.Errorf("failed to flush: %s", err.Error())
			return
		}

		// Verify the written data.
		data := buf.String()
		var repetitions int
		for ; strings.HasPrefix(data, baseStr); data = data[len(baseStr):] {
			repetitions++
		}
		if data != "" {
			var info string
			if len(data) > 64 {
				info = fmt.Sprintf(" (data truncated from %d bytes)", len(data))
				data = data[:64]
			}
			t.Errorf("unexpected data after %d repetitions: %q%s", repetitions, data, info)
		}
		if repetitions < len(tx.buf) {
			t.Errorf("incomplete data: %d of %d repetitions", repetitions, len(tx.buf))
		}
	})

	// Test using io.StringWriter for large strings.
	t.Run("StringWriter", func(t *testing.T) {
		t.Parallel()

		str := strings.Repeat("  ", len(txbuf{})-1)

		cases := []struct {
			name              string
			preBuffered       int
			expectBytes       int
			expectStringBytes int
		}{
			{
				// The string should be written directly to WriteString.
				name:              "Direct",
				expectStringBytes: len(str),
			},
			{
				// The start of the string should be merged into the buffer, and the rest written to WriteString.
				name:              "Partial",
				preBuffered:       len(txbuf{}) - 1,
				expectBytes:       len(txbuf{}),
				expectStringBytes: len(str) - 1,
			},
			{
				// The start of the string should be merged into the buffer and flushed.
				// The rest of the string should be placed into the buffer to batch with other data.
				name:              "Buffered",
				preBuffered:       1,
				expectBytes:       2*len(txbuf{}) - 1,
				expectStringBytes: 0,
			},
		}
		for _, c := range cases {
			c := c
			t.Run(c.name, func(t *testing.T) {
				t.Parallel()

				// Create a transaction to test with.
				var tsw testStringWriter
				tx := startTx(&tsw)
				defer tx.abort()

				// Buffer some garbage data if requested.
				tx.buffered = uint(c.preBuffered)
				*tx.buf = txbuf{}

				// Write a huge string.
				err := tx.writeString(str)
				if err != nil {
					t.Errorf("failed to write string: %s", err.Error())
					return
				}
				err = tx.flush()
				if err != nil {
					t.Errorf("failed to flush: %s", err.Error())
					return
				}

				if tsw.writeBytes != int64(c.expectBytes) || tsw.stringWriteBytes != int64(c.expectStringBytes) {
					t.Errorf("Write:%d WriteString:%d (expected Write:%d WriteString:%d)", tsw.writeBytes, tsw.stringWriteBytes, c.expectBytes, c.expectStringBytes)
				}
			})
		}
	})
}

type testStringWriter struct {
	data             []byte
	writeBytes       int64
	stringWriteBytes int64
}

var _ io.Writer = (*testStringWriter)(nil)
var _ io.StringWriter = (*testStringWriter)(nil)

func (w *testStringWriter) Write(data []byte) (int, error) {
	n := len(data)
	w.data = append(w.data, data...)
	w.writeBytes += int64(n)
	return n, nil
}

func (w *testStringWriter) WriteString(data string) (int, error) {
	n := len(data)
	w.data = append(w.data, data...)
	w.stringWriteBytes += int64(n)
	return n, nil
}

func BenchmarkWriteLengths(b *testing.B) {
	for i := 0; i <= 0; i++ {
		for j := 0; j <= 0; j++ {
			size := i + j + 2
			kmask := (uint32(1) << (6 + 8*i)) - 1
			vmask := (uint32(1) << (6 + 8*i)) - 1
			b.Run(fmt.Sprintf("%d,%d", i+1, j+1), func(b *testing.B) {
				// Generate some random pairs.
				var r rand.PCGSource
				r.Seed(56)
				pairs := make([][2]uint32, b.N)
				for i := range pairs {
					raw := r.Uint64()
					pairs[i] = [2]uint32{
						uint32(raw) & kmask,
						uint32(raw>>32) & vmask,
					}
				}

				// Create a transaction on a discard writer.
				tx := startTx(io.Discard)
				b.SetBytes(int64(size))
				defer tx.abort()

				// Write all of the length pairs.
				b.ResetTimer()
				for _, p := range pairs {
					tx.writeLengths(p[0], p[1])
				}
			})
		}
	}
}

func BenchmarkTinyTxEncode(b *testing.B) {
	// Generate a random key and value.
	r := rand.New(rand.NewSource(478))
	keyDat := make([]byte, 10)
	r.Read(keyDat)
	key := string(keyDat)
	valDat := make([]byte, 65)
	r.Read(valDat)
	value := string(valDat)
	b.SetBytes(int64(2 + len(key) + len(value) + 1))

	// Encode a bunch of transactions, just updating this pair.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := startTx(io.Discard)
		err := tx.writePair(key, value)
		if err != nil {
			b.Fatal(err)
		}
		_, err = tx.finish()
		if err != nil {
			b.Fatal(err)
		}
	}
}
