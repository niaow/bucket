package bucket

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"golang.org/x/exp/rand"
)

func TestKV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.kv")

	// Open the KV.
	kv, err := open(path)
	if err != nil {
		t.Errorf("failed to open KV: %s", err.Error())
		return
	}
	defer func() {
		if kv != nil {
			err := kv.Close()
			if err != nil {
				t.Errorf("failed to close KV: %s", err.Error())
			}
		}
	}()

	t.Run("Insert", func(t *testing.T) {
		// Insert a key in an edit transaction.
		err := kv.Edit(func(tx Transaction) error {
			return tx.Set("wow", "data")
		})
		if err != nil {
			t.Errorf("failed to insert key: %s", err.Error())
			return
		}

		// Read the key back in a view transaction.
		err = kv.View(func(v View) error {
			val, err := v.Get("wow")
			if err != nil {
				return err
			}
			if val != "data" {
				t.Errorf("wrote data but got back %q", val)
			}

			return nil
		})
		if err != nil {
			t.Errorf("failed to look up key: %s", err.Error())
		}
	})

	t.Run("ConcurrentReads", func(t *testing.T) {
		// Test that multiple view transactions can run at once.
		err := kv.View(func(v View) error {
			ch := make(chan error, 2)
			go func() {
				defer func() {
					ch <- errors.New("if you are seeing this, the goroutine crashed")
					close(ch)
				}()
				ch <- kv.View(func(v View) error {
					return nil
				})
			}()

			return <-ch
		})
		if err != nil {
			t.Errorf("failed to test concurrent views: %s", err.Error())
		}
	})

	t.Run("Abort", func(t *testing.T) {
		// Try aborting an edit transaction.
		abortErr := errors.New("aborted")
		err := kv.Edit(func(tx Transaction) error {
			err := tx.Set("404", "bad")
			if err != nil {
				return err
			}

			return abortErr
		})
		switch err {
		case nil:
			t.Error("expected aborted transaction to return error, but it did not")
		case abortErr:
		default:
			t.Errorf(`unexpected transaction error (expected "aborted"): %s`, err.Error())
			return
		}

		// Verify that the uncommitted data is gone.
		err = kv.View(func(v View) error {
			val, err := v.Get("404")
			if err != nil {
				return err
			}
			if val != "" {
				t.Errorf("404 found: %q", val)
			}

			return nil
		})
		if err != nil {
			t.Errorf("failed to view after abort: %s", err.Error())
		}
	})

	t.Run("BulkInsert", func(t *testing.T) {
		n := 100

		// Insert 100 key-value pairs.
		err := kv.Edit(func(tx Transaction) error {
			for i := 1; i <= n; i++ {
				istr := strconv.Itoa(i)
				err := tx.Set("k"+istr, "v"+istr)
				if err != nil {
					return err
				}
			}

			return nil
		})
		if err != nil {
			t.Errorf("bulk insert failed: %s", err)
		}

		// Check that all inserted key-value pairs exist.
		err = kv.View(func(v View) error {
			for i := 1; i <= n; i++ {
				istr := strconv.Itoa(i)
				key := "k" + istr
				val, err := v.Get(key)
				if err != nil {
					return err
				}
				expect := "v" + istr
				switch val {
				case "":
					t.Errorf("key %q missing", key)
				case expect:
				default:
					t.Errorf("key %q should be %q, but got %q", key, expect, val)
				}
			}

			return nil
		})
		if err != nil {
			t.Errorf("view failed: %s", err.Error())
		}
	})

	t.Run("Update", func(t *testing.T) {
		// Insert a test key.
		err := kv.Edit(func(tx Transaction) error {
			return tx.Set("x", "a")
		})
		if err != nil {
			t.Errorf("insert failed: %s", err.Error())
			return
		}

		// Repeatedly append to the test key.
		for nextRune := 'b'; nextRune <= 'g'; nextRune++ {
			err := kv.Edit(func(tx Transaction) error {
				prev, err := tx.Get("x")
				if err != nil {
					return err
				}

				return tx.Set("x", prev+string(nextRune))
			})
			if err != nil {
				t.Errorf("update failed: %s", err.Error())
				return
			}
		}

		// Verify the end result.
		err = kv.View(func(v View) error {
			expect := "abcdefg"
			val, err := v.Get("x")
			if err != nil {
				return err
			}
			switch val {
			case "":
				t.Error("x not found")
			case expect:
			default:
				t.Errorf("value should be %q, but got %q", expect, val)
			}

			return nil
		})
		if err != nil {
			t.Errorf("view failed: %s", err.Error())
		}

		// Delete the key.
		err = kv.Edit(func(tx Transaction) error {
			return tx.Delete("x")
		})
		if err != nil {
			t.Errorf("delete failed: %s", err.Error())
			return
		}

		// Verify that the key is actually deleted.
		err = kv.View(func(v View) error {
			val, err := v.Get("x")
			if err != nil {
				return err
			}
			if val != "" {
				t.Error("key found after deletion")
			}

			return nil
		})
		if err != nil {
			t.Errorf("view failed: %s", err.Error())
		}
	})

	t.Run("Rewrite", func(t *testing.T) {
		// Delete every available key.
		err := kv.Edit(func(tx Transaction) error {
			// List all of the keys and then delete them all.
			var keys []string
			err := tx.Each(func(key, value string) error {
				keys = append(keys, key)
				return nil
			})
			if err != nil {
				return err
			}
			for _, k := range keys {
				err := tx.Delete(k)
				if err != nil {
					return err
				}
			}

			// Delete an extra key.
			return tx.Delete("fake")
		})
		if err != nil {
			t.Errorf("failed to wipe: %s", err.Error())
			return
		}

		// Check for any remaining keys.
		err = kv.View(func(v View) error {
			return v.Each(func(key, value string) error {
				t.Errorf("unexpected surviving pair: %q=%q", key, value)
				return nil
			})
		})
		if err != nil {
			t.Errorf("failed to view: %s", err.Error())
		}

		// The file should now be zero bytes.
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("failed to stat: %s", err.Error())
			return
		}
		if size := info.Size(); size != 0 {
			t.Errorf("file not empty: %d bytes", size)
		}
	})
}

// BenchmarkBulkInsert benchmarks bulk insertion of small pairs.
func BenchmarkBulkInsert(b *testing.B) {
	b.StopTimer()

	// Generate some random key-value pairs.
	buf := make([]byte, 16*b.N)
	rand.New(rand.NewSource(666)).Read(buf)
	mergeStr := string(buf)
	pairs := make([][2]string, b.N)
	for i := range pairs {
		pairs[i][0] = mergeStr[16*i:][:8]
		pairs[i][1] = mergeStr[(16*i)+8:][:8]
	}

	// Create a key-value store for the benchmark.
	kv, err := open(filepath.Join(b.TempDir(), "bench.kv"))
	if err != nil {
		b.Fatalf("failed to open KV: %s", err.Error())
	}
	defer func() {
		err := kv.Close()
		if err != nil {
			b.Errorf("failed to close KV: %s", err)
		}
	}()

	b.SetBytes(18)
	b.StartTimer()
	b.ResetTimer()

	// Insert all of the key-value pairs in a single transaction.
	err = kv.Edit(func(tx Transaction) error {
		for i := range pairs {
			err := tx.Set(pairs[i][0], pairs[i][1])
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		b.Fatalf("failed to edit: %s", err.Error())
	}
}

// BenchmarkRandomUpdate64K8 is a benchmark of the batching system with a large number of tiny transactions.
// It runs 1000 concurrent transactions per core on 2^16 pairs of 8-byte keys and 8-byte values.
func BenchmarkRandomUpdate64K8(b *testing.B) {
	b.StopTimer()

	// Generate some random strings to use.
	rsrc := rand.NewSource(666)
	strs := make([]string, 1<<16)
	buf := make([]byte, 8*(1<<16))
	rand.New(rsrc).Read(buf)
	mergeStr := string(buf)
	for i := range strs {
		strs[i] = mergeStr[8*i:][:8]
	}

	// Create a key-value store for the benchmark.
	kv, err := open(filepath.Join(b.TempDir(), "bench.kv"))
	if err != nil {
		b.Fatalf("failed to open KV: %s", err.Error())
	}
	defer kv.Close()

	// Dump some initial data into the key-value storage.
	err = kv.Edit(func(tx Transaction) error {
		var buf [1 << 16]uint16
		for i := 0; i < len(buf); i += 4 {
			v := rsrc.Uint64()
			buf[i+0] = uint16(v << 0)
			buf[i+1] = uint16(v << 16)
			buf[i+2] = uint16(v << 32)
			buf[i+3] = uint16(v << 48)
		}

		for i, j := range buf {
			err := tx.Set(strs[i], strs[j])
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		b.Fatalf("failed to populate KV: %s", err.Error())
	}

	b.SetBytes(18)
	b.SetParallelism(1000)
	b.StartTimer()
	b.ResetTimer()

	// Repeatedly update random keys in individual transactions.
	seed := rsrc.Uint64()
	b.RunParallel(func(p *testing.PB) {
		var src rand.PCGSource
		src.Seed(atomic.AddUint64(&seed, 1))
		var k, v string
		editFn := func(tx Transaction) error {
			return tx.Set(k, v)
		}
		for p.Next() {
			rval := src.Uint64()
			k, v = strs[uint16(rval)], strs[uint16(rval>>16)]
			err := kv.Edit(editFn)
			if err != nil {
				b.Fatalf("failed to edit KV: %s", err.Error())
			}
		}
	})
	b.ReportMetric(float64(kv.transactions)/float64(kv.batches), "tx/batch")
}
