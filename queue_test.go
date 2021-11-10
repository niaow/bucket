package bucket

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
)

// TestTxQueuePushPopSingle pushes then pops a single element in a queue.
func TestTxQueuePushPopSingle(t *testing.T) {
	t.Parallel()

	var queue txQueue

	// Push a test element.
	pushed := queue.push(func(tx Transaction) error {
		return nil
	})

	// Pop it back off the queue.
	switch popped := queue.pop(); popped {
	case nil:
		t.Error("pushed element was lost")
	case pushed:
	default:
		t.Error("popped an element that we never pushed")
	}

	// Verify that the queue is empty.
	switch popped := queue.pop(); popped {
	case nil:
	case pushed:
		t.Error("unexpectedly popped the pushed element later")
	default:
		t.Error("popped an element that was never pushed when queue should be empty")
	}
}

// TestTxQueuePushRemoveSingle pushes then removes a single element in a queue.
func TestTxQueuePushRemoveSingle(t *testing.T) {
	t.Parallel()

	var queue txQueue

	// Push a test element.
	pushed := queue.push(func(tx Transaction) error {
		return nil
	})

	// Remove it from the queue.
	queue.remove(pushed)

	// Verify that the queue is empty.
	switch popped := queue.pop(); popped {
	case nil:
	case pushed:
		t.Error("unexpectedly popped the pushed element after removal")
	default:
		t.Error("popped an element that was never pushed when queue should be empty")
	}
}

// TestTxQueuePushPopMany pushes then pops many elements in a queue, verifying that they are processed in the correct order.
func TestTxQueuePushPopMany(t *testing.T) {
	t.Parallel()

	// Push many elements onto a queue.
	var queue txQueue
	var elems []*txQueueElem
	var i int
	for len(elems) < 100 {
		j := len(elems) + 1
		elems = append(elems, queue.push(func(tx Transaction) error {
			// Check that this was run in the correct order.
			prev := i
			i = j
			if prev != j-1 {
				return fmt.Errorf("ran %d after %d", j, i)
			}

			return nil
		}))
	}

	// Pop everything off of the queue and verify the order.
	var n int
	var nerrs int
	for {
		elem := queue.pop()
		if elem == nil {
			break
		}
		defer elem.done()

		err := elem.fn(nil)
		if err != nil {
			t.Error(err)
			nerrs++
			if nerrs == 10 {
				t.Error("too many errors")
				return
			}
		}

		n++
	}
	if n != len(elems) {
		t.Errorf("pushed %d elements but popped %d", n, len(elems))
	}
}

// TestTxQueuePushRemoveMany pushes then removes many elements in a queue.
func TestTxQueuePushRemoveMany(t *testing.T) {
	t.Parallel()

	// Push many elements onto the queue.
	var queue txQueue
	var elems []*txQueueElem
	for len(elems) < 100 {
		elems = append(elems, queue.push(func(tx Transaction) error {
			return nil
		}))
	}

	// Randomly shuffle the elements.
	rand.New(rand.NewSource(4)).Shuffle(len(elems), func(i, j int) {
		elems[i], elems[j] = elems[j], elems[i]
	})

	// Remove the elements in the shuffled order.
	for _, e := range elems {
		queue.remove(e)
		e.done()
	}
}

// TestTxQueuePushPopConcurrent pushes many elements concurrently while popping them.
func TestTxQueuePushPopConcurrent(t *testing.T) {
	t.Parallel()

	var queue txQueue
	var wg sync.WaitGroup
	defer wg.Wait()

	// Concurrently push things onto the queue.
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			elem := queue.push(func(tx Transaction) error {
				n--
				return nil
			})
			<-elem.commitNotify
			elem.done()
		}()
	}

	// Pop the elements (while the pushes are maybe still running).
	for n > 0 {
		elem := queue.pop()
		if elem == nil {
			// Wait a short time for stuff to happen.
			runtime.Gosched()
			continue
		}

		elem.commitNotify <- elem.fn(nil)
	}
}

// TestTxQueuePushRemoveConcurrent pushes and removes many elements concurrently.
func TestTxQueuePushRemoveConcurrent(t *testing.T) {
	t.Parallel()

	// Run a bunch of operations concurrently.
	var queue txQueue
	var group errgroup.Group
	var mu sync.Mutex
	for i := 0; i < 100; i++ {
		group.Go(func() error {
			// Push an element without holding a lock.
			tx := queue.push(func(tx Transaction) error {
				return nil
			})

			// Remove the element from the queue under a lock.
			mu.Lock()
			defer mu.Unlock()
			queue.remove(tx)
			tx.done()

			return nil
		})
	}
	err := group.Wait()
	if err != nil {
		t.Error(err)
	}
}

// TestTxQueuePushPopRemoveConcurrent pushes and pops/removes many elements concurrently.
func TestTxQueueConcurrentPushPopRemove(t *testing.T) {
	t.Parallel()

	var queue txQueue
	var group errgroup.Group
	var mu sync.Mutex
	for i := 0; i < 100; i++ {
		group.Go(func() error {
			// Push an element without holding a lock.
			tx := queue.push(func(tx Transaction) error {
				return nil
			})

			// Acquire the lock.
			mu.Lock()
			defer mu.Unlock()

			select {
			case err := <-tx.commitNotify:
				// This element has already been popped.
				tx.done()
				return err
			default:
			}

			// Remove this element from the queue.
			queue.remove(tx)
			tx.done()

			// Pop the next element and notify it (if present).
			if next := queue.pop(); next != nil {
				next.commitNotify <- next.fn(nil)
			}

			return nil
		})
	}
	err := group.Wait()
	if err != nil {
		t.Error(err)
	}
}
