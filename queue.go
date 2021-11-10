package bucket

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// txQueue is a queue of transactions, implemented as a multi-appender-single-deleter atomic linked list.
type txQueue struct {
	// head is a pointer to the most recently inserted element of the list.
	// It is always up to date.
	head *txQueueElem

	// tail is a pointer to the last element of the list.
	// It may be stale.
	tail *txQueueElem
}

// push a transaction function onto the queue, returning the queue element.
// This may be invoked concurrently.
func (l *txQueue) push(fn func(Transaction) error) *txQueueElem {
	// Create an element.
	tx := queueElem(fn)

doPush:
	// Set next to the current queue head.
	rawNext := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)))
	tx.next = (*txQueueElem)(rawNext)

	// Attempt to atomically swap the queue head for this tx element.
	if !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)), rawNext, unsafe.Pointer(tx)) {
		// Update the "next" field and try again.
		goto doPush
	}

	// The "prev" field is set by the consumer.

	return tx
}

// pop a transaction off of the queue.
// This must not be invoked concurrently with itself or remove.
func (l *txQueue) pop() *txQueueElem {
	if l.tail == nil || l.tail.prev == nil {
		// Look at the head of the queue.
	doPopAtomic:
		head := (*txQueueElem)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head))))
		if head == nil {
			// The queue is empty.
			return nil
		}
		if head.next == nil {
			// This is the only remaining element in the list.
			// Try to remove it directly.
			if !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)), unsafe.Pointer(head), nil) {
				goto doPopAtomic
			}
			l.tail = nil
			return head
		}

		// Compute prev for all new elements of the list.
		// This means that "pop" and "remove" have a O(n) peak complexity and O(1) amortized complexity.
		// This is not ideal, but allows us to avoid stalling.
		prev := head
		for elem := head.next; elem != nil && elem.prev == nil; elem = elem.next {
			elem.prev = prev
			prev = elem
		}
		l.tail = prev
	}

	// Remove the tail element.
	tail := l.tail
	if tail.next != nil {
		panic("tail is not the tail")
	}
	l.tail = tail.prev
	tail.prev.next = nil
	tail.prev = nil
	return tail
}

// remove a specific transaction from the queue.
// This must not be invoked concurrently with itself or pop.
func (l *txQueue) remove(tx *txQueueElem) {
	next := tx.next

	if tx.prev == nil {
		// The queue element cannot be unlinked directly unless we compute prev.

	doRemoveHead:
		head := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)))
		if (*txQueueElem)(head) == tx {
			// The transaction is currently at the head of the queue.
			// Try to remove it from there.
			if next != nil {
				next.prev = nil
			}
			if !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)), head, unsafe.Pointer(next)) {
				goto doRemoveHead
			}
			tx.next = nil
			if l.tail == tx {
				l.tail = nil
			}
			return
		}

		// Compute prev for all new elements of the list.
		// This means that "pop" and "remove" has a O(n) peak complexity and O(1) amortized complexity.
		// This is not ideal, but allows us to avoid stalling.
		prev := (*txQueueElem)(head)
		for elem := prev.next; elem != nil && elem.prev == nil; elem = elem.next {
			elem.prev = prev
			prev = elem
		}
		if prev.next == nil {
			// Update the tail.
			l.tail = prev
		}
	}

	if tx.prev == nil {
		// The transaction does not appear to exist within the list.
		panic("tx not in list")
	}

	// Unlink the element.
	tx.prev.next = next
	if next != nil {
		next.prev = tx.prev
	}
	if tx == l.tail {
		l.tail = tx.prev
	}
	tx.next, tx.prev = nil, nil
}

// queueElem returns a queue element with the provided transaction function.
func queueElem(fn func(Transaction) error) *txQueueElem {
	tx := txQueueElemPool.Get().(*txQueueElem)
	if tx.prev != nil || tx.next != nil {
		panic("tx list elem still in use")
	}
	tx.fn = fn
	return tx
}

type txQueueElem struct {
	prev, next   *txQueueElem
	fn           func(Transaction) error
	commitNotify chan error
}

// done cleans up the element and returns it for re-use.
func (tx *txQueueElem) done() {
	if tx.prev != nil || tx.next != nil {
		panic("tx list elem still in use")
	}
	tx.fn = nil
	txQueueElemPool.Put(tx)
}

var txQueueElemPool = sync.Pool{
	New: func() interface{} {
		return &txQueueElem{
			commitNotify: make(chan error, 1),
		}
	},
}
