package binlog

import (
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// Offsets tracks which emitted batches have been acknowledged so that only a
// contiguous, fully delivered prefix of the binlog is ever committed.
//
// Bento acknowledges batches as their outputs complete, which is not
// necessarily the order they were read in. Committing the position of whichever
// batch acked last would skip over an earlier batch that is still in flight, so
// every batch gets a sequence number and the committed position only moves up
// to the newest batch whose predecessors have all been acked.
type Offsets struct {
	mu      sync.Mutex
	nextSeq uint64
	pending []slot
}

type slot struct {
	seq   uint64
	pos   mysql.Position
	acked bool
}

// NewOffsets returns an empty tracker.
func NewOffsets() *Offsets {
	return &Offsets{}
}

// Track registers a batch ending at pos and returns its sequence number.
func (o *Offsets) Track(pos mysql.Position) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()

	seq := o.nextSeq
	o.nextSeq++
	o.pending = append(o.pending, slot{seq: seq, pos: pos})
	return seq
}

// Ack marks a batch as delivered and reports the new watermark: the position of
// the newest batch whose predecessors have all been acked. The boolean is false
// when the watermark did not move, which happens whenever an earlier batch is
// still outstanding.
func (o *Offsets) Ack(seq uint64) (mysql.Position, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for i := range o.pending {
		if o.pending[i].seq == seq {
			o.pending[i].acked = true
			break
		}
	}

	var (
		watermark mysql.Position
		moved     bool
		cut       int
	)
	for _, s := range o.pending {
		if !s.acked {
			break
		}
		watermark, moved = s.pos, true
		cut++
	}
	o.pending = o.pending[cut:]
	return watermark, moved
}

// Pending reports how many batches are still awaiting acknowledgement.
func (o *Offsets) Pending() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.pending)
}
