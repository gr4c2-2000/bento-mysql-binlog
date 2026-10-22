package binlog

import (
	"context"
	"sync"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/warpstreamlabs/bento/public/service"
)

// txn is a group of row events that share a commit position.
type txn struct {
	events []Event
	pos    mysql.Position
}

// handler turns canal's push-based callbacks into transaction-sized units on a
// channel. Rows are buffered until the commit event so that every position the
// input can commit is a transaction boundary, never the middle of one.
type handler struct {
	canal.DummyEventHandler

	ctx       context.Context
	out       chan<- txn
	maxTxRows int
	logger    *service.Logger

	mu   sync.Mutex
	file string
	buf  []Event
}

func newHandler(ctx context.Context, out chan<- txn, file string, maxTxRows int, logger *service.Logger) *handler {
	return &handler{ctx: ctx, out: out, file: file, maxTxRows: maxTxRows, logger: logger}
}

func (h *handler) String() string { return "bento-mysql-binlog" }

// OnRotate records the name of the binlog file the following events belong to.
// Positions in row events are file-relative, so the name has to be tracked
// separately.
func (h *handler) OnRotate(_ *replication.EventHeader, e *replication.RotateEvent) error {
	h.mu.Lock()
	h.file = string(e.NextLogName)
	h.mu.Unlock()
	return nil
}

func (h *handler) OnRow(e *canal.RowsEvent) error {
	h.mu.Lock()
	events, err := EventsFrom(e, h.file)
	if err != nil {
		h.mu.Unlock()
		return err
	}
	h.buf = append(h.buf, events...)

	// A transaction larger than the cap would otherwise be held entirely in
	// memory. Flush early and accept that this one position is mid-transaction.
	overflow := len(h.buf) >= h.maxTxRows
	pos := mysql.Position{Name: h.file, Pos: e.Header.LogPos}
	h.mu.Unlock()

	if overflow {
		h.logger.Warnf("transaction exceeded max_transaction_rows (%d), flushing at mid-transaction position %s:%d",
			h.maxTxRows, pos.Name, pos.Pos)
		return h.flush(pos)
	}
	return nil
}

// OnXID fires at the commit of every transaction against a transactional
// engine, and is the only position this input treats as safe to resume from.
func (h *handler) OnXID(_ *replication.EventHeader, nextPos mysql.Position) error {
	return h.flush(nextPos)
}

func (h *handler) flush(pos mysql.Position) error {
	h.mu.Lock()
	buf := h.buf
	h.buf = nil
	h.mu.Unlock()

	if len(buf) == 0 {
		return nil
	}

	// A blocking send is the backpressure: when the pipeline stalls, canal
	// stops reading the binlog rather than buffering it in memory.
	select {
	case h.out <- txn{events: buf, pos: pos}:
		return nil
	case <-h.ctx.Done():
		return h.ctx.Err()
	}
}
