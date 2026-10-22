package binlog

import (
	"context"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/warpstreamlabs/bento/public/service"
)

func newTestHandler(t *testing.T, maxTxRows int) (*handler, chan txn, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan txn, 8)
	h := newHandler(ctx, out, "mysql-bin.000001", maxTxRows, service.MockResources().Logger())
	return h, out, cancel
}

func recv(t *testing.T, out <-chan txn) (txn, bool) {
	t.Helper()
	select {
	case tx := <-out:
		return tx, true
	case <-time.After(time.Second):
		return txn{}, false
	}
}

// Rows are held until the commit event so the position the input can resume
// from is always a transaction boundary.
func TestHandlerEmitsOnCommitOnly(t *testing.T) {
	h, out, cancel := newTestHandler(t, 1000)
	defer cancel()

	if err := h.OnRow(rowsEvent(canal.InsertAction, [][]any{{int64(1), []byte("new"), "1.00"}})); err != nil {
		t.Fatal(err)
	}
	if err := h.OnRow(rowsEvent(canal.InsertAction, [][]any{{int64(2), []byte("new"), "2.00"}})); err != nil {
		t.Fatal(err)
	}

	select {
	case tx := <-out:
		t.Fatalf("rows emitted before commit: %+v", tx)
	default:
	}

	commit := mysql.Position{Name: "mysql-bin.000001", Pos: 5000}
	if err := h.OnXID(nil, commit); err != nil {
		t.Fatal(err)
	}

	tx, ok := recv(t, out)
	if !ok {
		t.Fatal("no transaction emitted after commit")
	}
	if len(tx.events) != 2 {
		t.Errorf("want 2 events in the transaction, got %d", len(tx.events))
	}
	if tx.pos != commit {
		t.Errorf("want the commit position %v, got %v", commit, tx.pos)
	}
}

func TestHandlerEmptyCommitEmitsNothing(t *testing.T) {
	h, out, cancel := newTestHandler(t, 1000)
	defer cancel()

	if err := h.OnXID(nil, mysql.Position{Name: "mysql-bin.000001", Pos: 120}); err != nil {
		t.Fatal(err)
	}
	select {
	case tx := <-out:
		t.Fatalf("emitted an empty transaction: %+v", tx)
	case <-time.After(50 * time.Millisecond):
	}
}

// Row events only carry a file-relative position, so the file name has to come
// from the rotate event that preceded them.
func TestHandlerTracksBinlogFileAcrossRotate(t *testing.T) {
	h, out, cancel := newTestHandler(t, 1000)
	defer cancel()

	if err := h.OnRotate(nil, &replication.RotateEvent{NextLogName: []byte("mysql-bin.000042")}); err != nil {
		t.Fatal(err)
	}
	if err := h.OnRow(rowsEvent(canal.InsertAction, [][]any{{int64(1), []byte("new"), "1.00"}})); err != nil {
		t.Fatal(err)
	}
	if err := h.OnXID(nil, mysql.Position{Name: "mysql-bin.000042", Pos: 777}); err != nil {
		t.Fatal(err)
	}

	tx, ok := recv(t, out)
	if !ok {
		t.Fatal("no transaction emitted")
	}
	if tx.events[0].File != "mysql-bin.000042" {
		t.Errorf("want the rotated file name, got %q", tx.events[0].File)
	}
}

// A transaction bigger than the cap must not be buffered indefinitely.
func TestHandlerFlushesOversizedTransaction(t *testing.T) {
	h, out, cancel := newTestHandler(t, 2)
	defer cancel()

	for i := 0; i < 2; i++ {
		if err := h.OnRow(rowsEvent(canal.InsertAction, [][]any{{int64(i), []byte("new"), "1.00"}})); err != nil {
			t.Fatal(err)
		}
	}

	tx, ok := recv(t, out)
	if !ok {
		t.Fatal("oversized transaction was not flushed")
	}
	if len(tx.events) != 2 {
		t.Errorf("want 2 events flushed, got %d", len(tx.events))
	}
	if tx.pos.Pos != 4242 {
		t.Errorf("want the mid-transaction row position 4242, got %d", tx.pos.Pos)
	}
}

// A blocked consumer must not wedge the handler forever once the input closes.
func TestHandlerUnblocksOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan txn) // unbuffered, nobody reading
	h := newHandler(ctx, out, "mysql-bin.000001", 1000, service.MockResources().Logger())

	if err := h.OnRow(rowsEvent(canal.InsertAction, [][]any{{int64(1), []byte("new"), "1.00"}})); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- h.OnXID(nil, mysql.Position{Name: "mysql-bin.000001", Pos: 9}) }()

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want a context error once the input is closed")
		}
	case <-time.After(time.Second):
		t.Fatal("handler stayed blocked after cancellation")
	}
}

func TestHandlerPropagatesConversionError(t *testing.T) {
	h, _, cancel := newTestHandler(t, 1000)
	defer cancel()

	err := h.OnRow(rowsEvent(canal.UpdateAction, [][]any{{int64(1), []byte("new"), "1.00"}}))
	if err == nil {
		t.Fatal("want the odd-row-count error to reach canal")
	}
}
