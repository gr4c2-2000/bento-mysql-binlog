package binlog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/warpstreamlabs/bento/public/service"
)

func newTestInput(maxRows int, maxWait time.Duration) (*Input, chan txn, *MemoryStore) {
	txns := make(chan txn, 16)
	store := &MemoryStore{}
	return &Input{
		store:   store,
		maxRows: maxRows,
		maxWait: maxWait,
		logger:  service.MockResources().Logger(),
		txns:    txns,
		errCh:   make(chan error, 1),
		offsets: NewOffsets(),
	}, txns, store
}

func makeTxn(rows int, p uint32) txn {
	events := make([]Event, rows)
	for i := range events {
		events[i] = Event{
			Schema: "shop", Table: "orders", Action: "insert",
			After: map[string]any{"id": int64(i)},
			File:  "mysql-bin.000001", Pos: p,
		}
	}
	return txn{events: events, pos: mysql.Position{Name: "mysql-bin.000001", Pos: p}}
}

// The headline behaviour: many small transactions turn into one batch instead
// of one message per row.
func TestReadBatchCoalescesTransactions(t *testing.T) {
	in, txns, _ := newTestInput(500, 5*time.Second)
	txns <- makeTxn(2, 100)
	txns <- makeTxn(3, 200)
	txns <- makeTxn(1, 300)
	close(txns)

	batch, _, err := in.ReadBatch(context.Background())
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(batch) != 6 {
		t.Fatalf("want all 6 rows in one batch, got %d", len(batch))
	}
}

// A batch is cut at the first transaction boundary at or past max_batch_rows,
// so it may overshoot slightly rather than splitting a transaction.
func TestReadBatchStopsAtMaxRowsWithoutSplittingTransactions(t *testing.T) {
	in, txns, _ := newTestInput(5, 50*time.Millisecond)
	txns <- makeTxn(4, 100)
	txns <- makeTxn(4, 200)
	txns <- makeTxn(4, 300)

	batch, _, err := in.ReadBatch(context.Background())
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(batch) != 8 {
		t.Fatalf("want 8 rows (two whole transactions), got %d", len(batch))
	}

	second, _, err := in.ReadBatch(context.Background())
	if err != nil {
		t.Fatalf("second ReadBatch: %v", err)
	}
	if len(second) != 4 {
		t.Errorf("want the remaining transaction in the next batch, got %d", len(second))
	}
}

// A trickle of traffic must not sit in the batcher forever.
func TestReadBatchFlushesOnMaxWait(t *testing.T) {
	in, txns, _ := newTestInput(500, 50*time.Millisecond)
	txns <- makeTxn(1, 100)

	start := time.Now()
	batch, _, err := in.ReadBatch(context.Background())
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("want 1 row, got %d", len(batch))
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("batch window did not fire, waited %s", elapsed)
	}
}

func TestReadBatchSetsRoutingMetadata(t *testing.T) {
	in, txns, _ := newTestInput(500, 10*time.Millisecond)
	txns <- txn{
		events: []Event{{
			Schema: "shop", Table: "orders", Action: "delete",
			Before: map[string]any{"id": int64(1)},
			File:   "mysql-bin.000003", Pos: 980,
		}},
		pos: mysql.Position{Name: "mysql-bin.000003", Pos: 980},
	}

	batch, _, err := in.ReadBatch(context.Background())
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}

	want := map[string]string{
		"db": "shop", "table": "orders", "action": "delete",
		"binlog_file": "mysql-bin.000003", "binlog_pos": "980",
	}
	for k, v := range want {
		got, ok := batch[0].MetaGet(k)
		if !ok {
			t.Errorf("missing metadata %q", k)
			continue
		}
		if got != v {
			t.Errorf("metadata %q = %q, want %q", k, got, v)
		}
	}
}

func TestAckCommitsPosition(t *testing.T) {
	in, txns, store := newTestInput(500, 10*time.Millisecond)
	txns <- makeTxn(1, 100)

	_, ack, err := in.ReadBatch(context.Background())
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if err := ack(context.Background(), nil); err != nil {
		t.Fatalf("ack: %v", err)
	}

	got, _ := store.Load()
	if got.Pos != 100 {
		t.Errorf("want the committed position 100, got %v", got)
	}
}

// A failed batch must leave the offset alone so the rows are replayed.
func TestNackDoesNotCommitPosition(t *testing.T) {
	in, txns, store := newTestInput(500, 10*time.Millisecond)
	txns <- makeTxn(1, 100)

	_, ack, err := in.ReadBatch(context.Background())
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if err := ack(context.Background(), errors.New("kafka is down")); err != nil {
		t.Fatalf("ack: %v", err)
	}

	got, _ := store.Load()
	if got.Pos != 0 {
		t.Errorf("nacked batch advanced the offset to %v", got)
	}
}

// Outputs acknowledge batches as they complete, not in read order.
func TestOutOfOrderAcksCommitOnlyTheContiguousPrefix(t *testing.T) {
	in, txns, store := newTestInput(1, 10*time.Millisecond)
	txns <- makeTxn(1, 100)
	txns <- makeTxn(1, 200)

	_, ackFirst, err := in.ReadBatch(context.Background())
	if err != nil {
		t.Fatalf("first ReadBatch: %v", err)
	}
	_, ackSecond, err := in.ReadBatch(context.Background())
	if err != nil {
		t.Fatalf("second ReadBatch: %v", err)
	}

	if err := ackSecond(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Load(); got.Pos != 0 {
		t.Fatalf("committed past an in-flight batch: %v", got)
	}

	if err := ackFirst(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Load(); got.Pos != 200 {
		t.Errorf("want the full prefix committed at 200, got %v", got)
	}
}

func TestReadBatchReportsDisconnect(t *testing.T) {
	in, txns, _ := newTestInput(500, time.Second)
	close(txns)

	if _, _, err := in.ReadBatch(context.Background()); !errors.Is(err, service.ErrNotConnected) {
		t.Fatalf("want ErrNotConnected, got %v", err)
	}
}

func TestReadBatchSurfacesReplicationError(t *testing.T) {
	in, txns, _ := newTestInput(500, time.Second)
	want := errors.New("ERROR 1236: could not find first log file name")
	in.errCh <- want
	close(txns)

	_, _, err := in.ReadBatch(context.Background())
	if err == nil || err.Error() != want.Error() {
		t.Fatalf("want the replication error, got %v", err)
	}
}

func TestReadBatchHonoursContextCancellation(t *testing.T) {
	in, _, _ := newTestInput(500, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := in.ReadBatch(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}
