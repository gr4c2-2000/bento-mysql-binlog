package binlog

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func pos(p uint32) mysql.Position {
	return mysql.Position{Name: "mysql-bin.000001", Pos: p}
}

func TestOffsetsInOrderAcksAdvance(t *testing.T) {
	o := NewOffsets()
	a, b := o.Track(pos(100)), o.Track(pos(200))

	if got, moved := o.Ack(a); !moved || got.Pos != 100 {
		t.Fatalf("first ack: got %v moved=%v, want 100", got, moved)
	}
	if got, moved := o.Ack(b); !moved || got.Pos != 200 {
		t.Fatalf("second ack: got %v moved=%v, want 200", got, moved)
	}
	if o.Pending() != 0 {
		t.Errorf("want nothing pending, got %d", o.Pending())
	}
}

// The whole point of the tracker: a later batch acking first must not commit
// past a batch that is still in flight, or a crash would skip those rows.
func TestOffsetsOutOfOrderAckHoldsWatermark(t *testing.T) {
	o := NewOffsets()
	a, b, c := o.Track(pos(100)), o.Track(pos(200)), o.Track(pos(300))

	if _, moved := o.Ack(c); moved {
		t.Fatal("watermark moved while the two earlier batches were in flight")
	}
	if _, moved := o.Ack(b); moved {
		t.Fatal("watermark moved while the first batch was in flight")
	}

	got, moved := o.Ack(a)
	if !moved {
		t.Fatal("watermark did not move once the gap was filled")
	}
	if got.Pos != 300 {
		t.Errorf("want the whole contiguous prefix committed at 300, got %d", got.Pos)
	}
	if o.Pending() != 0 {
		t.Errorf("want nothing pending, got %d", o.Pending())
	}
}

func TestOffsetsRepeatedAckDoesNotMoveTwice(t *testing.T) {
	o := NewOffsets()
	a := o.Track(pos(100))
	o.Track(pos(200))

	if _, moved := o.Ack(a); !moved {
		t.Fatal("first ack should move the watermark")
	}
	if _, moved := o.Ack(a); moved {
		t.Fatal("re-acking a retired batch moved the watermark")
	}
}

func TestOffsetsUnknownSeqIsIgnored(t *testing.T) {
	o := NewOffsets()
	o.Track(pos(100))

	if _, moved := o.Ack(9999); moved {
		t.Fatal("unknown sequence moved the watermark")
	}
	if o.Pending() != 1 {
		t.Errorf("want 1 pending, got %d", o.Pending())
	}
}

func TestOffsetsNoAckMeansNoCommit(t *testing.T) {
	o := NewOffsets()
	o.Track(pos(100))
	if o.Pending() != 1 {
		t.Fatalf("want 1 pending, got %d", o.Pending())
	}
}
