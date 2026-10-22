package binlog

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

func testTable(cols ...string) *schema.Table {
	t := &schema.Table{Schema: "shop", Name: "orders"}
	for _, c := range cols {
		t.Columns = append(t.Columns, schema.TableColumn{Name: c})
	}
	return t
}

func rowsEvent(action string, rows [][]any) *canal.RowsEvent {
	return &canal.RowsEvent{
		Table:  testTable("id", "status", "amount"),
		Action: action,
		Rows:   rows,
		Header: &replication.EventHeader{LogPos: 4242, Timestamp: 1700000000},
	}
}

func TestEventsFromInsert(t *testing.T) {
	got, err := EventsFrom(rowsEvent(canal.InsertAction, [][]any{
		{int64(1), []byte("new"), "10.50"},
		{int64(2), []byte("new"), "99.00"},
	}), "mysql-bin.000007")
	if err != nil {
		t.Fatalf("EventsFrom: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("want 2 events, got %d", len(got))
	}
	first := got[0]
	if first.Schema != "shop" || first.Table != "orders" || first.Action != "insert" {
		t.Errorf("wrong identity: %+v", first)
	}
	if first.Before != nil {
		t.Errorf("insert must not carry a before image, got %v", first.Before)
	}
	if first.After["id"] != int64(1) || first.After["status"] != "new" || first.After["amount"] != "10.50" {
		t.Errorf("wrong after image: %#v", first.After)
	}
	if first.File != "mysql-bin.000007" || first.Pos != 4242 || first.TsUnix != 1700000000 {
		t.Errorf("wrong coordinates: %+v", first)
	}
}

// An update rows event carries [before, after] pairs: four rows describe two
// updates. Treating them as four separate rows would double every event.
func TestEventsFromUpdatePairsRows(t *testing.T) {
	got, err := EventsFrom(rowsEvent(canal.UpdateAction, [][]any{
		{int64(1), []byte("new"), "10.50"},
		{int64(1), []byte("paid"), "10.50"},
		{int64(2), []byte("new"), "99.00"},
		{int64(2), []byte("paid"), "99.00"},
	}), "mysql-bin.000001")
	if err != nil {
		t.Fatalf("EventsFrom: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("want 2 update events from 4 rows, got %d", len(got))
	}
	if got[0].Before["status"] != "new" || got[0].After["status"] != "paid" {
		t.Errorf("before/after not paired up: %#v -> %#v", got[0].Before, got[0].After)
	}
	if got[1].Before["id"] != int64(2) {
		t.Errorf("second pair lost: %#v", got[1].Before)
	}
}

func TestEventsFromUpdateOddRowsFails(t *testing.T) {
	_, err := EventsFrom(rowsEvent(canal.UpdateAction, [][]any{
		{int64(1), []byte("new"), "10.50"},
	}), "mysql-bin.000001")
	if err == nil {
		t.Fatal("want an error for an odd number of update rows")
	}
	if !strings.Contains(err.Error(), "odd row count") {
		t.Errorf("unhelpful error: %v", err)
	}
}

func TestEventsFromDelete(t *testing.T) {
	got, err := EventsFrom(rowsEvent(canal.DeleteAction, [][]any{
		{int64(7), []byte("paid"), "1.00"},
	}), "mysql-bin.000001")
	if err != nil {
		t.Fatalf("EventsFrom: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 event, got %d", len(got))
	}
	if got[0].After != nil {
		t.Errorf("delete must not carry an after image, got %v", got[0].After)
	}
	if got[0].Before["id"] != int64(7) {
		t.Errorf("wrong before image: %#v", got[0].Before)
	}
}

func TestEventsFromUnknownAction(t *testing.T) {
	if _, err := EventsFrom(rowsEvent("truncate", [][]any{{int64(1)}}), "f"); err == nil {
		t.Fatal("want an error for an unknown action")
	}
}

// MySQL keeps sending the old row format for a moment after an ALTER, so a row
// may be shorter or longer than the cached column list.
func TestRowToMapToleratesColumnCountDrift(t *testing.T) {
	short := rowToMap(testTable("id", "status", "amount"), []any{int64(1)})
	if len(short) != 1 || short["id"] != int64(1) {
		t.Errorf("short row: %#v", short)
	}

	long := rowToMap(testTable("id"), []any{int64(1), []byte("extra")})
	if len(long) != 1 {
		t.Errorf("extra values must be dropped, got %#v", long)
	}
}

// Strings, blobs and decimals arrive as []byte and would be base64 encoded.
func TestEventJSONKeepsTextReadable(t *testing.T) {
	got, err := EventsFrom(rowsEvent(canal.InsertAction, [][]any{
		{int64(1), []byte("paid"), nil},
	}), "mysql-bin.000001")
	if err != nil {
		t.Fatalf("EventsFrom: %v", err)
	}

	raw, err := json.Marshal(got[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(raw), `"status":"paid"`) {
		t.Errorf("text column not readable in JSON: %s", raw)
	}
	if !strings.Contains(string(raw), `"amount":null`) {
		t.Errorf("NULL column lost: %s", raw)
	}
	if strings.Contains(string(raw), `"before"`) {
		t.Errorf("empty before image should be omitted: %s", raw)
	}
}
