package binlog

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
)

// Event is a single row change extracted from the binary log.
type Event struct {
	Schema string         `json:"schema"`
	Table  string         `json:"table"`
	Action string         `json:"action"`
	Before map[string]any `json:"before,omitempty"`
	After  map[string]any `json:"after,omitempty"`
	File   string         `json:"file"`
	Pos    uint32         `json:"pos"`
	TsUnix uint32         `json:"ts_unix"`
}

// AsStructured renders the event the way Bloblang and the output codecs see
// it. A Go struct set as a message payload is opaque to Bloblang - mappings
// such as `this.after.amount` would silently evaluate to null - so the payload
// has to be a plain map.
func (e Event) AsStructured() map[string]any {
	m := map[string]any{
		"schema":  e.Schema,
		"table":   e.Table,
		"action":  e.Action,
		"file":    e.File,
		"pos":     e.Pos,
		"ts_unix": e.TsUnix,
	}
	if e.Before != nil {
		m["before"] = e.Before
	}
	if e.After != nil {
		m["after"] = e.After
	}
	return m
}

// EventsFrom converts a canal rows event into one Event per changed row.
//
// Update events carry their rows as [before, after] pairs, so a single rows
// event with four rows describes two updates, not four.
func EventsFrom(e *canal.RowsEvent, file string) ([]Event, error) {
	base := Event{
		Schema: e.Table.Schema,
		Table:  e.Table.Name,
		Action: e.Action,
		File:   file,
		Pos:    e.Header.LogPos,
		TsUnix: e.Header.Timestamp,
	}

	switch e.Action {
	case canal.UpdateAction:
		if len(e.Rows)%2 != 0 {
			return nil, fmt.Errorf("update event for %s.%s has an odd row count (%d)",
				e.Table.Schema, e.Table.Name, len(e.Rows))
		}
		out := make([]Event, 0, len(e.Rows)/2)
		for i := 0; i+1 < len(e.Rows); i += 2 {
			ev := base
			ev.Before = rowToMap(e.Table, e.Rows[i])
			ev.After = rowToMap(e.Table, e.Rows[i+1])
			out = append(out, ev)
		}
		return out, nil

	case canal.DeleteAction:
		out := make([]Event, 0, len(e.Rows))
		for _, row := range e.Rows {
			ev := base
			ev.Before = rowToMap(e.Table, row)
			out = append(out, ev)
		}
		return out, nil

	case canal.InsertAction:
		out := make([]Event, 0, len(e.Rows))
		for _, row := range e.Rows {
			ev := base
			ev.After = rowToMap(e.Table, row)
			out = append(out, ev)
		}
		return out, nil

	default:
		return nil, fmt.Errorf("unknown row action %q", e.Action)
	}
}

// rowToMap pairs positional binlog values with their column names. Rows with
// more or fewer values than the cached schema has columns are tolerated: MySQL
// keeps serving the old row format for a while after an ALTER.
func rowToMap(t *schema.Table, row []any) map[string]any {
	m := make(map[string]any, len(row))
	for i, col := range t.Columns {
		if i >= len(row) {
			break
		}
		m[col.Name] = normalise(row[i])
	}
	return m
}

// normalise turns binlog values into something that survives JSON encoding.
// Strings, blobs, decimals and JSON columns all arrive as []byte, which would
// otherwise be base64 encoded.
func normalise(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}
