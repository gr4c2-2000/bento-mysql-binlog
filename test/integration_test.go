//go:build integration

// Package integration exercises the input against the MySQL instance from
// deploy/docker-compose.yml. Run it with:
//
//	docker compose -f deploy/docker-compose.yml up -d --wait mysql
//	go test -tags integration ./test/...
package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/warpstreamlabs/bento/public/service"

	"github.com/gr4c2-2000/bento-mysql-binlog/internal/binlog"
)

const (
	mysqlAddr = "127.0.0.1:13306"
	mysqlUser = "root"
	mysqlPass = "rootpass"
)

func mustConn(t *testing.T) *client.Conn {
	t.Helper()
	conn, err := client.Connect(mysqlAddr, mysqlUser, mysqlPass, "shop")
	if err != nil {
		t.Skipf("MySQL not reachable at %s (%v); start it with `make up`", mysqlAddr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func exec(t *testing.T, conn *client.Conn, query string, args ...any) {
	t.Helper()
	if _, err := conn.Execute(fmt.Sprintf(query, args...)); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

func newInput(t *testing.T, table string) *binlog.Input {
	t.Helper()
	yaml := fmt.Sprintf(`
addr: %s
user: %s
password: %s
server_id: 2001
include_tables: [ 'shop\.%s' ]
start_from: current
position_file: %s
max_batch_rows: 100
max_batch_wait: 300ms
`, mysqlAddr, mysqlUser, mysqlPass, table, filepath.Join(t.TempDir(), "binlog.pos"))

	conf, err := binlog.ConfigSpec().ParseYAML(yaml, nil)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	in, err := binlog.FromConfig(conf, service.MockResources())
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	return in
}

// readAtLeast drains batches until it has n messages or the deadline passes.
func readAtLeast(t *testing.T, in *binlog.Input, n int) ([]*service.Message, service.AckFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		got     []*service.Message
		lastAck service.AckFunc
	)
	for len(got) < n {
		batch, ack, err := in.ReadBatch(ctx)
		if err != nil {
			t.Fatalf("ReadBatch after %d messages: %v", len(got), err)
		}
		got = append(got, batch...)
		lastAck = ack
	}
	return got, lastAck
}

func field(t *testing.T, msg *service.Message, path ...string) any {
	t.Helper()
	v, err := msg.AsStructured()
	if err != nil {
		t.Fatalf("AsStructured: %v", err)
	}
	cur := v
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			t.Fatalf("%v is not an object", path)
		}
		cur = m[p]
	}
	return cur
}

func TestInputStreamsRealBinlog(t *testing.T) {
	conn := mustConn(t)
	exec(t, conn, "CREATE TABLE IF NOT EXISTS shop.it_basic (id BIGINT PRIMARY KEY, status VARCHAR(32)) ENGINE=InnoDB")
	exec(t, conn, "TRUNCATE TABLE shop.it_basic")

	in := newInput(t, "it_basic")
	if err := in.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer in.Close(context.Background())

	// Give the replication stream a moment to attach before writing.
	time.Sleep(2 * time.Second)

	exec(t, conn, "INSERT INTO shop.it_basic VALUES (1,'new'),(2,'new')")
	exec(t, conn, "UPDATE shop.it_basic SET status='paid' WHERE id=1")
	exec(t, conn, "DELETE FROM shop.it_basic WHERE id=2")

	msgs, _ := readAtLeast(t, in, 4)

	wantActions := []string{"insert", "insert", "update", "delete"}
	for i, want := range wantActions {
		got, _ := msgs[i].MetaGet("action")
		if got != want {
			t.Errorf("message %d: action = %q, want %q", i, got, want)
		}
	}

	if got := field(t, msgs[2], "before", "status"); got != "new" {
		t.Errorf("update before image = %v, want \"new\"", got)
	}
	if got := field(t, msgs[2], "after", "status"); got != "paid" {
		t.Errorf("update after image = %v, want \"paid\"", got)
	}
	if got := field(t, msgs[3], "before", "id"); got != int64(2) {
		t.Errorf("delete before image id = %v, want 2", got)
	}
}

// Rows from tables outside include_tables must never reach the pipeline.
func TestInputHonoursTableFilter(t *testing.T) {
	conn := mustConn(t)
	exec(t, conn, "CREATE TABLE IF NOT EXISTS shop.it_wanted (id BIGINT PRIMARY KEY) ENGINE=InnoDB")
	exec(t, conn, "CREATE TABLE IF NOT EXISTS shop.it_ignored (id BIGINT PRIMARY KEY) ENGINE=InnoDB")
	exec(t, conn, "TRUNCATE TABLE shop.it_wanted")
	exec(t, conn, "TRUNCATE TABLE shop.it_ignored")

	in := newInput(t, "it_wanted")
	if err := in.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer in.Close(context.Background())
	time.Sleep(2 * time.Second)

	exec(t, conn, "INSERT INTO shop.it_ignored VALUES (1)")
	exec(t, conn, "INSERT INTO shop.it_wanted VALUES (1)")

	msgs, _ := readAtLeast(t, in, 1)
	for _, m := range msgs {
		if tbl, _ := m.MetaGet("table"); tbl != "it_wanted" {
			t.Errorf("filtered table leaked through: %q", tbl)
		}
	}
}

// A multi-row transaction arrives as one batch, and the position is only
// committed after the batch is acknowledged.
func TestInputBatchesTransactionAndCommitsOnAck(t *testing.T) {
	conn := mustConn(t)
	exec(t, conn, "CREATE TABLE IF NOT EXISTS shop.it_batch (id BIGINT PRIMARY KEY) ENGINE=InnoDB")
	exec(t, conn, "TRUNCATE TABLE shop.it_batch")

	posFile := filepath.Join(t.TempDir(), "binlog.pos")
	conf, err := binlog.ConfigSpec().ParseYAML(fmt.Sprintf(`
addr: %s
user: %s
password: %s
server_id: 2002
include_tables: [ 'shop\.it_batch' ]
start_from: current
position_file: %s
max_batch_rows: 100
max_batch_wait: 300ms
`, mysqlAddr, mysqlUser, mysqlPass, posFile), nil)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	in, err := binlog.FromConfig(conf, service.MockResources())
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	if err := in.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer in.Close(context.Background())
	time.Sleep(2 * time.Second)

	exec(t, conn, "INSERT INTO shop.it_batch VALUES (1),(2),(3),(4),(5)")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	batch, ack, err := in.ReadBatch(ctx)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(batch) != 5 {
		t.Fatalf("want the 5-row transaction in one batch, got %d", len(batch))
	}

	store, err := binlog.NewFileStore(posFile)
	if err != nil {
		t.Fatal(err)
	}
	if pos, _ := store.Load(); pos.Pos != 0 {
		t.Errorf("position committed before the batch was acked: %v", pos)
	}

	if err := ack(context.Background(), nil); err != nil {
		t.Fatalf("ack: %v", err)
	}
	pos, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if pos.Name == "" || pos.Pos == 0 {
		t.Errorf("position not committed after ack: %v", pos)
	}
}
