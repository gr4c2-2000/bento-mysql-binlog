package binlog

import (
	"strings"
	"testing"

	"github.com/warpstreamlabs/bento/public/service"

	// Registering the plugin into a real stream needs the standard components.
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func parse(t *testing.T, yaml string) (*Input, error) {
	t.Helper()
	conf, err := ConfigSpec().ParseYAML(yaml, nil)
	if err != nil {
		return nil, err
	}
	return FromConfig(conf, service.MockResources())
}

func TestConfigDefaults(t *testing.T) {
	in, err := parse(t, `
user: repl
`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if in.canalCfg.Addr != "127.0.0.1:3306" {
		t.Errorf("addr = %q", in.canalCfg.Addr)
	}
	if in.canalCfg.ServerID != 1001 {
		t.Errorf("server_id = %d", in.canalCfg.ServerID)
	}
	if in.maxRows != 500 || in.maxWait.String() != "200ms" {
		t.Errorf("batching defaults: rows=%d wait=%s", in.maxRows, in.maxWait)
	}
	if in.startFrom != startSaved {
		t.Errorf("start_from = %q", in.startFrom)
	}
	// A mysqldump snapshot would be a second, very different source of truth.
	if in.canalCfg.Dump.ExecutionPath != "" {
		t.Errorf("mysqldump snapshot left enabled: %q", in.canalCfg.Dump.ExecutionPath)
	}
}

func TestConfigFullyPopulated(t *testing.T) {
	in, err := parse(t, `
addr: mysql:3306
user: repl
password: secret
flavor: mariadb
server_id: 4242
include_tables: [ 'shop\..*' ]
exclude_tables: [ 'shop\.audit_.*' ]
start_from: current
position_file: /tmp/does-not-matter/binlog.pos
max_batch_rows: 50
max_batch_wait: 1s
max_transaction_rows: 100
`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if in.canalCfg.Flavor != "mariadb" || in.canalCfg.ServerID != 4242 {
		t.Errorf("flavor/server_id: %q %d", in.canalCfg.Flavor, in.canalCfg.ServerID)
	}
	if len(in.canalCfg.IncludeTableRegex) != 1 || in.canalCfg.IncludeTableRegex[0] != `shop\..*` {
		t.Errorf("include_tables = %v", in.canalCfg.IncludeTableRegex)
	}
	if len(in.canalCfg.ExcludeTableRegex) != 1 {
		t.Errorf("exclude_tables = %v", in.canalCfg.ExcludeTableRegex)
	}
	if in.startFrom != startCurrent || in.maxRows != 50 || in.maxTxRows != 100 {
		t.Errorf("unexpected: start=%q rows=%d tx=%d", in.startFrom, in.maxRows, in.maxTxRows)
	}
}

// Empty lists must stay nil: canal treats an empty include list as "everything"
// but a list containing an empty string as a regex that matches nothing useful.
func TestConfigEmptyTableListsBecomeNil(t *testing.T) {
	in, err := parse(t, "user: repl\n")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if in.canalCfg.IncludeTableRegex != nil || in.canalCfg.ExcludeTableRegex != nil {
		t.Errorf("want nil regex lists, got %v / %v",
			in.canalCfg.IncludeTableRegex, in.canalCfg.ExcludeTableRegex)
	}
}

func TestConfigRejectsBadValues(t *testing.T) {
	cases := map[string]struct{ yaml, want string }{
		"zero server_id": {"user: repl\nserver_id: 0\n", "server_id must be positive"},
		"zero batch":     {"user: repl\nmax_batch_rows: 0\n", "max_batch_rows must be positive"},
		"tx cap below batch size": {
			"user: repl\nmax_batch_rows: 500\nmax_transaction_rows: 10\n",
			"must be at least max_batch_rows",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parse(t, tc.yaml)
			if err == nil {
				t.Fatalf("want an error mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// The input has to satisfy Bento's BatchInput contract, not just compile.
func TestRegisterInstallsPlugin(t *testing.T) {
	if err := Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}

	builder := service.NewStreamBuilder()
	if err := builder.SetYAML(`
input:
  mysql_binlog:
    addr: 127.0.0.1:3306
    user: repl
    include_tables: [ 'shop\..*' ]
output:
  drop: {}
`); err != nil {
		t.Fatalf("stream config rejected: %v", err)
	}
	if _, err := builder.Build(); err != nil {
		t.Fatalf("stream build failed: %v", err)
	}
}

var _ service.BatchInput = (*Input)(nil)
