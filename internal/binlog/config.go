package binlog

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/warpstreamlabs/bento/public/service"
)

const inputName = "mysql_binlog"

// ConfigSpec describes the mysql_binlog input.
func ConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Streams row changes from a MySQL binary log.").
		Description(`
Connects to MySQL as a replica and emits one message per changed row.

Messages are grouped into batches of whole transactions: a transaction is never
split across two batches, and the binlog position is only committed once every
message of a batch has been delivered to all outputs. Delivery is at-least-once,
so sinks must tolerate duplicates after a restart.

Each message is a JSON object with `+"`schema`, `table`, `action`, `before`, `after`, `file`, `pos` and `ts_unix`"+` fields,
and carries `+"`db`, `table`, `action`, `binlog_file` and `binlog_pos`"+` metadata for cheap routing.

The server must run with `+"`binlog_format=ROW`"+` and, for complete `+"`before`"+` images,
`+"`binlog_row_image=FULL`"+`. The user needs REPLICATION SLAVE and REPLICATION CLIENT.`).
		Field(service.NewStringField("addr").
			Description("Host and port of the MySQL server.").
			Default("127.0.0.1:3306")).
		Field(service.NewStringField("user").
			Description("User with REPLICATION SLAVE and REPLICATION CLIENT grants.")).
		Field(service.NewStringField("password").
			Description("Password for the replication user.").
			Secret().Default("")).
		Field(service.NewStringEnumField("flavor", "mysql", "mariadb").
			Description("Server dialect.").
			Default("mysql")).
		Field(service.NewStringField("charset").
			Description("Connection charset.").
			Default("utf8mb4").Advanced()).
		Field(service.NewIntField("server_id").
			Description("Replica server id. Must be unique across the whole replication topology.").
			Default(1001)).
		Field(service.NewStringListField("include_tables").
			Description("Regular expressions, including the schema name, of tables to replicate. Empty means every table.").
			Example([]string{`shop\..*`}).
			Default([]any{})).
		Field(service.NewStringListField("exclude_tables").
			Description("Regular expressions of tables to skip, applied after include_tables.").
			Default([]any{})).
		Field(service.NewStringEnumField("start_from", startSaved, startCurrent, startEarliest).
			Description("Where to begin when no position has been committed yet. `saved` resumes from position_file and falls back to `current`.").
			Default(startSaved)).
		Field(service.NewStringField("position_file").
			Description("File the last fully delivered binlog position is written to.").
			Default("./binlog.pos")).
		Field(service.NewIntField("max_batch_rows").
			Description("Maximum rows per batch. A batch is cut at the first transaction boundary at or past this count.").
			Default(500)).
		Field(service.NewDurationField("max_batch_wait").
			Description("How long to wait for more transactions before emitting a partial batch.").
			Default("200ms")).
		Field(service.NewIntField("max_transaction_rows").
			Description("Safety valve: a transaction larger than this is flushed early at a mid-transaction position.").
			Default(10000).Advanced()).
		Example(
			"Fan out to Kafka and RabbitMQ",
			"Each output filters the shared stream down to the rows it cares about.",
			`
input:
  mysql_binlog:
    addr: mysql:3306
    user: repl
    password: "${MYSQL_REPL_PASSWORD}"
    server_id: 1001
    include_tables: [ 'shop\..*' ]
    position_file: /var/lib/bento/binlog.pos

output:
  broker:
    pattern: fan_out
    outputs:
      - processors:
          - mapping: root = if meta("table") != "orders" { deleted() }
        kafka_franz:
          seed_brokers: [ kafka:9092 ]
          topic: shop.orders
      - processors:
          - mapping: root = if meta("action") != "delete" { deleted() }
        amqp_0_9:
          urls: [ amqp://rabbitmq:5672/ ]
          exchange: shop.deletes
`)
}

// FromConfig builds an Input from parsed plugin configuration.
func FromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*Input, error) {
	cfg := canal.NewDefaultConfig()

	strField := func(name string, dst *string) error {
		v, err := conf.FieldString(name)
		if err != nil {
			return err
		}
		*dst = v
		return nil
	}

	for name, dst := range map[string]*string{
		"addr":     &cfg.Addr,
		"user":     &cfg.User,
		"password": &cfg.Password,
		"flavor":   &cfg.Flavor,
		"charset":  &cfg.Charset,
	} {
		if err := strField(name, dst); err != nil {
			return nil, err
		}
	}

	serverID, err := conf.FieldInt("server_id")
	if err != nil {
		return nil, err
	}
	if serverID <= 0 {
		return nil, fmt.Errorf("server_id must be positive, got %d", serverID)
	}
	cfg.ServerID = uint32(serverID)

	if cfg.IncludeTableRegex, err = conf.FieldStringList("include_tables"); err != nil {
		return nil, err
	}
	if cfg.ExcludeTableRegex, err = conf.FieldStringList("exclude_tables"); err != nil {
		return nil, err
	}
	if len(cfg.IncludeTableRegex) == 0 {
		cfg.IncludeTableRegex = nil
	}
	if len(cfg.ExcludeTableRegex) == 0 {
		cfg.ExcludeTableRegex = nil
	}

	// No mysqldump snapshot: this input is a tail of the binlog, nothing else.
	cfg.Dump.ExecutionPath = ""

	startFrom, err := conf.FieldString("start_from")
	if err != nil {
		return nil, err
	}

	positionFile, err := conf.FieldString("position_file")
	if err != nil {
		return nil, err
	}
	store, err := NewFileStore(positionFile)
	if err != nil {
		return nil, fmt.Errorf("preparing position file: %w", err)
	}

	maxRows, err := conf.FieldInt("max_batch_rows")
	if err != nil {
		return nil, err
	}
	if maxRows <= 0 {
		return nil, fmt.Errorf("max_batch_rows must be positive, got %d", maxRows)
	}

	maxWait, err := conf.FieldDuration("max_batch_wait")
	if err != nil {
		return nil, err
	}

	maxTxRows, err := conf.FieldInt("max_transaction_rows")
	if err != nil {
		return nil, err
	}
	if maxTxRows < maxRows {
		return nil, fmt.Errorf("max_transaction_rows (%d) must be at least max_batch_rows (%d)", maxTxRows, maxRows)
	}

	logger := mgr.Logger()
	cfg.Logger = slogFor(logger)

	return &Input{
		canalCfg:  cfg,
		store:     store,
		startFrom: startFrom,
		maxRows:   maxRows,
		maxWait:   maxWait,
		maxTxRows: maxTxRows,
		logger:    logger,
	}, nil
}

// Register adds the input to Bento's global plugin environment.
func Register() error {
	return service.RegisterBatchInput(inputName, ConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			in, err := FromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatched(in), nil
		})
}
