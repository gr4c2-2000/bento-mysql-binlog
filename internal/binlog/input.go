// Package binlog implements a Bento input that streams MySQL row changes from
// the binary log.
package binlog

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	startSaved   = "saved"
	startCurrent = "current"
	startEarliest = "earliest"
)

// Input reads row changes from a MySQL binary log and emits them in batches.
type Input struct {
	canalCfg  *canal.Config
	store     PositionStore
	startFrom string
	maxRows   int
	maxWait   time.Duration
	maxTxRows int
	logger    *service.Logger

	c       *canal.Canal
	txns    chan txn
	errCh   chan error
	cancel  context.CancelFunc
	offsets *Offsets
}

// Connect opens the replication stream, resuming from the stored position when
// there is one.
func (in *Input) Connect(ctx context.Context) error {
	start, err := in.startPosition()
	if err != nil {
		return err
	}

	c, err := canal.NewCanal(in.canalCfg)
	if err != nil {
		return fmt.Errorf("creating canal: %w", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	in.c = c
	in.cancel = cancel
	in.txns = make(chan txn)
	in.errCh = make(chan error, 1)
	in.offsets = NewOffsets()

	c.SetEventHandler(newHandler(runCtx, in.txns, start.Name, in.maxTxRows, in.logger))

	go func() {
		defer close(in.txns)

		var runErr error
		switch {
		case start.Name != "":
			in.logger.Infof("resuming replication from %s:%d", start.Name, start.Pos)
			runErr = c.RunFrom(start)
		case in.startFrom == startEarliest:
			in.logger.Info("starting replication from the oldest retained binlog")
			runErr = c.RunFrom(mysql.Position{Name: "", Pos: 4})
		default:
			in.logger.Info("starting replication from the current master position")
			runErr = c.Run()
		}

		if runErr != nil && runCtx.Err() == nil {
			select {
			case in.errCh <- runErr:
			default:
			}
		}
	}()

	return nil
}

func (in *Input) startPosition() (mysql.Position, error) {
	if in.startFrom != startSaved {
		return mysql.Position{}, nil
	}
	pos, err := in.store.Load()
	if err != nil {
		return mysql.Position{}, fmt.Errorf("loading stored position: %w", err)
	}
	return pos, nil
}

// ReadBatch groups whole transactions into one batch, bounded by
// max_batch_rows and max_batch_wait. Transactions are never split across
// batches, so a batch always ends on a commit boundary.
func (in *Input) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	first, err := in.next(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	batch := make(service.MessageBatch, 0, len(first.events))
	batch = appendEvents(batch, first.events)
	last := first.pos

	timer := time.NewTimer(in.maxWait)
	defer timer.Stop()

collect:
	for len(batch) < in.maxRows {
		t, err := in.next(ctx, timer.C)
		switch {
		case err == errBatchFull:
			break collect
		case err != nil:
			// The stream ended or failed after we already have rows. Deliver
			// what we have; the next call surfaces the error.
			break collect
		}
		batch = appendEvents(batch, t.events)
		last = t.pos
	}

	seq := in.offsets.Track(last)
	ack := func(_ context.Context, err error) error {
		if err != nil {
			// Nacked: leave the watermark where it is so the batch is replayed.
			return nil
		}
		if pos, moved := in.offsets.Ack(seq); moved {
			return in.store.Save(pos)
		}
		return nil
	}

	return batch, ack, nil
}

var errBatchFull = fmt.Errorf("batch window elapsed")

// next waits for the next transaction. When deadline is non-nil it also
// returns errBatchFull once the batching window elapses.
func (in *Input) next(ctx context.Context, deadline <-chan time.Time) (txn, error) {
	select {
	case t, ok := <-in.txns:
		if !ok {
			select {
			case err := <-in.errCh:
				return txn{}, err
			default:
			}
			return txn{}, service.ErrNotConnected
		}
		return t, nil
	case err := <-in.errCh:
		return txn{}, err
	case <-deadline:
		return txn{}, errBatchFull
	case <-ctx.Done():
		return txn{}, ctx.Err()
	}
}

func appendEvents(batch service.MessageBatch, events []Event) service.MessageBatch {
	for _, ev := range events {
		msg := service.NewMessage(nil)
		msg.SetStructured(ev)
		msg.MetaSetMut("db", ev.Schema)
		msg.MetaSetMut("table", ev.Table)
		msg.MetaSetMut("action", ev.Action)
		msg.MetaSetMut("binlog_file", ev.File)
		msg.MetaSetMut("binlog_pos", strconv.FormatUint(uint64(ev.Pos), 10))
		batch = append(batch, msg)
	}
	return batch
}

// Close stops replication. Any batch still in flight is simply not committed,
// so the next start resumes from the last acknowledged position.
func (in *Input) Close(ctx context.Context) error {
	if in.cancel != nil {
		in.cancel()
	}
	if in.c != nil {
		in.c.Close()
	}
	return nil
}
