package binlog

import (
	"context"
	"log/slog"
	"strings"

	"github.com/warpstreamlabs/bento/public/service"
)

// slogFor adapts canal's *slog.Logger to Bento's logger so that replication
// messages honour the pipeline's log level and format instead of going
// straight to stderr.
func slogFor(l *service.Logger) *slog.Logger {
	return slog.New(&bentoHandler{logger: l})
}

type bentoHandler struct {
	logger *service.Logger
	attrs  []slog.Attr
}

func (h *bentoHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *bentoHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Message)

	write := func(a slog.Attr) bool {
		b.WriteString(" ")
		b.WriteString(a.Key)
		b.WriteString("=")
		b.WriteString(a.Value.String())
		return true
	}
	for _, a := range h.attrs {
		write(a)
	}
	r.Attrs(write)

	msg := b.String()
	switch {
	case r.Level >= slog.LevelError:
		h.logger.Error(msg)
	case r.Level >= slog.LevelWarn:
		h.logger.Warn(msg)
	case r.Level >= slog.LevelInfo:
		h.logger.Info(msg)
	default:
		h.logger.Debug(msg)
	}
	return nil
}

func (h *bentoHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	merged := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	merged = append(merged, h.attrs...)
	merged = append(merged, attrs...)
	return &bentoHandler{logger: h.logger, attrs: merged}
}

func (h *bentoHandler) WithGroup(string) slog.Handler { return h }
