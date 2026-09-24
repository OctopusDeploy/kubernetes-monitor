package logger

import (
	"log/slog"

	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"
)

func AddCorrelationId(logger *slog.Logger, correlationId string) *slog.Logger {
	return logger.With(slog.String("CorrelationId", correlationId))
}

func New(debug bool) *slog.Logger {
	var zapL *zap.Logger
	if debug {
		zapL = zap.Must(zap.NewDevelopment())
	} else {
		zapL = zap.Must(zap.NewProduction())
	}

	defer func() {
		_ = zapL.Sync()
	}()

	return slog.New(zapslog.NewHandler(zapL.Core()))
}
