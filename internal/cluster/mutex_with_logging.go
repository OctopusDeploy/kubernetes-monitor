package cluster

import (
	"log/slog"
	"sync"
)

type MutexWithLogging struct {
	mutex  sync.Mutex
	logger *slog.Logger
}

func NewMutexWithLogging(
	logger *slog.Logger, component string,
) MutexWithLogging {
	return MutexWithLogging{
		logger: logger.With(slog.Any("component", component)),
	}
}

func (a *MutexWithLogging) Lock(caller string) {
	a.logger.With(slog.String("caller", caller)).Debug("lock req")
	a.mutex.Lock()
	a.logger.With(slog.String("caller", caller)).Debug("lock taken")
}

func (a *MutexWithLogging) Unlock(caller string) {
	a.logger.With(slog.String("caller", caller)).Debug("unlock req")
	a.mutex.Unlock()
	a.logger.With(slog.String("caller", caller)).Debug("unlock complete")
}
