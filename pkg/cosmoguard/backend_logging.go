package cosmoguard

import (
	"errors"
	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
	"github.com/voluzi/cosmoguard/v6/pkg/cache"
)

func logCacheBackendError(logger *Entry, err error, message string) {
	if boundedcall.IsFailure(err) || errors.Is(err, cache.ErrL2Skipped) {
		logger.Debugf("%s: %v", message, err)
	} else {
		logger.Errorf("%s: %v", message, err)
	}
}

func logLimiterBackendError(logger *Entry, err error, message string) {
	entry := logger.WithError(err)
	if boundedcall.IsFailure(err) {
		entry.Debug(message)
	} else {
		entry.Warn(message)
	}
}
