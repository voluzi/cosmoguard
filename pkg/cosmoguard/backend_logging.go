package cosmoguard

import (
	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
	"github.com/voluzi/cosmoguard/v6/pkg/cache"
)

func logCacheBackendError(logger *Entry, err error, message string) {
	if boundedcall.IsFailure(err) || cache.IsExpectedL2Skip(err) {
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
