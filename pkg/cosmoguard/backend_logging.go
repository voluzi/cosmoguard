package cosmoguard

import "github.com/voluzi/cosmoguard/v5/internal/boundedcall"

func logCacheBackendError(logger *Entry, err error, message string) {
	if boundedcall.IsFailure(err) {
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
