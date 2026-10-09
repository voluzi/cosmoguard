package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric"

	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
)

type observedEncoder struct {
	calls *atomic.Int32
	bytes int
}

func (v observedEncoder) MarshalMsgpack() ([]byte, error) {
	v.calls.Add(1)
	return make([]byte, v.bytes), nil
}
func TestWeightedGateRejectsBeforeEncode(t *testing.T) {
	var calls atomic.Int32
	o := defaultOptions()
	BoundedOperations(128, time.Second, 1, nil, nil)(o)
	dm := &blockedCacheDMap{release: make(chan struct{})}
	c := &OlricCache[string, observedEncoder]{dm: dm, cfg: o}
	err := c.Set(t.Context(), "k", observedEncoder{&calls, 100}, time.Second)
	require.ErrorIs(t, err, boundedcall.ErrRejected)
	require.ErrorIs(t, err, ErrL2Skipped)
	require.Zero(t, calls.Load())
	require.Zero(t, dm.calls.Load())
}
func TestBoundedEncoderRejectsBeforeGrowth(t *testing.T) {
	var calls atomic.Int32
	o := defaultOptions()
	BoundedOperations(128, time.Second, 16<<20, nil, nil)(o)
	dm := &blockedCacheDMap{release: make(chan struct{})}
	c := &OlricCache[string, observedEncoder]{dm: dm, cfg: o}
	require.ErrorIs(t, c.Set(t.Context(), "k", observedEncoder{&calls, 2 << 20}, time.Second), olric.ErrEntryTooLarge)
	require.Zero(t, dm.calls.Load())
	require.Equal(t, int32(1), calls.Load())
	require.Zero(t, o.operationBytes.Snapshot().Reserved)
}
func TestWeightedGateAllNamespacesShareBudget(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	dm := &blockedCacheDMap{release: release, entered: make(chan struct{}, 1)}
	option := BoundedOperations(128, 10*time.Millisecond, (2<<20)+4096, nil, nil)
	a, b := defaultOptions(), defaultOptions()
	option(a)
	option(b)
	one := &OlricCache[string, []byte]{dm: dm, cfg: a}
	two := &OlricCache[string, []byte]{dm: dm, cfg: b}
	_, err := one.Get(t.Context(), "one")
	require.ErrorIs(t, err, boundedcall.ErrTimeout)
	_, err = two.Get(t.Context(), "two")
	require.ErrorIs(t, err, boundedcall.ErrRejected)
	require.Equal(t, int32(1), dm.calls.Load())
	reserved, capacity := option.OperationBytes()
	require.Equal(t, uint64((2<<20)+4096), reserved)
	require.Equal(t, uint64((2<<20)+4096), capacity)
}
func TestL2CapacityQuorumErrorClassification(t *testing.T) {
	for _, err := range []error{olric.ErrWriteQuorum, errors.New("opaque failure")} {
		require.Equal(t, "backend", writeSkipReason(err))
	}
}
func TestWeightedGateNoWaiterQueue(t *testing.T) {
	o := defaultOptions()
	BoundedOperations(128, time.Second, 1, nil, nil)(o)
	c := &OlricCache[string, []byte]{cfg: o}
	_, err := c.Get(context.Background(), "key")
	require.ErrorIs(t, err, boundedcall.ErrRejected)
}

func TestExpectedL2SkipClassification(t *testing.T) {
	for _, tc := range []struct {
		cause error
		want  bool
	}{
		{olric.ErrWriteQuorum, false},
		{errors.New("opaque backend failure"), false},
		{boundedcall.ErrRejected, true},
		{olric.ErrEntryTooLarge, true},
		{errEncode, true},
	} {
		err := skippedWrite{tc.cause}
		require.ErrorIs(t, err, ErrL2Skipped)
		require.ErrorIs(t, err, tc.cause)
		require.Equal(t, tc.want, IsExpectedL2Skip(err), tc.cause)
	}
	require.False(t, IsExpectedL2Skip(olric.ErrEntryTooLarge))
}
