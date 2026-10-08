package olricstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/voluzi/olric/pkg/storage"
)

func TestEngineExpiryNeverReusesAnotherKeysBytes(t *testing.T) {
	_, e := testEngine(t, 8<<20, Response)
	ctx, cancel := context.WithCancel(t.Context())
	var sweep sync.WaitGroup
	sweep.Go(func() {
		for ctx.Err() == nil {
			_, _ = e.Compaction()
		}
	})
	defer func() { cancel(); sweep.Wait() }()
	var writers sync.WaitGroup
	for worker := range 8 {
		writers.Go(func() {
			for i := range 1000 {
				h := uint64(worker*32 + i%32)
				key := fmt.Sprintf("key/%08d", h)
				value := bytes.Repeat([]byte(key), 16)
				v := NewEntry()
				v.SetKey(key)
				v.SetValue(value)
				v.SetTTL(time.Now().Add(time.Duration(i%3-1) * time.Millisecond).UnixMilli())
				var err error
				if i%2 == 0 {
					err = e.Put(h, v)
				} else {
					err = e.PutRaw(h, v.Encode())
				}
				if err != nil {
					t.Error(err)
					return
				}
				v.SetTTL(time.Now().Add(time.Millisecond).UnixMilli())
				if err := e.UpdateTTL(h, v); err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
					t.Error(err)
					return
				}
				got, err := e.Get(h)
				if err == nil && (got.Key() != key || !bytes.Equal(got.Value(), value)) {
					t.Errorf("cross-key bytes for %s: %q", key, got.Value())
					return
				}
				if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
					t.Error(err)
					return
				}
				if err := e.Delete(h); err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
					t.Error(err)
					return
				}
			}
		})
	}
	writers.Wait()
}
