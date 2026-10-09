package olricstore

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestCompactionTracksEarlierDeadlines(t *testing.T) {
	for _, mutation := range []string{"put", "raw", "ttl"} {
		t.Run(mutation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := NewPool(8<<20, Response, nil)
				defer p.Close(context.Background())
				e := NewEngine(p)
				live := NewEntry()
				live.SetKey("live")
				live.SetValue([]byte("preserved"))
				live.SetTTL(time.Now().Add(time.Hour).UnixMilli())
				iferr := e.Put(1, live)
				if iferr != nil {
					t.Fatal(iferr)
				}
				short := NewEntry()
				short.SetKey("short")
				short.SetValue([]byte("expires"))
				short.SetTTL(time.Now().Add(time.Second).UnixMilli())
				var err error
				switch mutation {
				case "put":
					err = e.Put(2, short)
				case "raw":
					err = e.PutRaw(2, short.Encode())
				case "ttl":
					short.SetTTL(0)
					err = e.Put(2, short)
					if err == nil {
						short.SetTTL(time.Now().Add(time.Second).UnixMilli())
						err = e.UpdateTTL(2, short)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err = e.Compaction(); err != nil {
					t.Fatal(err)
				}
				if !e.Check(2) {
					t.Fatal("not-yet-due entry removed")
				}
				time.Sleep(time.Second)
				if _, err = e.Compaction(); err != nil {
					t.Fatal(err)
				}
				if e.Check(2) || !e.Check(1) {
					t.Fatal("earlier expiry was skipped or live entry lost")
				}
				if err = e.Delete(1); err != nil {
					t.Fatal(err)
				}
				if _, err = e.Compaction(); err != nil {
					t.Fatal(err)
				}
				if p.Snapshot().Entries != 0 {
					t.Fatal("entries retained")
				}
			})
		})
	}
}

func BenchmarkCompactionBeforeDeadline(b *testing.B) {
	p := NewPool(128<<20, Response, nil)
	defer p.Close(context.Background())
	e := NewEngine(p)
	v := NewEntry()
	v.SetKey("small")
	v.SetValue(make([]byte, 100))
	v.SetTTL(time.Now().Add(time.Hour).UnixMilli())
	raw := v.Encode()
	for i := range 100000 {
		if err := e.PutRaw(uint64(i), raw); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for b.Loop() {
		if _, err := e.Compaction(); err != nil {
			b.Fatal(err)
		}
	}
}
