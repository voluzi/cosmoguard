package olricstore

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/voluzi/olric/pkg/storage"
)

type pressureModelRecord struct {
	value []byte
	ttl   int64
}

func FuzzEnginePressureModel(f *testing.F) {
	f.Add([]byte("2"))
	f.Add([]byte{0, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88})
	f.Add([]byte{0, 4, 8, 12, 16, 20, 1, 24, 2, 3, 5, 6, 7, 255})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 128 {
			ops = ops[:128]
		}
		for _, policy := range []Policy{Response, Security} {
			variablePressureModel(t, ops, policy)
			p, e := testEngine(t, slabCharge+2*fragmentCharge, policy)
			child, err := e.Fork(nil)
			if err != nil {
				t.Fatal(err)
			}
			engines := []*Engine{e, child.(*Engine)}
			live := []map[uint64]pressureModelRecord{{}, {}}
			order := [][]uint64{{}, {}}
			remove := func(n int, h uint64) {
				delete(live[n], h)
				for i, x := range order[n] {
					if x == h {
						order[n] = append(order[n][:i], order[n][i+1:]...)
						break
					}
				}
			}
			touch := func(n int, h uint64) { r := live[n][h]; remove(n, h); live[n][h] = r; order[n] = append(order[n], h) }
			for step, op := range ops {
				n := int(op >> 7)
				h := uint64((op >> 3) & 15)
				engine := engines[n]
				before := len(live[0]) + len(live[1])
				switch op & 7 {
				case 0, 4, 6:
					entry := item(fmt.Sprint(h), 256<<10)
					entry.Value()[0] = byte(step)
					if op&7 == 6 {
						entry.SetTTL(time.Now().Add(-time.Hour).UnixMilli())
					}
					_, exists := live[n][h]
					admit := exists || before < 4 || policy == Response && len(order[n]) > 0
					if op&7 == 4 {
						err = engine.Put(h, entry)
					} else {
						err = engine.PutRaw(h, entry.Encode())
					}
					if !admit {
						if !errors.Is(err, ErrCapacity) {
							t.Fatal("foreign/security pressure admitted", err)
						}
					} else {
						if err != nil {
							t.Fatal("local admission", err)
						}
						if !exists && before == 4 {
							remove(n, order[n][0])
						}
						remove(n, h)
						live[n][h] = pressureModelRecord{value: append([]byte(nil), entry.Value()...), ttl: entry.TTL()}
						order[n] = append(order[n], h)
					}
				case 1:
					_, err = engine.Get(h)
					if _, ok := live[n][h]; ok {
						if err != nil {
							t.Fatal(err)
						}
						touch(n, h)
					} else if !errors.Is(err, storage.ErrKeyNotFound) {
						t.Fatal(err)
					}
				case 2:
					if err := engine.Delete(h); err != nil {
						t.Fatal(err)
					}
					remove(n, h)
				case 3:
					_, err = engine.Compaction()
					if err != nil {
						t.Fatal(err)
					}
					for h, r := range live[n] {
						if r.ttl != 0 && r.ttl <= time.Now().UnixMilli() {
							remove(n, h)
						}
					}
				case 5:
					if err := engine.Destroy(); err != nil {
						t.Fatal(err)
					}
					if err := engine.Put(h, item("dead", 8)); !errors.Is(err, ErrClosed) {
						t.Fatal("resurrection", err)
					}
					live[n] = map[uint64]pressureModelRecord{}
					order[n] = nil
					replacement, err := NewEngine(p).Fork(nil)
					if err != nil {
						t.Fatal(err)
					}
					engines[n] = replacement.(*Engine)
				case 7:
					if err := engine.PutRaw(h, []byte{0}); !errors.Is(err, ErrInvalidEntry) {
						t.Fatal(err)
					}
				}
				total := 0
				for n, engine := range engines {
					total += len(live[n])
					for h := uint64(0); h < 16; h++ {
						raw, err := engine.GetRaw(h)
						r, ok := live[n][h]
						if !ok {
							if !errors.Is(err, storage.ErrKeyNotFound) {
								t.Fatal("unexpected survivor", n, h, err)
							}
							continue
						}
						if err != nil {
							t.Fatal("missing survivor", n, h, err)
						}
						entry := NewEntry()
						entry.Decode(raw)
						if entry.Key() != fmt.Sprint(h) || entry.Timestamp() != 123 || entry.TTL() != r.ttl || !bytes.Equal(entry.Value(), r.value) {
							t.Fatal("model bytes/deadline mismatch", n, h)
						}
					}
					if engine.Stats().Length != len(live[n]) {
						t.Fatal("engine count")
					}
				}
				stats := p.Snapshot()
				if stats.Entries != uint64(total) || stats.Inuse != uint64(2*fragmentCharge+total*(512<<10)) || stats.Allocated > stats.Capacity {
					t.Fatal("model accounting", stats, total)
				}
			}
			if err := p.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if s := p.Snapshot(); s.Allocated != 0 || s.Inuse != 0 || s.Entries != 0 {
				t.Fatal("cleanup", s)
			}
		}
	})
}

func variablePressureModel(t *testing.T, ops []byte, policy Policy) {
	p, e := testEngine(t, 3<<20, policy)
	child, err := e.Fork(nil)
	if err != nil {
		t.Fatal(err)
	}
	engines := []*Engine{e, child.(*Engine)}
	live := []map[uint64]*Entry{{}, {}}
	order := [][]uint64{{}, {}}
	remove := func(n int, h uint64) {
		delete(live[n], h)
		for i, x := range order[n] {
			if x == h {
				order[n] = append(order[n][:i], order[n][i+1:]...)
				break
			}
		}
	}
	for step, op := range ops {
		n := int(op >> 7)
		h := uint64((op >> 3) & 15)
		engine := engines[n]
		switch op & 3 {
		case 0, 3:
			sizes := []int{8, 8 << 10, 128 << 10, 256 << 10, 900 << 10}
			v := item(fmt.Sprint(h), sizes[int(op>>2)%len(sizes)])
			v.Value()[0] = byte(step)
			beforeRaw, _ := engine.GetRaw(h)
			beforeOrder := append([]uint64(nil), order[n]...)
			before := p.Snapshot()
			err = engine.PutRaw(h, v.Encode())
			after := p.Snapshot()
			victims := []uint64{}
			for _, old := range beforeOrder {
				if old != h && !engine.Check(old) {
					victims = append(victims, old)
				}
			}
			eligible := []uint64{}
			for _, old := range beforeOrder {
				if old != h {
					eligible = append(eligible, old)
				}
			}
			if len(victims) > 32 || uint64(len(victims)) != after.PressureEvictions-before.PressureEvictions {
				t.Fatal("victim accounting")
			}
			if policy == Security && len(victims) != 0 {
				t.Fatal("security victim")
			}
			for i, victim := range victims {
				if victim != eligible[i] {
					t.Fatal("non-oldest victim")
				}
				remove(n, victim)
			}
			if err != nil {
				if !errors.Is(err, ErrCapacity) {
					t.Fatal(err)
				}
				got, _ := engine.GetRaw(h)
				if !bytes.Equal(got, beforeRaw) {
					t.Fatal("failed overwrite changed target")
				}
			} else {
				remove(n, h)
				live[n][h] = v
				order[n] = append(order[n], h)
			}
		case 1:
			_, err := engine.Get(h)
			if v, ok := live[n][h]; ok {
				if err != nil {
					t.Fatal(err)
				}
				remove(n, h)
				live[n][h] = v
				order[n] = append(order[n], h)
			} else if !errors.Is(err, storage.ErrKeyNotFound) {
				t.Fatal(err)
			}
		case 2:
			if err := engine.Delete(h); err != nil {
				t.Fatal(err)
			}
			remove(n, h)
		}
		var inuse uint64 = 1024
		total := 0
		for n, engine := range engines {
			total += len(live[n])
			for h := uint64(0); h < 16; h++ {
				raw, err := engine.GetRaw(h)
				want, ok := live[n][h]
				if !ok {
					if !errors.Is(err, storage.ErrKeyNotFound) {
						t.Fatal("foreign/unmodelled record", err)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				got := NewEntry()
				got.Decode(raw)
				if got.Key() != want.Key() || got.TTL() != want.TTL() || got.Timestamp() != want.Timestamp() || !bytes.Equal(got.Value(), want.Value()) {
					t.Fatal("variable model corruption")
				}
				charge := uint64(128)
				for charge < uint64(48+29+len(want.Key())+len(want.Value())) {
					charge *= 2
				}
				inuse += charge
			}
		}
		if s := p.Snapshot(); s.Entries != uint64(total) || s.Inuse != inuse || s.Allocated > s.Capacity {
			t.Fatal("variable accounting", s, inuse)
		}
	}
	_ = p.Close(t.Context())
	if s := p.Snapshot(); s.Inuse != 0 || s.Allocated != 0 || s.Entries != 0 {
		t.Fatal("variable cleanup", s)
	}
}
