package olricstore

import (
	"bytes"
	"math/rand/v2"
	"testing"
	"unsafe"
)

func TestAllocatorAllOrders(t *testing.T) {
	for n := leafSize; n <= slabSize; n *= 2 {
		a := arena{limit: slabCharge}
		loc, ok := a.allocate(n)
		if !ok {
			t.Fatal(n)
		}
		if len(a.block(loc)) < n || a.allocated > a.limit {
			t.Fatal("extent/cap")
		}
		a.free(loc, n)
		if a.allocated != 0 || a.head != nil {
			t.Fatal("not released")
		}
	}
	if unsafe.Sizeof(slab{}) > slabDescriptor || unsafe.Sizeof(slabIndex{}) > indexNodeCharge {
		t.Fatal("descriptor charge")
	}
}
func TestAllocatorFragmentationRejectsWithinCap(t *testing.T) {
	a := arena{limit: slabCharge}
	var locs []uint64
	for range slabSize / leafSize {
		loc, ok := a.allocate(leafSize)
		if !ok {
			t.Fatal("early rejection")
		}
		locs = append(locs, loc)
	}
	for i, l := range locs {
		if i%2 == 0 {
			a.free(l, leafSize)
		}
	}
	if _, ok := a.allocate(leafSize * 2); ok {
		t.Fatal("fragmented allocation admitted")
	}
	if a.allocated > a.limit {
		t.Fatal("cap exceeded")
	}
	for i, l := range locs {
		if i%2 != 0 {
			a.free(l, leafSize)
		}
	}
	if a.allocated != 0 {
		t.Fatal("leak")
	}
}
func TestAllocatorDescriptorChurnDoesNotGrow(t *testing.T) {
	a := arena{limit: slabCharge}
	for range 100 {
		l, ok := a.allocate(slabSize)
		if !ok {
			t.Fatal("admission")
		}
		a.free(l, slabSize)
		if a.head != nil || a.index != nil || a.allocated != 0 {
			t.Fatal("retained descriptor")
		}
	}
}
func FuzzAllocatorModel(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 14, 15, 16, 255})
	f.Add([]byte{14, 14, 14, 14, 0, 255, 14})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 1000 {
			ops = ops[:1000]
		}
		a := arena{limit: 2 * slabCharge}
		type record struct {
			loc   uint64
			size  int
			value byte
		}
		var live []record
		for _, op := range ops {
			if op&128 != 0 && len(live) > 0 {
				i := int(op) % len(live)
				a.free(live[i].loc, live[i].size)
				live = append(live[:i], live[i+1:]...)
			} else {
				size := leafSize << uint(op%15)
				loc, ok := a.allocate(size)
				if ok {
					v := byte(rand.IntN(255) + 1)
					copy(a.block(loc)[:size], bytes.Repeat([]byte{v}, size))
					live = append(live, record{loc, size, v})
				}
			}
			if a.allocated > a.limit {
				t.Fatal("cap")
			}
			for _, r := range live {
				for _, b := range a.block(r.loc)[:r.size] {
					if b != r.value {
						t.Fatal("overlap/corruption")
					}
				}
			}
		}
		for _, r := range live {
			a.free(r.loc, r.size)
		}
		if a.allocated != 0 || a.head != nil {
			t.Fatal("leak")
		}
	})
}
