package olricstore

import "math"

const (
	slabSize = 2 << 20
	leafSize = 128
	treeSize = 2 * (slabSize / leafSize)
	// The descriptor is at most 128 bytes on amd64 and arm64. Linked registries
	// have no retained slice capacity or map buckets after a slab is released.
	slabMetadata = 128
	slabCharge   = slabSize + treeSize + slabMetadata
)

type slab struct {
	data, tree []byte
	next       *slab
	id         uint32
	live       int
	owner      uint64
}
type arena struct {
	head             *slab
	nextID           uint32
	limit, allocated uint64
}

func (a *arena) reserve(n uint64) bool {
	if n > math.MaxUint64-a.allocated || (a.limit != 0 && (n > a.limit || a.allocated > a.limit-n)) {
		return false
	}
	a.allocated += n
	return true
}
func (s *slab) allocate(i, off, size, want int) (int, bool) {
	if s.tree[i] >= 2 {
		return 0, false
	}
	if size == want {
		if s.tree[i] != 0 {
			return 0, false
		}
		s.tree[i] = 2
		return off, true
	}
	for child := 0; child < 2; child++ {
		if p, ok := s.allocate(2*i+child, off+child*size/2, size/2, want); ok {
			s.tree[i] = 1
			if s.tree[i*2] >= 2 && s.tree[i*2+1] >= 2 {
				s.tree[i] = 3
			}
			return p, true
		}
	}
	return 0, false
}
func (s *slab) free(off, size int) {
	i := slabSize/size + off/size
	s.tree[i] = 0
	for i > 1 {
		i /= 2
		s.tree[i] = 0
		if s.tree[i*2] != 0 || s.tree[i*2+1] != 0 {
			s.tree[i] = 1
			if s.tree[i*2] >= 2 && s.tree[i*2+1] >= 2 {
				s.tree[i] = 3
			}
		}
	}
}
func (a *arena) allocate(want int) (uint64, bool) {
	if want < leafSize || want > slabSize || want&(want-1) != 0 {
		return 0, false
	}
	for s := a.head; s != nil; s = s.next {
		if off, ok := s.allocate(1, 0, slabSize, want); ok {
			s.live++
			return uint64(s.id)<<32 | uint64(off+1), true
		}
	}
	if a.nextID == math.MaxUint32 || !a.reserve(slabCharge) {
		return 0, false
	}
	a.nextID++
	s := &slab{data: make([]byte, slabSize), tree: make([]byte, treeSize), id: a.nextID, next: a.head, live: 1}
	a.head = s
	off, _ := s.allocate(1, 0, slabSize, want)
	return uint64(s.id)<<32 | uint64(off+1), true
}
func (a *arena) slab(loc uint64) *slab {
	for s := a.head; s != nil; s = s.next {
		if s.id == uint32(loc>>32) {
			return s
		}
	}
	return nil
}
func (a *arena) block(loc uint64) []byte { return a.slab(loc).data[int(uint32(loc)-1):] }
func (a *arena) free(loc uint64, size int) {
	var prev *slab
	for s := a.head; s != nil; s = s.next {
		if s.id != uint32(loc>>32) {
			prev = s
			continue
		}
		s.free(int(uint32(loc)-1), size)
		s.live--
		if s.live == 0 {
			if prev == nil {
				a.head = s.next
			} else {
				prev.next = s.next
			}
			s.data = nil
			s.tree = nil
			s.next = nil
			a.allocated -= slabCharge
		}
		return
	}
}
