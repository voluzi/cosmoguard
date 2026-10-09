package olricstore

import "math"

const (
	slabSize = 2 << 20
	leafSize = 128
	treeSize = 2 * (slabSize / leafSize)
	// Each slab reserves its descriptor and at most nine radix-index nodes.
	slabDescriptor  = 128
	indexNodeCharge = 160
	slabMetadata    = slabDescriptor + 9*indexNodeCharge
	slabCharge      = slabSize + treeSize + slabMetadata
)

type slab struct {
	data, tree []byte
	next, prev *slab
	id         uint32
	live       int
}
type slabIndex struct {
	branches [16]*slabIndex
	value    *slab
	count    uint8
}

type arena struct {
	index            *slabIndex
	slabs            uint64
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
	if a.head != nil {
		a.head.prev = s
	}
	a.head = s
	a.slabs++
	a.addSlab(s)
	off, _ := s.allocate(1, 0, slabSize, want)
	return uint64(s.id)<<32 | uint64(off+1), true
}
func (a *arena) addSlab(s *slab) {
	if a.index == nil {
		a.index = &slabIndex{}
	}
	node := a.index
	for shift := 28; shift >= 0; shift -= 4 {
		i := s.id >> shift & 15
		if node.branches[i] == nil {
			node.branches[i] = &slabIndex{}
			node.count++
		}
		node = node.branches[i]
	}
	node.value = s
}
func (a *arena) slab(loc uint64) *slab {
	node := a.index
	id := uint32(loc >> 32)
	for shift := 28; shift >= 0 && node != nil; shift -= 4 {
		node = node.branches[id>>shift&15]
	}
	if node == nil {
		return nil
	}
	return node.value
}
func (a *arena) dropSlab(id uint32) {
	var path [9]*slabIndex
	path[0] = a.index
	for i := 1; i <= 8; i++ {
		path[i] = path[i-1].branches[id>>(32-i*4)&15]
	}
	path[8].value = nil
	for i := 8; i > 0; i-- {
		if path[i].count != 0 || path[i].value != nil {
			break
		}
		parent := path[i-1]
		parent.branches[id>>(32-i*4)&15] = nil
		parent.count--
	}
	if a.index.count == 0 {
		a.index = nil
	}
}
func (a *arena) block(loc uint64) []byte { return a.slab(loc).data[int(uint32(loc)-1):] }
func (a *arena) free(loc uint64, size int) {
	s := a.slab(loc)
	if s == nil {
		return
	}
	s.free(int(uint32(loc)-1), size)
	s.live--
	if s.live != 0 {
		return
	}
	if s.prev == nil {
		a.head = s.next
	} else {
		s.prev.next = s.next
	}
	if s.next != nil {
		s.next.prev = s.prev
	}
	a.dropSlab(s.id)
	s.data, s.tree, s.next, s.prev = nil, nil, nil, nil
	a.slabs--
	a.allocated -= slabCharge
}
