package util

import (
	"math"
	"math/rand"
	"strconv"
	"sync"
)

type UniqueID struct {
	generated sync.Map
	source    func() string
}

// NewUniqueID constructs a generator with a custom candidate source.
func NewUniqueID(source func() string) *UniqueID {
	return &UniqueID{source: source}
}

func (u *UniqueID) ID() string {
	for {
		id := ""
		if u.source == nil {
			id = strconv.Itoa(rand.Intn(math.MaxInt32))
		} else {
			id = u.source()
		}
		if _, loaded := u.generated.LoadOrStore(id, true); !loaded {
			return id
		}
	}
}

func (u *UniqueID) Release(id string) {
	u.generated.Delete(id)
}
