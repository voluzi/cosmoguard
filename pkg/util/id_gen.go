package util

import (
	"math"
	"math/rand"
	"strconv"
	"sync"
)

type UniqueID struct {
	generated sync.Map
}

func (u *UniqueID) ID() string {
	for {
		id := strconv.Itoa(rand.Intn(math.MaxInt32))
		if _, loaded := u.generated.LoadOrStore(id, true); !loaded {
			return id
		}
	}
}

func (u *UniqueID) Release(id string) {
	u.generated.Delete(id)
}
