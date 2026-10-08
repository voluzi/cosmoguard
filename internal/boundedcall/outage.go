package boundedcall

import (
	"errors"
	"sync"
	"time"
)

var ErrUnavailable = errors.New("backend operation unavailable")

// Three timeouts filter isolated delays; one second limits recovery traffic.
const outageThreshold = 3
const outageCooldown = time.Second

type operation struct {
	generation uint64
	probe      bool
}

type outage struct {
	sync.Mutex
	generation      uint64
	timeouts        int
	open            bool
	nextProbe       time.Time
	probe           bool
	probeGeneration uint64
	probeResolved   bool
	probeFinished   bool
	onUnavailable   func(bool)
}

// NewRecovering stops repeated waits during an outage. New and NewWaiting
// retain per-request admission without outage suppression.
func NewRecovering(capacity int, budget time.Duration, observe func(string), onUnavailable func(bool)) *Gate {
	g := New(capacity, budget, observe)
	g.outage = &outage{onUnavailable: onUnavailable}
	return g
}

func (o *outage) admit() (operation, bool) {
	if o == nil {
		return operation{}, true
	}
	o.Lock()
	defer o.Unlock()
	op := operation{generation: o.generation}
	if o.open {
		if o.probe || time.Now().Before(o.nextProbe) {
			return op, false
		}
		op.probe = true
		o.probe = true
		o.probeGeneration = o.generation
		o.probeResolved = false
		o.probeFinished = false
	}
	return op, true
}

func (o *outage) current(op operation) (operation, bool) {
	if o == nil {
		return op, true
	}
	o.Lock()
	defer o.Unlock()
	if op.probe && op.generation != o.generation {
		return op, false
	}
	if o.open && (!op.probe || op.generation != o.generation) {
		return op, false
	}
	if !o.open {
		op.generation = o.generation
	}
	return op, true
}

func (o *outage) resolve(op operation, timedOut, healthy, executed bool) {
	if o == nil {
		return
	}
	o.Lock()
	defer o.Unlock()
	if op.generation != o.generation {
		return
	}
	if op.probe {
		o.probeResolved = true
		if o.probeFinished {
			o.probe = false
		}
		o.nextProbe = time.Now().Add(outageCooldown)
		if healthy && executed {
			o.open = false
			o.timeouts = 0
			o.generation++
			if o.onUnavailable != nil {
				o.onUnavailable(false)
			}
		}
		return
	}
	if !executed || o.open {
		return
	}
	if !timedOut {
		o.timeouts = 0
		return
	}
	o.timeouts++
	if o.timeouts == outageThreshold {
		o.open = true
		o.generation++
		o.nextProbe = time.Now().Add(outageCooldown)
		if o.onUnavailable != nil {
			o.onUnavailable(true)
		}
	}
}

func (o *outage) finish(op operation) {
	if o == nil || !op.probe {
		return
	}
	o.Lock()
	defer o.Unlock()
	if o.probeGeneration == op.generation {
		o.probeFinished = true
		if o.probeResolved {
			o.probe = false
		}
	}
}

func (o *outage) close() {
	if o == nil {
		return
	}
	o.Lock()
	defer o.Unlock()
	o.generation++
	if o.open {
		o.open = false
		if o.onUnavailable != nil {
			o.onUnavailable(false)
		}
	}
}
