package olricstore

import (
	"context"
	"time"
)

func (p *Pool) sweep(ctx context.Context) {
	p.mu.Lock()
	cursor := p.nextID
	p.mu.Unlock()
	for cursor > 0 && ctx.Err() == nil {
		var batch [32]*Engine
		n := 0
		p.mu.Lock()
		for e := p.engines; e != nil && n < len(batch); e = e.next {
			if e.id <= cursor {
				batch[n] = e
				n++
			}
		}
		p.mu.Unlock()
		if n == 0 {
			return
		}
		cursor = batch[n-1].id - 1
		for _, e := range batch[:n] {
			p.mu.Lock()
			if p.closed || ctx.Err() != nil {
				p.mu.Unlock()
				return
			}
			if e.readyLocked() == nil {
				e.sweepLocked(time.Now().UnixMilli())
			}
			p.mu.Unlock()
		}
	}
}
