package gitlfsfuse

import "sync"

type lock struct {
	mu sync.Mutex
	rc int
}
type plock struct {
	lk map[string]*lock
	mu sync.Mutex
}

func (p *plock) Lock(name string) {
	p.mu.Lock()
	l, ok := p.lk[name]
	if !ok {
		l = &lock{}
		p.lk[name] = l
	}
	l.rc++
	p.mu.Unlock()
	l.mu.Lock()
}

func (p *plock) TryLock(name string) (ok bool) {
	p.mu.Lock()
	l, ok := p.lk[name]
	if !ok {
		l = &lock{}
		p.lk[name] = l
	}
	if ok = l.mu.TryLock(); ok {
		l.rc++
	}
	p.mu.Unlock()
	return ok
}

func (p *plock) Unlock(name string) {
	p.mu.Lock()
	l := p.lk[name]
	if l.rc--; l.rc == 0 {
		delete(p.lk, name)
	}
	p.mu.Unlock()
	l.mu.Unlock()
}
