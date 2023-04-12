package basefs

import (
	"sync"
	"sync/atomic"
)

func NewMutex() *Mutex {
	return &Mutex{
		sync.Mutex{},
		atomic.Bool{},
	}
}

type Mutex struct {
	sync.Mutex
	locked atomic.Bool
}

func (m *Mutex) Lock() {
	m.Mutex.Lock()
	m.locked.Swap(true)
}

func (m *Mutex) Unlock() {
	m.Mutex.Unlock()
	m.locked.Swap(false)
}

func (m *Mutex) IsLocked() bool {
	return m.locked.Load()
}
