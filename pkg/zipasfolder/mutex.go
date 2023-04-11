package zipasfolder

import (
	"sync"
	"sync/atomic"
)

func NewMutex() *mutex {
	return &mutex{
		sync.Mutex{},
		atomic.Bool{},
	}
}

type mutex struct {
	sync.Mutex
	locked atomic.Bool
}

func (m *mutex) Lock() {
	m.Mutex.Lock()
	m.locked.Swap(true)
}

func (m *mutex) Unlock() {
	m.Mutex.Unlock()
	m.locked.Swap(false)
}

func (m *mutex) IsLocked() bool {
	return m.locked.Load()
}
