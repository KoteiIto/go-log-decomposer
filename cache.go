package decomposer

import (
	"sync"
)

type replacementCache struct {
	cache map[string]string
	mutex *sync.Mutex
}

func newReplacementCache() *replacementCache {
	return &replacementCache{
		cache: map[string]string{},
		mutex: &sync.Mutex{},
	}
}

func (c replacementCache) Set(key string, value string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.cache[key] = value
}

func (c replacementCache) Get(key string) (value string, ok bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	value, ok = c.cache[key]
	return
}
