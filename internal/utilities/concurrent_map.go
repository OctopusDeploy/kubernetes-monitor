package utilities

import "sync"

// ConcurrentMap is a wrapper around sync.Map that provides a thread-safe map implementation with strong typing
type ConcurrentMap[K, V comparable] struct {
	values sync.Map
}

// NewConcurrentMap creates a new ConcurrentMap instance using the provided generic type parameters.
func NewConcurrentMap[K, V comparable]() *ConcurrentMap[K, V] {
	return &ConcurrentMap[K, V]{
		values: sync.Map{},
	}
}

// NewConcurrentMapFromMap creates a new ConcurrentMap instance and populates it with the provided map.
func NewConcurrentMapFromMap[K, V comparable](replacement map[K]V) *ConcurrentMap[K, V] {
	cm := &ConcurrentMap[K, V]{
		values: sync.Map{},
	}
	cm.ReplaceAll(replacement)
	return cm
}

// Set Sets the value for the key in the map. If the key already exists, it will be replaced.
func (c *ConcurrentMap[K, V]) Set(key K, value V) {
	c.values.Store(key, value)
}

// ReplaceAll replaces the entire contents of the ConcurrentMap with the provided map.
func (c *ConcurrentMap[K, V]) ReplaceAll(replacement map[K]V) {
	c.values.Clear()
	for k, v := range replacement {
		c.values.Store(k, v)
	}
}

// Get retrieves the value for the key from the map. If the key does not exist, it returns the zero value and false.
func (c *ConcurrentMap[K, V]) Get(key K) (V, bool) {
	value, ok := c.values.Load(key)
	if !ok {
		// Can't use nil here because V might not be a pointer type
		var zero V
		return zero, false
	}
	return value.(V), true
}

// GetAll retrieves all values from the map as a slice.
func (c *ConcurrentMap[K, V]) GetAll() []V {
	var values []V
	c.values.Range(func(key, value any) bool {
		values = append(values, value.(V))
		return true
	})
	return values
}

// Remove deletes the key from the map.
func (c *ConcurrentMap[K, V]) Remove(key K) {
	c.values.Delete(key)
}

// Exists checks if the key exists in the map. Returns true if the specified key exists.
func (c *ConcurrentMap[K, V]) Exists(key K) bool {
	_, ok := c.values.Load(key)
	return ok
}

// Iterate iterates over all key-value pairs in the map and applies the callback function to each pair. If the callback function returns false, the iteration stops.
func (c *ConcurrentMap[K, V]) Iterate(callback func(K, V) bool) {
	c.values.Range(func(key, value any) bool {
		return callback(key.(K), value.(V))
	})
}

// GetAsMap creates a copy the ConcurrentMap as a standard Go map.
func (c *ConcurrentMap[K, V]) GetAsMap() map[K]V {
	m := make(map[K]V)
	c.values.Range(func(key, value any) bool {
		m[key.(K)] = value.(V)
		return true
	})
	return m
}

// Filter returns a subset of the map based on the provided filter function.
// The filter function should return true for keys that should be included in the result.
func (c *ConcurrentMap[K, V]) Filter(filter func(K, V) bool) *ConcurrentMap[K, V] {
	m := make(map[K]V)
	c.values.Range(func(key, value any) bool {
		if filter(key.(K), value.(V)) {
			m[key.(K)] = value.(V)
		}
		return true
	})
	return NewConcurrentMapFromMap(m)
}
