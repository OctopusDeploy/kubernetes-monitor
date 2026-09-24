package cluster

type CircularBuffer struct {
	buf      []byte
	start    int // read position
	end      int // write position
	full     bool
	capacity int
}

func NewCircularBuffer(capacity int) *CircularBuffer {
	return &CircularBuffer{
		buf:      make([]byte, capacity),
		capacity: capacity,
	}
}

func (c *CircularBuffer) Write(p []byte) {
	for _, b := range p {
		c.buf[c.end] = b
		c.end = (c.end + 1) % c.capacity
		if c.full {
			c.start = (c.start + 1) % c.capacity // Increment start as we've overwritten the old start value
		}
		if c.end == c.start {
			c.full = true
		}
	}
}

func (c *CircularBuffer) Bytes() []byte {
	if c.end > c.start {
		return c.buf[c.start:c.end]
	}

	out := make([]byte, 0, c.capacity)
	out = append(out, c.buf[c.start:]...)
	out = append(out, c.buf[:c.end]...)
	return out
}
