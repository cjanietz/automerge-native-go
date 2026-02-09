package changegraph

// Clock is an actor->max-seq visibility structure for historical reads.
type Clock struct {
	maxByActor map[uint32]uint64
}

func NewClock() Clock {
	return Clock{maxByActor: make(map[uint32]uint64)}
}

func (c *Clock) Observe(actor uint32, seq uint64) {
	if c.maxByActor == nil {
		c.maxByActor = make(map[uint32]uint64)
	}
	if seq > c.maxByActor[actor] {
		c.maxByActor[actor] = seq
	}
}

func (c Clock) MaxSeq(actor uint32) uint64 {
	return c.maxByActor[actor]
}

func (c Clock) Covers(actor uint32, seq uint64) bool {
	return c.maxByActor[actor] >= seq
}

func (c Clock) Snapshot() map[uint32]uint64 {
	out := make(map[uint32]uint64, len(c.maxByActor))
	for actor, seq := range c.maxByActor {
		out[actor] = seq
	}
	return out
}
