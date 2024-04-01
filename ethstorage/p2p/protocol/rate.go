package protocol

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

// measurementImpact is the impact a single measurement has on a peer's final
// capacity value. A value closer to 0 reacts slower to sudden network changes,
// but it is also more stable against temporary hiccups. 0.1 worked well for
// most of Ethereum's existence, so might as well go with it.
const measurementImpact = 0.1

// capacityOverestimation is the ratio of items to over-estimate when retrieving
// a peer's capacity to avoid locking into a lower value due to never attempting
// to fetch more than some local stable value.
const capacityOverestimation = 1.01

const expectRequestTimeFactor = 0.8

// Tracker estimates the throughput capacity of a peer with regard to each data
// type it can deliver. The goal is to dynamically adjust request sizes to max
// out network throughput without overloading either the peer or th elocal node.
//
// By tracking in real time the latencies and bandiwdths peers exhibit for each
// packet type, it's possible to prevent overloading by detecting a slowdown on
// one type when another type is pushed too hard.
//
// Similarly, real time measurements also help avoid overloading the local net
// connection if our peers would otherwise be capable to deliver more, but the
// local link is saturated. In that case, the live measurements will force us
// to reduce request sizes until the throughput gets stable.
//
// Lastly, message rate measurements allows us to detect if a peer is unsuaully
// slow compared to other peers, in which case we can decide to keep it around
// or free up the slot so someone closer.
//
// Since throughput tracking and estimation adapts dynamically to live network
// conditions, it's fine to have multiple trackers locally track the same peer
// in different subsystem. The throughput will simply be distributed across the
// two trackers if both are highly active.
type Tracker struct {
	// capacity is the number of items retrievable per second of a given type.
	// It is analogous to bandwidth, but we deliberately avoided using bytes
	// as the unit, since serving nodes also spend a lot of time loading data
	// from disk, which is linear in the number of items, but mostly constant
	// in their sizes.
	capacity float64

	lock sync.RWMutex
}

// NewTracker creates a new message rate tracker for a specific peer.
func NewTracker(cap float64) *Tracker {
	return &Tracker{
		capacity: cap,
	}
}

// Capacity calculates the number of items the peer is estimated to be able to
// retrieve within the alloted time slot. The method will round up any division
// errors and will add an additional overestimation ratio on top. The reason for
// overshooting the capacity is because certain message types might not increase
// the load proportionally to the requested items, so fetching a bit more might
// still take the same RTT. By forcefully overshooting by a small amount, we can
// avoid locking into a lower-that-real capacity.
func (t *Tracker) Capacity(targetRTT time.Duration) int {
	t.lock.RLock()
	defer t.lock.RUnlock()

	// Calculate the actual measured throughput
	throughput := t.capacity * float64(targetRTT) / float64(time.Second)

	// Return an overestimation to force the peer out of a stuck minima, adding
	// +1 in case the item count is too low for the overestimator to dent
	return roundCapacity(1 + capacityOverestimation*throughput)
}

// roundCapacity gives the integer value of a capacity.
// The result fits int32, and is guaranteed to be positive.
func roundCapacity(cap float64) int {
	return int(math.Min(maxMessageSize, math.Max(1, math.Ceil(cap)))) // TODO
}

// Update modifies the peer's capacity values for a specific data type with a new
// measurement. If the delivery is zero, the peer is assumed to have either timed
// out or to not have the requested data, resulting in a slash to 0 capacity. This
// avoids assigning the peer retrievals that it won't be able to honour.
func (t *Tracker) Update(elapsed time.Duration, items int) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Otherwise update the throughput with a new measurement
	if elapsed <= 0 {
		elapsed = 1 // +1 (ns) to ensure non-zero divisor
	}
	measured := float64(items) / (float64(elapsed) / float64(time.Second))

	t.capacity = (1-measurementImpact)*(t.capacity) + measurementImpact*measured
}

// Trackers is a set of message rate trackers across a number of peers with the
// goal of aggregating certain measurements across the entire set for outlier
// filtering and newly joining initialization.
type Trackers struct {
	trackers map[string]*Tracker

	log  log.Logger
	lock sync.RWMutex
}

// NewTrackers creates an empty set of trackers to be filled with peers.
func NewTrackers(log log.Logger) *Trackers {
	return &Trackers{
		trackers: make(map[string]*Tracker),
		log:      log,
	}
}

// Track inserts a new tracker into the set.
func (t *Trackers) Track(id string, tracker *Tracker) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.trackers[id]; ok {
		return errors.New("already tracking")
	}
	t.trackers[id] = tracker

	return nil
}

// Untrack stops tracking a previously added peer.
func (t *Trackers) Untrack(id string) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.trackers[id]; !ok {
		return errors.New("not tracking")
	}
	delete(t.trackers, id)
	return nil
}

// Capacity is a helper function to access a specific tracker without having to
// track it explicitly outside.
func (t *Trackers) Capacity(id string, targetRTT time.Duration) int {
	t.lock.RLock()
	defer t.lock.RUnlock()

	tracker := t.trackers[id]
	if tracker == nil {
		return 1 // Unregister race, don't return 0, it's a dangerous number
	}
	return tracker.Capacity(targetRTT)
}

// Update is a helper function to access a specific tracker without having to
// track it explicitly outside.
func (t *Trackers) Update(id string, elapsed time.Duration, items int) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	if tracker := t.trackers[id]; tracker != nil {
		tracker.Update(elapsed, items)
	}
}
