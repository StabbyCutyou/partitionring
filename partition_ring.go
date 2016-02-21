// Package partitionring represents a datastructure for reserving "partitions",
// or ranges of key space, between 0 and the maximum value of an int64.
// A PartitionRing accepts any arbitrary division of partitions you want to reserve,
// and allow you to test if a particular value or range is already locked.
// Additionally, starting from 0, you can have it reserve spaces for you, in chunks
// of a value you provide. In other words, slowly filling the range of partitions with
// values you specify. The partitionring will also allow you to set, and alter
// a maximum number of partitions, as well as define a timeout function for them,
// so that partitions will expire after their lease is up (which can be changed after
// the PartitionRing has been created, but only takes effect on a per-item basis).
// You can always evict a range, or whatever range a value falls into, when you
// want to explcitly
package partitionring

// TODO re-write whatever nonsense i just babbled on about up there.

import (
	"errors"
	"log"
	"math"
	"sync"
	"time"
)

// ErrNoPartitionsAvailable is
var ErrNoPartitionsAvailable = errors.New("No Partitions Available")

// ErrCouldNotReservePartition is
var ErrCouldNotReservePartition = errors.New("Could not reserve partition")

// MaxPartitionUpperBound is
var MaxPartitionUpperBound int64 = math.MaxInt64

// PartitionEntry is an array of 3 int64 values
// 0: lowerBound: int64
// 1: upperBound: int64
// 2: expirytime: int64
type PartitionEntry [3]int64

// PartitionRing is
type PartitionRing struct {
	partitionAge time.Duration

	expirationTicker  *time.Ticker
	stopExpiration    chan bool
	restartExpiration chan bool
	lowerLimit        int64
	upperLimit        int64
	partitionStep     int64
	sync.RWMutex
	data []PartitionEntry
}

// New is
func New(lowerLimit int64, upperLimit int64, defaultStep int64, partitionAge time.Duration) *PartitionRing {
	p := &PartitionRing{
		partitionAge:      partitionAge,
		data:              make([]PartitionEntry, 0),
		expirationTicker:  time.NewTicker(partitionAge),
		stopExpiration:    make(chan bool, 1),
		restartExpiration: make(chan bool, 1),
		lowerLimit:        lowerLimit,
		upperLimit:        upperLimit,
		partitionStep:     defaultStep,
	}
	go p.scheduleExpiration()
	return p
}

// Begins the ticker and handles removing outdated partitions
func (p *PartitionRing) scheduleExpiration() {
	// Go routine to listen to either the scheduler or the killer
	for {
		select {
		// Check to see if we have a tick
		case <-p.expirationTicker.C:
			// Check to see if we've been stopped
			p.expireAny()
		case <-p.stopExpiration:
			// Wait for the all-clear to start again
			<-p.restartExpiration
		}
	}
}

// Size gets the number of items in the Ring
func (p *PartitionRing) Size() int {
	p.RLock()
	defer p.RUnlock()
	return len(p.data)
}

// If the list is full, bail out
// If the list is empty, reserve first
// else
//   beginning at the front, loop through the elements
//   if it's the first item in the list
//    (and we know it can't be full, see the first check)
//     if it starts at zero && there is only 1 value in the list
//       reserve it's uppberbound+1 to that value + stepoffset
//     else if it doesn't start at zero
//       reserve the space between 0 and it's lowerbound-1, maxed by the step size
//     else
//       its the first item, but it started at zero and there are more items after it
//       Keep going
//   else
//     compare i to i-1
//     if the difference between i lowerbound and i-1 upperbound is > 1
//       reserve the space between them
//     else
//       noop in this element, no space

// ReserveNext is
func (p *PartitionRing) ReserveNext() (int64, int64, error) {
	p.Lock()
	defer p.Unlock()
	// We need to check for the next available free space
	// Because we're soft-tuning the max size down, partition steps will increase
	// as we get more efficient at sizing the Ring. Thus, we'll always go left to right,
	// trying to fill from 0 to N, meaning at first  values near N will go unprioritized
	// This also means, due to the current design, the partitions will occasionally be larger than
	// anticipated, and will not self-balance until they expire.
	size := len(p.data)
	// if we're full up, scram
	if size == 0 {
		// if we're empty
		// partitionStep-1 because we want pS # of values, not last boundary + pS
		obj := [3]int64{p.lowerLimit, p.partitionStep - 1, time.Now().Add(p.partitionAge).Unix()}
		p.data = append(p.data, obj)
		return obj[0], obj[1], nil
	}

	// We need to go through all of them, and find where a gap exists.
	// Since we support arbitrary expirations, free space could be anywhere
	// Iterate the data collection
	for i := 0; i < size; i++ {
		if i == 0 {
			if p.data[i][0] == p.lowerLimit && size == 1 {
				// if we've hit the end of the list, or if its the first element and there is only one element
				// we know theres room for it, so append it right after the only element
				obj := PartitionEntry{p.data[i][1] + 1, p.data[i][1] + p.partitionStep, time.Now().Add(p.partitionAge).Unix()}
				p.data = append(p.data, obj)
				return obj[0], obj[1], nil
			} else if p.data[i][0] != p.lowerLimit {
				// if this is the first element, but it isn't the 0 value for lowerBound
				// we've got the room at the front
				// Get the available room, make a new partition as -1 to the current partitions lowerbound
				newUpperBound := p.partitionStep - 1
				// We want to fit it in the space we have, so check to see if less than the Step value is free
				if newUpperBound > p.data[i][0]-1 {
					newUpperBound = p.data[i][0] - 1
				}
				obj := PartitionEntry{p.lowerLimit, newUpperBound, time.Now().Add(p.partitionAge).Unix()}
				// Add the new one infront of everything else
				p.data = append([]PartitionEntry{obj}, p.data[:]...)
				return obj[0], obj[1], nil
			} else {
				// NOOP
				continue
			}
		} else if p.data[i-1][1]+1 < p.data[i][0] {
			// since nothing yet matched, check this one against the one before it. If the space between upperBound
			// and lowerBound is greater than than 1, we've got some amount of space to Lock
			lowerBound := p.data[i-1][1] + 1
			upperBound := lowerBound + p.partitionStep
			if upperBound >= p.data[i][0] || upperBound < p.lowerLimit {
				// Make the new upperBound -1 from the current nodes lowerbound
				upperBound = p.data[i][0] - 1
			}
			obj := [3]int64{lowerBound, upperBound, time.Now().Add(p.partitionAge).Unix()}
			// Insert the data at position i+1
			// This above is wrong - we should insert at i-1:
			// if i is 3, we found space between 2 and 3, so we create a new entry for the space between 2 and 3
			// making the current object at i end up at i + 1
			p.data = append(p.data[:i], append([]PartitionEntry{obj}, p.data[i:]...)...)
			return obj[0], obj[1], nil
		} else {
			// NOOP
			continue
		}
	}

	// If we got here, and didn't reserve anything, and the final element is the final value, we must be full
	if p.data[size-1][1] == p.upperLimit {
		return -1, -1, ErrNoPartitionsAvailable
	}

	// If we got here, and the list wasn't full from the prior check, reserve a slot at the end

	newLower := p.data[size-1][1] + 1
	newUpper := newLower + p.partitionStep
	if newUpper < newLower {
		// We only had room for one more, and the addition of the partition step caused an overflow
		newUpper = p.upperLimit
	}
	obj := [3]int64{newLower, newUpper, time.Now().Add(p.partitionAge).Unix()}
	p.data = append(p.data, obj)
	return obj[0], obj[1], nil

	// obligatory shouldn't get here comment
	return 0, 0, ErrCouldNotReservePartition
}

// ExpireRange is
func (p *PartitionRing) ExpireRange(lowerBound int64, upperBound int64) int {
	// Freeze time so we don't corrupt anything
	p.Lock()
	defer p.Unlock()
	// As in expireAny, i'd much prefer to use slice powers to do something efficient here
	// put i'm concerned since I want to physically remove the item, not just make a new
	// pointer to a subspace in the original array
	d := make([]PartitionEntry, 0)
	removed := 0
	remove_until_finished := false
	for i, obj := range p.data {
		// Cheat - check the easy case first, if we luck out and find out
		// exit fast
		removed_node := false
		if lowerBound == obj[0] && upperBound == obj[1] {
			// The entire range matched. Do not carve it or re-add it to the set

			// Quickly append the rest of the array onto d, then break
			d = append(d, p.data[i+1:]...)
			removed++
			break
		}

		if obj[1] >= lowerBound && lowerBound >= obj[0] {
			// The lower end of the range to expire fits in the current nodes range

			if obj[0] != lowerBound {
				// If obj[1] were to  == upperbound, it means we wanted to remove this entire partition
				// don't carve a new position, just leave it out of the list, but still return early.

				// Since it wasn't, the new partition value is the old lowerbound obj[0], and one less than
				// the expired ranges lower bound - we've carved out a new partition
				d = append(d, [3]int64{obj[0], lowerBound - 1, time.Now().Add(p.partitionAge).Unix()})

			}
			// Now that we've removed the lower end, there could be anywhere from 0 to n
			// nodes to remove before we hit the upperBound. Until that hapens, we'll remove every node
			// unless the upperBound is between this range as well

			remove_until_finished = true
			removed_node = true
		}

		if obj[1] >= upperBound && upperBound >= obj[0] {
			// The upper end of the range to expire fits in the current nodes range

			if obj[1] != upperBound {
				// If obj[1] were to  == upperbound, it means we wanted to remove this entire partition
				// don't carve a new position, just leave it out of the list, but still return early.

				// Since it wasn't, the new partition value is one more than the expired ranges upper bound
				// and the old upperbound obj[1] - we've carved out a new partition
				d = append(d, [3]int64{upperBound + 1, obj[1], time.Now().Add(p.partitionAge).Unix()})

			}
			// Quickly append the rest of the array onto d, then break
			d = append(d, p.data[i+1:]...)
			removed++
			break

		}

		if removed_node == true || remove_until_finished {
			removed++
		} else {
			d = append(d, obj)
		}
	}
	p.data = d
	return removed
}

func (p *PartitionRing) expireAny() {
	// Noone else can play for now
	p.Lock()
	defer p.Unlock()
	// lose a tiny bit of accuracy, avoid needless repeated calls to time.Now().Unix()
	t := time.Now().Unix()
	// We don't want to iterate over something and pop from it, so we'll build a replace
	// dataset, and put it back in the originals place... wise? i want for there to be a more
	// efficient way to do this, but don't wanna write to the array will i iterate it...
	d := make([]PartitionEntry, 0)
	// I'd like to use i as a pivot point, and :slice: my way around this...
	for _, obj := range p.data {
		// If the expiration date is less than the current time
		if obj[2] < t {
			d = append(d, obj)
		} else {
			// Let it expire
		}
	}
	p.data = d
}

// Print is
func (p *PartitionRing) Print() {
	p.Lock()
	defer p.Unlock()

	for _, obj := range p.data {
		log.Printf("%d\t%d\t%d", obj[0], obj[1], obj[2])
	}
}
