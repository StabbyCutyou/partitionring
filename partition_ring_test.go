package partitionring

import (
	"math/rand"
	"testing"
	"time"
)

func TestPartitionRing(t *testing.T) {
	step := int64(922337203685477579)
	p := New(0, MaxPartitionUpperBound, step, 2*time.Second)
	//log.Printf("%d", p.partitionStep)
	for i := int64(0); i < 11; i++ {
		lowerBound, upperBound, err := p.ReserveNext()

		lowerShould, upperShould := int64(0), int64(0)

		if i == 0 {
			lowerShould = 0
			upperShould = p.partitionStep - 1
		} else {
			previousUpperShould := i * p.partitionStep
			//previousUpperShould += -i
			//log.Printf("PREVIOUS UPPER: %d", previousUpperShould)
			lowerShould = previousUpperShould + i
			upperShould = lowerShould + p.partitionStep
			// The first one is at 1 * partitionStep, effectively prior upperBound + 1
			// it's upperBound should then be lowerBound + partitionStep, putting ...
			// this whole model of calculating it is wrong. Arg.
			if i == 1 {
				lowerShould += -1
				upperShould += -2
			} else if i == 10 {
				lowerShould += -2
				upperShould = MaxPartitionUpperBound
			} else {
				lowerShould += -2
				upperShould += -2
			}
		}
		//log.Printf("%d , %d", lowerShould-lowerBound, upperShould-upperBound)
		//log.Printf("%d - %d VS %d - %d", lowerBound, upperBound, lowerShould, upperShould)
		if lowerBound != lowerShould || upperBound != upperShould || err != nil {
			t.Errorf("FAIL %d != %d || %d != %d || %s", lowerBound, lowerShould, upperBound, upperShould, err)
		}
	}
}

func TestErrorOnFullPartitionRing(t *testing.T) {
	step := int64(922337203685477579)
	p := New(0, MaxPartitionUpperBound, step, 2*time.Minute)
	for i := int64(0); i < 11; i++ {
		p.ReserveNext()
	}
	_, _, err := p.ReserveNext()
	if err != nil && err != ErrNoPartitionsAvailable {
		t.Error(err)
	}
}

func TestExpiration(t *testing.T) {
	p := New(0, MaxPartitionUpperBound, 922337203685477579, 1*time.Second)
	//log.Printf("%d", p.partitionStep)
	for i := int64(0); i < 10; i++ {
		p.ReserveNext()
	}
	// We see random timing failures here, so padding a bit since the expiration isn't 100% on the picosecond with accuracy
	time.Sleep((1 * time.Second) + (10 * time.Millisecond))
	size := p.Size()
	if size > 0 {
		t.Errorf("P isn't empty: %d", size)
	}
}

// This tests expiration where the range exactly == a partition
func TestExpireRange(t *testing.T) {
	step := int64(922337203685477579)
	p := New(0, MaxPartitionUpperBound, step, 1*time.Minute)

	for i := int64(0); i < 10; i++ {
		p.ReserveNext()
	}

	lowerBound := int64(3689348814741910318)
	upperBound := int64(4611686018427387897)

	res := p.ExpireRange(lowerBound, upperBound)
	newLower, newUpper, err := p.ReserveNext()
	if newLower != lowerBound || newUpper != upperBound || err != nil {
		t.Errorf("Didn't work: %d, %d, %d, %s", newLower, newUpper, res, err)
	}
}

// This tests expiration where we expire two exact ranges in a row, then re-reserve them
func TestExpireRange2(t *testing.T) {
	step := int64(922337203685477579)
	p := New(0, MaxPartitionUpperBound, step, 1*time.Minute)

	for i := int64(0); i < 10; i++ {
		p.ReserveNext()
	}

	lowerBound := int64(3689348814741910318)
	upperBound := int64(4611686018427387897)
	lowerBound2 := int64(4611686018427387898)
	upperBound2 := lowerBound2 + step

	res := p.ExpireRange(lowerBound, upperBound)
	res = p.ExpireRange(lowerBound2, upperBound2)
	newLower, newUpper, err := p.ReserveNext()
	if newLower != lowerBound || newUpper != upperBound || err != nil {
		t.Errorf("Didn't work: %d, %d, %d, %s", newLower, newUpper, res, err)
	}
	newLower, newUpper, err = p.ReserveNext()
	if newLower != lowerBound2 || newUpper != upperBound2 || err != nil {
		t.Errorf("Didn't work: %d, %d, %d, %s", newLower, newUpper, res, err)
	}
}

// This tests expiration where the range exactly == a 2 partitions, and then re-reserving them
func TestExpireRange3(t *testing.T) {
	step := int64(922337203685477579)
	p := New(0, MaxPartitionUpperBound, step, 1*time.Minute)

	for i := int64(0); i < 10; i++ {
		p.ReserveNext()
	}

	lowerBoundM := int64(3689348814741910318)
	upperBoundM := int64(4611686018427387898) + step

	lowerBound := int64(3689348814741910318)
	upperBound := int64(4611686018427387897)
	lowerBound2 := int64(4611686018427387898)
	upperBound2 := lowerBound2 + step

	res := p.ExpireRange(lowerBoundM, upperBoundM)
	newLower, newUpper, err := p.ReserveNext()
	if newLower != lowerBound || newUpper != upperBound || err != nil {
		t.Errorf("Didn't work: %d, %d, %d, %s", newLower, newUpper, res, err)
	}
	newLower, newUpper, err = p.ReserveNext()
	if newLower != lowerBound2 || newUpper != upperBound2 || err != nil {
		t.Errorf("Didn't work: %d, %d, %d, %s", newLower, newUpper, res, err)
	}
}

// This tests the case where we expire a range that is completely within a partition
func TestExpireRange4(t *testing.T) {
	step := int64(922337203685477579)
	p := New(0, MaxPartitionUpperBound, step, 1*time.Minute)

	for i := int64(0); i < 10; i++ {
		p.ReserveNext()
	}

	lowerBound := int64(3689348814741910319)
	upperBound := int64(4611686018427387896)

	res := p.ExpireRange(lowerBound, upperBound)
	newLower, newUpper, err := p.ReserveNext()
	if newLower != lowerBound || newUpper != upperBound || err != nil {
		t.Errorf("Didn't work: %d, %d, %d, %s", newLower, newUpper, res, err)
	}
}

// This tests expiration where the range is exactly the upperbound of a partition,
// but the lowerbound leaves a remainder
func TestExpireRange5(t *testing.T) {
	step := int64(922337203685477579)
	p := New(0, MaxPartitionUpperBound, step, 1*time.Minute)

	for i := int64(0); i < 10; i++ {
		p.ReserveNext()
	}

	lowerBound := int64(3689348814741910319)
	upperBound := int64(4611686018427387897)

	res := p.ExpireRange(lowerBound, upperBound)
	newLower, newUpper, err := p.ReserveNext()
	if newLower != lowerBound || newUpper != upperBound || err != nil {
		t.Errorf("Didn't work: %d, %d, %d, %s", newLower, newUpper, res, err)
	}
}

// This tests expiration where the range is exactly the upperbound of a partition,
// but the lowerbound leaves a remainder
func TestExpireRange6(t *testing.T) {
	step := int64(922337203685477579)
	p := New(0, MaxPartitionUpperBound, step, 1*time.Minute)

	for i := int64(0); i < 10; i++ {
		p.ReserveNext()
	}

	lowerBound := int64(3689348814741910318)
	upperBound := int64(4611686018427387896)
	res := p.ExpireRange(lowerBound, upperBound)
	newLower, newUpper, err := p.ReserveNext()
	if newLower != lowerBound || newUpper != upperBound || err != nil {
		t.Errorf("Didn't work: %d, %d, %d, %s", newLower, newUpper, res, err)
	}
}

// This tests concurrent expiration and reservations
func TestExpireRangeConcurrent(t *testing.T) {
	step := int64(922337203685477579)
	//maxIDSize := *big.NewInt(math.MaxInt64)
	//randy, _ := rand.Int(rand.Reader, &maxIDSize)
	p := New(0, MaxPartitionUpperBound, step, 100*time.Millisecond)

	// Do one pass to reserve everything, record the offsets reserved for later use
	partitions := make([][2]int64, 0)
	for i := int64(0); i < 10; i++ {
		lower, upper, err := p.ReserveNext()
		if err != nil && err != ErrNoPartitionsAvailable && err != ErrCouldNotReservePartition {
			t.Fatal("We've lost the fight: ", err)
		}
		partitions = append(partitions, [2]int64{lower, upper})
	}
	// Let them all get close
	time.Sleep(90 * time.Millisecond)
	// Now, start 2 concurrent processes
	// 1. Beginning calling ReserveNext as fast as possible.

	killReserve70 := make(chan bool)
	killReserve100 := make(chan bool)
	killRelease50 := make(chan bool)

	go func(pr *PartitionRing, killChan chan bool) {
		for {
			select {
			case <-killChan:
				return
			default:
				/*low, up, _ := */ p.ReserveNext()
				//t.Logf("Reserving %d,%d = %d", low, up, time.Now().UnixNano())
				time.Sleep(85 * time.Microsecond)
			}
		}
	}(p, killReserve70)
	// 2. Begin releasing random partitions as fast as possible
	go func(pr *PartitionRing, parts [][2]int64, killChan chan bool) {
		for {
			select {
			case <-killChan:
				return
			default:
				randy := rand.Int() % 10
				//t.Logf("Popping %d,%d - %d", parts[randy][0], parts[randy][1], time.Now().UnixNano())
				pr.ExpireRange(parts[randy][0], parts[randy][1])
				time.Sleep(25 * time.Microsecond)
			}
		}
	}(p, partitions, killRelease50)

	go func(pr *PartitionRing, killChan chan bool) {
		for {
			select {
			case <-killChan:
				return
			default:
				p.ReserveNext()
				/*low, up, _ := */ //p.Print()
				//t.Logf("Reserving %d,%d = %d", low, up, time.Now().UnixNano())
				time.Sleep(150 * time.Microsecond)
			}
		}
	}(p, killReserve100)
	// Test succeeds if no race condition / deadlock / my laptops cpu doesn't melt
	timer := time.NewTimer(1 * time.Second)
	<-timer.C
	killRelease50 <- true
	killReserve70 <- true
	killReserve100 <- true
	// Not sure what I'm going to anchor my test to w/r/t a success condition, but using p.print seems to verify
	// it's working as expecteds

	p.Print()
}

func BenchmarkReserveNext(b *testing.B) {
	step := int64(922337203685477579)
	//maxIDSize := *big.NewInt(math.MaxInt64)
	//randy, _ := rand.Int(rand.Reader, &maxIDSize)
	p := New(0, MaxPartitionUpperBound, step, 100*time.Microsecond)

	for n := 0; n < b.N; n++ {
		p.ReserveNext()
	}
}
