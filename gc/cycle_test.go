package gc

import (
	"context"
	"errors"
	"hash/crc32"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jobs-build/amber-store-core/fstree"
	"github.com/jobs-build/amber-store-core/key"
	"github.com/jobs-build/amber-store-core/packstore"
)

// storeInterleavedTrees stores two FileNode trees of n incompressible
// 256-byte blobs each, alternating one blob of "a" and one of "b", so every
// pack holds records of both. Roots go last. Returns each tree's root and
// keys.
func storeInterleavedTrees(t *testing.T, st *packstore.Store, n int) (rootA key.Key, keysA []key.Key, rootB key.Key, keysB []key.Key) {
	t.Helper()
	blob := func(seed string, i int) key.Key {
		rng := rand.New(rand.NewPCG(uint64(crc32.ChecksumIEEE([]byte(seed))), uint64(i)))
		data := make([]byte, 256)
		for j := range data {
			data[j] = byte(rng.UintN(256))
		}
		o, err := fstree.EncodeBlob(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
		return o.Key
	}
	for i := 0; i < n; i++ {
		keysA = append(keysA, blob("a", i))
		keysB = append(keysB, blob("b", i))
	}
	root := func(children []key.Key) key.Key {
		o, err := fstree.EncodeFileNode(children)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
		return o.Key
	}
	rootA, rootB = root(keysA), root(keysB)
	return rootA, append(keysA, rootA), rootB, append(keysB, rootB)
}

// backdatePacks pushes every sealed .seg mtime two hours into the past so
// grace does not protect them.
func backdatePacks(t *testing.T, ts *testStore) {
	t.Helper()
	dir := filepath.Join(ts.dir, "packstore")
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".seg") {
			if err := os.Chtimes(filepath.Join(dir, e.Name()), old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func segFiles(t *testing.T, ts *testStore) int {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(ts.dir, "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".seg") {
			n++
		}
	}
	return n
}

func TestCycleReapsDeadPacks(t *testing.T) {
	ts := newTestStore(t, 4<<10) // small packs: lots of segments
	rootLive, liveKeys := storeTree(t, ts.objects, "live", 8)
	rootDead, _ := storeTree(t, ts.objects, "dead", 40) // most bytes die
	c := ts.openCollector(t, Options{Grace: time.Hour})
	putTestRef(t, c, ts.refs, "live", rootLive)
	putTestRef(t, c, ts.refs, "dead", rootDead)
	// Drop the dead root.
	if err := ts.refs.Delete("dead"); err != nil {
		t.Fatal(err)
	}
	if err := c.ReleaseRef(rootDead); err != nil {
		t.Fatal(err)
	}
	backdatePacks(t, ts)
	before := segFiles(t, ts)
	stats, err := c.Run(context.Background(), -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats.Reaped) == 0 {
		t.Fatal("no packs reaped")
	}
	if stats.FreedBytes <= 0 {
		t.Error("nothing freed")
	}
	if stats.FreedBytes < stats.CopiedBytes {
		t.Errorf("freed %d < copied %d: victims must free at least what they copy", stats.FreedBytes, stats.CopiedBytes)
	}
	if after := segFiles(t, ts); after > before {
		t.Errorf("segment files %d -> %d, must not grow", before, after)
	}
	// Every live object still reads back.
	for _, k := range liveKeys {
		if _, err := ts.objects.Get(k); err != nil {
			t.Fatalf("live object %s lost: %v", k, err)
		}
	}
}

func TestCycleSparesProtectedPacks(t *testing.T) {
	ts := newTestStore(t, 4<<10)
	root, _ := storeTree(t, ts.objects, "gr", 20)
	c := ts.openCollector(t, Options{Grace: time.Hour})
	putTestRef(t, c, ts.refs, "v", root)
	if err := ts.refs.Delete("v"); err != nil {
		t.Fatal(err)
	}
	if err := c.ReleaseRef(root); err != nil {
		t.Fatal(err)
	}
	// No backdating: every pack sealed within grace.
	before := segFiles(t, ts)
	stats, err := c.Run(context.Background(), -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats.Reaped) != 0 || segFiles(t, ts) != before {
		t.Error("cycle reaped packs inside grace")
	}
}

func TestCycleForcedLine(t *testing.T) {
	ts := newTestStore(t, 4<<10)
	rootA, keysA := storeTree(t, ts.objects, "aa", 10)
	rootB, _ := storeTree(t, ts.objects, "bb", 10)
	c := ts.openCollector(t, Options{Grace: time.Hour})
	putTestRef(t, c, ts.refs, "a", rootA)
	putTestRef(t, c, ts.refs, "b", rootB)
	if err := ts.refs.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if err := c.ReleaseRef(rootB); err != nil {
		t.Fatal(err)
	}
	backdatePacks(t, ts)
	// Line 1.0: nothing has garbage > 1.0, nothing reaped.
	stats, err := c.Run(context.Background(), 1.0)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats.Reaped) != 0 {
		t.Error("line 1.0 still reaped")
	}
	// Line 0: every eligible pack with any garbage goes.
	stats, err = c.Run(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats.Reaped) == 0 {
		t.Error("line 0 reaped nothing")
	}
	for _, k := range keysA {
		if _, err := ts.objects.Get(k); err != nil {
			t.Fatalf("live object lost: %v", err)
		}
	}
	if stats.Threshold != 0 {
		t.Errorf("Threshold = %v, want 0", stats.Threshold)
	}
}

func TestCycleSkipsWhenNothingChanged(t *testing.T) {
	ts := newTestStore(t, 4<<10)
	root, _ := storeTree(t, ts.objects, "sk", 10)
	c := ts.openCollector(t, Options{Grace: time.Hour})
	putTestRef(t, c, ts.refs, "v", root)
	backdatePacks(t, ts)
	first, err := c.Run(context.Background(), -1)
	if err != nil {
		t.Fatal(err)
	}
	if first.Skipped {
		t.Fatal("first cycle skipped")
	}
	second, err := c.Run(context.Background(), -1)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Skipped {
		t.Error("unchanged second cycle not skipped")
	}
	// A reference write un-skips.
	root2, _ := storeTree(t, ts.objects, "sk2", 3)
	putTestRef(t, c, ts.refs, "v2", root2)
	third, err := c.Run(context.Background(), -1)
	if err != nil {
		t.Fatal(err)
	}
	if third.Skipped {
		t.Error("cycle after union change skipped")
	}
}

func TestCyclePendingClosureWalkedFirst(t *testing.T) {
	ts := newTestStore(t, 4<<10)
	root, keys := storeTree(t, ts.objects, "pw", 10)
	c := ts.openCollector(t, Options{Grace: time.Hour})
	putTestRef(t, c, ts.refs, "v", root)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// Corrupt the closure; reopen leaves the root pending.
	path := filepath.Join(ts.dir, "closures", root.String()+tailsSuffix)
	if err := os.WriteFile(path, []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	c2 := ts.openCollector(t, Options{Grace: time.Hour})
	backdatePacks(t, ts)
	if _, err := c2.Run(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	// The walk restored ground truth: nothing referenced was reaped.
	for _, k := range keys {
		if _, err := ts.objects.Get(k); err != nil {
			t.Fatalf("referenced object %s reaped: %v", k, err)
		}
	}
	if _, ok := c2.d.read(root); !ok {
		t.Error("closure not rewritten before the cycle")
	}
}

func TestCycleOverlapRejected(t *testing.T) {
	ts := newTestStore(t, 4<<10)
	c := ts.openCollector(t, Options{})
	c.cycleMu.Lock()
	defer c.cycleMu.Unlock()
	if _, err := c.Run(context.Background(), -1); !errors.Is(err, ErrCycleRunning) {
		t.Errorf("overlapping Run: err = %v, want ErrCycleRunning", err)
	}
}

func TestThrottle(t *testing.T) {
	th := newThrottle(1 << 20) // 1 MiB/s
	start := time.Now()
	th.pace(1 << 18) // 256 KiB -> ~250ms owed
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Errorf("throttle slept %v, want ≈250ms", elapsed)
	}
	unlimited := newThrottle(0)
	start = time.Now()
	unlimited.pace(1 << 30)
	if time.Since(start) > 50*time.Millisecond {
		t.Error("unlimited throttle slept")
	}
}

// TestThrottleConcurrent: the copier's workers share one throttle, so
// pacing from several goroutines must be race-free and must bound the
// aggregate rate, not each goroutine's own.
func TestThrottleConcurrent(t *testing.T) {
	th := newThrottle(1 << 20) // 1 MiB/s
	start := time.Now()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 4 {
				th.pace(16 << 10) // 8 × 4 × 16 KiB = 512 KiB -> ~500ms owed in total
			}
		})
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Errorf("concurrent pacing took %v, want ≈500ms for 512 KiB at 1 MiB/s", elapsed)
	}
}

// TestReapParallelCopiesEveryLiveRecord pins the copier's accounting under
// a worker pool: a forced sweep over packs holding a mix of live and dead
// records must copy exactly the live records of every pack that has any
// garbage — each once, none from the active segment or from all-live
// packs — and every live object must still read back.
func TestReapParallelCopiesEveryLiveRecord(t *testing.T) {
	ts := newTestStore(t, 64<<10) // ~200 records per pack: real fan-out per victim
	rootLive, liveKeys, _, _ := storeInterleavedTrees(t, ts.objects, 400)
	c := ts.openCollector(t, Options{Grace: time.Hour, Jobs: 8})
	putTestRef(t, c, ts.refs, "live", rootLive)
	backdatePacks(t, ts)

	isLive := make(map[key.Key]bool, len(liveKeys))
	for _, k := range liveKeys {
		isLive[k] = true
	}
	segs, err := ts.objects.Segments()
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 3 {
		t.Fatalf("%d sealed packs, want several", len(segs))
	}
	var wantRecords int
	var wantBytes int64
	for _, seg := range segs {
		var live, dead int
		var liveBytes int64
		err := ts.objects.ScanIndex(seg.ID, func(k key.Key, off uint64, slen uint32) {
			if isLive[k] {
				live++
				liveBytes += 46 + int64(slen)
			} else {
				dead++
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if dead > 0 { // garbage > 0: reaped at the forced line
			wantRecords += live
			wantBytes += liveBytes
		}
	}
	if wantRecords == 0 {
		t.Fatal("no pack mixes live and dead records; the test needs interleaving")
	}

	stats, err := c.Run(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if stats.CopiedRecords != wantRecords || stats.CopiedBytes != wantBytes {
		t.Errorf("copied %d records / %d bytes, want %d / %d", stats.CopiedRecords, stats.CopiedBytes, wantRecords, wantBytes)
	}
	for _, k := range liveKeys {
		if _, err := ts.objects.Get(k); err != nil {
			t.Fatalf("live object %s lost: %v", k, err)
		}
	}
	// Reaped packs are gone; what remains scores clean.
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.GarbageBytes != 0 {
		t.Errorf("garbage after a forced sweep: %d bytes", st.GarbageBytes)
	}
}

// TestReapHonoursCancellation: a cancelled context stops the copier and
// surfaces as the context's error, not as a silent partial success.
func TestReapHonoursCancellation(t *testing.T) {
	ts := newTestStore(t, 64<<10)
	root, _ := storeTree(t, ts.objects, "cx", 400)
	c := ts.openCollector(t, Options{Grace: time.Hour, Jobs: 4})
	putTestRef(t, c, ts.refs, "cx", root)
	backdatePacks(t, ts)
	segs, err := ts.objects.Segments()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := c.reap(ctx, c.union.Load(), segs[0].ID, newThrottle(0)); !errors.Is(err, context.Canceled) {
		t.Errorf("reap with a cancelled context: err = %v, want context.Canceled", err)
	}
}

// TestThrottleOwed exercises throttleOwed directly, across the boundary
// where the old time.Duration(bytes)*time.Second formula overflowed int64
// (bytes > ~8.6 GiB): divide-before-multiply must stay exact and never go
// negative.
func TestThrottleOwed(t *testing.T) {
	const rate = int64(1 << 20) // 1 MiB/s
	cases := []struct {
		name string
		n    int64
		want time.Duration
	}{
		{"1MiB_exactly_1s", 1 << 20, time.Second},
		{"512KiB_exactly_500ms", 512 << 10, 500 * time.Millisecond},
		{"8GiB_below_old_boundary", 8 << 30, 8192 * time.Second},
		{"10GiB_above_old_boundary", 10 << 30, 10240 * time.Second},
		{"100GiB_no_overflow", 100 << 30, 102400 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := throttleOwed(c.n, rate)
			if got != c.want {
				t.Errorf("throttleOwed(%d, %d) = %v, want %v", c.n, rate, got, c.want)
			}
			if got < 0 {
				t.Errorf("throttleOwed(%d, %d) = %v, negative", c.n, rate, got)
			}
		})
	}
	// Monotonic across the old int64 overflow boundary (~8.6 GiB): the
	// pre-fix formula wrapped negative here, which would have made pace
	// stop throttling entirely partway through a large copy.
	if got8, got10 := throttleOwed(8<<30, rate), throttleOwed(10<<30, rate); got10 <= got8 {
		t.Errorf("throttleOwed not monotonic across the old overflow boundary: owed(8GiB)=%v, owed(10GiB)=%v", got8, got10)
	}
}

// TestDeletePackReTestsCurrentUnion exercises deletePack's delta-copy
// branch: reap() runs against a snapshot union that has nothing live, so it
// copies nothing; a reference then arrives naming the victim's objects
// before deletePack's exclusive re-test. deletePack must catch that under
// the removal lock, or the objects are lost when the pack is removed.
func TestDeletePackReTestsCurrentUnion(t *testing.T) {
	ts := newTestStore(t, 4<<10)
	root, keys := storeTree(t, ts.objects, "rt", 20)
	c := ts.openCollector(t, Options{Grace: time.Hour})
	backdatePacks(t, ts)
	segs, err := ts.objects.Segments()
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) == 0 {
		t.Fatal("no sealed segments")
	}
	victim := segs[0]
	// Reap against the snapshot union, which is empty: nothing is copied.
	if _, _, err := c.reap(context.Background(), c.union.Load(), victim.ID, newThrottle(0)); err != nil {
		t.Fatal(err)
	}
	// A reference arrives between reap and delete — the exact window the
	// exclusive re-test exists for.
	putTestRef(t, c, ts.refs, "late", root)
	if _, err := c.deletePack(victim.ID); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if _, err := ts.objects.Get(k); err != nil {
			t.Fatalf("object %s lost — deletePack did not re-test against the current union: %v", k, err)
		}
	}
}
