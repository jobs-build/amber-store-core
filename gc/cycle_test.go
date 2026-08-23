package gc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
