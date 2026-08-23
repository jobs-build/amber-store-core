package packstore

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jobs-build/amber-store-core/amberpack"
	"github.com/jobs-build/amber-store-core/key"
)

// gcStore returns an open store with objs sealed into segments of 8 KiB.
func gcStore(t *testing.T, objs []Object) *Store {
	t.Helper()
	dir := sealedStore(t, objs) // Put + Close seals via rotation at 8 KiB
	return openStore(t, dir)
}

func TestSegments(t *testing.T) {
	objs := testObjects(t, 24) // ~2 KiB each: several 8 KiB segments
	s := gcStore(t, objs)
	segs, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) == 0 {
		t.Fatal("no sealed segments")
	}
	var keys uint64
	for _, g := range segs {
		if g.Body <= 0 {
			t.Errorf("segment %d: Body = %d", g.ID, g.Body)
		}
		if g.Sealed.IsZero() || g.Sealed.After(time.Now()) {
			t.Errorf("segment %d: Sealed = %v", g.ID, g.Sealed)
		}
		keys += g.Keys
	}
	// Keys across sealed segments plus whatever stayed active must cover objs.
	if keys > uint64(len(objs)) {
		t.Errorf("Keys total %d > %d objects", keys, len(objs))
	}
}

func TestScanIndexCoversBody(t *testing.T) {
	s := gcStore(t, testObjects(t, 24))
	segs, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range segs {
		var sum int64
		var n uint64
		err := s.ScanIndex(g.ID, func(k key.Key, off uint64, slen uint32) {
			sum += int64(amberpack.RecHeaderSize) + int64(slen)
			n++
			if off < 8 { // records start after the header magic
				t.Errorf("segment %d: off %d inside magic", g.ID, off)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if n != g.Keys {
			t.Errorf("segment %d: scanned %d entries, Keys = %d", g.ID, n, g.Keys)
		}
		if sum != g.Body {
			t.Errorf("segment %d: Σ(46+slen) = %d, Body = %d", g.ID, sum, g.Body)
		}
	}
	if err := s.ScanIndex(99999, func(key.Key, uint64, uint32) {}); !errors.Is(err, ErrUnknownSegment) {
		t.Errorf("unknown id: err = %v, want ErrUnknownSegment", err)
	}
}

func TestRecordRoundTrip(t *testing.T) {
	objs := testObjects(t, 24)
	s := gcStore(t, objs)
	segs, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range segs {
		var scanErr error
		err := s.ScanIndex(g.ID, func(k key.Key, off uint64, slen uint32) {
			raw, err := s.Record(g.ID, off)
			if err != nil {
				scanErr = err
				return
			}
			rec, err := amberpack.ParseRecord(raw)
			if err != nil {
				scanErr = err
				return
			}
			if rec.Key != k || rec.Slen != slen || len(raw) != amberpack.RecHeaderSize+int(slen) {
				t.Errorf("segment %d off %d: record mismatch", g.ID, off)
			}
		})
		if err != nil || scanErr != nil {
			t.Fatal(err, scanErr)
		}
	}
	// Bad offsets fail loudly, never panic.
	if _, err := s.Record(segs[0].ID, 0); err == nil {
		t.Error("Record at offset 0 (magic) should fail")
	}
	if _, err := s.Record(segs[0].ID, 1<<40); err == nil {
		t.Error("Record far out of range should fail")
	}
	// A CRC-corrupt record is reported: flip one payload byte via the mmap?
	// The mmap is read-only; instead corrupt the file on disk of a closed
	// store and reopen — covered separately in TestRecordCorrupt below.
}

func TestRecordCorrupt(t *testing.T) {
	// 8 objects: testObjects alternates highly-compressible (~78-byte record)
	// and incompressible (~2048-byte record) payloads, so 6 would stay under
	// the 8 KiB rotation threshold and never seal. 8 crosses it deterministically.
	objs := testObjects(t, 8)
	dir := sealedStore(t, objs)
	// Find the sealed file and flip a byte inside the first record's payload
	// (offset 8 is the first record header; 8+46 is its first payload byte).
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var seg string
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".seg") {
			seg = filepath.Join(dir, e.Name())
			break
		}
	}
	if seg == "" {
		t.Fatal("no sealed segment")
	}
	f, err := os.OpenFile(seg, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	buf := []byte{0}
	if _, err := f.ReadAt(buf, 8+46); err != nil {
		t.Fatal(err)
	}
	buf[0] ^= 0xFF
	if _, err := f.WriteAt(buf, 8+46); err != nil {
		t.Fatal(err)
	}
	f.Close()
	s := openStore(t, dir)
	segs, err := s.Segments()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Record(segs[0].ID, 8); !errors.Is(err, ErrCorrupt) {
		t.Errorf("Record of corrupt payload: err = %v, want ErrCorrupt", err)
	}
}
