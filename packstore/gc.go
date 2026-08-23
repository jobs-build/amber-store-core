// The GC surface: format-neutral views of sealed segments for the collector
// in package gc. See architecture/simple-gc.md.

package packstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jobs-build/amber-store-core/amberpack"
	"github.com/jobs-build/amber-store-core/key"
)

// ErrUnknownSegment reports an id that names no sealed segment (never
// sealed, or already removed).
var ErrUnknownSegment = errors.New("packstore: no such segment")

// SegmentInfo describes one sealed segment.
type SegmentInfo struct {
	ID     uint64
	Sealed time.Time // .seg mtime: the footer write is the file's last write
	Body   int64     // record bytes: body length minus the header magic
	Keys   uint64
}

// Segments lists the sealed segments, ascending id. The active segment is
// not listed; it is never a reaping victim.
func (s *Store) Segments() ([]SegmentInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	out := make([]SegmentInfo, 0, len(s.sealed))
	for _, g := range s.sealed {
		st, err := os.Stat(g.path)
		if err != nil {
			return nil, err
		}
		out = append(out, SegmentInfo{
			ID:     g.id,
			Sealed: st.ModTime(),
			Body:   g.fv.bodyLen - int64(len(magicHeader)),
			Keys:   g.fv.keyCount,
		})
	}
	return out, nil
}

// pinSegment finds sealed segment id and registers a scrub so the mmap
// outlives the caller's walk (Remove waits on scrubs before munmap). The
// caller must s.scrubs.Done() when finished with the segment.
func (s *Store) pinSegment(id uint64) (*sealedSegment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	for _, g := range s.sealed {
		if g.id == id {
			s.scrubs.Add(1)
			return g, nil
		}
	}
	return nil, ErrUnknownSegment
}

// ScanIndex walks segment id's footer index in index order (fanout on the
// key's last byte, then full key), calling fn with each entry's key, record
// offset and stored payload length. No pack body is read.
func (s *Store) ScanIndex(id uint64, fn func(k key.Key, off uint64, slen uint32)) error {
	seg, err := s.pinSegment(id)
	if err != nil {
		return err
	}
	defer s.scrubs.Done()
	fv := seg.fv
	for i := uint64(0); i < fv.keyCount; i++ {
		e := fv.entries[i*indexEntrySize : (i+1)*indexEntrySize]
		var k key.Key
		copy(k[:], e[:key.Size])
		fn(k, binary.BigEndian.Uint64(e[32:40]), binary.BigEndian.Uint32(e[40:44]))
	}
	return nil
}

// Record returns the raw record at off in segment id, CRC-checked. The
// returned bytes are caller-owned and re-appendable verbatim.
func (s *Store) Record(id, off uint64) ([]byte, error) {
	seg, err := s.pinSegment(id)
	if err != nil {
		return nil, err
	}
	defer s.scrubs.Done()
	body := uint64(seg.fv.bodyLen)
	// Overflow-safe: off may come from a crafted index; off+RecHeaderSize
	// could wrap. Same shape as the sealed-segment readers in footer.go.
	if off < uint64(len(magicHeader)) || off > body || uint64(amberpack.RecHeaderSize) > body-off {
		return nil, fmt.Errorf("%w: %s: record offset %d out of range", ErrCorrupt, seg.path, off)
	}
	rec, err := amberpack.ParseRecord(seg.mm[off:body])
	if err != nil {
		return nil, fmt.Errorf("%s: offset %d: %w", seg.path, off, err)
	}
	end := off + amberpack.RecHeaderSize + uint64(rec.Slen)
	out := make([]byte, end-off)
	copy(out, seg.mm[off:end])
	return out, nil
}

// HasOutside reports whether k is stored anywhere but segment id: the
// active segment or any other sealed segment. Reaping uses it to skip
// records that already survived.
func (s *Store) HasOutside(id uint64, k key.Key) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false, ErrClosed
	}
	if s.active != nil {
		if _, ok := s.active.index[k]; ok {
			return true, nil
		}
	}
	for i := len(s.sealed) - 1; i >= 0; i-- {
		if s.sealed[i].id == id {
			continue
		}
		if s.sealed[i].has(k) {
			return true, nil
		}
	}
	return false, nil
}

// AppendRecord re-appends an already-encoded record through the normal
// append path: no decode, no re-encode, no fsync — callers batch appends
// and call Sync. raw must be exactly one record, CRC-valid, keyed k.
func (s *Store) AppendRecord(k key.Key, raw []byte) error {
	rec, err := amberpack.ParseRecord(raw)
	if err != nil {
		return err
	}
	if rec.Key != k {
		return fmt.Errorf("%w: record key %s does not match %s", ErrCorrupt, rec.Key, k)
	}
	if len(raw) != amberpack.RecHeaderSize+int(rec.Slen) {
		return fmt.Errorf("%w: record is %d bytes, want %d", ErrCorrupt, len(raw), amberpack.RecHeaderSize+int(rec.Slen))
	}
	return s.append(k, raw, false)
}

// Sync fsyncs the active segment, making every append so far durable —
// AppendRecord batches end with one Sync.
func (s *Store) Sync() error {
	return s.syncActive()
}
