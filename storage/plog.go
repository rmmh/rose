package storage

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrBitrot is returned when a data sector's content no longer matches the
// hash recorded for it. Callers protecting a virtual log with redundancy treat
// it like a missing shard and fall through to another copy.
var ErrBitrot = errors.New("bitrot detected")

// Plog represents an append-only physical log file on disk.
//
// Data is stored in fixed 4KB sectors. After every 255 data sectors the plog
// emits a 4KB hash sector holding the 16-byte hash of each preceding data
// sector plus an HMAC over those hashes, giving 0.4% overhead and letting any
// data sector be validated against bitrot. Logical offsets address the data
// stream only; the hash sectors are interposed transparently.
//
// The trailing partial sector is held in memory and persisted on Commit as a
// "ragged edge": subsequent writes overwrite it in place so sectors stay 4KB
// aligned and immutable once sealed. Its hash is only recorded once it fills,
// so within-session reads of it are trusted from the buffer.
//
// The sealed sectors of the still-open block (those before a block completes and
// emits its hash sector) have their hashes written inline on Commit, in an
// HMAC-protected "open trailer" sector placed immediately after the ragged-edge
// sector. Continued writes overwrite the trailer as the block grows, and the
// block's real hash sector replaces it once the block completes. On reload a
// valid trailer is authoritative: it yields the exact committed length and the
// hashes the sectors had when last made durable, so a sector that rotted while
// the process was down no longer matches and reads fail with ErrBitrot. Without
// it (a fresh block, or a torn write that overwrote the old trailer before the
// next Commit) the loader falls back to recomputing (trusting) the sectors,
// exactly the pre-trailer behavior.
type Plog struct {
	mu            sync.Mutex
	id            uint32
	file          *os.File
	logicalLength int64 // total logical bytes, including the open buffered sector

	buf        []byte // open trailing sector, 0..4096 bytes (sealed once full)
	hashes     []byte // hashes of sealed sectors in the current open block
	hashSector [SectorSize]byte
}

// openTrailerMagic tags the inline open-block trailer sector Commit writes after
// the ragged edge. openTrailerHeader is its fixed prefix before the sealed-sector
// hashes: the magic, a uint16 sealed-sector count, and a uint16 ragged-edge
// length. The whole prefix plus the hashes is then covered by a trailing HMAC.
const (
	openTrailerMagic  = "ROSEOPB1"
	openTrailerHeader = 12
)

const (
	SectorSize     = 4096
	HashesPerBlock = 255
	HashSize       = 16

	// blockPhysical is the on-disk span of a full hash-protected block: 255
	// data sectors followed by a single hash sector.
	blockPhysical = (HashesPerBlock + 1) * SectorSize
	dataPerBlock  = HashesPerBlock * SectorSize
)

// bitrotKey keys the HMAC stored alongside each block of sector hashes. It is a
// placeholder until per-volume keys are provisioned.
var bitrotKey = []byte("rose-bitrot-key-todo")

func sectorHash(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:HashSize]
}

func OpenPlog(path string, id uint32) (*Plog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open plog: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	p := &Plog{
		id:            id,
		file:          f,
		logicalLength: calcLogical(info.Size()),
		buf:           make([]byte, 0, SectorSize),
		hashes:        make([]byte, 0, HashesPerBlock*HashSize),
	}
	if err := p.reload(); err != nil {
		f.Close()
		return nil, err
	}
	return p, nil
}

// reload reconstructs the in-memory open sector and the hashes of already
// sealed sectors in the current open block, so writing can continue and recent
// data stays verifiable after a restart.
func (p *Plog) reload() error {
	info, err := p.file.Stat()
	if err != nil {
		return err
	}

	// Prefer the inline open-block trailer: the last sector of a cleanly committed
	// open block is an HMAC-protected record of that block's sealed-sector hashes,
	// its sector count, and the ragged-edge length. When it validates it is
	// authoritative -- it gives the exact committed length and the hashes the
	// sectors had when last made durable, so a sector that rotted while we were
	// down no longer matches its recorded hash and reads fail with ErrBitrot.
	if p.recoverFromTrailer(info.Size()) {
		return nil
	}

	// No valid trailer (a fresh/just-completed block, or a torn write that
	// overwrote the old trailer before the next Commit): trust the bytes. The
	// length is the size-derived value OpenPlog already set; recompute the open
	// block's sealed hashes from the very sectors they protect, the original
	// pre-trailer behavior.
	partial := p.logicalLength % SectorSize
	sealed := p.logicalLength - partial
	if partial > 0 {
		p.buf = p.buf[:partial]
		if _, err := p.file.ReadAt(p.buf, calcPhysical(sealed)); err != nil {
			return fmt.Errorf("reload plog %d open sector: %w", p.id, err)
		}
	}
	blockStart := (sealed / dataPerBlock) * dataPerBlock
	for s := blockStart; s < sealed; s += SectorSize {
		sector := make([]byte, SectorSize)
		if _, err := p.file.ReadAt(sector, calcPhysical(s)); err != nil {
			return fmt.Errorf("reload plog %d sector at %d: %w", p.id, s, err)
		}
		p.hashes = append(p.hashes, sectorHash(sector)...)
	}
	return nil
}

// recoverFromTrailer reconstructs the open block's geometry and sealed-sector
// hashes from the inline trailer Commit writes as the last sector of a cleanly
// committed open block. It returns true and sets logicalLength, buf, and hashes
// when a valid trailer is found; false leaves the Plog for reload's
// trust-the-bytes fallback. The HMAC (and the requirement that the implied block
// start be block-aligned) means a real data sector left in the trailer's place
// by a torn write is rejected rather than mistaken for one.
func (p *Plog) recoverFromTrailer(size int64) bool {
	if size < 2*SectorSize {
		return false
	}
	trailer := make([]byte, SectorSize)
	if n, err := p.file.ReadAt(trailer, size-SectorSize); err != nil && n < SectorSize {
		return false // a trailer is always a full sector; a short read means none
	}
	if string(trailer[:len(openTrailerMagic)]) != openTrailerMagic {
		return false
	}
	c := int(binary.LittleEndian.Uint16(trailer[8:10]))
	raggedLen := int(binary.LittleEndian.Uint16(trailer[10:12]))
	// The open block never holds a full block's worth of sealed sectors (the
	// 255th seal emits the hash sector and clears them), so c is 1..254.
	if c < 1 || c >= HashesPerBlock || raggedLen >= SectorSize {
		return false
	}
	hashesEnd := openTrailerHeader + c*HashSize
	mac := hmac.New(sha256.New, bitrotKey)
	mac.Write(trailer[:hashesEnd])
	if !hmac.Equal(mac.Sum(nil)[:HashSize], trailer[hashesEnd:hashesEnd+HashSize]) {
		return false
	}
	// The trailer sits at block-position c+1, so the block begins c+1 sectors
	// before it; that start must land on a block boundary to be the real thing.
	blockStartPhys := (size - SectorSize) - int64(c+1)*SectorSize
	if blockStartPhys < 0 || blockStartPhys%blockPhysical != 0 {
		return false
	}
	blockStartLogical := (blockStartPhys / blockPhysical) * dataPerBlock
	sealed := blockStartLogical + int64(c)*SectorSize
	if raggedLen > 0 {
		p.buf = p.buf[:raggedLen]
		if _, err := p.file.ReadAt(p.buf, calcPhysical(sealed)); err != nil {
			p.buf = p.buf[:0]
			return false
		}
	}
	p.logicalLength = sealed + int64(raggedLen)
	p.hashes = append(p.hashes[:0], trailer[openTrailerHeader:hashesEnd]...)
	return true
}

func calcLogical(phys int64) int64 {
	// Every 255 * 4096 bytes of data is followed by 1 * 4096 bytes of hashes.
	// We need to calculate how many data bytes are in `phys` bytes.
	chunks := phys / blockPhysical
	rem := phys % blockPhysical

	logical := chunks * dataPerBlock
	if rem > int64(dataPerBlock) {
		logical += int64(dataPerBlock) // the rest is the hash block itself
	} else {
		logical += rem
	}
	return logical
}

func calcPhysical(logical int64) int64 {
	chunks := logical / dataPerBlock
	rem := logical % dataPerBlock
	return chunks*blockPhysical + rem
}

// Write appends data to the plog and returns the starting logical offset.
func (p *Plog) Write(txnID int64, data []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writeLocked(data)
}

func (p *Plog) writeLocked(data []byte) (int64, error) {

	offset := p.logicalLength
	pos := 0
	for pos < len(data) {
		space := SectorSize - len(p.buf)
		toWrite := len(data) - pos
		if toWrite > space {
			toWrite = space
		}
		p.buf = append(p.buf, data[pos:pos+toWrite]...)
		pos += toWrite
		p.logicalLength += int64(toWrite)

		if len(p.buf) == SectorSize {
			if err := p.sealSector(); err != nil {
				return 0, err
			}
		}
	}
	return offset, nil
}

// EnsureAppend makes the byte range [offset, offset+len(data)) present without
// ever appending a duplicate.  It is the physical retry primitive used by a
// leased vlog after an RPC or a multi-shard fan-out has an unknown outcome.
// Existing bytes must match exactly; a mismatch is corruption or a conflicting
// reservation, not a condition that can safely be retried.
func (p *Plog) EnsureAppend(offset int64, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset < 0 || offset > p.logicalLength {
		return fmt.Errorf("ensure append plog %d: offset %d beyond length %d", p.id, offset, p.logicalLength)
	}
	overlap := p.logicalLength - offset
	if overlap > int64(len(data)) {
		overlap = int64(len(data))
	}
	if overlap > 0 {
		existing, err := p.readLocked(offset, int(overlap))
		if err != nil {
			return err
		}
		if !bytes.Equal(existing, data[:overlap]) {
			return fmt.Errorf("ensure append plog %d: existing bytes differ at offset %d", p.id, offset)
		}
	}
	if overlap == int64(len(data)) {
		return nil
	}
	_, err := p.writeLocked(data[overlap:])
	return err
}

// sealSector writes the now-full open sector to its fixed physical position and
// records its hash, emitting a hash sector when the block completes.
func (p *Plog) sealSector() error {
	sectorStart := p.logicalLength - int64(len(p.buf))
	if _, err := p.file.WriteAt(p.buf, calcPhysical(sectorStart)); err != nil {
		return fmt.Errorf("seal plog %d sector: %w", p.id, err)
	}
	p.hashes = append(p.hashes, sectorHash(p.buf)...)
	p.buf = p.buf[:0]

	if len(p.hashes) == HashesPerBlock*HashSize {
		for i := range p.hashSector {
			p.hashSector[i] = 0
		}
		copy(p.hashSector[:], p.hashes)
		mac := hmac.New(sha256.New, bitrotKey)
		mac.Write(p.hashes)
		copy(p.hashSector[HashesPerBlock*HashSize:], mac.Sum(nil)[:HashSize])

		// The hash sector sits right after the 255 data sectors just sealed.
		sealed := p.logicalLength - int64(len(p.buf))
		blockIdx := sealed/dataPerBlock - 1
		if _, err := p.file.WriteAt(p.hashSector[:], blockIdx*blockPhysical+dataPerBlock); err != nil {
			return fmt.Errorf("write plog %d hash sector: %w", p.id, err)
		}
		p.hashes = p.hashes[:0]
	}
	return nil
}

// Read reads length bytes from logical offset, verifying the recorded hash of
// every sealed data sector it touches. A sector whose content no longer matches
// its hash fails the read with ErrBitrot rather than returning corrupt bytes.
func (p *Plog) Read(offset int64, length int) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.readLocked(offset, length)
}

func (p *Plog) readLocked(offset int64, length int) ([]byte, error) {

	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("read plog %d: invalid offset %d length %d", p.id, offset, length)
	}
	end := offset + int64(length)
	if end > p.logicalLength {
		return nil, fmt.Errorf("read plog %d past end: %d > %d", p.id, end, p.logicalLength)
	}

	sealed := p.logicalLength - int64(len(p.buf))
	out := make([]byte, 0, length)
	for cur := offset; cur < end; {
		sectorIdx := cur / SectorSize
		sectorStart := sectorIdx * SectorSize

		var sector []byte
		if sectorStart >= sealed {
			// The open, not-yet-sealed sector: trusted from the buffer.
			sector = p.buf
		} else {
			var err error
			sector, err = p.readDataSector(sectorIdx, sealed)
			if err != nil {
				return nil, err
			}
		}

		inner := cur - sectorStart
		innerEnd := int64(SectorSize)
		if innerEnd > int64(len(sector)) {
			innerEnd = int64(len(sector))
		}
		if sectorStart+innerEnd > end {
			innerEnd = end - sectorStart
		}
		out = append(out, sector[inner:innerEnd]...)
		cur = sectorStart + innerEnd
	}
	return out, nil
}

// readDataSector reads one sealed data sector and verifies it against its
// recorded hash.
func (p *Plog) readDataSector(sectorIdx, sealed int64) ([]byte, error) {
	sectorStart := sectorIdx * SectorSize
	size := int64(SectorSize)
	if sectorStart+size > sealed {
		size = sealed - sectorStart
	}
	sector := make([]byte, size)
	if _, err := p.file.ReadAt(sector, calcPhysical(sectorStart)); err != nil {
		return nil, fmt.Errorf("read plog %d sector %d: %w", p.id, sectorIdx, err)
	}
	expected, ok, err := p.sectorHashFor(sectorIdx, sealed)
	if err != nil {
		return nil, err
	}
	if ok && !bytes.Equal(sectorHash(sector), expected) {
		return nil, fmt.Errorf("plog %d sector %d (logical %d): %w", p.id, sectorIdx, sectorStart, ErrBitrot)
	}
	return sector, nil
}

// sectorHashFor returns the recorded 16-byte hash of a sealed data sector.
// Sectors in a completed block read their hash from the on-disk hash sector;
// sectors still in the open block read it from the in-memory accumulator.
func (p *Plog) sectorHashFor(sectorIdx, sealed int64) ([]byte, bool, error) {
	blockIdx := sectorIdx / HashesPerBlock
	posInBlock := sectorIdx % HashesPerBlock
	blockEndLogical := (blockIdx + 1) * dataPerBlock

	if sealed >= blockEndLogical {
		hashSectorPhys := blockIdx*blockPhysical + dataPerBlock
		hash := make([]byte, HashSize)
		if _, err := p.file.ReadAt(hash, hashSectorPhys+posInBlock*HashSize); err != nil {
			return nil, false, fmt.Errorf("read plog %d hash sector %d: %w", p.id, blockIdx, err)
		}
		return hash, true, nil
	}

	start := int(posInBlock) * HashSize
	if start+HashSize > len(p.hashes) {
		return nil, false, nil
	}
	return p.hashes[start : start+HashSize], true, nil
}

// ScrubResult reports the outcome of validating a plog's persisted blocks.
type ScrubResult struct {
	SectorsChecked int64
	// CorruptSectors lists the logical byte offsets of data sectors whose hash
	// no longer matches their content.
	CorruptSectors []int64
	// BadHMACBlocks lists block indices whose hash sector failed its HMAC, which
	// indicates the integrity metadata itself was damaged.
	BadHMACBlocks []int64
}

func (r ScrubResult) Healthy() bool {
	return len(r.CorruptSectors) == 0 && len(r.BadHMACBlocks) == 0
}

// Scrub sequentially validates every completed hash-protected block,
// recomputing each data sector's hash and the per-block HMAC. It reads strictly
// forward to stay friendly to bulk sequential IO, matching the README's
// scrubbing goal. The open trailing block (data not yet sealed by a hash
// sector) is left to read-time verification within the writing session.
func (p *Plog) Scrub() (ScrubResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	sealed := p.logicalLength - int64(len(p.buf))
	completeBlocks := sealed / dataPerBlock

	var res ScrubResult
	for blockIdx := int64(0); blockIdx < completeBlocks; blockIdx++ {
		hashSector := make([]byte, SectorSize)
		if _, err := p.file.ReadAt(hashSector, blockIdx*blockPhysical+dataPerBlock); err != nil {
			return res, fmt.Errorf("scrub plog %d hash sector %d: %w", p.id, blockIdx, err)
		}
		recorded := hashSector[:HashesPerBlock*HashSize]
		mac := hmac.New(sha256.New, bitrotKey)
		mac.Write(recorded)
		if !hmac.Equal(mac.Sum(nil)[:HashSize], hashSector[HashesPerBlock*HashSize:HashesPerBlock*HashSize+HashSize]) {
			res.BadHMACBlocks = append(res.BadHMACBlocks, blockIdx)
		}
		for pos := int64(0); pos < HashesPerBlock; pos++ {
			sectorIdx := blockIdx*HashesPerBlock + pos
			sector := make([]byte, SectorSize)
			if _, err := p.file.ReadAt(sector, calcPhysical(sectorIdx*SectorSize)); err != nil {
				return res, fmt.Errorf("scrub plog %d sector %d: %w", p.id, sectorIdx, err)
			}
			res.SectorsChecked++
			expected := recorded[pos*HashSize : pos*HashSize+HashSize]
			if !bytes.Equal(sectorHash(sector), expected) {
				res.CorruptSectors = append(res.CorruptSectors, sectorIdx*SectorSize)
			}
		}
	}
	return res, nil
}

// Commit flushes buffered data, including its integrity metadata, and makes it
// durable before metadata may publish references to the written range. The open
// partial sector is written as a ragged edge that later writes overwrite in
// place.
func (p *Plog) Commit() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	raggedLen := len(p.buf)
	sealed := p.logicalLength - int64(raggedLen)
	if raggedLen > 0 {
		if _, err := p.file.WriteAt(p.buf, calcPhysical(sealed)); err != nil {
			return fmt.Errorf("commit plog %d ragged edge: %w", p.id, err)
		}
	}
	// Record the open block's sealed-sector hashes inline, in an HMAC-protected
	// trailer sector one position past the ragged edge. Continued writes overwrite
	// it as the block grows, and the block's real hash sector replaces it on
	// completion. The trailer only describes sectors already written above, so a
	// single fsync makes the ragged edge, the trailer, and any sectors sealed this
	// session durable together -- no second sync. A fresh or just-completed block
	// holds no sealed sectors, so nothing to protect and no trailer; c only rises
	// within a block, so no stale trailer can survive for the loader to mistake.
	if len(p.hashes) > 0 {
		if _, err := p.file.WriteAt(p.buildOpenTrailer(raggedLen), calcPhysical(sealed)+SectorSize); err != nil {
			return fmt.Errorf("commit plog %d open trailer: %w", p.id, err)
		}
	}
	return p.file.Sync()
}

// buildOpenTrailer assembles the inline open-block trailer sector: the magic, the
// sealed-sector count and ragged-edge length, the sealed-sector hashes, then an
// HMAC over all of that, zero-padded to a full sector.
func (p *Plog) buildOpenTrailer(raggedLen int) []byte {
	trailer := make([]byte, SectorSize)
	copy(trailer, openTrailerMagic)
	binary.LittleEndian.PutUint16(trailer[8:10], uint16(len(p.hashes)/HashSize))
	binary.LittleEndian.PutUint16(trailer[10:12], uint16(raggedLen))
	copy(trailer[openTrailerHeader:], p.hashes)
	hashesEnd := openTrailerHeader + len(p.hashes)
	mac := hmac.New(sha256.New, bitrotKey)
	mac.Write(trailer[:hashesEnd])
	copy(trailer[hashesEnd:], mac.Sum(nil)[:HashSize])
	return trailer
}

// Sync is retained as the low-level durability primitive.
func (p *Plog) Sync() error { return p.Commit() }

// LogicalLength reports the total logical bytes stored, including the open
// trailing sector. Reprotect reads a shard's whole stream by this length to
// regenerate a sibling shard lost to a failed disk.
func (p *Plog) LogicalLength() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.logicalLength
}

// Close closes the underlying file.
func (p *Plog) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.file.Close()
}
