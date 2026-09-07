package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/rmmh/rose/proto"
	"google.golang.org/protobuf/proto"
)

// ErrBitrot is returned when a data sector's content no longer matches the
// hash recorded for it. Callers protecting a virtual log with redundancy treat
// it like a missing shard and fall through to another copy.
var ErrBitrot = errors.New("bitrot detected")

// ErrUnrecognizedPlogFormat is returned when a plog file lacks the superblock
// magic: a foreign file, or a legacy headerless plog that predates the on-disk
// format. Such files are not supported and must be wiped and recreated.
var ErrUnrecognizedPlogFormat = errors.New("unrecognized plog format (missing superblock magic)")

// ErrPlogHeaderCorrupt is returned when a plog superblock has the magic but
// fails its HMAC or length checks: a torn write to sector 0, or tampering.
var ErrPlogHeaderCorrupt = errors.New("plog superblock corrupt")

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
// aligned and immutable once sealed. The open trailer authenticates its bytes
// across restarts; within-session reads use the trusted in-memory buffer.
//
// The sealed sectors of the still-open block (those before a block completes and
// emits its hash sector) have their hashes written inline on Commit, in an
// HMAC-protected "open trailer" sector placed immediately after the ragged-edge
// sector. Continued writes overwrite the trailer as the block grows, and the
// block's real hash sector replaces it once the block completes. On reload a
// valid trailer is authoritative: it yields the exact committed length and the
// hashes the sectors had when last made durable, so a sector that rotted while
// the process was down no longer matches and reads fail with ErrBitrot. Without
// it, open-block bytes remain unverified until catalog recovery authenticates
// them. A missing trailer must never make damaged bytes their own evidence.
type Plog struct {
	undoActive        bool
	undoStart         int64
	mu                sync.Mutex
	id                uint32
	file              *os.File
	writeAt           func([]byte, int64) (int, error) // optional per-file fault injection
	logicalLength     int64                            // total logical bytes, including the open buffered sector
	loadedFromTrailer bool                             // set when geometry came from an open-block trailer
	openUnverified    bool                             // some open-block bytes have no independent integrity evidence

	buf        []byte // open trailing sector, 0..4096 bytes (sealed once full)
	bufCorrupt bool   // ragged bytes failed authentication or await catalog verification
	hashes     []byte // hashes of sealed sectors in the current open block
	writeBuf   []byte // reusable batched-write scratch, grown under p.mu

	header *pb.PlogHeader // parsed superblock (sector 0)
}

// openTrailerMagic tags the inline open-block trailer sector Commit writes after
// the ragged edge. openTrailerHeader is its fixed prefix before the sealed-sector
// hashes: the magic, a uint16 sealed-sector count, and a uint16 ragged-edge
// length. The whole prefix plus the hashes is then covered by a trailing HMAC.
const (
	openTrailerMagic  = "ROSEOPB1"
	openTrailerHeader = 12
)

// The plog superblock occupies sector 0, before any data. Its fixed prefix is an
// 8-byte magic, a uint16 format version, and a uint32 protobuf payload length;
// the marshaled PlogHeader follows, and the last HashSize bytes of the sector
// hold an HMAC over everything before them. Data sectors begin at sector 1, so
// every physical offset is shifted by plogHeaderSize (see CalcPhysical).
// PlogFormatVersion is the on-disk plog superblock format version, also stamped
// into the singleton cluster record so a format migration has a value to branch
// on.
const PlogFormatVersion = plogFormatVersion

const (
	plogMagic            = "ROSEPLG1"
	plogFormatVersion    = 4
	plogHeaderSize       = SectorSize
	plogHeaderPrefix     = 14 // magic(8) + version(2) + payloadLen(4)
	plogHeaderHMACOffset = SectorSize - HashSize
	plogHeaderMaxPayload = plogHeaderHMACOffset - plogHeaderPrefix
)

const (
	SectorSize     = 4096
	HashesPerBlock = 255
	HashSize       = 16

	BlockPhysical = (HashesPerBlock + 1) * SectorSize
	DataPerBlock  = HashesPerBlock * SectorSize

	// blockPhysical is the on-disk span of a full hash-protected block: 255
	// data sectors followed by a single hash sector.
	blockPhysical = BlockPhysical
	dataPerBlock  = DataPerBlock
)

// RecoveredChunk describes a chunk's location and expected content hash.
type RecoveredChunk struct {
	Hash          []byte
	LogicalStart  int64
	Length        int
	PayloadOffset int
	Validate      func([]byte) bool
}

// ChunkRecoverer is called when reload finds no valid trailer to recover the expected sector hashes
// for the open block of a plog.
type ChunkRecoverer interface {
	RecoverChunks(ctx context.Context, plogID uint32, blockStartPhys, sealedPhys int64) ([]RecoveredChunk, error)
}

// bitrotKey keys the HMAC stored alongside each block of sector hashes. It is a
// placeholder until per-volume keys are provisioned.
var bitrotKey = []byte("rose-bitrot-key-todo")

func sectorHash(data []byte) [HashSize]byte {
	sum := sha256.Sum256(data)
	var out [HashSize]byte
	copy(out[:], sum[:HashSize])
	return out
}

func completedBlockMAC(blockIdx int64, hashes []byte) [HashSize]byte {
	var index [8]byte
	binary.LittleEndian.PutUint64(index[:], uint64(blockIdx))
	mac := hmac.New(sha256.New, bitrotKey)
	mac.Write(index[:])
	mac.Write(hashes)
	var out [HashSize]byte
	copy(out[:], mac.Sum(nil)[:HashSize])
	return out
}

func openBlockMAC(blockIdx int64, trailerPrefix, ragged []byte) [HashSize]byte {
	var index [8]byte
	binary.LittleEndian.PutUint64(index[:], uint64(blockIdx))
	mac := hmac.New(sha256.New, bitrotKey)
	mac.Write(index[:])
	mac.Write(trailerPrefix)
	mac.Write(ragged)
	var out [HashSize]byte
	copy(out[:], mac.Sum(nil)[:HashSize])
	return out
}

// OpenOption configures OpenPlog.
type OpenOption func(*openOptions)

type openOptions struct {
	header *pb.PlogHeader
}

// WithHeader supplies the superblock header stamped into a freshly created plog.
// Production callers always pass it so the plog is self-describing; if omitted,
// a freshly created plog gets an empty (identity-less) but otherwise valid
// superblock. An existing file's own superblock is always authoritative.
func WithHeader(h *pb.PlogHeader) OpenOption {
	return func(o *openOptions) { o.header = h }
}

// OpenPlog opens or creates a plog. A freshly created (zero-length) file is
// stamped with the WithHeader superblock (or an empty one if none is given); an
// existing file's own superblock is authoritative and any WithHeader is ignored.
func OpenPlog(path string, id uint32, opts ...OpenOption) (*Plog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	var o openOptions
	for _, opt := range opts {
		opt(&o)
	}
	header := o.header
	if header == nil {
		header = &pb.PlogHeader{PlogId: id}
	}
	return openPlogFile(path, id, os.O_RDWR|os.O_CREATE, header)
}

// OpenExistingPlog opens an already-provisioned plog without creating it. If the
// backing file is absent it returns an error satisfying errors.Is(err,
// fs.ErrNotExist), letting recovery distinguish a genuinely lost shard (stub it
// offline, or fail the durability gate) from one that is merely unreadable.
// Unlike OpenPlog, whose O_CREATE would silently resurrect a missing shard as an
// empty file and present it as valid, this never fabricates a shard. It requires
// a valid superblock and never writes one.
func OpenExistingPlog(path string, id uint32) (*Plog, error) {
	return openPlogFile(path, id, os.O_RDWR, nil)
}

func openPlogFile(path string, id uint32, flag int, header *pb.PlogHeader) (*Plog, error) {
	f, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, fmt.Errorf("open plog: %w", err)
	}
	if flag&os.O_RDWR != 0 {
		if err := restoreUndo(f); err != nil {
			f.Close()
			return nil, fmt.Errorf("restore plog %d prefix: %w", id, err)
		}
		if err := discardUndoTemp(f); err != nil {
			f.Close()
			return nil, fmt.Errorf("clean plog %d temporary journal: %w", id, err)
		}
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	p := &Plog{
		id:       id,
		file:     f,
		buf:      make([]byte, 0, SectorSize),
		hashes:   make([]byte, 0, HashesPerBlock*HashSize),
		writeBuf: make([]byte, 0, 2*SectorSize),
	}

	if info.Size() == 0 {
		// Fresh file: stamp the superblock and start with an empty data region.
		if header == nil {
			f.Close()
			return nil, fmt.Errorf("open plog %d: new file requires a header", id)
		}
		if err := writeSuperblock(f, header); err != nil {
			f.Close()
			return nil, fmt.Errorf("open plog %d: %w", id, err)
		}
		// The catalog may reference this plog as soon as OpenPlog returns. Make
		// both its superblock and directory entry durable first, so a crash cannot
		// leave committed placement pointing at a file that never reached disk.
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, fmt.Errorf("open plog %d: sync superblock: %w", id, err)
		}
		dir, err := os.Open(filepath.Dir(path))
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("open plog %d directory for sync: %w", id, err)
		}
		if err := dir.Sync(); err != nil {
			_ = dir.Close()
			f.Close()
			return nil, fmt.Errorf("open plog %d directory sync: %w", id, err)
		}
		if err := dir.Close(); err != nil {
			f.Close()
			return nil, fmt.Errorf("close plog %d directory: %w", id, err)
		}
		p.header = header
		p.logicalLength = 0
	} else {
		hdr, err := readSuperblock(f)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("open plog %d: %w", id, err)
		}
		p.header = hdr
		p.logicalLength = CalcLogical(info.Size())
	}
	if err := p.reload(); err != nil {
		f.Close()
		return nil, err
	}
	return p, nil
}

// Header returns the parsed superblock of the plog. It is never nil for an open
// plog.
func (p *Plog) Header() *pb.PlogHeader { return p.header }

// RebindDiskUID updates the physical-disk identity in this plog's superblock and
// makes it durable. Relocation calls this on the copied destination before
// publishing the catalog move, so a crash leaves either the old authoritative
// copy or a fully self-consistent new one.
func (p *Plog) RebindDiskUID(diskUID []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// The rollback journal is bound to this superblock. Relocation must commit
	// the source before copying; changing identity with a pending journal would
	// make its original prefix impossible to restore.
	if _, err := os.Stat(p.file.Name() + ".undo"); err == nil {
		return fmt.Errorf("rebind plog %d: uncommitted prefix journal", p.id)
	} else if !os.IsNotExist(err) {
		return err
	}
	header := proto.Clone(p.header).(*pb.PlogHeader)
	header.DiskUid = append([]byte(nil), diskUID...)
	if err := writeSuperblock(p.file, header); err != nil {
		return err
	}
	if err := p.file.Sync(); err != nil {
		return fmt.Errorf("sync rebound plog %d superblock: %w", p.id, err)
	}
	p.header = header
	return nil
}

// writeSuperblock marshals hdr into sector 0 with magic, version, length, and a
// trailing HMAC, then writes it at offset 0.
func writeSuperblock(f *os.File, hdr *pb.PlogHeader) error {
	payload, err := proto.Marshal(hdr)
	if err != nil {
		return fmt.Errorf("marshal plog header: %w", err)
	}
	if len(payload) > plogHeaderMaxPayload {
		return fmt.Errorf("plog header too large: %d > %d", len(payload), plogHeaderMaxPayload)
	}
	sec := make([]byte, plogHeaderSize)
	copy(sec, plogMagic)
	binary.LittleEndian.PutUint16(sec[8:10], plogFormatVersion)
	binary.LittleEndian.PutUint32(sec[10:14], uint32(len(payload)))
	copy(sec[plogHeaderPrefix:], payload)
	mac := hmac.New(sha256.New, bitrotKey)
	mac.Write(sec[:plogHeaderHMACOffset])
	copy(sec[plogHeaderHMACOffset:], mac.Sum(nil)[:HashSize])
	if _, err := f.WriteAt(sec, 0); err != nil {
		return fmt.Errorf("write plog superblock: %w", err)
	}
	return nil
}

// readSuperblock reads and validates sector 0, returning the parsed header.
func readSuperblock(f *os.File) (*pb.PlogHeader, error) {
	sec := make([]byte, plogHeaderSize)
	if n, err := f.ReadAt(sec, 0); err != nil && n < plogHeaderSize {
		return nil, ErrUnrecognizedPlogFormat
	}
	if string(sec[:len(plogMagic)]) != plogMagic {
		return nil, ErrUnrecognizedPlogFormat
	}
	if ver := binary.LittleEndian.Uint16(sec[8:10]); ver != plogFormatVersion {
		return nil, fmt.Errorf("plog format version %d unsupported (want %d)", ver, plogFormatVersion)
	}
	mac := hmac.New(sha256.New, bitrotKey)
	mac.Write(sec[:plogHeaderHMACOffset])
	if !hmac.Equal(mac.Sum(nil)[:HashSize], sec[plogHeaderHMACOffset:plogHeaderHMACOffset+HashSize]) {
		return nil, ErrPlogHeaderCorrupt
	}
	n := binary.LittleEndian.Uint32(sec[10:14])
	if int(n) > plogHeaderMaxPayload {
		return nil, ErrPlogHeaderCorrupt
	}
	var hdr pb.PlogHeader
	if err := proto.Unmarshal(sec[plogHeaderPrefix:plogHeaderPrefix+int(n)], &hdr); err != nil {
		return nil, fmt.Errorf("unmarshal plog header: %w", err)
	}
	return &hdr, nil
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
		p.loadedFromTrailer = true
		return nil
	}

	p.loadedFromTrailer = false
	// A missing trailer supplies no integrity evidence. Recover geometry now;
	// catalog recovery must authenticate open-block bytes before they are read.
	return p.rebuildOpenBlock()
}

// rebuildOpenBlock loads geometry without inventing integrity evidence. Until
// RecoverHashes validates cataloged records, reads of the partial sector fail
// and sealed sectors carry deliberately nonmatching hashes.
func (p *Plog) rebuildOpenBlock() error {
	p.buf = p.buf[:0]
	partial := p.logicalLength % SectorSize
	sealed := p.logicalLength - partial
	p.bufCorrupt = partial > 0
	if partial > 0 {
		p.buf = p.buf[:partial]
		if _, err := p.file.ReadAt(p.buf, CalcPhysical(sealed)); err != nil {
			return fmt.Errorf("reload plog %d open sector: %w", p.id, err)
		}
	}
	blockStart := (sealed / dataPerBlock) * dataPerBlock
	p.hashes = make([]byte, int((sealed-blockStart)/SectorSize)*HashSize)
	p.openUnverified = blockStart < p.logicalLength
	return nil
}

// TruncateTo discards any uncommitted tail beyond logical, making the committed
// length authoritative again. It is called on mount to reconcile a plog whose
// file grew past the length the metadata DB recorded for its vlog: a crash that
// sealed new data to the file after the previous open-block trailer was
// overwritten but before the vlog length was committed leaves the plog reloading
// an inflated size-derived length. The orphan tail is referenced by nobody,
// so dropping it keeps the plog cursor aligned with where the vlog expects the
// next append (otherwise that append is placed past where reads resolve). It only
// ever shrinks; a target beyond the current length is a different inconsistency
// and errors.
func (p *Plog) TruncateTo(logical int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if logical < 0 {
		return fmt.Errorf("truncate plog %d: negative target %d", p.id, logical)
	}
	if logical > p.logicalLength {
		return fmt.Errorf("truncate plog %d: target %d beyond length %d", p.id, logical, p.logicalLength)
	}
	if logical == p.logicalLength {
		return nil
	}
	// A valid old trailer or completed block can authenticate the retained
	// prefix even though it describes a longer log. Preserve that evidence;
	// in particular EC parity has no catalog chunks to authenticate it later.
	blockStart := (logical / dataPerBlock) * dataPerBlock
	verified, verifyErr := p.readLocked(blockStart, int(logical-blockStart))
	if err := p.protectPrefixLocked(CalcPhysical(blockStart)); err != nil {
		return err
	}
	if err := p.file.Truncate(CalcPhysical(logical)); err != nil {
		return fmt.Errorf("truncate plog %d: %w", p.id, err)
	}
	p.logicalLength = logical
	if err := p.rebuildOpenBlock(); err != nil {
		return err
	}
	// The old trailer no longer describes this geometry. Catalog recovery must
	// establish fresh evidence for the retained open-block bytes.
	p.loadedFromTrailer = false
	if verifyErr == nil {
		sealedLen := len(verified) / SectorSize * SectorSize
		for offset := 0; offset < sealedLen; offset += SectorSize {
			h := sectorHash(verified[offset : offset+SectorSize])
			copy(p.hashes[offset/SectorSize*HashSize:], h[:])
		}
		copy(p.buf, verified[sealedLen:])
		p.bufCorrupt = false
		p.openUnverified = false
		// The retained bytes were checked against existing integrity evidence,
		// not blessed by hashing unknown disk contents.
		p.loadedFromTrailer = true
	}
	return nil
}

// recoverFromTrailer reconstructs the open block's geometry and sealed-sector
// hashes from the inline trailer Commit writes as the last sector of a cleanly
// committed open block. It returns true and sets logicalLength, buf, and hashes
// when a recognized trailer is found; false leaves the Plog for reload's
// unverified fallback. The HMAC (and the requirement that the implied block
// start be block-aligned) means a real data sector left in the trailer's place
// by a torn write is rejected rather than mistaken for one.
func (p *Plog) recoverFromTrailer(size int64) bool {
	if size < plogHeaderSize+2*SectorSize {
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
	raggedField := binary.LittleEndian.Uint16(trailer[10:12])
	authenticatesRagged := raggedField&0x8000 != 0
	raggedLen := int(raggedField & 0x7fff)
	// The open block never holds a full block's worth of sealed sectors (the
	// 255th seal emits the hash sector and clears them). A trailer with no
	// sealed sectors is valid only when it authenticates a ragged edge.
	if c >= HashesPerBlock || raggedLen >= SectorSize || (c == 0 && raggedLen == 0) {
		return false
	}
	hashesEnd := openTrailerHeader + c*HashSize
	// The trailer sits at block-position c+1, so the block begins c+1 sectors
	// before it; that start must land on a block boundary to be the real thing.
	// blockStartPhys is a physical file offset (superblock-inclusive), so the
	// alignment check works in data-relative coordinates.
	blockStartPhys := (size - SectorSize) - int64(c+1)*SectorSize
	dataRelStart := blockStartPhys - plogHeaderSize
	if dataRelStart < 0 || dataRelStart%blockPhysical != 0 {
		return false
	}
	blockStartLogical := (dataRelStart / blockPhysical) * dataPerBlock
	sealed := blockStartLogical + int64(c)*SectorSize
	if raggedLen > 0 {
		p.buf = p.buf[:raggedLen]
		if _, err := p.file.ReadAt(p.buf, CalcPhysical(sealed)); err != nil {
			p.buf = p.buf[:0]
			return false
		}
	}
	p.logicalLength = sealed + int64(raggedLen)
	p.hashes = append(p.hashes[:0], trailer[openTrailerHeader:hashesEnd]...)
	var ragged []byte
	if authenticatesRagged {
		ragged = p.buf
	}
	want := openBlockMAC(blockStartLogical/dataPerBlock, trailer[:hashesEnd], ragged)
	if !hmac.Equal(want[:], trailer[hashesEnd:hashesEnd+HashSize]) {
		if authenticatesRagged {
			// Preserve the trailer's bounded geometry so reads report the
			// corruption instead of trusting and re-hashing the damaged tail.
			// None of the sealed-sector hashes are authenticated either, so
			// poison them as well rather than accepting data that still matches
			// an integrity slot from the invalid trailer.
			p.bufCorrupt = true
			clear(p.hashes)
			return true
		}
		return false
	}
	return true
}

// CalcLogical converts a physical plog file offset to logical data bytes. The
// first sector is the superblock, so it is subtracted before the block math.
func CalcLogical(phys int64) int64 {
	phys -= plogHeaderSize
	if phys < 0 {
		return 0
	}
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

// CalcPhysical converts logical data bytes to a physical plog file offset,
// accounting for the leading superblock sector.
func CalcPhysical(logical int64) int64 {
	chunks := logical / dataPerBlock
	rem := logical % dataPerBlock
	return plogHeaderSize + chunks*blockPhysical + rem
}

// hashSectorPhys returns the physical file offset of block blockIdx's hash
// sector, which immediately follows that block's 255 data sectors. Like
// CalcPhysical it includes the leading superblock sector.
func hashSectorPhys(blockIdx int64) int64 {
	return plogHeaderSize + blockIdx*blockPhysical + dataPerBlock
}

// Write appends data to the plog and returns the starting logical offset.
func (p *Plog) Write(txnID int64, data []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writeLocked(data)
}

func (p *Plog) writeLocked(data []byte) (int64, error) {
	if p.bufCorrupt || p.openUnverified {
		return 0, fmt.Errorf("plog %d open sector: %w", p.id, ErrBitrot)
	}
	offset := p.logicalLength
	pos := 0

	// If the write fits entirely in the remaining space of the open sector,
	// just append it to p.buf in memory. No sectors are sealed, so no disk write.
	if len(p.buf)+len(data) < SectorSize {
		p.buf = append(p.buf, data...)
		p.logicalLength += int64(len(data))
		return offset, nil
	}

	if err := p.protectPrefixLocked(CalcPhysical((p.logicalLength / dataPerBlock) * dataPerBlock)); err != nil {
		return 0, err
	}
	// We are going to seal at least one sector.
	oldLogicalLength := p.logicalLength
	oldBuf := append([]byte(nil), p.buf...)
	oldHashes := append([]byte(nil), p.hashes...)
	firstSectorLogicalStart := p.logicalLength - int64(len(p.buf))
	firstSectorPhysicalStart := CalcPhysical(firstSectorLogicalStart)

	// Reuse the batched-write scratch instead of allocating a fresh buffer for
	// every EnsureWrite; it stays under p.mu and grows geometrically as needed.
	estimatedHashSectors := len(data)/dataPerBlock + 2
	writeBuf := p.writeBuf[:0]
	neededCap := len(p.buf) + len(data) + estimatedHashSectors*SectorSize
	if cap(writeBuf) < neededCap {
		writeBuf = make([]byte, 0, neededCap)
	}

	// Construct the first sector (which seals the current p.buf)
	var sector [SectorSize]byte
	space := SectorSize - len(p.buf)
	copy(sector[:len(p.buf)], p.buf)
	copy(sector[len(p.buf):], data[:space])
	pos += space
	p.logicalLength += int64(space)

	writeBuf = append(writeBuf, sector[:]...)
	h := sectorHash(sector[:])
	p.hashes = append(p.hashes, h[:]...)

	if len(p.hashes) == HashesPerBlock*HashSize {
		var hashSec [SectorSize]byte
		copy(hashSec[:], p.hashes)
		blockIdx := p.logicalLength/dataPerBlock - 1
		mac := completedBlockMAC(blockIdx, p.hashes)
		copy(hashSec[HashesPerBlock*HashSize:], mac[:])

		writeBuf = append(writeBuf, hashSec[:]...)
		p.hashes = p.hashes[:0]
	}

	// Process subsequent full sectors
	for pos+SectorSize <= len(data) {
		secBytes := data[pos : pos+SectorSize]
		pos += SectorSize
		p.logicalLength += SectorSize

		writeBuf = append(writeBuf, secBytes...)
		h := sectorHash(secBytes)
		p.hashes = append(p.hashes, h[:]...)

		if len(p.hashes) == HashesPerBlock*HashSize {
			var hashSec [SectorSize]byte
			copy(hashSec[:], p.hashes)
			blockIdx := p.logicalLength/dataPerBlock - 1
			mac := completedBlockMAC(blockIdx, p.hashes)
			copy(hashSec[HashesPerBlock*HashSize:], mac[:])

			writeBuf = append(writeBuf, hashSec[:]...)
			p.hashes = p.hashes[:0]
		}
	}

	// Store the remaining incomplete sector in p.buf
	p.buf = p.buf[:0]
	if pos < len(data) {
		p.buf = append(p.buf, data[pos:]...)
		p.logicalLength += int64(len(data) - pos)
	}

	// Perform the batched write
	if len(writeBuf) > 0 {
		if err := p.writePhysical(writeBuf, firstSectorPhysicalStart); err != nil {
			p.logicalLength = oldLogicalLength
			p.buf = append(p.buf[:0], oldBuf...)
			p.hashes = append(p.hashes[:0], oldHashes...)
			p.writeBuf = writeBuf[:0]
			return 0, fmt.Errorf("write plog %d: %w", p.id, err)
		}
	}
	p.writeBuf = writeBuf[:0]

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
	if int64(length) > math.MaxInt64-offset {
		return nil, fmt.Errorf("read plog %d: range overflows int64", p.id)
	}
	end := offset + int64(length)
	if end > p.logicalLength {
		return nil, fmt.Errorf("read plog %d past end: %d > %d", p.id, end, p.logicalLength)
	}

	sealed := p.logicalLength - int64(len(p.buf))
	out := make([]byte, 0, length)
	hashSectors := make(map[int64][]byte)
	for cur := offset; cur < end; {
		sectorIdx := cur / SectorSize
		sectorStart := sectorIdx * SectorSize

		var sector []byte
		if sectorStart >= sealed {
			// Authentication of the open, not-yet-sealed sector happens while
			// loading its trailer; retain that failure for every later read.
			if p.bufCorrupt {
				return nil, fmt.Errorf("plog %d sector %d (logical %d): %w", p.id, sectorIdx, sectorStart, ErrBitrot)
			}
			sector = p.buf
		} else {
			var err error
			sector, err = p.readDataSector(sectorIdx, sealed, hashSectors)
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
func (p *Plog) readDataSector(sectorIdx, sealed int64, hashSectors map[int64][]byte) ([]byte, error) {
	sectorStart := sectorIdx * SectorSize
	size := int64(SectorSize)
	if sectorStart+size > sealed {
		size = sealed - sectorStart
	}
	sector := make([]byte, size)
	if _, err := p.file.ReadAt(sector, CalcPhysical(sectorStart)); err != nil {
		return nil, fmt.Errorf("read plog %d sector %d: %w", p.id, sectorIdx, err)
	}
	expected, ok, err := p.sectorHashFor(sectorIdx, sealed, hashSectors)
	if err != nil {
		return nil, err
	}
	if ok {
		hash := sectorHash(sector)
		if !bytes.Equal(hash[:], expected) {
			return nil, fmt.Errorf("plog %d sector %d (logical %d): %w", p.id, sectorIdx, sectorStart, ErrBitrot)
		}
	}
	return sector, nil
}

// sectorHashFor returns the recorded 16-byte hash of a sealed data sector.
// Sectors in a completed block read their hash from the on-disk hash sector;
// sectors still in the open block read it from the in-memory accumulator.
func (p *Plog) sectorHashFor(sectorIdx, sealed int64, hashSectors map[int64][]byte) ([]byte, bool, error) {
	blockIdx := sectorIdx / HashesPerBlock
	posInBlock := sectorIdx % HashesPerBlock
	blockEndLogical := (blockIdx + 1) * dataPerBlock

	if sealed >= blockEndLogical {
		recorded := hashSectors[blockIdx]
		if recorded == nil {
			hashSector := make([]byte, SectorSize)
			if _, err := p.file.ReadAt(hashSector, hashSectorPhys(blockIdx)); err != nil {
				return nil, false, fmt.Errorf("read plog %d hash sector %d: %w", p.id, blockIdx, err)
			}
			recorded = hashSector[:HashesPerBlock*HashSize]
			want := completedBlockMAC(blockIdx, recorded)
			if !hmac.Equal(want[:], hashSector[HashesPerBlock*HashSize:]) {
				return nil, false, fmt.Errorf("plog %d hash sector %d authentication: %w", p.id, blockIdx, ErrBitrot)
			}
			hashSectors[blockIdx] = recorded
		}
		start := int(posInBlock) * HashSize
		return recorded[start : start+HashSize], true, nil
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

// Verify reads the complete logical stream through the normal authenticated
// read path without retaining it. Relocation uses this before publishing a
// copied plog, catching a destination disk that silently changed data or
// integrity sectors during the copy.
func (p *Plog) Verify() error {
	const batch = 1 << 20
	length := p.LogicalLength()
	for offset := int64(0); offset < length; offset += batch {
		n := min(int64(batch), length-offset)
		if _, err := p.Read(offset, int(n)); err != nil {
			return fmt.Errorf("verify plog %d at %d: %w", p.id, offset, err)
		}
	}
	return nil
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
		if _, err := p.file.ReadAt(hashSector, hashSectorPhys(blockIdx)); err != nil {
			return res, fmt.Errorf("scrub plog %d hash sector %d: %w", p.id, blockIdx, err)
		}
		recorded := hashSector[:HashesPerBlock*HashSize]
		want := completedBlockMAC(blockIdx, recorded)
		if !hmac.Equal(want[:], hashSector[HashesPerBlock*HashSize:HashesPerBlock*HashSize+HashSize]) {
			res.BadHMACBlocks = append(res.BadHMACBlocks, blockIdx)
		}
		for pos := int64(0); pos < HashesPerBlock; pos++ {
			sectorIdx := blockIdx*HashesPerBlock + pos
			sector := make([]byte, SectorSize)
			if _, err := p.file.ReadAt(sector, CalcPhysical(sectorIdx*SectorSize)); err != nil {
				return res, fmt.Errorf("scrub plog %d sector %d: %w", p.id, sectorIdx, err)
			}
			res.SectorsChecked++
			expected := recorded[pos*HashSize : pos*HashSize+HashSize]
			hash := sectorHash(sector)
			if !bytes.Equal(hash[:], expected) {
				res.CorruptSectors = append(res.CorruptSectors, sectorIdx*SectorSize)
			}
		}
	}
	// The current block has not emitted its standalone hash sector yet, but every
	// sealed sector in it still has an authenticated hash (persisted in the open
	// trailer on Commit and retained in memory after verification on open).
	// Scrubbing only complete blocks would leave the common sub-megabyte plog
	// entirely unscrubbed.
	openBlockStart := completeBlocks * dataPerBlock
	for pos := 0; pos < len(p.hashes)/HashSize; pos++ {
		sectorStart := openBlockStart + int64(pos)*SectorSize
		sector := make([]byte, SectorSize)
		if _, err := p.file.ReadAt(sector, CalcPhysical(sectorStart)); err != nil {
			return res, fmt.Errorf("scrub plog %d open sector %d: %w", p.id, pos, err)
		}
		res.SectorsChecked++
		expected := p.hashes[pos*HashSize : pos*HashSize+HashSize]
		hash := sectorHash(sector)
		if !bytes.Equal(hash[:], expected) {
			res.CorruptSectors = append(res.CorruptSectors, sectorStart)
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
	if p.bufCorrupt || p.openUnverified {
		return fmt.Errorf("commit plog %d open sector: %w", p.id, ErrBitrot)
	}
	if len(p.hashes) > 0 || len(p.buf) > 0 {
		start := ((p.logicalLength - int64(len(p.buf))) / dataPerBlock) * dataPerBlock
		if err := p.protectPrefixLocked(CalcPhysical(start)); err != nil {
			return err
		}
	}
	raggedLen := len(p.buf)
	sealed := p.logicalLength - int64(raggedLen)
	if raggedLen > 0 {
		if err := p.writePhysical(p.buf, CalcPhysical(sealed)); err != nil {
			return fmt.Errorf("commit plog %d ragged edge: %w", p.id, err)
		}
	}
	// Record the open block's sealed-sector hashes inline, in an HMAC-protected
	// trailer sector one position past the ragged edge. Continued writes overwrite
	// it as the block grows, and the block's real hash sector replaces it on
	// completion. The trailer only describes sectors already written above, so a
	// data fsync makes the ragged edge, the trailer, and any sectors sealed this
	// session durable together. Retiring the undo journal also syncs its directory.
	// A just-completed block with no
	// ragged edge has nothing to protect and needs no trailer; c only rises within
	// a block, so no stale trailer can survive for the loader to mistake.
	wroteTrailer := len(p.hashes) > 0 || raggedLen > 0
	if wroteTrailer {
		if err := p.writePhysical(p.buildOpenTrailer(raggedLen), CalcPhysical(sealed)+SectorSize); err != nil {
			return fmt.Errorf("commit plog %d open trailer: %w", p.id, err)
		}
	}
	// A failed append can have written a longer physical suffix even though its
	// in-memory cursor was rolled back. Remove that suffix before retiring the
	// journal; otherwise reopen may interpret failed bytes as the new trailer or
	// as data beyond the acknowledged prefix.
	end := CalcPhysical(p.logicalLength)
	if wroteTrailer {
		end = CalcPhysical(sealed) + 2*SectorSize
	}
	if err := p.file.Truncate(end); err != nil {
		return fmt.Errorf("commit plog %d physical length: %w", p.id, err)
	}
	if err := commitUndo(p.file); err != nil {
		p.undoActive = false
		return err
	}
	p.undoActive = false
	if wroteTrailer {
		// The in-memory hashes and ragged bytes produced this now-durable
		// authenticated trailer. A later hot-return recovery pass must not treat
		// them as unauthenticated fallback bytes and poison the rebuilt copy.
		p.loadedFromTrailer = true
	}
	return nil
}

func (p *Plog) writePhysical(data []byte, offset int64) error {
	write := p.writeAt
	if write == nil {
		write = p.file.WriteAt
	}
	n, err := write(data, offset)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}

// buildOpenTrailer assembles the inline open-block trailer sector: the magic, the
// sealed-sector count and flagged ragged-edge length, the sealed-sector hashes,
// then an HMAC over that metadata plus the ragged bytes, zero-padded to a full
// sector. Including the bytes in the MAC avoids needing another hash slot when
// the trailer already contains all 254 possible sealed-sector hashes.
func (p *Plog) buildOpenTrailer(raggedLen int) []byte {
	trailer := make([]byte, SectorSize)
	copy(trailer, openTrailerMagic)
	binary.LittleEndian.PutUint16(trailer[8:10], uint16(len(p.hashes)/HashSize))
	raggedField := uint16(raggedLen)
	if raggedLen > 0 {
		raggedField |= 0x8000
	}
	binary.LittleEndian.PutUint16(trailer[10:12], raggedField)
	copy(trailer[openTrailerHeader:], p.hashes)
	hashesEnd := openTrailerHeader + len(p.hashes)
	var ragged []byte
	if raggedLen > 0 {
		ragged = p.buf
	}
	sealed := p.logicalLength - int64(raggedLen)
	mac := openBlockMAC(sealed/dataPerBlock, trailer[:hashesEnd], ragged)
	copy(trailer[hashesEnd:], mac[:])
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

// ReadLogicalUnverified reads logical bytes from the plog file without verifying sector hashes.
func (p *Plog) ReadLogicalUnverified(offset int64, length int) ([]byte, error) {
	out := make([]byte, 0, length)
	end := offset + int64(length)
	for cur := offset; cur < end; {
		sectorIdx := cur / SectorSize
		sectorStart := sectorIdx * SectorSize
		posInSector := cur - sectorStart
		n := SectorSize - posInSector
		if cur+n > end {
			n = end - cur
		}

		buf := make([]byte, n)
		physOffset := CalcPhysical(sectorStart) + posInSector
		if _, err := p.file.ReadAt(buf, physOffset); err != nil {
			return nil, err
		}
		out = append(out, buf...)
		cur += n
	}
	return out, nil
}

// RecoverHashes authenticates the complete open block, including its partial
// final sector, against independently validated catalog records.
func (p *Plog) RecoverHashes(ctx context.Context, recoverer ChunkRecoverer) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.loadedFromTrailer {
		return nil
	}

	partial := p.logicalLength % SectorSize
	sealed := p.logicalLength - partial
	blockStart := (sealed / dataPerBlock) * dataPerBlock
	sealedCount := int((sealed - blockStart) / SectorSize)
	// Invalidate evidence before consulting the catalog, including on errors.
	p.hashes = make([]byte, sealedCount*HashSize)
	p.bufCorrupt = partial > 0
	p.openUnverified = blockStart < p.logicalLength
	if blockStart == p.logicalLength {
		return nil
	}
	chunks, err := recoverer.RecoverChunks(ctx, p.id, CalcPhysical(blockStart), CalcPhysical(p.logicalLength))
	if err != nil {
		return err
	}

	// Key by record index, not hash: repeated content may have different
	// encrypted headers and different physical locations.
	verified := make([][]byte, len(chunks))
	for i, c := range chunks {
		if c.LogicalStart < 0 || c.Length <= 0 || int64(c.Length) > p.logicalLength-c.LogicalStart {
			continue
		}
		data, err := p.ReadLogicalUnverified(c.LogicalStart, c.Length)
		if err != nil {
			continue
		}
		if c.Validate != nil {
			if !c.Validate(data) {
				continue
			}
		} else {
			if c.PayloadOffset < 0 || c.PayloadOffset > len(data) {
				continue
			}
			sum := sha256.Sum256(data[c.PayloadOffset:])
			if !bytes.Equal(sum[:15], c.Hash) {
				continue
			}
		}
		verified[i] = data
	}

	allVerified := true
	for start := blockStart; start < p.logicalLength; start += SectorSize {
		end := min(start+SectorSize, p.logicalLength)
		sector := make([]byte, int(end-start))
		covered := make([]bool, len(sector))
		valid := true
		for i, c := range chunks {
			if c.LogicalStart >= end || c.LogicalStart+int64(c.Length) <= start {
				continue
			}
			if verified[i] == nil {
				valid = false
				break
			}
			lo, hi := max(start, c.LogicalStart), min(end, c.LogicalStart+int64(c.Length))
			copy(sector[lo-start:hi-start], verified[i][lo-c.LogicalStart:hi-c.LogicalStart])
			for j := lo - start; j < hi-start; j++ {
				covered[j] = true
			}
		}
		for _, ok := range covered {
			valid = valid && ok
		}
		if !valid {
			allVerified = false
			continue // Unknown and corrupt bytes remain unreadable.
		}
		if start == sealed {
			copy(p.buf, sector)
			p.bufCorrupt = false
		} else {
			h := sectorHash(sector)
			copy(p.hashes[int((start-blockStart)/SectorSize)*HashSize:], h[:])
		}
	}
	p.openUnverified = !allVerified
	return nil
}

func (p *Plog) protectPrefixLocked(start int64) error {
	if p.undoActive && start >= p.undoStart {
		return nil
	}
	if err := saveUndo(p.file, start); err != nil {
		return fmt.Errorf("protect plog %d prefix: %w", p.id, err)
	}
	p.undoActive, p.undoStart = true, start
	return nil
}
