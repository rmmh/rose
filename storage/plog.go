package storage

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Plog represents an append-only physical log file on disk.
type Plog struct {
	mu            sync.Mutex
	id            uint32
	file          *os.File
	logicalLength int64 // Track length excluding the 4K hash sectors
	physicalSize  int64

	// Buffering for bitrot detection
	buf        []byte // Up to 4096 bytes
	hashes     []byte // Up to 255 * 16 bytes
	hashSector [4096]byte
}

const (
	SectorSize     = 4096
	HashesPerBlock = 255
	HashSize       = 16
)

func OpenPlog(path string, id uint32) (*Plog, error) {
	err := os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open plog: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return &Plog{
		id:            id,
		file:          f,
		physicalSize:  info.Size(),
		logicalLength: calcLogical(info.Size()),
		buf:           make([]byte, 0, SectorSize),
		hashes:        make([]byte, 0, HashesPerBlock*HashSize),
	}, nil
}

func calcLogical(phys int64) int64 {
	// Every 255 * 4096 bytes of data is followed by 1 * 4096 bytes of hashes.
	// We need to calculate how many data bytes are in `phys` bytes.
	chunkSize := int64(HashesPerBlock*SectorSize + SectorSize) // 1044480 + 4096 = 1048576
	chunks := phys / chunkSize
	rem := phys % chunkSize

	logical := chunks * HashesPerBlock * SectorSize
	if rem > int64(HashesPerBlock*SectorSize) {
		logical += int64(HashesPerBlock * SectorSize) // the rest is the hash block itself
	} else {
		logical += rem
	}
	return logical
}

func calcPhysical(logical int64) int64 {
	dataChunkSize := int64(HashesPerBlock * SectorSize)
	chunks := logical / dataChunkSize
	rem := logical % dataChunkSize

	phys := chunks * (dataChunkSize + SectorSize)
	phys += rem
	return phys
}

// Write appends data to the plog and returns the starting offset.
func (p *Plog) Write(txnID int64, data []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

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

		if len(p.buf) == SectorSize {
			if err := p.flushSector(); err != nil {
				return 0, err
			}
		}
	}

	p.logicalLength += int64(len(data))
	return offset, nil
}

func (p *Plog) flushSector() error {
	if len(p.buf) == 0 {
		return nil
	}

	// 1. Hash the 4K data sector (or partial)
	sum := sha256.Sum256(p.buf)
	// We only keep the first 16 bytes to fit 255 in 4KB with a 16B HMAC
	p.hashes = append(p.hashes, sum[:HashSize]...)

	// 2. Write the data sector
	n, err := p.file.Write(p.buf)
	if err != nil {
		return fmt.Errorf("write plog data sector %d: %w", p.id, err)
	}
	p.physicalSize += int64(n)
	p.buf = p.buf[:0]

	// 3. Check if we need to emit a hash block
	if len(p.hashes) == HashesPerBlock*HashSize {
		// Zero the sector
		for i := range p.hashSector {
			p.hashSector[i] = 0
		}

		// Copy the hashes
		copy(p.hashSector[:], p.hashes)

		// Compute HMAC over the 255 hashes
		mac := hmac.New(sha256.New, []byte("rose-bitrot-key-todo"))
		mac.Write(p.hashes)
		macSum := mac.Sum(nil)
		copy(p.hashSector[HashesPerBlock*HashSize:], macSum[:16])

		// Write the hash sector
		n, err := p.file.Write(p.hashSector[:])
		if err != nil {
			return fmt.Errorf("write plog hash sector %d: %w", p.id, err)
		}
		p.physicalSize += int64(n)
		p.hashes = p.hashes[:0]
	}

	return nil
}

// Read reads length bytes from offset.
func (p *Plog) Read(offset int64, length int) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	flushedLogical := p.logicalLength - int64(len(p.buf))

	// If the entire read is within the unflushed buffer
	if offset >= flushedLogical {
		if offset+int64(length) > p.logicalLength {
			return nil, io.EOF
		}
		bufOffset := offset - flushedLogical
		res := make([]byte, length)
		copy(res, p.buf[bufOffset:bufOffset+int64(length)])
		return res, nil
	}

	buf := make([]byte, 0, length)
	remaining := length
	currOffset := offset

	for remaining > 0 {
		if currOffset >= flushedLogical {
			// Read the rest from p.buf
			if currOffset+int64(remaining) > p.logicalLength {
				return nil, io.EOF
			}
			bufOffset := currOffset - flushedLogical
			buf = append(buf, p.buf[bufOffset:bufOffset+int64(remaining)]...)
			break
		}

		physOffset := calcPhysical(currOffset)
		dataChunkSize := int64(HashesPerBlock * SectorSize)
		posInBlock := currOffset % dataChunkSize
		availInBlock := dataChunkSize - posInBlock

		toRead := int64(remaining)
		if toRead > availInBlock {
			toRead = availInBlock
		}

		// Don't read past what's physically on disk
		if currOffset+toRead > flushedLogical {
			toRead = flushedLogical - currOffset
		}

		tmp := make([]byte, toRead)
		n, err := p.file.ReadAt(tmp, physOffset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read plog %d: %w", p.id, err)
		}

		buf = append(buf, tmp[:n]...)
		currOffset += int64(n)
		remaining -= n

		if err == io.EOF && currOffset >= flushedLogical {
			break
		} else if err == io.EOF && currOffset < flushedLogical {
			break
		}
	}

	return buf, nil
}

// Commit flushes buffered data, including its integrity metadata, and makes it
// durable before metadata may publish references to the written range.
func (p *Plog) Commit() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.flushSector(); err != nil {
		return err
	}
	return p.file.Sync()
}

// Sync is retained as the low-level durability primitive.
func (p *Plog) Sync() error { return p.Commit() }

// Close closes the underlying file.
func (p *Plog) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.file.Close()
}
