package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	ChunkHeaderSize    = 64
	chunkHeaderMagic   = "RCHK"
	ChunkHeaderVersion = 1

	ChunkFlagInitial byte = 1 << 0
	ChunkFlagLast    byte = 1 << 1
)

var chunkHeaderCRCTable = crc32.MakeTable(crc32.Castagnoli)

// ChunkHeader is the fixed-size recovery header stored before every new chunk
// payload in the vlog byte stream. Bytes 32..47 are conditional: initial chunks
// use them as path hint extension; non-initial chunks use them as file offset
// and previous content hash.
type ChunkHeader struct {
	Flags      byte
	FileID     uint64
	ChunkHash  uint64
	PayloadLen uint32
	FileOffset uint64
	PrevHash   uint64
	PathHint   [32]byte
}

func (h ChunkHeader) Encode() [ChunkHeaderSize]byte {
	var out [ChunkHeaderSize]byte
	copy(out[0:4], chunkHeaderMagic)
	out[4] = ChunkHeaderVersion
	out[5] = h.Flags
	binary.LittleEndian.PutUint64(out[8:16], h.FileID)
	binary.LittleEndian.PutUint64(out[16:24], h.ChunkHash)
	binary.LittleEndian.PutUint32(out[24:28], h.PayloadLen)
	if h.Flags&ChunkFlagInitial != 0 {
		copy(out[32:64], h.PathHint[:])
	} else {
		binary.LittleEndian.PutUint64(out[32:40], h.FileOffset)
		binary.LittleEndian.PutUint64(out[40:48], h.PrevHash)
		copy(out[48:64], h.PathHint[:16])
	}
	crcInput := out
	clear(crcInput[28:32])
	binary.LittleEndian.PutUint32(out[28:32], crc32.Checksum(crcInput[:], chunkHeaderCRCTable))
	return out
}

func DecodeChunkHeader(buf []byte) (ChunkHeader, error) {
	if len(buf) != ChunkHeaderSize {
		return ChunkHeader{}, fmt.Errorf("chunk header length %d != %d", len(buf), ChunkHeaderSize)
	}
	if string(buf[:4]) != chunkHeaderMagic {
		return ChunkHeader{}, fmt.Errorf("chunk header missing magic")
	}
	if buf[4] != ChunkHeaderVersion {
		return ChunkHeader{}, fmt.Errorf("chunk header version %d unsupported", buf[4])
	}
	if buf[5]&^(ChunkFlagInitial|ChunkFlagLast) != 0 {
		return ChunkHeader{}, fmt.Errorf("chunk header has unknown flags 0x%x", buf[5])
	}
	if binary.LittleEndian.Uint16(buf[6:8]) != 0 {
		return ChunkHeader{}, fmt.Errorf("chunk header reserved bytes are non-zero")
	}
	crcInput := [ChunkHeaderSize]byte{}
	copy(crcInput[:], buf)
	clear(crcInput[28:32])
	got := crc32.Checksum(crcInput[:], chunkHeaderCRCTable)
	if want := binary.LittleEndian.Uint32(buf[28:32]); got != want {
		return ChunkHeader{}, fmt.Errorf("chunk header crc mismatch")
	}
	h := ChunkHeader{
		Flags:      buf[5],
		FileID:     binary.LittleEndian.Uint64(buf[8:16]),
		ChunkHash:  binary.LittleEndian.Uint64(buf[16:24]),
		PayloadLen: binary.LittleEndian.Uint32(buf[24:28]),
	}
	if h.Flags&ChunkFlagInitial != 0 {
		copy(h.PathHint[:], buf[32:64])
	} else {
		h.FileOffset = binary.LittleEndian.Uint64(buf[32:40])
		h.PrevHash = binary.LittleEndian.Uint64(buf[40:48])
		copy(h.PathHint[:16], buf[48:64])
	}
	return h, nil
}
