package storage

import (
	"encoding/binary"
	"testing"
)

func TestChunkHeaderEncodeDecodeInitialLayout(t *testing.T) {
	var hint [32]byte
	copy(hint[:], "short/path")
	h := ChunkHeader{
		Flags:      ChunkFlagInitial | ChunkFlagLast,
		FileID:     0x0102030405060708,
		ChunkHash:  0x1112131415161718,
		PayloadLen: 1234,
		PathHint:   hint,
	}
	encoded := h.Encode()
	if len(encoded) != ChunkHeaderSize {
		t.Fatalf("encoded header size = %d, want %d", len(encoded), ChunkHeaderSize)
	}
	if string(encoded[0:4]) != "RCHK" {
		t.Fatalf("magic = %q", encoded[0:4])
	}
	if binary.LittleEndian.Uint64(encoded[8:16]) != h.FileID {
		t.Fatal("file_id64 not at bytes 8..15")
	}
	if binary.LittleEndian.Uint64(encoded[16:24]) != h.ChunkHash {
		t.Fatal("chunk_hash64 not at bytes 16..23")
	}
	if binary.LittleEndian.Uint32(encoded[24:28]) != h.PayloadLen {
		t.Fatal("payload_len not at bytes 24..27")
	}
	if got := string(encoded[32:42]); got != "short/path" {
		t.Fatalf("initial path hint not contiguous at bytes 32..63: %q", got)
	}
	decoded, err := DecodeChunkHeader(encoded[:])
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Flags != h.Flags || decoded.FileID != h.FileID || decoded.ChunkHash != h.ChunkHash || decoded.PayloadLen != h.PayloadLen || decoded.PathHint != h.PathHint {
		t.Fatalf("decoded header mismatch: %+v", decoded)
	}
}

func TestChunkHeaderEncodeDecodeNonInitialLayout(t *testing.T) {
	var hint [32]byte
	copy(hint[:], "rotating-fragment")
	h := ChunkHeader{
		FileID:     0x0102030405060708,
		ChunkHash:  0x1112131415161718,
		PayloadLen: 1234,
		FileOffset: 0x2122232425262728,
		PrevHash:   0x3132333435363738,
		PathHint:   hint,
	}
	encoded := h.Encode()
	if binary.LittleEndian.Uint64(encoded[32:40]) != h.FileOffset {
		t.Fatal("file_offset not at bytes 32..39")
	}
	if binary.LittleEndian.Uint64(encoded[40:48]) != h.PrevHash {
		t.Fatal("prev_hash64 not at bytes 40..47")
	}
	if got := string(encoded[48:64]); got != "rotating-fragmen" {
		t.Fatalf("rotating path hint = %q", got)
	}
	decoded, err := DecodeChunkHeader(encoded[:])
	if err != nil {
		t.Fatal(err)
	}
	if decoded.FileOffset != h.FileOffset || decoded.PrevHash != h.PrevHash {
		t.Fatalf("decoded conditional fields mismatch: %+v", decoded)
	}
	encoded[12] ^= 0xff
	if _, err := DecodeChunkHeader(encoded[:]); err == nil {
		t.Fatal("bad crc accepted")
	}
}
