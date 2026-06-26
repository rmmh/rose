package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/rmmh/rose/uid"
)

const (
	VlogEncryptionAlgorithm = "AES-128-CTR-HMAC-SHA256-v1"

	vlogKeyLabel     = "rose vlog aes-ctr key v1"
	chunkStreamLabel = "rose chunk stream v1"
)

// DeriveVlogKey derives the AES-128 key used for one vlog from the cluster key
// and that vlog's persistent UID. The small numeric vlog id is deliberately not
// part of the derivation.
func DeriveVlogKey(clusterKey, vlogUID uid.UID) [16]byte {
	sum := hmacSHA256(clusterKey[:], []byte(vlogKeyLabel), vlogUID[:])
	var out [16]byte
	copy(out[:], sum[:16])
	return out
}

// DeriveChunkStream derives the base CTR block for one plaintext chunk hash.
// The clear chunk header stores the low 64 bits so recovery can derive the same
// stream before metadata has been rebuilt.
func DeriveChunkStream(vlogKey [16]byte, chunkHash []byte) ([16]byte, error) {
	if len(chunkHash) < 8 {
		return [16]byte{}, fmt.Errorf("chunk hash length %d < 8", len(chunkHash))
	}
	return DeriveChunkStreamHash64(vlogKey, binary.LittleEndian.Uint64(chunkHash[:8])), nil
}

func DeriveChunkStreamHash64(vlogKey [16]byte, chunkHash64 uint64) [16]byte {
	var hashBuf [8]byte
	binary.LittleEndian.PutUint64(hashBuf[:], chunkHash64)
	sum := hmacSHA256(vlogKey[:], []byte(chunkStreamLabel), hashBuf[:])
	var out [16]byte
	copy(out[:], sum[:16])
	return out
}

func hmacSHA256(key, a, b []byte) [32]byte {
	var ipad, opad [64]byte
	copy(ipad[:], key)
	copy(opad[:], key)
	for i := 0; i < len(ipad); i++ {
		ipad[i] ^= 0x36
		opad[i] ^= 0x5c
	}
	var innerBuf [128]byte
	n := copy(innerBuf[:], ipad[:])
	n += copy(innerBuf[n:], a)
	n += copy(innerBuf[n:], b)
	inner := sha256.Sum256(innerBuf[:n])
	var outerBuf [96]byte
	n = copy(outerBuf[:], opad[:])
	copy(outerBuf[n:], inner[:])
	return sha256.Sum256(outerBuf[:n+sha256.Size])
}

// ApplyAES128CTR XORs buf with the AES-CTR stream positioned at recordOffset
// bytes from the beginning of the chunk record. It supports small point ranges:
// callers do not need to read or transform the preceding bytes.
func ApplyAES128CTR(vlogKey, chunkStream [16]byte, recordOffset int64, buf []byte) error {
	if recordOffset < 0 {
		return fmt.Errorf("negative record offset %d", recordOffset)
	}
	if len(buf) == 0 {
		return nil
	}
	block, err := aes.NewCipher(vlogKey[:])
	if err != nil {
		return err
	}
	counter := chunkStream
	blockSkip := uint64(recordOffset / aes.BlockSize)
	byteSkip := int(recordOffset % aes.BlockSize)
	addCounter(&counter, blockSkip)
	stream := cipher.NewCTR(block, counter[:])
	if byteSkip != 0 {
		var discard [aes.BlockSize]byte
		stream.XORKeyStream(discard[:byteSkip], discard[:byteSkip])
	}
	stream.XORKeyStream(buf, buf)
	return nil
}

func addCounter(counter *[16]byte, n uint64) {
	lo := binary.BigEndian.Uint64(counter[8:16])
	hi := binary.BigEndian.Uint64(counter[0:8])
	lo2 := lo + n
	if lo2 < lo {
		hi++
	}
	binary.BigEndian.PutUint64(counter[0:8], hi)
	binary.BigEndian.PutUint64(counter[8:16], lo2)
}
