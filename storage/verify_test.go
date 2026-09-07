package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

type verificationClient struct {
	data []byte
	fail bool
}

func (c *verificationClient) Write(context.Context, int64, []byte) (int64, error) {
	return 0, fmt.Errorf("unused")
}
func (c *verificationClient) Read(_ context.Context, off int64, n int) ([]byte, error) {
	if c.fail {
		return nil, fmt.Errorf("unavailable")
	}
	if off < 0 || off > int64(len(c.data)) {
		return nil, fmt.Errorf("outside storage")
	}
	return append([]byte(nil), c.data[off:min(off+int64(n), int64(len(c.data)))]...), nil
}

func TestVerifyAllRejectsReadableButUnderprotectedMirrors(t *testing.T) {
	ctx := context.Background()
	expected := []byte("independently verified bytes")
	for _, fault := range []string{"healthy", "missing", "short", "inconsistent"} {
		t.Run(fault, func(t *testing.T) {
			a := &verificationClient{data: append([]byte(nil), expected...)}
			b := &verificationClient{data: append([]byte(nil), expected...)}
			switch fault {
			case "missing":
				b.fail = true
			case "short":
				b.data = b.data[:len(b.data)-1]
			case "inconsistent":
				b.data[0] ^= 1
			}
			v, err := NewVlog(1, "DUPLICATE", 1, 0, []PlogClient{a, b}, int64(len(expected)))
			if err != nil {
				t.Fatal(err)
			}
			err = v.VerifyAll(ctx, 0, expected)
			if (err == nil) != (fault == "healthy") {
				t.Fatalf("VerifyAll=%v", err)
			}
		})
	}
}

func TestVerifyAllChecksEveryIntersectingECRowAndParity(t *testing.T) {
	defer SetECColumnBytesForTest(8)()
	ctx := context.Background()
	data := []byte("0123456789abcdefABCDEFGHIJKLMNOP")
	clients := make([]PlogClient, 3)
	for i := range clients {
		clients[i] = &verificationClient{}
	}
	v, err := NewVlog(1, "EC", 2, 1, clients, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < len(data); row += 16 {
		shards, err := v.encodeRow(data[row : row+16])
		if err != nil {
			t.Fatal(err)
		}
		for i := range clients {
			c := clients[i].(*verificationClient)
			c.data = append(c.data, shards[i]...)
		}
	}
	// This range crosses both a data-column and a stripe-row boundary.
	expected := data[7:25]
	if err := v.VerifyAll(ctx, 7, expected); err != nil {
		t.Fatal(err)
	}
	parity := clients[2].(*verificationClient)
	parity.data[9] ^= 1
	got, err := v.Read(ctx, 7, len(expected))
	if err != nil || !bytes.Equal(got, expected) {
		t.Fatalf("healthy data read=%q err=%v", got, err)
	}
	if err := v.VerifyAll(ctx, 7, expected); err == nil {
		t.Fatal("corrupt parity passed protection verification")
	}
	parity.data[9] ^= 1
	wrong := append([]byte(nil), expected...)
	wrong[3] ^= 1
	if err := v.VerifyAll(ctx, 7, wrong); err == nil {
		t.Fatal("consistent but incorrect codeword matched expected bytes")
	}
}
