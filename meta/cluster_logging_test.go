package meta

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestClusterBootstrapDoesNotLogKey(t *testing.T) {
	if path := os.Getenv("ROSE_CLUSTER_LOG_TEST_DB"); path != "" {
		db, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}

	path := filepath.Join(t.TempDir(), "cluster.db")
	command := exec.Command(os.Args[0], "-test.run=^TestClusterBootstrapDoesNotLogKey$")
	command.Env = append(os.Environ(), "ROSE_CLUSTER_LOG_TEST_DB="+path)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("bootstrap subprocess failed: %v (output omitted because it may contain key material)", err)
	}
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	encryption, err := db.ClusterEncryption(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, representation := range [][]byte{encryption.Key[:], []byte(encryption.Key.String()), []byte(encryption.Formatted), []byte(hex.EncodeToString(encryption.Key[:]))} {
		if bytes.Contains(output, representation) {
			t.Fatal("bootstrap emitted cluster encryption key material")
		}
	}
}
