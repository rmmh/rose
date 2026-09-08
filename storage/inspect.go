package storage

import (
	"os"

	pb "github.com/rmmh/rose/proto"
)

// PlogInspection describes stored evidence without repairing or sealing it.
// VerificationError distinguishes valid headers from damaged/unverifiable data.
type PlogInspection struct {
	Header            *pb.PlogHeader
	LogicalLength     int64
	VerificationError error
}

// InspectionReader exposes reads only and opens its file O_RDONLY. In particular
// it never replays prefix journals or seals a tail as writable recovery would.
type InspectionReader struct{ plog *Plog }

func OpenInspectionReader(path string, id uint32) (*InspectionReader, error) {
	p, err := openPlogFile(path, id, os.O_RDONLY, nil)
	if err != nil {
		return nil, err
	}
	return &InspectionReader{plog: p}, nil
}

func (r *InspectionReader) Read(offset int64, length int) ([]byte, error) {
	return r.plog.Read(offset, length)
}
func (r *InspectionReader) Close() error { return r.plog.Close() }

func InspectPlog(path string, id uint32) (PlogInspection, error) {
	// O_RDONLY is an OS-enforced boundary, including if the open/reload code later
	// gains a write path. No parent directory or missing file is created.
	p, err := openPlogFile(path, id, os.O_RDONLY, nil)
	if err != nil {
		return PlogInspection{}, err
	}
	defer p.Close()
	return PlogInspection{Header: p.Header(), LogicalLength: p.LogicalLength(), VerificationError: p.Verify()}, nil
}
