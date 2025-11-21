package sepia

import (
	"encoding/binary"
	"hash/crc32"
	"io"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/vfs"
)

// LogWriter writes records to a WAL file.
// Format: Checksum (4) + Length (2) + Type (1) + Data.
// Type: 1 = Full, 2 = First, 3 = Middle, 4 = Last.
// Type: 1 = Full, 2 = First, 3 = Middle, 4 = Last.
// We use Full records for now.
type LogWriter struct {
	file vfs.File
}

func NewLogWriter(file vfs.File) *LogWriter {
	return &LogWriter{file: file}
}

func (w *LogWriter) AddRecord(data []byte) error {
	// Header: Checksum (4) + Length (2) + Type (1)
	// Type 1 = Full
	header := make([]byte, 7)
	length := len(data)
	binary.LittleEndian.PutUint16(header[4:], uint16(length))
	header[6] = 1 // Full Type

	// Checksum covers Type + Data
	crc := crc32.NewIEEE()
	crc.Write(header[6:])
	crc.Write(data)
	sum := crc.Sum32()
	binary.LittleEndian.PutUint32(header[:4], sum)

	if _, err := w.file.Write(header); err != nil {
		return err
	}
	if _, err := w.file.Write(data); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *LogWriter) Close() error {
	return w.file.Close()
}

// LogReader reads records from a WAL file.
type LogReader struct {
	file vfs.File
	buf  []byte
}

func NewLogReader(file vfs.File) *LogReader {
	return &LogReader{
		file: file,
		buf:  make([]byte, 32*1024), // 32KB buffer
	}
}

func (r *LogReader) ReadRecord() ([]byte, error) {
	// Read Header
	header := make([]byte, 7)
	if _, err := io.ReadFull(r.file, header); err != nil {
		return nil, err
	}

	sum := binary.LittleEndian.Uint32(header[:4])
	length := binary.LittleEndian.Uint16(header[4:])
	typ := header[6]

	data := make([]byte, length)
	if _, err := io.ReadFull(r.file, data); err != nil {
		return nil, err
	}

	// Verify Checksum
	crc := crc32.NewIEEE()
	crc.Write(header[6:])
	crc.Write(data)
	if crc.Sum32() != sum {
		return nil, io.ErrUnexpectedEOF // Checksum mismatch
	}

	if typ != 1 {
		// TODO: Handle fragmentation
		return nil, io.ErrUnexpectedEOF
	}

	return data, nil
}
