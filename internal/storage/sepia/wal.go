package sepia

import (
	"encoding/binary"
	"hash/crc32"
	"io"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/vfs"
)

// LogWriter writes records to Write-Ahead Log (WAL) for durability.
// Record format: Checksum(4) + Length(2) + Type(1) + Data
// Type values: 1=Full, 2=First, 3=Middle, 4=Last
// Currently only uses Full records (Type=1).
type LogWriter struct {
	file vfs.File
}

func NewLogWriter(file vfs.File) *LogWriter {
	return &LogWriter{file: file}
}

func (w *LogWriter) AddRecord(data []byte) error {
	// Write record header: Checksum(4) + Length(2) + Type(1)
	header := make([]byte, 7)
	length := len(data)
	binary.LittleEndian.PutUint16(header[4:], uint16(length))
	header[6] = 1 // Full record type

	// Calculate checksum over Type + Data
	crc := crc32.NewIEEE()
	crc.Write(header[6:])
	crc.Write(data)
	sum := crc.Sum32()
	binary.LittleEndian.PutUint32(header[:4], sum)

	// Write header, data, and sync to disk
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

// LogReader reads records from a Write-Ahead Log (WAL) file.
type LogReader struct {
	file vfs.File
	buf  []byte // Read buffer
}

func NewLogReader(file vfs.File) *LogReader {
	return &LogReader{
		file: file,
		buf:  make([]byte, 32*1024), // 32KB buffer
	}
}

func (r *LogReader) ReadRecord() ([]byte, error) {
	// Read and validate record header
	header := make([]byte, 7)
	if _, err := io.ReadFull(r.file, header); err != nil {
		return nil, err
	}

	sum := binary.LittleEndian.Uint32(header[:4])
	length := binary.LittleEndian.Uint16(header[4:])
	typ := header[6]

	// Read record data
	data := make([]byte, length)
	if _, err := io.ReadFull(r.file, data); err != nil {
		return nil, err
	}

	// Verify checksum
	crc := crc32.NewIEEE()
	crc.Write(header[6:])
	crc.Write(data)
	if crc.Sum32() != sum {
		return nil, io.ErrUnexpectedEOF
	}

	// TODO: Implement fragmented record handling (Types 2,3,4)
	if typ != 1 {
		return nil, io.ErrUnexpectedEOF
	}

	return data, nil
}
