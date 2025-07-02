package dwal

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/valyala/bytebufferpool"
	"github.com/zeebo/blake3"
	"pkg.gfire.dev/controlplane/internal/vfs"
)

const (
	walFilePrefix = "wal-"
	walFileSuffix = ".log"
)

var (
	ErrChecksumMismatch   = errors.New("checksum mismatch")
	ErrCorruptedEntry     = errors.New("corrupted entry")
	ErrPartialEntry       = errors.New("partial entry")
	ErrInvalidEntryFormat = errors.New("invalid entry format")
)

// WAL represents a Write-Ahead Log.
type WAL struct {
	fs              vfs.VFS
	dir             string
	currentWriteSeg vfs.File
	currentReadSeg  vfs.File
	currentSegSize  int64 // Current size of the active segment file
	currentSeq      uint64
	lastKnownSeq    uint64 // Last successfully written entry sequence number
	mu              sync.Mutex
	segmentSize     int64    // Configurable segment size
	activeSegments  []uint64 // List of active segment sequence numbers

	// Read-related fields
	currentReadSegIndex int
	currentReadOffset   int64

	// Encryption fields
	encryptionKey []byte
	aesgcm        cipher.AEAD
}

// NewWAL creates a new WAL instance.
func NewWAL(fs vfs.VFS, dir string, segmentSize int64, encryptionKey []byte) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	wal := &WAL{
		fs:            fs,
		dir:           dir,
		segmentSize:   segmentSize,
		encryptionKey: encryptionKey,
	}

	if len(encryptionKey) > 0 {
		block, err := aes.NewCipher(encryptionKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create AES cipher: %w", err)
		}
		wal.aesgcm, err = cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCM: %w", err)
		}
	}

	// Recover existing segments and find the last sequence
	if err := wal.recoverSegments(); err != nil {
		return nil, fmt.Errorf("failed to recover WAL segments: %w", err)
	}

	// If no segments found or all are full, create a new one
	if wal.currentWriteSeg == nil {
		if err := wal.createNewSegment(); err != nil {
			return nil, fmt.Errorf("failed to create initial WAL segment: %w", err)
		}
	}

	return wal, nil
}

// recoverSegments scans the WAL directory, identifies existing segments, and finds the last valid sequence number.
func (w *WAL) recoverSegments() error {
	files, err := w.fs.List(w.dir)
	if err != nil {
		return fmt.Errorf("failed to list WAL directory: %w", err)
	}

	var segmentFiles []string
	for _, file := range files {
		if filepath.Ext(file) == walFileSuffix && strings.HasPrefix(filepath.Base(file), walFilePrefix) {
			segmentFiles = append(segmentFiles, file)
		}
	}

	sort.Strings(segmentFiles) // Sort to process in order

	for _, fileName := range segmentFiles {
		// Extract sequence number from file name
		baseName := filepath.Base(fileName)
		seqStr := strings.TrimPrefix(baseName, walFilePrefix)
		seqStr = strings.TrimSuffix(seqStr, walFileSuffix)
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			// Log error but continue with other files
			fmt.Printf("Warning: failed to parse WAL segment sequence from %s: %v\n", fileName, err)
			continue
		}

		w.activeSegments = append(w.activeSegments, seq)
		w.currentSeq = seq // Update current sequence to the latest found

		// TODO: Validate segment integrity (e.g., read last entry, check checksums)
		// For now, assume segments are valid and find the last one
	}

	if len(w.activeSegments) > 0 {
		// Open the last segment for appending
		lastSegmentFileName := filepath.Join(w.dir, fmt.Sprintf("%s%016d%s", walFilePrefix, w.currentSeq, walFileSuffix))
		file, err := w.fs.OpenFile(lastSegmentFileName, os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("failed to open last WAL segment %s: %w", lastSegmentFileName, err)
		}
		w.currentWriteSeg = file

		// Seek to the end of the file to append new entries
		// TODO: This assumes the last segment is fully valid. Need proper recovery to find the last valid entry.
		info, err := file.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat last WAL segment: %w", err)
		}
		w.currentSegSize = info.Size()

		// Find the last known sequence number by iterating through the last segment
		// This is a simplified recovery. A full recovery would re-apply entries.
		// For now, we'll just set lastKnownSeq to the current segment's sequence.
		w.lastKnownSeq = w.currentSeq
		w.currentReadSegIndex = 0 // Start reading from the first segment
	} else {
		w.currentReadSegIndex = 0 // No segments, start from 0
	}

	return nil
}

// createNewSegment creates a new WAL segment file.
func (w *WAL) createNewSegment() error {
	if w.currentWriteSeg != nil {
		if err := w.currentWriteSeg.Close(); err != nil {
			return fmt.Errorf("failed to close current WAL segment: %w", err)
		}
	}

	w.currentSeq++ // Increment sequence for new segment
	segmentFileName := filepath.Join(w.dir, fmt.Sprintf("%s%016d%s", walFilePrefix, w.currentSeq, walFileSuffix))

	file, err := w.fs.OpenFile(segmentFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open new WAL segment file %s: %w", segmentFileName, err)
	}
	w.currentWriteSeg = file
	w.currentSegSize = 0 // Reset size for new segment
	w.activeSegments = append(w.activeSegments, w.currentSeq)
	return nil
}

// WriteEntry appends a new entry to the WAL.
func (w *WAL) WriteEntry(data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Use bytebufferpool for efficient buffer management
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	// Calculate checksum using blake3
	hasher := blake3.New()
	hasher.Write(data)
	checksum := hasher.Sum(nil)

	// Format: [checksum_len (1 byte)] [checksum] [nonce_len (1 byte)] [nonce] [encrypted_data_len (4 bytes)] [encrypted_data]
	// TODO: Consider more robust framing, e.g., CRC32 for data integrity, magic bytes, versioning

	var encryptedData []byte
	var nonce []byte

	if w.aesgcm != nil {
		nonce = make([]byte, w.aesgcm.NonceSize())
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return 0, fmt.Errorf("failed to generate nonce: %w", err)
		}
		encryptedData = w.aesgcm.Seal(nil, nonce, data, nil)
	} else {
		encryptedData = data
	}

	// Checksum length (1 byte)
	buf.WriteByte(byte(len(checksum)))
	// Checksum
	buf.Write(checksum)

	if w.aesgcm != nil {
		// Nonce length (1 byte)
		buf.WriteByte(byte(len(nonce)))
		// Nonce
		buf.Write(nonce)
	}

	// Data length (4 bytes) - now encrypted data length
	buf.Write(uint32ToBytes(uint32(len(encryptedData))))
	// Data - now encrypted data
	buf.Write(encryptedData)

	// Handle segment rotation based on segmentSize
	if w.currentSegSize+int64(buf.Len()) > w.segmentSize {
		if err := w.createNewSegment(); err != nil {
			return 0, fmt.Errorf("failed to rotate WAL segment: %w", err)
		}
	}

	n, err := w.currentWriteSeg.Write(buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("failed to write to WAL segment: %w", err)
	}
	w.currentSegSize += int64(n)

	// Update lastKnownSeq for each successful write
	w.lastKnownSeq = w.currentSeq // This is simplified; should be per-entry sequence

	// TODO: Return actual sequence number of the entry, not just segment sequence
	return w.currentSeq, nil
}

// Sync flushes the current WAL segment to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentWriteSeg == nil {
		return nil // Nothing to sync
	}
	return w.currentWriteSeg.Sync()
}

// Close closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentWriteSeg != nil {
		if err := w.currentWriteSeg.Close(); err != nil {
			return err
		}
	}
	if w.currentReadSeg != nil {
		if err := w.currentReadSeg.Close(); err != nil {
			fmt.Printf("Warning: Error closing read segment: %v\n", err)
		}
	}
	return nil
}

// Helper to convert uint32 to byte slice (little-endian)
func uint32ToBytes(n uint32) []byte {
	b := make([]byte, 4)
	b[0] = byte(n)
	b[1] = byte(n >> 8)
	b[2] = byte(n >> 16)
	b[3] = byte(n >> 24)
	return b
}

// Helper to convert byte slice to uint32 (little-endian)
func bytesToUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

// ReadEntry reads the next entry from the WAL.
// It returns the entry data, its sequence number, and an error if any.
// Returns io.EOF if no more entries are available.
func (w *WAL) ReadEntry() ([]byte, uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for {
		if w.currentReadSeg == nil {
			// Try to open the next segment
			if w.currentReadSegIndex >= len(w.activeSegments) {
				return nil, 0, io.EOF // No more segments
			}

			segmentSeq := w.activeSegments[w.currentReadSegIndex]
			segmentFileName := filepath.Join(w.dir, fmt.Sprintf("%s%016d%s", walFilePrefix, segmentSeq, walFileSuffix))
			file, err := w.fs.Open(segmentFileName)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to open WAL segment %s for reading: %w", segmentFileName, err)
			}
			w.currentReadSeg = file
			w.currentReadOffset = 0 // Reset offset for new segment
		}

		// Attempt to read an entry
		data, bytesRead, err := w.readSingleEntry(w.currentReadSeg)
		if err == nil {
			// Successfully read an entry
			w.currentReadOffset += int64(bytesRead)
			return data, w.activeSegments[w.currentReadSegIndex], nil
		}

		// Handle errors
		if errors.Is(err, io.EOF) {
			// End of current segment, move to next
			if w.currentReadSeg != nil {
				if err := w.currentReadSeg.Close(); err != nil {
					fmt.Printf("Warning: Error closing segment %d: %v\n", w.activeSegments[w.currentReadSegIndex], err)
				}
				w.currentReadSeg = nil
			}
			w.currentReadSegIndex++
			continue // Try reading from the next segment
		} else if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrPartialEntry) {
			// Partial entry, likely due to incomplete write. Truncate and move to next segment.
			// TODO: Implement actual truncation
			fmt.Printf("Warning: Partial entry detected in segment %d at offset %d. Moving to next segment.\n",
				w.activeSegments[w.currentReadSegIndex], w.currentReadOffset)
			if w.currentReadSeg != nil {
				if err := w.currentReadSeg.Close(); err != nil {
					fmt.Printf("Warning: Error closing segment %d: %v\n", w.activeSegments[w.currentReadSegIndex], err)
				}
				w.currentReadSeg = nil
			}
			w.currentReadSegIndex++
			continue
		} else if errors.Is(err, ErrChecksumMismatch) || errors.Is(err, ErrCorruptedEntry) || errors.Is(err, ErrInvalidEntryFormat) {
			// Corrupted entry, skip to the end of the current segment and move to next.
			// TODO: Implement skipping to the end of the current segment
			fmt.Printf("Warning: Corrupted entry detected in segment %d at offset %d. Skipping to next segment.\n",
				w.activeSegments[w.currentReadSegIndex], w.currentReadOffset)
			if w.currentReadSeg != nil {
				if err := w.currentReadSeg.Close(); err != nil {
					fmt.Printf("Warning: Error closing segment %d: %v\n", w.activeSegments[w.currentReadSegIndex], err)
				}
				w.currentReadSeg = nil
			}
			w.currentReadSegIndex++
			continue
		} else {
			// Other error, return it
			return nil, 0, err
		}
	}
}

// readSingleEntry attempts to read a single entry from the given reader.
// It returns the decrypted data, the number of bytes read, or an error.
func (w *WAL) readSingleEntry(r io.Reader) ([]byte, int, error) {
	var totalBytesRead int

	// Read checksum length
	checksumLenByte := make([]byte, 1)
	n, err := r.Read(checksumLenByte)
	if err != nil {
		return nil, totalBytesRead, err
	}
	if n != 1 {
		return nil, totalBytesRead, ErrPartialEntry
	}
	totalBytesRead += n
	checksumLen := int(checksumLenByte[0])

	// Read checksum
	checksum := make([]byte, checksumLen)
	n, err = r.Read(checksum)
	if err != nil {
		return nil, totalBytesRead, err
	}
	if n != checksumLen {
		return nil, totalBytesRead, ErrPartialEntry
	}
	totalBytesRead += n

	var nonce []byte
	if w.aesgcm != nil {
		// Read nonce length
		nonceLenByte := make([]byte, 1)
		n, err = r.Read(nonceLenByte)
		if err != nil {
			return nil, totalBytesRead, err
		}
		if n != 1 {
			return nil, totalBytesRead, ErrPartialEntry
		}
		totalBytesRead += n
		nonceLen := int(nonceLenByte[0])

		// Read nonce
		nonce = make([]byte, nonceLen)
		n, err = r.Read(nonce)
		if err != nil {
			return nil, totalBytesRead, err
		}
		if n != nonceLen {
			return nil, totalBytesRead, ErrPartialEntry
		}
		totalBytesRead += n
	}

	// Read data length (now encrypted data length)
	dataLenBytes := make([]byte, 4)
	n, err = r.Read(dataLenBytes)
	if err != nil {
		return nil, totalBytesRead, err
	}
	if n != 4 {
		return nil, totalBytesRead, ErrPartialEntry
	}
	totalBytesRead += n
	encryptedDataLen := bytesToUint32(dataLenBytes)

	// Read data (now encrypted data)
	encryptedData := make([]byte, encryptedDataLen)
	n, err = r.Read(encryptedData)
	if err != nil {
		return nil, totalBytesRead, err
	}
	if n != int(encryptedDataLen) {
		return nil, totalBytesRead, ErrPartialEntry
	}
	totalBytesRead += n

	var decryptedData []byte
	if w.aesgcm != nil {
		decryptedData, err = w.aesgcm.Open(nil, nonce, encryptedData, nil)
		if err != nil {
			return nil, totalBytesRead, ErrCorruptedEntry // Decryption failed, likely corrupted
		}
	} else {
		decryptedData = encryptedData
	}

	// Verify checksum
	hasher := blake3.New()
	hasher.Write(decryptedData)
	calculatedChecksum := hasher.Sum(nil)
	if !bytes.Equal(checksum, calculatedChecksum) {
		return nil, totalBytesRead, ErrChecksumMismatch
	}

	return decryptedData, totalBytesRead, nil
}
