package dsst

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

var (
	// DefaultKey is used when encryption is disabled or no key is provided.
	// It ensures the file format remains consistent.
	// In a real scenario, this might be a fixed known key or handled differently.
	DefaultKey = []byte("sepia-default-encryption-key-32b")
)

// Encryptor handles encryption and decryption of SST data.
type Encryptor struct {
	kek []byte // Key Encryption Key (Master Key)
	dek []byte // Data Encryption Key (Randomly generated per file)
}

// NewEncryptor creates a new Encryptor.
// If masterKey is nil, the DefaultKey is used.
func NewEncryptor(masterKey []byte) (*Encryptor, error) {
	if masterKey == nil {
		masterKey = DefaultKey
	}
	if len(masterKey) != 32 {
		return nil, errors.New("master key must be 32 bytes")
	}

	// Generate a random DEK
	dek := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, err
	}

	return &Encryptor{
		kek: masterKey,
		dek: dek,
	}, nil
}

// NewEncryptorWithDEK creates an Encryptor with a known DEK (used for reading).
func NewEncryptorWithDEK(masterKey, dek []byte) (*Encryptor, error) {
	if masterKey == nil {
		masterKey = DefaultKey
	}
	if len(masterKey) != 32 {
		return nil, errors.New("master key must be 32 bytes")
	}
	if len(dek) != 32 {
		return nil, errors.New("DEK must be 32 bytes")
	}
	return &Encryptor{
		kek: masterKey,
		dek: dek,
	}, nil
}

// EncryptDEK encrypts the DEK using the KEK with AES-GCM.
// Returns nonce + ciphertext.
func (e *Encryptor) EncryptDEK() ([]byte, error) {
	block, err := aes.NewCipher(e.kek)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	return gcm.Seal(nonce, nonce, e.dek, nil), nil
}

// DecryptDEK decrypts the DEK using the KEK with AES-GCM.
// Expects nonce + ciphertext.
func DecryptDEK(masterKey, encryptedDEK []byte) ([]byte, error) {
	if masterKey == nil {
		masterKey = DefaultKey
	}
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(encryptedDEK) < nonceSize {
		return nil, errors.New("malformed encrypted DEK")
	}

	nonce, ciphertext := encryptedDEK[:nonceSize], encryptedDEK[nonceSize:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}

// NewStreamWriter returns a writer that encrypts data using AES-CTR with the DEK.
// The iv must be unique for each stream (e.g., block index or offset).
func (e *Encryptor) NewStreamWriter(w io.Writer, iv []byte) (io.Writer, error) {
	block, err := aes.NewCipher(e.dek)
	if err != nil {
		return nil, err
	}

	// CTR mode requires an IV of block size (16 bytes for AES).
	// We assume the caller provides a suitable IV (e.g. derived from offset).
	if len(iv) != aes.BlockSize {
		return nil, errors.New("IV must be 16 bytes")
	}

	stream := cipher.NewCTR(block, iv)
	return &cipher.StreamWriter{S: stream, W: w}, nil
}

// NewStreamReader returns a reader that decrypts data using AES-CTR with the DEK.
func (e *Encryptor) NewStreamReader(r io.Reader, iv []byte) (io.Reader, error) {
	block, err := aes.NewCipher(e.dek)
	if err != nil {
		return nil, err
	}

	if len(iv) != aes.BlockSize {
		return nil, errors.New("IV must be 16 bytes")
	}

	stream := cipher.NewCTR(block, iv)
	return &cipher.StreamReader{S: stream, R: r}, nil
}

// BlockCipher returns the cipher.Block for the DEK.
// Useful for manual block encryption if needed.
func (e *Encryptor) BlockCipher() (cipher.Block, error) {
	return aes.NewCipher(e.dek)
}
