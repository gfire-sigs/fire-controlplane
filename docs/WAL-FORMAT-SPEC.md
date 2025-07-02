# RFC: Encrypted Write-Ahead Log (WAL) File Format Specification - V2

**Category:** Informational  
**Status:** Active  
**Date:** July 2, 2025

---

## Table of Contents

1. Abstract
2. Introduction
3. Terminology
4. File Format Overview (V2)
5. File Header (V2)
6. Block Format (V2)
7. Encryption and Integrity Model (V2)
8. Integration with Sepia Storage Engine
9. Security Considerations
10. References

---

## 1. Abstract

This document specifies the V2 encrypted Write-Ahead Log (WAL) file format for the Sepia storage engine. This version moves from per-record encryption to a more performant and secure streaming model. The entire WAL file, excluding a plaintext header, is treated as a sequence of encrypted, authenticated blocks. This design minimizes metadata leakage and leverages `AES-256-CTR` for stream encryption and the `BLAKE3` keyed hash function for high-speed, robust integrity verification of each block.

---

## 2. Introduction

Write-Ahead Logging (WAL) is fundamental for durability and crash recovery. The V2 format enhances the previous design by adopting a streaming architecture. Instead of encrypting each database operation (record) individually, V2 groups records into blocks, which are then encrypted and authenticated. This approach significantly reduces the overhead of cryptographic operations, improves throughput, and enhances security by obscuring record boundaries.

---

## 3. Terminology

- **WAL (Write-Ahead Log):** A sequential log of database operations.
- **Record:** A single logged operation (e.g., put or delete).
- **Block:** A variable-sized container for one or more records, which serves as the fundamental unit of encryption and authentication.
- **AES-256-CTR:** A stream cipher mode of AES used for encrypting blocks.
- **BLAKE3:** A high-performance cryptographic hash function used in keyed mode as a Message Authentication Code (MAC).
- **Stream Cipher:** An encryption algorithm that encrypts data one byte or bit at a time, suitable for data of unknown length.

---

## 4. File Format Overview (V2)

A V2 WAL file consists of a single plaintext header followed by a sequence of encrypted and authenticated blocks.

```
+---------------------------+
| File Header (Plaintext)   |
+---------------------------+
| Block 1 (Authenticated)   |
+---------------------------+
| Block 2 (Authenticated)   |
+---------------------------+
| ...                       |
+---------------------------+
```

---

## 5. File Header (V2)

The header contains metadata required to initialize the decryption and verification processes. It remains in plaintext.

| Field                     | Description                                         | Size (Bytes) |
|---------------------------|-----------------------------------------------------|--------------|
| Magic Number              | Fixed identifier: `SEPIAWALMAGICV02`                | 16           |
| Version                   | Format version: `2`                                 | 1            |
| File ID                   | Unique identifier for the WAL file                  | 8            |
| Creation Timestamp        | Timestamp of file creation (Unix nano)              | 8            |
| Encryption Algorithm      | `1` for AES-256-CTR                                 | 1            |
| Hashing Algorithm         | `1` for BLAKE3                                      | 1            |
| File IV / Nonce           | 16-byte base IV for AES-CTR encryption              | 16           |
| BLAKE3 Key                | 32-byte key for the keyed BLAKE3 hash function      | 32           |
| Reserved                  | Reserved for future use (must be zero)              | 21           |
| **Total Header Size**     |                                                     | **104**      |

*Note: The `File IV` and `BLAKE3 Key` are derived from a master key at runtime and are not stored directly if a key management system is in use.*

---

## 6. Block Format (V2)

Each block is an independently verifiable unit containing a chunk of the WAL data stream.

| Field                     | Description                                         | Size (Bytes) |
|---------------------------|-----------------------------------------------------|--------------|
| Block Length              | 4-byte integer specifying the size of `Encrypted Data` | 4            |
| Encrypted Data            | A sequence of WAL records, encrypted with AES-256-CTR | Variable     |
| Authentication Tag (MAC)  | 32-byte BLAKE3 hash of (`Block Length` + `Encrypted Data`) | 32           |

- **WAL Records within Data:** The `Encrypted Data` field, when decrypted, reveals a sequence of tightly packed WAL records. Each record follows the format defined below, but without individual encryption or checksums.

### Inner Record Format (within a decrypted block)

| Field                     | Description                                         | Size (Bytes) |
|---------------------------|-----------------------------------------------------|--------------|
| Record Length             | Total length of this record                         | 4            |
| Record Type               | 1-byte enum (e.g., Put=1, Delete=2)                | 1            |
| Sequence Number           | 8-byte integer for ordering operations             | 8            |
| Key Length                | 4-byte integer for key length                      | 4            |
| Value Length              | 4-byte integer for value length (0 for Delete)     | 4            |
| Key Bytes                 | Key data                                           | Variable     |
| Value Bytes               | Value data (only for Put)                          | Variable     |

---

## 7. Encryption and Integrity Model (V2)

- **Encryption:** `AES-256-CTR` is used to encrypt the stream of WAL records. The CTR mode allows for parallel encryption/decryption and does not require padding, making it ideal for streaming. A unique IV is constructed for each block by combining the `File IV` from the header with the block's sequence number (or offset) to prevent nonce reuse.
- **Integrity:** The integrity of each block is protected by a **keyed BLAKE3 hash**. This acts as a high-performance MAC. The MAC is computed over the `Block Length` field and the `Encrypted Data`. This prevents any tampering with the block's size or its content. Using a keyed hash function like BLAKE3 is critical for preventing forgery, as an unkeyed hash would be vulnerable.

---

## 8. Integration with Sepia Storage Engine

- **Writing to WAL:**
  1. Operations are serialized into the inner record format and written to an in-memory buffer.
  2. When the buffer reaches a predefined capacity (e.g., 64KB) or a `Sync` operation is triggered, the buffer's content is treated as a block.
  3. The block is encrypted with AES-CTR, and a BLAKE3 MAC is computed.
  4. The `Block Length`, `Encrypted Data`, and `MAC` are written to the WAL file as a single atomic unit.
- **Recovery Process:**
  1. On startup, Sepia reads the WAL file header to initialize the AES and BLAKE3 ciphers.
  2. It then reads the file block by block. For each block:
     a. Read `Block Length`, `Encrypted Data`, and `MAC`.
     b. Verify the `MAC` using the `BLAKE3 Key`. If verification fails, the WAL is considered corrupt.
     c. If successful, decrypt `Encrypted Data` using AES-CTR.
  3. The decrypted data (containing inner records) is then parsed, and operations are replayed to rebuild the memtable. This can be implemented via a streaming `io.Reader` that transparently handles the block-level verification and decryption.

---

## 9. Security Considerations

1.  **Key Management:** The master encryption key (from which the AES key and BLAKE3 key are derived) must be managed securely (e.g., via HSM, KMS, or OS keyring).
2.  **Nonce Reuse:** It is critical to ensure the (IV, Key) pair is never reused for AES-CTR. The implementation must guarantee a unique IV for each block.
3.  **Integrity:** The use of a strong keyed MAC (BLAKE3) is essential. Without it, an attacker could manipulate the encrypted ciphertext even without knowing the key.
4.  **Metadata Protection:** This V2 format is superior to V1 as it hides individual record boundaries and lengths within the encrypted blocks, reducing information leakage.

---

## 10. References

- Sepia Storage Module SST File Format Specification (`SST-FORMAT-SPEC.md`)
- BLAKE3 Hash Function: [github.com/BLAKE3-team/BLAKE3](https://github.com/BLAKE3-team/BLAKE3)
- AES-CTR Mode of Operation