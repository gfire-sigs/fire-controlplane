# RFC: Efficient SST File Format with Prefix Compression

**Category:** Informational  
**Status:** Draft  
**Date:** April 6, 2025

---

## Table of Contents

1. Abstract
2. Introduction
3. Terminology
4. File Format Overview
5. Data Block Format
   5.1 Entry Encoding
   5.2 Restart Points
   5.3 Block Trailer
6. Index Block
7. Metaindex Block
8. Footer
9. Optional Features
   9.1 Block Compression
   9.2 Filter Blocks
   9.3 Encryption
10. Tuning Parameters
11. Security Considerations
12. References

---

## 1. Abstract

This document specifies an efficient Sorted String Table (SST) file format optimized for prefix compression. The format leverages the sorted order of keys to reduce storage space while maintaining fast lookup capabilities, suitable for use in LSM-tree based storage engines.

---

## 2. Introduction

Sorted String Tables (SSTs) are immutable, sorted key-value stores fundamental to Log-Structured Merge-tree (LSM-tree) databases. This specification describes an SST format that employs prefix compression to exploit common key prefixes, reducing storage requirements without sacrificing lookup performance.

---

## 3. Terminology

- **SST (Sorted String Table):** Immutable, sorted key-value storage file.
- **Prefix Compression:** Technique storing only the differing suffix of a key relative to the previous key.
- **Restart Point:** Entry within a block where prefix compression resets, enabling efficient seeking.
- **Block:** Fixed-size unit of storage within the SST file.
- **Footer:** Fixed-size metadata at the end of the SST file.

---

## 4. File Format Overview

An SST file consists of a sequence of data blocks, followed by an index block, a metaindex block, and a footer.

```
+-------------------+
| Data Block 1      |
+-------------------+
| Data Block 2      |
+-------------------+
| ...               |
+-------------------+
| Index Block       |
+-------------------+
| Metaindex Block   |
+-------------------+
| Footer            |
+-------------------+
```

---

## 5. Data Block Format

Data blocks store sorted key-value entries with prefix compression.

### 5.1 Block Header

Each data block is prefixed with a fixed-size header containing metadata about the block. This header is included in the checksum calculation.

| Field           | Description                                         |
|-----------------|-----------------------------------------------------|
| compression_type| 1-byte enum indicating the compression algorithm used for the block (e.g., None, Snappy, Zstd). |

### 5.2 Entry Encoding

Each entry encodes:

| Field               | Description                                         |
|---------------------|-----------------------------------------------------|
| shared_prefix_len   | Length of prefix shared with previous key (varint)  |
| unshared_key_len    | Length of non-shared key suffix (varint)           |
| value_len           | Length of value (varint)                            |
| unshared_key_bytes  | Suffix bytes                                       |
| value_bytes         | Value bytes                                        |

- The first entry in a block has `shared_prefix_len = 0`.

### 5.3 Restart Points

- Full keys are stored at regular intervals (e.g., every 16 entries).
- Restart points enable fast seeking within a block.
- A list of restart offsets is stored at the end of each block.

### 5.4 Block Trailer

The trailer contains:

| Field                     | Description                     |
|---------------------------|---------------------------------|
| restart_point_count       | Number of restart points        |
| restart_point_offsets[]   | List of restart offsets         |
| initialization_vector     | 12-byte unique IV (if encrypted)|
| authentication_tag        | 16-byte GCM tag (if encrypted)  |
| block_checksum (optional) | 8-byte wyhash checksum (covers block header + compressed data + restart points + restart point count) |

---

## 6. Index Block

The index block contains entries mapping key ranges to data block offsets. Typically, it stores the first key of each data block (uncompressed) and the corresponding block offset, enabling fast block-level binary search.

---

## 7. Metaindex Block

The metaindex block currently stores the `SSTableConfigs` structure, which defines the global configuration parameters for the SST file. In future versions, it may be extended to include pointers to other auxiliary metadata, such as filter blocks or properties.

| Field             | Description                                         |
|-------------------|-----------------------------------------------------|
| CompressionType   | 1-byte enum indicating the compression algorithm used for data blocks. |
| BlockSize         | 4-byte unsigned integer representing the target size for data blocks in bytes (e.g., 64KB). |
| RestartInterval   | 4-byte unsigned integer representing the number of keys between restart points within a data block. |
| WyhashSeed        | 8-byte unsigned integer used as the seed for wyhash checksums throughout the file. |

---

## 8. Footer

The footer is a fixed-size structure containing:

- Offset and size of the metaindex block
- Offset and size of the index block
- `wyhash_seed` (8 bytes)
- Magic number identifying the file format
- Version information

---

## 9. Optional Features

### 9.1 Block Compression

Data blocks may be compressed using lightweight algorithms such as Snappy or zstd after prefix compression to further reduce storage size.

### 9.2 Filter Blocks

Filter blocks, such as Bloom filters, may be included to quickly rule out non-existent keys without reading data blocks.

### 9.3 Encryption

Data blocks may be encrypted using AES-GCM-256 with the following parameters:

- **Key Derivation**: HKDF-SHA256 with:
  - Master key (32 bytes)
  - Per-file salt (16 bytes random)
  - Context string "sst_encryption"
- **IV Generation**: 12-byte random nonce per block
- **Authentication**: 16-byte GCM tag per block
- **Header Fields**:
  - Encryption algorithm (1 byte)
  - Key derivation salt (16 bytes)
  - KDF iteration count (4 bytes)

---

## 10. Tuning Parameters

These parameters are now part of the `SSTableConfigs` structure stored in the Metaindex Block, allowing for file-specific configuration.

| Parameter            | Description                                    | Typical Value |
|----------------------|------------------------------------------------|---------------|
| Block size           | Size of each data block                       | 64KB          |
| Restart interval     | Entries between restart points                | 16            |

---

## 11. Security Considerations

1. **Encryption Security**:
   - Master keys must be stored securely (HSM or OS keyring)
   - Per-file keys must never be stored persistently
   - IVs must be unique for each block

2. **Data Integrity**:
   - Use authenticated encryption (GCM) for encrypted blocks
   - Validate checksums before decryption

3. **Access Control**:
   - Encrypt sensitive metadata in footer
   - Rate-limit decryption attempts

4. **Key Rotation**:
   - Re-encrypt data when master keys are rotated
   - Maintain versioning for active keys

---

## 12. References

- LevelDB SSTable Format  
- RocksDB Prefix Compression  
- LSM-tree Design Principles
