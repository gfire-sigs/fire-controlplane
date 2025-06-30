# RFC: Efficient SST File Format with Prefix Compression (Revised)

**Category:** Informational  
**Status:** Draft  
**Date:** June 29, 2025

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
   9.2 Filter Blocks with DBloom
   9.3 Encryption
10. Tuning Parameters
11. Security Considerations
12. References

---

## 1. Abstract

This revised document specifies an efficient Sorted String Table (SST) file format optimized for prefix compression, incorporating implementation details from the `dsst` and `dbloom` components of the Sepia storage module. The format leverages the sorted order of keys to reduce storage space while maintaining fast lookup capabilities through Bloom filters, suitable for use in LSM-tree based storage engines.

---

## 2. Introduction

Sorted String Tables (SSTs) are immutable, sorted key-value stores fundamental to Log-Structured Merge-tree (LSM-tree) databases. This specification describes an SST format that employs prefix compression via the `dsst` package to exploit common key prefixes, reducing storage requirements, and integrates `dbloom` for probabilistic filtering to enhance lookup performance.

---

## 3. Terminology

- **SST (Sorted String Table):** Immutable, sorted key-value storage file.
- **Prefix Compression:** Technique storing only the differing suffix of a key relative to the previous key, implemented in `dsst`.
- **Restart Point:** Entry within a block where prefix compression resets, enabling efficient seeking.
- **Block:** Fixed-size unit of storage within the SST file.
- **Footer:** Fixed-size metadata at the end of the SST file.
- **DBloom:** A Bloom filter implementation for quick key existence checks, reducing unnecessary disk reads.

---

## 4. File Format Overview

An SST file consists of a sequence of data blocks, followed by an index block, a metaindex block, a Bloom filter block, and a footer, as defined by the `dsst` package.

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
| Bloom Filter Block|
+-------------------+
| Footer            |
+-------------------+
```

---

## 5. Data Block Format

Data blocks store sorted key-value entries with prefix compression, managed by the `dsst` package.

### 5.1 Block Header

Each data block is prefixed with a fixed-size header containing metadata about the block. This header is included in the checksum calculation, as per `dsst.BlockHeader`.

| Field                     | Description                                         |
|---------------------------|-----------------------------------------------------|
| compression_type          | 1-byte enum indicating the compression algorithm used for the block (e.g., None, Snappy, Zstd). |
| initialization_vector     | 12-byte unique IV for AES-GCM encryption.           |

*Note*: The `authentication_tag` (16-byte GCM tag for authenticated encryption) is placed immediately after the encrypted data, not in the header, to optimize performance by reducing memory operations during data appending.

### 5.1 Entry Encoding

Each entry encodes key-value pairs or tombstones using prefix compression, following the `dsst.encodeEntry` method:

| Field               | Description                                         |
|---------------------|-----------------------------------------------------|
| entry_type          | 1-byte enum indicating type (KeyValue or Tombstone) |
| shared_prefix_len   | Length of prefix shared with previous key (8-byte integer)  |
| unshared_key_len    | Length of non-shared key suffix (8-byte integer)           |
| value_len           | Length of value (8-byte integer, only for KeyValue entries)|
| unshared_key_bytes  | Suffix bytes                                       |
| value_bytes         | Value bytes (only for KeyValue entries)            |

- The first entry in a block has `shared_prefix_len = 0`.
- The `dsstCommonPrefix` function determines the shared prefix length between consecutive keys.

### 5.2 Restart Points

- Full keys are stored at regular intervals (e.g., every 16 entries) to facilitate seeking.
- Restart points enable fast binary search within a block.
- A list of restart offsets is stored at the end of each block.

### 5.3 Block Trailer

The trailer contains:

| Field                     | Description                     |
|---------------------------|---------------------------------|
| restart_point_offsets[]   | List of restart offsets         |
| restart_point_count       | Number of restart points        |
| block_checksum            | 8-byte wyhash checksum (covers block header + compressed data + restart points + restart point count) |

---

## 6. Index Block

The index block contains entries mapping key ranges to data block offsets, implemented as `dsst.indexEntry`. It stores the first key of each data block (uncompressed) and the corresponding block offset, enabling fast block-level binary search.

---

## 7. Metaindex Block

The metaindex block stores the `SSTableConfigs` structure from `dsst`, which defines the global configuration parameters for the SST file.

| Field                     | Description                                         |
|---------------------------|-----------------------------------------------------|
| CompressionType           | 1-byte enum indicating the compression algorithm used for data blocks. |
| BlockSize                 | 4-byte unsigned integer representing the target size for data blocks in bytes (e.g., 64KB). |
| RestartInterval           | 4-byte unsigned integer representing the number of keys between restart points within a data block. |
| WyhashSeed                | 8-byte unsigned integer used as the seed for wyhash checksums throughout the file. |
| BloomFilterBitsPerKey     | Integer defining bits per key for the Bloom filter. |
| BloomFilterNumHashFuncs   | Integer defining number of hash functions for the Bloom filter. |

---

## 8. Footer

The footer is a fixed-size structure as defined by `dsst.SSTFooter`, containing:

- Offset and size of the metaindex block (`MetaindexHandle`)
- Offset and size of the index block (`IndexHandle`)
- Offset and size of the Bloom filter block (`BloomFilterHandle`)
- `WyhashSeed` (8 bytes)
- Magic number identifying the file format (`SEPIASSTMAGICV01`)
- Version information

The footer size is fixed at 80 bytes, accommodating all handles and metadata.

---

## 9. Optional Features

### 9.1 Block Compression

Data blocks may be compressed using lightweight algorithms such as Snappy or Zstd after prefix compression to further reduce storage size, supported by `dsst.CompressionType`.

### 9.2 Filter Blocks with DBloom

Filter blocks utilize the `dbloom.Bloom` structure to implement Bloom filters, allowing quick checks for key existence without reading data blocks. Key features include:
- Configurable parameters via `BloomFilterBitsPerKey` and `BloomFilterNumHashFuncs` in `SSTableConfigs`.
- Methods for setting and checking membership (`Set`, `Get`, `SetString`, `GetString`) using wyhash for hashing.
- Serialization support for persistence within the SST file.

Each data block or range may have an associated Bloom filter entry (`blockBloomFilterEntry`) linking the first key and block handle to the filter.

### 9.3 Encryption

Data blocks are always encrypted using AES-GCM-256. The encryption key is provided to the reader and writer at runtime and is not stored in the SST file.

- **IV Generation**: 12-byte random nonce per block
- **Authentication**: 16-byte GCM tag per block, placed immediately after the encrypted data to optimize performance.
- **Header Fields**
  - `initialization_vector` (12 bytes)

---

## 10. Tuning Parameters

These parameters are part of the `SSTableConfigs` structure stored in the Metaindex Block, allowing for file-specific configuration.

| Parameter                 | Description                                    | Typical Value |
|---------------------------|------------------------------------------------|---------------|
| Block size                | Size of each data block                       | 64KB          |
| Restart interval          | Entries between restart points                | 16            |
| Bloom filter bits per key | Bits allocated per key in Bloom filter        | 10            |
| Bloom filter hash funcs   | Number of hash functions for Bloom filter     | 5             |

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
- Sepia Storage Module (`dsst` and `dbloom` packages)
