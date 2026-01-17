# Iron-Python Storage Engine

![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Architecture](https://img.shields.io/badge/architecture-LSM--Tree-orange)

**Iron-Python** is a production-grade, persistent Key-Value Store built from scratch in Python. It implements a **Log-Structured Merge (LSM) Tree** architecture, prioritizing high write throughput and durability.

This project moves beyond standard library abstractions to interact directly with the "physics" of data: handling disk I/O, memory layout, and OS-level file locking.

---

## 🏗 Architecture & Design Decisions

Unlike relational databases (PostgreSQL/MySQL) that use B-Trees for read-heavy workloads, Iron-Python is optimized for **write-heavy** systems (similar to RocksDB or Cassandra). By treating the disk as a sequential log, it eliminates random disk seeks, maximizing write throughput.

```
sequenceDiagram
    participant User
    participant Engine as StorageEngine
    participant WAL as Write-Ahead Log (Disk)
    participant Mem as MemTable (RAM)
    participant SST as SSTable (Disk)

    Note over User, SST: 🚀 Phase 1: The Write Path (Durability First)
    User->>Engine: put(key, value)
    Engine->>WAL: append(key, value)
    WAL-->>Engine: ACK (fsync complete)
    [cite_start]Note right of WAL: Data is now safe from power loss [cite: 37, 44]
    Engine->>Mem: insert(key, value)
    Mem-->>Engine: OK

    Note over User, SST: ❄️ Phase 2: The Flush (Immutable Storage)
    Engine->>Mem: Check Size > Threshold
    alt MemTable Full
        Engine->>SST: Flush Sorted Data to .sst File
        [cite_start]Note right of SST: Sequential Write (High Throughput) [cite: 9-11]
        Engine->>Mem: Reset / Clear
    end

    Note over User, SST: 🔍 Phase 3: The Read Path (Tiered Lookup)
    User->>Engine: get(key)
    Engine->>Mem: search(key)
    alt Found in RAM
        Mem-->>User: return value
    else Not in RAM
        Engine->>SST: search(key) via mmap
        [cite_start]Note right of SST: Zero-Copy Read [cite: 51]
        SST-->>User: return value
    end
```

### 1. The MemTable (In-Memory Buffer)
* **Structure:** A **Skip List** (Probabilistic Data Structure).
* **Rationale:** While Red-Black trees offer $O(\log N)$ balanced access, they are complex to implement without object overhead. Skip Lists provide the same asymptotic complexity with significantly simpler concurrency logic.
* **Memory Optimization:** Standard Python classes consume high memory due to the internal `__dict__` attribute. This engine utilizes `__slots__` for all Node objects, statically allocating memory and preventing the "catastrophic" overhead of millions of dictionary creations.

### 2. Durability (Write-Ahead Log)
* **Mechanism:** Append-only log files.
* **Safety:** To protect against power loss, writes are appended to a WAL before touching memory. The engine uses `os.fsync` to force the OS kernel to flush its page cache to the physical disk hardware, ensuring strict durability.
* **Format:** Binary-packed `[KeyLen][Key][ValLen][Val]` for minimal storage overhead.

### 3. Storage (SSTables & mmap)
* **Immutable Files:** When the MemTable fills (e.g., 64MB), it is flushed to disk as a **Sorted String Table (SSTable)**.
* **Zero-Copy I/O:** Instead of standard file I/O (which copies data from Kernel Space -> User Space), this engine uses **Memory-Mapped I/O (`mmap`)**. This maps the file directly into the process's virtual address space, allowing the OS to manage paging transparently and reducing the memory footprint.

### 4. Compaction (Garbage Collection)
* **The Problem:** Continuous flushing creates many overlapping files, leading to "Read Amplification" (checking multiple files for one key).
* **The Solution:** A background process performs a **K-Way Merge Sort** on existing SSTables. It merges files and removes overwritten/deleted keys (deduplication) to reclaim space and restore read performance.

---

## 🚀 Quick Start

### Installation
```bash
git clone [https://github.com/your-username/iron-python-storage.git](https://github.com/your-username/iron-python-storage.git)
cd iron-python-storage
pip install -r requirements.txt
```

## Basic Usage
The API mimics a standard dictionary but persists to disk

```
from src.storage_engine.engine import StorageEngine

# Initialize the engine (creates ./data directory)
db = StorageEngine(dir_path="data")

# Write (Persists to WAL + MemTable)
db.put("user:101", "Alice")
db.put("user:102", "Bob")

# Read (Scans MemTable -> SSTables)
print(db.get("user:101"))  # Output: Alice

# Close (safely releases file handles)
db.close()
```

## Performance Benchmarks
To validate the architectural choices, the engine includes a `benchmark.py` script that simulates high-load ingestion.

### Test Environment
* **Records:** 100,000 Keys
* **Payload:** 16B Keys, 100B Values
* **Strategy:** Sequential Write/Random Read

### Results (Sample):
```
[Phase 1] Writing Data...
-> Write Throughput: ~2,500 ops/sec
   (High throughput due to Append-Only sequential patterns)

[Phase 2] Reading Data (Random Access)...
-> Read Latency: ~0.04 ms/op
   (Low latency due to Memory-Mapped OS caching)
```

To run the benchmark yourself:
```bash
python3 benchmark.py
```

## Development Roadmap
* **Core Engine:** MemTable, WAL, SSTable flushing.
* **Optimization:** `__slots__` for memory, `mmap` for I/O.
* **Maintenance:** Leveled Compaction strategy.
* **Recovery:** Reconstruct MemTable from WAL on startup.
* **Indexing:** Implement Bloom Filters to eliminate unnecessary disk lookups for non-existent keys.
* **Concurrency:** Move compaction to a separate `multiprocessing` worker.
