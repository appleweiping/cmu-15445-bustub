# CMU 15-445 BusTub — Disk-Oriented DBMS in C++

> A relational database management system built from the official BusTub skeleton — an independent,
> from-skeleton implementation of **CMU 15-445/645 — Database Systems** (Carnegie Mellon University,
> Fall 2023), part of a [csdiy.wiki](https://csdiy.wiki/) full-catalog build.

![status](https://img.shields.io/badge/status-P0--P4%20complete%20(2%20bonus%20partial)-brightgreen)
![language](https://img.shields.io/badge/C++17-informational)
![license](https://img.shields.io/badge/license-MIT-blue)

> The original upstream BusTub README is preserved as [`README_BUSTUB.md`](README_BUSTUB.md).

## Overview

BusTub is a disk-oriented relational DBMS used to teach CMU's database systems course. This repo
implements every project of the **Fall 2023** semester on top of the official
[`cmu-db/bustub`](https://github.com/cmu-db/bustub) skeleton (tag `v20231227-2023fall`):

- **P0** — a copy-on-write Trie key-value store, a thread-safe `TrieStore`, and the `upper`/`lower`
  SQL scalar functions (with planner wiring).
- **P1** — the storage / buffer-management layer: an LRU-K replacer, a disk scheduler with a
  background worker thread, and the buffer pool manager.
- **P2** — a disk-backed **extendible hash index**: RAII page guards, the header/directory/bucket
  pages, and the full extendible hashing container (grow/split/merge, latch crabbing).
- **P3** — the **query execution engine**: all access-method, join, aggregation, and sort/limit/topn
  executors plus the window-function executor, and three optimizer rules.
- **P4** — **multi-version concurrency control (MVCC)**: timestamps, an O(log N) watermark, tuple
  reconstruction, MVCC scans, versioned insert/update/delete with undo logs, commit, garbage
  collection, transaction abort, and primary-key index maintenance.

Every project is verified against the course's own test harness (the shipped gtest suites and the
`bustub-sqllogictest` runner). All captured outputs live in [`results/`](results/).

## Results (measured on Ubuntu 24.04 WSL2, g++ 13.3, CMake Release, 12 threads)

| Project | What it implements | Result (measured) |
|---|---|---|
| **P0** C++ Primer | COW Trie, TrieStore, upper/lower | `trie_test` 14/14, `trie_noncopy` 1/1, `trie_store` 4/4, `trie_store_noncopy` 2/2, `p0.01-lower-upper.slt` pass |
| **P1** Buffer Pool Manager | LRU-K replacer, disk scheduler, BPM | `lru_k_replacer_test` 1/1, `disk_scheduler_test` 1/1, `buffer_pool_manager_test` 2/2 |
| **P2** Extendible Hash Index | page guards, htable pages, extendible hashing | `page_guard_test` 1/1, `extendible_htable_page_test` 2/2, `extendible_htable_test` 3/3, `extendible_htable_concurrent_test` 6/6 |
| **P3** Query Execution | 12 executors + 3 optimizer rules | **21/21** official `p3.NN-*.slt` sqllogictest files pass |
| **P4** Concurrency Control | MVCC timestamps/scan/DML/GC/abort/PK-index | `txn_timestamp_test` 2/2, `txn_scan_test` 2/2, `txn_executor_test` 10/10, `txn_index_test` 5/5, concurrent-insert pass |

Total verified: **P0** 21 gtests + 1 slt, **P1** 4 gtests, **P2** 12 gtests, **P3** 21 slt files,
**P4** 19 serial gtests + concurrent-insert. See [`results/`](results/) for the raw run logs.

### P3 query execution — all 21 official sqllogictests pass

```
PASS p3.00-primer      PASS p3.07-simple-agg        PASS p3.14-hash-join
PASS p3.01-seqscan     PASS p3.08-group-agg-1       PASS p3.15-multi-way-hash-join
PASS p3.02-insert      PASS p3.09-group-agg-2       PASS p3.16-sort-limit
PASS p3.03-update      PASS p3.10-simple-join       PASS p3.17-topn
PASS p3.04-delete      PASS p3.11-multi-way-join    PASS p3.18-integration-1
PASS p3.05-index-scan  PASS p3.12-repeat-execute    PASS p3.19-integration-2
PASS p3.06-empty-table PASS p3.13-nested-index-join PASS p3.20-window-function
```

## Implemented assignments

- [x] **P0 — C++ Primer** — copy-on-write `Trie::Get/Put/Remove` with structural sharing; thread-safe
  `TrieStore` (snapshot-read, single-writer); `StringExpression` upper/lower + planner `PlanFuncCall`.
- [x] **P1 — Buffer Pool Manager** — LRU-K replacer (backward k-distance, +inf for <k, LRU tiebreak);
  disk scheduler (background thread over a request channel); BPM (free-list-first frames, dirty
  writeback, pin counts, page-guard wrappers).
- [x] **P2 — Extendible Hash Index** — `BasicPageGuard`/`ReadPageGuard`/`WritePageGuard` (move/drop/
  upgrade, latch ordering); header/directory/bucket pages; `DiskExtendibleHashTable` insert with
  directory growth + bucket split + entry migration, get, remove with merge + shrink; latch crabbing.
- [x] **P3 — Query Execution** — SeqScan, Insert, Update, Delete, IndexScan, Aggregation,
  NestedLoopJoin, HashJoin, NestedIndexJoin, Sort, Limit, TopN, WindowFunction; optimizer rules
  SeqScan→IndexScan, NLJ→HashJoin, Sort+Limit→TopN.
- [x] **P4 — Concurrency Control (MVCC)** — Begin/Commit timestamps, O(log N) watermark,
  `ReconstructTuple`, MVCC seq/index scan, versioned insert/update/delete with undo logs,
  garbage collection, transaction abort, primary-key index (dup detection, RID reuse, PK-update
  delete+insert), atomic version-link CAS on writes.
- [~] **P4 bonus** — serializable OCC verification and the concurrent delete+reinsert stress test are
  documented partials (see *Partials* below).

## Project structure

```
cmu-15445-bustub/
├── src/
│   ├── primer/                 # P0: trie, trie_store
│   ├── buffer/                 # P1: lru_k_replacer, buffer_pool_manager
│   ├── storage/
│   │   ├── disk/               # P1: disk_scheduler
│   │   └── page/               # P2: page_guard, extendible_htable_*_page
│   ├── container/disk/hash/    # P2: disk_extendible_hash_table
│   ├── execution/              # P3: *_executor, execution_common (P4 helpers)
│   ├── optimizer/              # P3: nlj_as_hash_join, sort_limit_as_topn, seqscan_as_indexscan
│   └── concurrency/            # P4: transaction_manager, watermark
├── test/                       # course-provided gtest + sqllogictest suites
├── results/                    # captured, measured test outputs (this build)
└── third_party/                # vendored deps (fmt, googletest, libpg_query, ...)
```

## How to run

BusTub is Linux/macOS-oriented; this build uses **WSL2 Ubuntu 24.04**. C: is kept clean by building on
a native ext4 path (git stays on the Windows drive; source is rsync'd to `~/bustub-build` to avoid 9p
filesystem slowness).

```bash
# 1. Install toolchain deps (Ubuntu):
sudo ./build_support/packages.sh -y   # or: build-essential cmake zlib1g-dev libelf-dev libdwarf-dev

# 2. Configure + build (Release):
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..

# 3. Run the tests. NOTE: the course ships public tests with a DISABLED_ prefix,
#    so pass --gtest_also_run_disabled_tests.
make trie_test trie_store_test lru_k_replacer_test buffer_pool_manager_test \
     disk_scheduler_test page_guard_test extendible_htable_test \
     txn_timestamp_test txn_scan_test txn_executor_test txn_index_test -j$(nproc)
./test/trie_test --gtest_also_run_disabled_tests
./test/buffer_pool_manager_test --gtest_also_run_disabled_tests
./test/extendible_htable_test --gtest_also_run_disabled_tests
./test/txn_executor_test --gtest_also_run_disabled_tests

# 4. P3 query execution is graded by the sqllogictest runner:
make sqllogictest -j$(nproc)
for f in ../test/sql/p3.[0-9]*.slt; do ./bin/bustub-sqllogictest "$f"; done
```

## Verification

Each project was verified with the course's **own** shipped tests, and the actual output was captured
to `results/`:

- `results/p0_primer.txt` — trie / trie_store gtests + `p0.01-lower-upper.slt`.
- `results/p1_buffer_pool.txt` — LRU-K, disk scheduler, buffer pool manager gtests.
- `results/p2_extendible_hash.txt` — page guard, htable page, htable, and concurrent htable gtests.
- `results/p3_query_execution.txt` — all 21 `p3.NN-*.slt` sqllogictest files (21 passed, 0 failed).
- `results/p4_concurrency_control.txt` — timestamp, scan, executor, and index MVCC gtests.

## Partials

Per the build spec, anything not fully passing is documented rather than faked:

- **P4 bonus — Serializable verification (`TxnBonusTest.SerializableTest`)**: `VerifyTxn` returns
  `true` (no OCC backward validation), so serializable-isolation abort-on-conflict is not enforced.
  Snapshot-isolation (the default, and everything P4 tasks 1–4 require) is fully implemented.
- **P4 — `IndexConcurrentUpdateTest`** (the 50-trial × 8-thread delete+reinsert stress test) is not
  passing. The single-writer serial correctness is complete and every serial P4 test passes; the
  write path installs new versions with an atomic compare-and-set on the version link, and
  `IndexConcurrentInsertTest` passes. The delete+reinsert stress path needs additional in-page
  version-chain locking to fully serialize concurrent reuse of a deleted RID, which is not
  implemented here. This is a documented partial, not a stub.

Everything else — P0, P1, P2, P3 in full, and P4 tasks 1–4 (timestamps, watermark, tuple
reconstruction, MVCC scans, versioned DML, commit, garbage collection, abort, primary-key index) —
is fully implemented and verified.

## Tech stack

C++17, CMake, GoogleTest, the BusTub sqllogictest harness; `fmt`, `libpg_query` (SQL parser),
`murmur3`, and other vendored `third_party` libraries. Built with g++ 13 on Ubuntu 24.04 (WSL2).

## Key ideas / what I learned

- **Copy-on-write persistent data structures** — the Trie rebuilds only the path to a changed node
  and shares all untouched subtrees, giving cheap immutable snapshots for lock-free reads.
- **Buffer management** — LRU-K's backward k-distance approximates access frequency far better than
  plain LRU; a disk scheduler decouples I/O from the caller via futures and a background thread.
- **Extendible hashing** — the directory doubles on overflow while buckets split locally; latch
  crabbing (header→directory→bucket) keeps concurrent operations correct without a global lock.
- **The Volcano / iterator model** — every operator is a pull-based `Init`/`Next`; the optimizer
  rewrites the plan tree (NLJ→hash join, sort+limit→top-N, seq-scan→index-scan) before execution.
- **MVCC** — tuples carry timestamps and a chain of undo logs; a reader reconstructs the version
  visible at its read timestamp, writers stamp a temporary (transaction-id) timestamp until commit,
  a watermark bounds what garbage collection may reclaim, and write-write conflicts are detected via
  an atomic compare-and-set on the version link.

## Credits & license

Based on the projects of **CMU 15-445/645 Database Systems (Fall 2023)** by Andy Pavlo and the
Carnegie Mellon University Database Group. This repository is an independent educational
implementation built on the official [`cmu-db/bustub`](https://github.com/cmu-db/bustub) skeleton
(tag `v20231227-2023fall`); all course materials, the skeleton, and the test suites belong to their
original authors and remain under CMU's MIT [`LICENSE`](LICENSE). Original implementation code in
this repo is released under the same MIT terms.
