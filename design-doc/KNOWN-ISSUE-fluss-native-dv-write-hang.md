<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Resolved: the E2E "DV write hang" is a local-environment issue, not a DV bug

Status: **proven by thread dumps — all writes hang identically, including a committed CI-passing
test. There is no deletion-vector write bug.**

## The evidence (failsafe JVM thread dumps)

Every hung run parks the JUnit `main` thread at the same place:

```
CountDownLatch.await
  WriteBatch$RequestFuture.await (WriteBatch.java:310)
  RecordAccumulator.awaitFlushCompletion (RecordAccumulator.java:380)
  WriterClient.flush
  FlinkIcebergTieringTestBase.writeRows (:317)   <- the first upsert+flush
```

and the client `fluss-write-sender-thread-1` parked at `Sender.sendWriteData` (`Sender.java:218/240`)
— i.e. looping on `unknownLeaderTables` / empty `readyNodes`. The batch never leaves the client
because **no bucket leader is ever resolved**; the tablet-server IO threads are idle (nothing to
serve). This is a cluster-formation / leader-metadata problem, not an ack/HWM/`dvRWLock` problem.

## Why it is environmental, not DV, not code

The identical hang reproduces for all three, ruling out deletion vectors and this branch's code:

| Run | main thread | sender |
| --- | --- | --- |
| DV table (`deletion-vectors.enabled=true`) | `writeRows`, no leader | `Sender:240` |
| non-DV control (same test, `false`) | `writeRows`, no leader | `Sender:240` |
| **committed `FlinkUnionReadPrimaryKeyTableITCase`** (must pass in CI) | `writeRows`, no leader | `Sender:218` |

A test that is green in CI hangs the same way locally. The dumps show `KQueue` selectors (macOS);
CI runs on Linux. So these full-cluster Iceberg union-read ITCases cannot form a writable cluster on
this machine — every write hangs at leader resolution regardless of DV.

## What this means for the Iceberg DV work

- The DV write path is **not** broken: DV and non-DV writes behave identically.
- Server-side DV write is independently unit-proven: `KvTabletTest#testDvEnabledInsertAndFlush`,
  `ReplicaTest#testDvReplicaWriteAdvancesHighWatermark`.
- The Iceberg v3 DV storage layer is unit-proven: `IcebergDeletionVectorTest`,
  `IcebergReadWithPosTest`, `IcebergLakeCatalogTest`, `FlussRecordAsIcebergRecordDvTest`,
  `IcebergDvWriteTest`.
- The full write → tier → union-read E2E (`FlinkUnionReadDvTableITCase`) must be validated in an
  environment where the base ITCases run — i.e. CI (Linux), not this machine.
