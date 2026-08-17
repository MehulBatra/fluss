/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.server.utils.RoaringBitmapUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DvManager}. */
class DvManagerTest {

    @TempDir Path tempDir;

    private DvRocksDB dvRocksDB;
    private DvRWLock dvRWLock;
    private DvManager dvManager;

    @BeforeEach
    void setUp() throws Exception {
        dvRocksDB = DvRocksDB.open(tempDir.resolve("dv").toString());
        dvRWLock = new DvRWLock();
        dvManager = new DvManager(dvRocksDB, dvRWLock);
    }

    @AfterEach
    void tearDown() {
        dvManager.close();
        dvRocksDB.close();
    }

    @Test
    void testPruneFilesRemovesOnlyMatchingEntries() throws Exception {
        // Two rowIds map into file 99 (to be removed), one into the surviving file 1.
        dvRocksDB.rowPosIndex().put(10L, new FilePos(99, 0L));
        dvRocksDB.rowPosIndex().put(11L, new FilePos(99, 5L));
        dvRocksDB.rowPosIndex().put(12L, new FilePos(1, 7L));

        dvRocksDB.rowPosIndex().pruneFiles(new HashSet<>(Collections.singletonList(99)));

        assertThat(dvRocksDB.rowPosIndex().get(10L)).isNull();
        assertThat(dvRocksDB.rowPosIndex().get(11L)).isNull();
        // Surviving file's mapping is untouched.
        assertThat(dvRocksDB.rowPosIndex().get(12L)).isNotNull();
        assertThat(dvRocksDB.rowPosIndex().get(12L).fileId()).isEqualTo(1);
    }

    @Test
    void testRowPosIndexMiss() throws Exception {
        // RowPosIndex has no mapping for these rowIds -> should produce pending markers
        long rowId1 = 100L;
        long rowId2 = 200L;
        List<DvEntry> entries = Arrays.asList(new DvEntry(rowId1), new DvEntry(rowId2));

        dvManager.handleChangelogSynced(entries);

        // Verify LogDv has deletion marks
        assertThat(dvRocksDB.logDv().isDeleted(rowId1)).isTrue();
        assertThat(dvRocksDB.logDv().isDeleted(rowId2)).isTrue();

        // Verify PendingDeletes has pending markers (no resolved position)
        PendingDeletes.PendingDeleteEntry entry1 = dvRocksDB.pendingDeletes().get(rowId1);
        assertThat(entry1).isNotNull();
        assertThat(entry1.isPending()).isTrue();

        PendingDeletes.PendingDeleteEntry entry2 = dvRocksDB.pendingDeletes().get(rowId2);
        assertThat(entry2).isNotNull();
        assertThat(entry2.isPending()).isTrue();

        // Verify RowPosIndex is still empty (nothing to delete)
        assertThat(dvRocksDB.rowPosIndex().get(rowId1)).isNull();
        assertThat(dvRocksDB.rowPosIndex().get(rowId2)).isNull();

        // Verify LakeDv has no entries (no file positions to mark)
        assertThat(dvRocksDB.lakeDv().getAll()).isEmpty();
    }

    @Test
    void testRowPosIndexHit() throws Exception {
        // Pre-populate RowPosIndex with known positions
        long rowId1 = 100L;
        long rowId2 = 200L;
        FilePos filePos1 = new FilePos(1, 10L);
        FilePos filePos2 = new FilePos(2, 20L);

        dvRocksDB.rowPosIndex().put(rowId1, filePos1);
        dvRocksDB.rowPosIndex().put(rowId2, filePos2);

        List<DvEntry> entries = Arrays.asList(new DvEntry(rowId1), new DvEntry(rowId2));
        dvManager.handleChangelogSynced(entries);

        // Verify LogDv has deletion marks
        assertThat(dvRocksDB.logDv().isDeleted(rowId1)).isTrue();
        assertThat(dvRocksDB.logDv().isDeleted(rowId2)).isTrue();

        // Verify RowPosIndex entries are deleted (consumed)
        assertThat(dvRocksDB.rowPosIndex().get(rowId1)).isNull();
        assertThat(dvRocksDB.rowPosIndex().get(rowId2)).isNull();

        // Verify LakeDv has per-file deletion marks
        assertThat(dvRocksDB.lakeDv().get(1).contains(10L)).isTrue();
        assertThat(dvRocksDB.lakeDv().get(2).contains(20L)).isTrue();

        // Verify PendingDeletes has resolved entries (with file positions)
        PendingDeletes.PendingDeleteEntry entry1 = dvRocksDB.pendingDeletes().get(rowId1);
        assertThat(entry1).isNotNull();
        assertThat(entry1.isPending()).isFalse();
        assertThat(entry1.getFilePos()).isEqualTo(filePos1);

        PendingDeletes.PendingDeleteEntry entry2 = dvRocksDB.pendingDeletes().get(rowId2);
        assertThat(entry2).isNotNull();
        assertThat(entry2.isPending()).isFalse();
        assertThat(entry2.getFilePos()).isEqualTo(filePos2);
    }

    @Test
    void testMixedHitAndMiss() throws Exception {
        // rowId 100: has RowPosIndex mapping (hit)
        // rowId 200: no RowPosIndex mapping (miss)
        long hitRowId = 100L;
        long missRowId = 200L;
        FilePos hitFilePos = new FilePos(5, 50L);

        dvRocksDB.rowPosIndex().put(hitRowId, hitFilePos);

        List<DvEntry> entries = Arrays.asList(new DvEntry(hitRowId), new DvEntry(missRowId));
        dvManager.handleChangelogSynced(entries);

        // Both should have LogDv marks
        assertThat(dvRocksDB.logDv().isDeleted(hitRowId)).isTrue();
        assertThat(dvRocksDB.logDv().isDeleted(missRowId)).isTrue();

        // Hit: RowPosIndex deleted, LakeDv marked, PendingDeletes resolved
        assertThat(dvRocksDB.rowPosIndex().get(hitRowId)).isNull();
        assertThat(dvRocksDB.lakeDv().get(5).contains(50L)).isTrue();
        PendingDeletes.PendingDeleteEntry hitEntry = dvRocksDB.pendingDeletes().get(hitRowId);
        assertThat(hitEntry).isNotNull();
        assertThat(hitEntry.isPending()).isFalse();
        assertThat(hitEntry.getFilePos()).isEqualTo(hitFilePos);

        // Miss: RowPosIndex still empty, no LakeDv for other files, PendingDeletes pending
        assertThat(dvRocksDB.rowPosIndex().get(missRowId)).isNull();
        PendingDeletes.PendingDeleteEntry missEntry = dvRocksDB.pendingDeletes().get(missRowId);
        assertThat(missEntry).isNotNull();
        assertThat(missEntry.isPending()).isTrue();
    }

    @Test
    void testEmptyEntries() throws Exception {
        // Pre-populate some data to verify no-op behavior
        dvRocksDB.rowPosIndex().put(999L, new FilePos(1, 1L));

        dvManager.handleChangelogSynced(Collections.<DvEntry>emptyList());

        // Nothing should have changed
        assertThat(dvRocksDB.rowPosIndex().get(999L)).isEqualTo(new FilePos(1, 1L));
        assertThat(dvRocksDB.lakeDv().getAll()).isEmpty();
        assertThat(dvRocksDB.pendingDeletes().get(999L)).isNull();
    }

    @Test
    void testMultipleEntriesForSameFile() throws Exception {
        // Two rows in the same file at different positions
        long rowId1 = 100L;
        long rowId2 = 200L;
        FilePos filePos1 = new FilePos(1, 10L);
        FilePos filePos2 = new FilePos(1, 20L);

        dvRocksDB.rowPosIndex().put(rowId1, filePos1);
        dvRocksDB.rowPosIndex().put(rowId2, filePos2);

        List<DvEntry> entries = Arrays.asList(new DvEntry(rowId1), new DvEntry(rowId2));
        dvManager.handleChangelogSynced(entries);

        // LakeDv for file 1 should contain both positions
        assertThat(dvRocksDB.lakeDv().get(1).contains(10L)).isTrue();
        assertThat(dvRocksDB.lakeDv().get(1).contains(20L)).isTrue();
    }

    @Test
    void testMultipleBatches() throws Exception {
        // First batch
        long rowId1 = 100L;
        dvRocksDB.rowPosIndex().put(rowId1, new FilePos(1, 10L));
        dvManager.handleChangelogSynced(Collections.singletonList(new DvEntry(rowId1)));

        // Second batch
        long rowId2 = 200L;
        dvManager.handleChangelogSynced(Collections.singletonList(new DvEntry(rowId2)));

        // First entry: hit (resolved)
        assertThat(dvRocksDB.logDv().isDeleted(rowId1)).isTrue();
        PendingDeletes.PendingDeleteEntry entry1 = dvRocksDB.pendingDeletes().get(rowId1);
        assertThat(entry1).isNotNull();
        assertThat(entry1.isPending()).isFalse();

        // Second entry: miss (pending)
        assertThat(dvRocksDB.logDv().isDeleted(rowId2)).isTrue();
        PendingDeletes.PendingDeleteEntry entry2 = dvRocksDB.pendingDeletes().get(rowId2);
        assertThat(entry2).isNotNull();
        assertThat(entry2.isPending()).isTrue();
    }

    // ---- getDvForUnionRead tests ----

    @Test
    void testGetDvForUnionReadWithLakeDvAndLogDv() throws Exception {
        long snapshotId = 10L;
        long snapshotStartOffset = 100L;
        long logEndOffset = 500L;

        // Set up readable snapshot state via handleReadableSwitch
        dvManager.handleReadableSwitch(0, snapshotId, snapshotStartOffset);

        // Populate FileDict: file_id -> file_path
        dvRocksDB.fileDict().put(1, "data/file1.parquet");
        dvRocksDB.fileDict().put(2, "data/file2.parquet");

        // Populate LakeDv: file_id -> deleted positions
        dvRocksDB.lakeDv().markDeleted(1, 10L);
        dvRocksDB.lakeDv().markDeleted(1, 20L);
        dvRocksDB.lakeDv().markDeleted(2, 5L);

        // Populate LogDv: mark some offsets in [snapshotStartOffset, logEndOffset) as deleted
        dvRocksDB.logDv().markDeleted(150L);
        dvRocksDB.logDv().markDeleted(300L);

        DvSnapshot snapshot = dvManager.getDvForUnionRead(snapshotId, logEndOffset);

        // Verify LakeDv entries (file_id resolved to file_path)
        Map<String, byte[]> lakeDv = snapshot.getLakeDvEntries();
        assertThat(lakeDv).hasSize(2);
        assertThat(lakeDv).containsKey("data/file1.parquet");
        assertThat(lakeDv).containsKey("data/file2.parquet");

        // Verify file1 bitmap contains positions 10 and 20
        Roaring64Bitmap file1Bitmap = new Roaring64Bitmap();
        RoaringBitmapUtils.deserializeRoaringBitmap64(
                file1Bitmap, lakeDv.get("data/file1.parquet"));
        assertThat(file1Bitmap.contains(10L)).isTrue();
        assertThat(file1Bitmap.contains(20L)).isTrue();
        assertThat(file1Bitmap.getLongCardinality()).isEqualTo(2);

        // Verify file2 bitmap contains position 5
        Roaring64Bitmap file2Bitmap = new Roaring64Bitmap();
        RoaringBitmapUtils.deserializeRoaringBitmap64(
                file2Bitmap, lakeDv.get("data/file2.parquet"));
        assertThat(file2Bitmap.contains(5L)).isTrue();
        assertThat(file2Bitmap.getLongCardinality()).isEqualTo(1);

        // Verify LogDv bitmap contains offsets 150 and 300
        assertThat(snapshot.getLogDvBitmap()).isNotNull();
        Roaring64Bitmap logBitmap = new Roaring64Bitmap();
        RoaringBitmapUtils.deserializeRoaringBitmap64(logBitmap, snapshot.getLogDvBitmap());
        assertThat(logBitmap.contains(150L)).isTrue();
        assertThat(logBitmap.contains(300L)).isTrue();
        assertThat(logBitmap.getLongCardinality()).isEqualTo(2);

        // Verify offsets
        assertThat(snapshot.getLogEndOffset()).isEqualTo(logEndOffset);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(snapshotStartOffset);
    }

    @Test
    void testGetDvForUnionReadStaleSnapshot() throws Exception {
        long snapshotId = 10L;
        dvManager.handleReadableSwitch(0, snapshotId, 100L);

        // Request with wrong snapshot ID
        assertThatThrownBy(() -> dvManager.getDvForUnionRead(99L, 500L))
                .isInstanceOf(StaleSnapshotException.class)
                .satisfies(
                        e -> {
                            StaleSnapshotException stale = (StaleSnapshotException) e;
                            assertThat(stale.getCurrentSnapshotId()).isEqualTo(snapshotId);
                            assertThat(stale.getRequestedSnapshotId()).isEqualTo(99L);
                        });
    }

    @Test
    void testGetDvForUnionReadEmptyDv() throws Exception {
        long snapshotId = 5L;
        long snapshotStartOffset = 50L;
        long logEndOffset = 200L;

        dvManager.handleReadableSwitch(0, snapshotId, snapshotStartOffset);

        // No LakeDv, no LogDv entries
        DvSnapshot snapshot = dvManager.getDvForUnionRead(snapshotId, logEndOffset);

        assertThat(snapshot.getLakeDvEntries()).isEmpty();
        assertThat(snapshot.getLogDvBitmap()).isNull();
        assertThat(snapshot.getLogEndOffset()).isEqualTo(logEndOffset);
        assertThat(snapshot.getSnapshotStartOffset()).isEqualTo(snapshotStartOffset);
    }

    @Test
    void testGetDvForUnionReadOnlyLakeDv() throws Exception {
        long snapshotId = 7L;
        long snapshotStartOffset = 100L;
        long logEndOffset = 100L; // same as start → no log data gap

        dvManager.handleReadableSwitch(0, snapshotId, snapshotStartOffset);

        dvRocksDB.fileDict().put(3, "data/file3.parquet");
        dvRocksDB.lakeDv().markDeleted(3, 42L);

        DvSnapshot snapshot = dvManager.getDvForUnionRead(snapshotId, logEndOffset);

        assertThat(snapshot.getLakeDvEntries()).hasSize(1);
        assertThat(snapshot.getLakeDvEntries()).containsKey("data/file3.parquet");
        // No log data gap → LogDv bitmap should be null
        assertThat(snapshot.getLogDvBitmap()).isNull();
    }

    @Test
    void testGetDvForUnionReadOnlyLogDv() throws Exception {
        long snapshotId = 8L;
        long snapshotStartOffset = 50L;
        long logEndOffset = 300L;

        dvManager.handleReadableSwitch(0, snapshotId, snapshotStartOffset);

        // No LakeDv entries, but mark some log offsets
        dvRocksDB.logDv().markDeleted(60L);
        dvRocksDB.logDv().markDeleted(200L);

        DvSnapshot snapshot = dvManager.getDvForUnionRead(snapshotId, logEndOffset);

        assertThat(snapshot.getLakeDvEntries()).isEmpty();
        assertThat(snapshot.getLogDvBitmap()).isNotNull();

        Roaring64Bitmap logBitmap = new Roaring64Bitmap();
        RoaringBitmapUtils.deserializeRoaringBitmap64(logBitmap, snapshot.getLogDvBitmap());
        assertThat(logBitmap.contains(60L)).isTrue();
        assertThat(logBitmap.contains(200L)).isTrue();
    }

    @Test
    void testGetLogDvSnapshot() throws Exception {
        // Mark some offsets as deleted
        long rowId1 = 50L;
        long rowId2 = 150L;
        long rowId3 = 250L;
        List<DvEntry> entries =
                Arrays.asList(new DvEntry(rowId1), new DvEntry(rowId2), new DvEntry(rowId3));
        dvManager.handleChangelogSynced(entries);

        // Get LogDv snapshot for range [0, 200)
        byte[] bitmapBytes = dvManager.getLogDvSnapshot(0L, 200L);
        assertThat(bitmapBytes).isNotNull();

        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        RoaringBitmapUtils.deserializeRoaringBitmap64(bitmap, bitmapBytes);
        assertThat(bitmap.contains(50L)).isTrue();
        assertThat(bitmap.contains(150L)).isTrue();
        assertThat(bitmap.contains(250L)).isFalse(); // outside range
    }

    @Test
    void testGetLogDvSnapshotEmpty() throws Exception {
        // No deleted offsets in range
        byte[] bitmapBytes = dvManager.getLogDvSnapshot(0L, 100L);
        assertThat(bitmapBytes).isNull();
    }

    @Test
    void testGetDvForUnionReadFileDictMissing() throws Exception {
        long snapshotId = 9L;
        dvManager.handleReadableSwitch(0, snapshotId, 100L);

        // LakeDv has entry for file_id=99 but FileDict has no mapping for it
        dvRocksDB.lakeDv().markDeleted(99, 1L);

        DvSnapshot snapshot = dvManager.getDvForUnionRead(snapshotId, 500L);

        // file_id 99 has no FileDict mapping, so it should be excluded
        assertThat(snapshot.getLakeDvEntries()).isEmpty();
    }
}
