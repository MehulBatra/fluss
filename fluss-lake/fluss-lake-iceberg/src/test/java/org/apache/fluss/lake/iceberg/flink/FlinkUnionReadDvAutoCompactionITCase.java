/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.testutils.common.CommonTestUtils;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for Flink union read on Iceberg primary-key tables with deletion vectors AND
 * auto-compaction enabled — the Iceberg sibling of {@code FlinkUnionReadDvAutoCompactionITCase} in
 * the Paimon module.
 *
 * <p>Unlike Paimon (background compaction triggered by {@code num-sorted-run.compaction-trigger}),
 * Iceberg compaction is the internal {@code IcebergRewriteDataFiles} that runs in the tiering
 * writer once a bucket has {@code >= MIN_FILES_TO_COMPACT} (3) small data files and {@code
 * table.datalake.auto-compaction=true}. It commits a {@code REPLACE} snapshot that rewrites data
 * files and shifts row positions.
 *
 * <p>This exercises the full compaction&harr;DV loop (FIP-31 §7 / FIP-47): compaction is detected
 * by {@code IcebergLakeSource.planDelta}, its compacted files are re-indexed into the RowPosIndex
 * and its removed files are reported so {@code DvManager.cleanupOldFiles} drops their LakeDv
 * entries. The load-bearing assertion is that the union read stays exactly-once/correct across
 * compaction.
 */
class FlinkUnionReadDvAutoCompactionITCase extends FlinkUnionReadTestBase {

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    @Test
    void testUnionReadDvTableWithAutoCompaction() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "dv_auto_compaction_table";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            int bucketNum = 1;
            long tableId = createDvAutoCompactionTable(tablePath, bucketNum);

            // expected[key] = latest value for that key.
            Map<Integer, String> expected = new HashMap<>();

            // Round 1: insert keys 0..4.
            write(tablePath, expected, range(0, 5, "v"));
            waitUntilBucketSynced(tablePath, tableId, bucketNum, false);

            // Round 2: overlapping updates to already-tiered keys 0,1,3 + a new key 5. These are
            // the
            // rows whose old versions must be masked by the LakeDv after compaction.
            Map<Integer, String> round2 = new LinkedHashMap<>();
            round2.put(0, "v0_updated");
            round2.put(1, "v1_updated");
            round2.put(3, "v3_updated");
            round2.put(5, "v5");
            write(tablePath, expected, round2);
            waitUntilBucketSynced(tablePath, tableId, bucketNum, false);

            // Rounds 3-4: more data so the bucket accumulates >= MIN_FILES_TO_COMPACT files,
            // driving
            // an internal compaction (REPLACE snapshot).
            write(tablePath, expected, range(6, 11, "v"));
            waitUntilBucketSynced(tablePath, tableId, bucketNum, false);
            write(tablePath, expected, range(11, 16, "v"));
            waitUntilBucketSynced(tablePath, tableId, bucketNum, false);

            // A compaction (REPLACE snapshot rewriting data files) must have run.
            CommonTestUtils.retry(
                    Duration.ofMinutes(2),
                    () -> assertThat(hasCompactionSnapshot(tablePath)).isTrue());

            // The DV readable path is active and serves a valid DV snapshot.
            CommonTestUtils.retry(
                    Duration.ofMinutes(2),
                    () -> {
                        long readableSnapshotId =
                                admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId();
                        assertThat(readableSnapshotId).isGreaterThan(0);
                        // getDvSnapshot must succeed for the readable snapshot (DV path wired).
                        assertThat(
                                        admin.getDvSnapshot(
                                                        tablePath,
                                                        tableId,
                                                        null,
                                                        0,
                                                        readableSnapshotId)
                                                .get()
                                                .getSnapshotStartOffset())
                                .isGreaterThanOrEqualTo(0L);
                    });

            // The load-bearing check: union read is exactly-once/correct ACROSS compaction — proves
            // compacted files were re-indexed and stale LakeDv entries were dropped, with no
            // over-masking or stale rows.
            CloseableIterator<Row> rowIter =
                    batchTEnv.executeSql("select * from " + tableName).collect();
            assertResultsIgnoreOrder(rowIter, expectedStrings(expected), true);
        } finally {
            jobClient.cancel().get();
        }
    }

    private long createDvAutoCompactionTable(TablePath tablePath, int bucketNum) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(bucketNum)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .property(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "true")
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true")
                        .build();
        return createTable(tablePath, tableDescriptor);
    }

    /**
     * True once the Iceberg table has a REPLACE snapshot that rewrote data files (a compaction).
     */
    private boolean hasCompactionSnapshot(TablePath tablePath) {
        Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        table.refresh();
        for (Snapshot snapshot : table.snapshots()) {
            if (DataOperations.REPLACE.equals(snapshot.operation())
                    && snapshot.removedDataFiles(table.io()).iterator().hasNext()) {
                return true;
            }
        }
        return false;
    }

    private void write(TablePath tablePath, Map<Integer, String> expected, Map<Integer, String> kv)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (Map.Entry<Integer, String> e : kv.entrySet()) {
            rows.add(row(e.getKey(), e.getValue()));
        }
        writeRows(tablePath, rows, false);
        expected.putAll(kv);
    }

    private static Map<Integer, String> range(int from, int to, String prefix) {
        Map<Integer, String> kv = new LinkedHashMap<>();
        for (int i = from; i < to; i++) {
            kv.put(i, prefix + "_" + i);
        }
        return kv;
    }

    private static List<String> expectedStrings(Map<Integer, String> expected) {
        return expected.entrySet().stream()
                .map(e -> "+I[" + e.getKey() + ", " + e.getValue() + "]")
                .collect(Collectors.toList());
    }
}
