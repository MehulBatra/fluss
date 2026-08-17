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
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.testutils.DataTestUtils.row;

/**
 * Union-read tests for Iceberg primary-key tables, comparing the deletion-vector path against the
 * default equality-delete / sort-merge path. Both must return identical exactly-once results.
 */
class FlinkUnionReadDvTableITCase extends FlinkUnionReadTestBase {

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    /** Update rows already tiered to Iceberg; union read must not surface stale versions. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUnionReadWithUpdates(boolean deletionVectorsEnabled) throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName =
                    deletionVectorsEnabled ? "dv_update_table" : "sort_merge_update_table";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createPkTable(tablePath, deletionVectorsEnabled);

            // round 1: insert keys 0..4, tier to Iceberg.
            writeRows(tablePath, upsertRows(0, 5, "v"), false);
            waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);

            // round 2: update keys 0..2 (already tiered), tier again.
            writeRows(tablePath, upsertRows(0, 3, "updated"), false);
            waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, false);

            List<String> expected =
                    Arrays.asList(
                            "+I[0, updated_0]",
                            "+I[1, updated_1]",
                            "+I[2, updated_2]",
                            "+I[3, v_3]",
                            "+I[4, v_4]");
            CloseableIterator<Row> rowIter =
                    batchTEnv.executeSql("select * from " + tableName).collect();
            assertResultsIgnoreOrder(rowIter, expected, true);
        } finally {
            jobClient.cancel().get();
        }
    }

    private long createPkTable(TablePath tablePath, boolean deletionVectorsEnabled)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .build();
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(DEFAULT_BUCKET_NUM)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));
        if (deletionVectorsEnabled) {
            builder.property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true");
        }
        return createTable(tablePath, builder.build());
    }

    private static List<InternalRow> upsertRows(int from, int to, String valuePrefix) {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(row(i, valuePrefix + "_" + i));
        }
        return rows;
    }
}
