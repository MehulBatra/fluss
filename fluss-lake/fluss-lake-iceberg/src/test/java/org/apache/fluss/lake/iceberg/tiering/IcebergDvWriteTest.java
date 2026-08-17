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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.IcebergLakeCatalog;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import org.apache.iceberg.io.WriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the DV-mode Iceberg tiering write path ({@link DvRecordWriter}). */
class IcebergDvWriteTest {

    private static final int BUCKET_NUM = 1;

    @TempDir private File tempWarehouseDir;

    private Configuration configuration;
    private IcebergLakeTieringFactory tieringFactory;

    @BeforeEach
    void beforeEach() {
        configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir);
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test");
        tieringFactory = new IcebergLakeTieringFactory(configuration);
    }

    @Test
    void dvModeWritesDataOnlyAndFiltersSupersededRecords() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "dv_write_table");

        Schema flussSchema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(BUCKET_NUM, "c1")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                        .build();

        try (IcebergLakeCatalog catalog = new IcebergLakeCatalog(configuration)) {
            catalog.createTable(tablePath, descriptor, new TestingLakeCatalogContext());
        }

        TableInfo tableInfo =
                TableInfo.of(tablePath, 0, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);

        // LogDv marks offset 1 as superseded within this round -> that +I must be skipped.
        byte[] logDvBitmap = serializeBitmap(1L);

        WriteResult writeResult;
        try (LakeWriter<IcebergWriteResult> writer =
                createDvWriter(tablePath, tableInfo, logDvBitmap)) {
            writer.write(insert(0, 1, "a")); // kept
            writer.write(insert(1, 2, "b")); // skipped: superseded by LogDv
            writer.write(record(2, 1, "a2", ChangeType.UPDATE_AFTER)); // kept
            writer.write(record(3, 1, "a", ChangeType.UPDATE_BEFORE)); // skipped: -U
            writer.write(record(4, 2, "b", ChangeType.DELETE)); // skipped: -D
            writeResult = writer.complete().getWriteResult();
        }

        // DV mode writes data files only; no equality/position delete files.
        assertThat(writeResult.deleteFiles()).isEmpty();
        long rows = 0;
        for (org.apache.iceberg.DataFile dataFile : writeResult.dataFiles()) {
            rows += dataFile.recordCount();
        }
        // surviving rows: offsets 0 and 2 only.
        assertThat(rows).isEqualTo(2L);
    }

    private LakeWriter<IcebergWriteResult> createDvWriter(
            TablePath tablePath, TableInfo tableInfo, @Nullable byte[] logDvBitmap)
            throws Exception {
        return tieringFactory.createLakeWriter(
                new WriterInitContext() {
                    @Override
                    public TablePath tablePath() {
                        return tablePath;
                    }

                    @Override
                    public TableBucket tableBucket() {
                        return new TableBucket(0, 0);
                    }

                    @Nullable
                    @Override
                    public String partition() {
                        return null;
                    }

                    @Override
                    public TableInfo tableInfo() {
                        return tableInfo;
                    }

                    @Nullable
                    @Override
                    public byte[] logDvBitmap() {
                        return logDvBitmap;
                    }
                });
    }

    private static LogRecord insert(long offset, int c1, String c2) {
        return record(offset, c1, c2, ChangeType.INSERT);
    }

    private static LogRecord record(long offset, int c1, String c2, ChangeType changeType) {
        GenericRow row = new GenericRow(2);
        row.setField(0, c1);
        row.setField(1, BinaryString.fromString(c2));
        return new GenericRecord(offset, 1_000_000_000L + offset, changeType, row);
    }

    private static byte[] serializeBitmap(long... offsets) throws IOException {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (long offset : offsets) {
            bitmap.addLong(offset);
        }
        bitmap.runOptimize();
        ByteBuffer buffer = ByteBuffer.allocate((int) bitmap.serializedSizeInBytes());
        bitmap.serialize(buffer);
        return buffer.array();
    }
}
