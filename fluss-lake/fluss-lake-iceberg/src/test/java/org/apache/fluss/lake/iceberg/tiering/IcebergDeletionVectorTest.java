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
import org.apache.fluss.lake.iceberg.tiering.writer.IcebergDvFileWriter;
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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end test that Fluss-written Iceberg v3 Puffin deletion vectors are honored on read. */
class IcebergDeletionVectorTest {

    private static final int BUCKET_NUM = 1;

    @TempDir private File tempWarehouseDir;

    private Configuration configuration;
    private IcebergLakeTieringFactory tieringFactory;
    private IcebergCatalogProvider catalogProvider;

    @BeforeEach
    void beforeEach() {
        configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir);
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test");
        tieringFactory = new IcebergLakeTieringFactory(configuration);
        catalogProvider = new IcebergCatalogProvider(configuration);
    }

    @Test
    void puffinDeletionVectorMasksRowOnRead() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "dv_roundtrip_table");

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

        // 1) write 3 rows (offsets 0,1,2) via the DV-mode writer and append-commit them.
        List<IcebergWriteResult> writeResults = new ArrayList<>();
        try (LakeWriter<IcebergWriteResult> writer = createDvWriter(tablePath, tableInfo)) {
            writer.write(insert(0, 1, "a"));
            writer.write(insert(1, 2, "b"));
            writer.write(insert(2, 3, "c"));
            writeResults.add(writer.complete());
        }

        long baseSnapshotId;
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            IcebergCommittable committable = committer.toCommittable(writeResults);
            baseSnapshotId =
                    committer
                            .commit(committable, Collections.singletonMap("round", "1"))
                            .getCommittedSnapshotId();
        }

        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        table.refresh();
        assertThat(countRows(table)).isEqualTo(3);

        // 2) materialize a Puffin DV deleting position 0 of the first data file.
        DataFile dataFile = table.currentSnapshot().addedDataFiles(table.io()).iterator().next();
        List<DeleteFile> dvFiles;
        try (IcebergDvFileWriter dvWriter = new IcebergDvFileWriter(table, 0)) {
            dvWriter.delete(dataFile, 0L);
            DeleteWriteResult result = dvWriter.complete();
            dvFiles = result.deleteFiles();
        }
        assertThat(dvFiles).isNotEmpty();

        // 3) commit the DV via validated RowDelta.
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.commitDeletionVectors(
                    dvFiles,
                    Collections.singletonList(dataFile.location()),
                    baseSnapshotId,
                    Collections.singletonMap("round", "2"));
        }

        // 4) the deleted row is masked on read.
        table.refresh();
        assertThat(countRows(table)).isEqualTo(2);
    }

    private static int countRows(Table table) throws Exception {
        int count = 0;
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record ignored : records) {
                count++;
            }
        }
        return count;
    }

    private LakeWriter<IcebergWriteResult> createDvWriter(TablePath tablePath, TableInfo tableInfo)
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
                });
    }

    private static LogRecord insert(long offset, int c1, String c2) {
        GenericRow row = new GenericRow(2);
        row.setField(0, c1);
        row.setField(1, BinaryString.fromString(c2));
        return new GenericRecord(offset, 1_000_000_000L + offset, ChangeType.INSERT, row);
    }
}
