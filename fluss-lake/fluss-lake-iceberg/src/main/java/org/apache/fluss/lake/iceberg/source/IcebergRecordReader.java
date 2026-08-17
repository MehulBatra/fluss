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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.IcebergGenericReader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/**
 * Iceberg record reader. The filter is applied during the plan phase of IcebergSplitPlanner, so the
 * RecordReader does not need to apply the filter again.
 *
 * <p>Refer to {@link org.apache.iceberg.data.GenericReader#open(FileScanTask)} and {@link
 * org.apache.iceberg.Scan#ignoreResiduals()} for details.
 */
public class IcebergRecordReader implements RecordReader {
    protected IcebergRecordAsFlussRecordIterator iterator;
    protected @Nullable int[][] project;
    protected Types.StructType struct;

    private final FileScanTask fileScanTask;
    private final Table table;
    // Catalog owning this reader's FileIO (S3 connection pool). Closed when the row iterator
    // closes,
    // else each split leaks an S3FileIO and exhausts the connection pool.
    private final @Nullable Catalog catalog;

    public IcebergRecordReader(FileScanTask fileScanTask, Table table, @Nullable int[][] project) {
        this(fileScanTask, table, project, null);
    }

    public IcebergRecordReader(
            FileScanTask fileScanTask,
            Table table,
            @Nullable int[][] project,
            @Nullable Catalog catalog) {
        this.fileScanTask = fileScanTask;
        this.table = table;
        this.project = project;
        this.catalog = catalog;
        TableScan tableScan = table.newScan();
        if (project != null) {
            tableScan = applyProject(tableScan, project);
        }
        IcebergGenericReader reader = new IcebergGenericReader(tableScan, true);
        struct = tableScan.schema().asStruct();
        this.iterator =
                new IcebergRecordAsFlussRecordIterator(reader.open(fileScanTask), struct, catalog);
    }

    @Override
    public CloseableIterator<LogRecord> read() throws IOException {
        return iterator;
    }

    @Override
    public CloseableIterator<RowWithPosResult> readWithPos() throws IOException {
        // Project only the caller's columns plus Iceberg's _pos metadata column (physical row
        // position), which reflects the original file position with DVs applied (gaps for deletes).
        Types.StructType tableStruct = table.schema().asStruct();
        List<Types.NestedField> cols = new ArrayList<>();
        if (project != null && project.length > 0) {
            for (int[] p : project) {
                cols.add(tableStruct.fields().get(p[0]));
            }
        } else {
            // No projection pushed down (e.g. SELECT *): project all user columns, excluding the
            // Fluss system columns (__bucket/__offset/__timestamp/__rowid). Never emit an empty
            // row.
            for (Types.NestedField f : tableStruct.fields()) {
                if (!f.name().startsWith("__")) {
                    cols.add(f);
                }
            }
        }
        cols.add(MetadataColumns.ROW_POSITION);
        TableScan tableScan = table.newScan().project(new Schema(cols));
        Types.StructType posStruct = tableScan.schema().asStruct();
        int posIndex =
                posStruct.fields().indexOf(posStruct.field(MetadataColumns.ROW_POSITION.name()));
        IcebergGenericReader reader = new IcebergGenericReader(tableScan, true);
        return new IcebergRowWithPosIterator(
                reader.open(fileScanTask),
                posStruct,
                posIndex,
                IcebergSplit.fileNameOf(fileScanTask),
                catalog);
    }

    private TableScan applyProject(TableScan tableScan, int[][] projects) {
        Types.StructType structType = tableScan.schema().asStruct();
        List<Types.NestedField> cols = new ArrayList<>(projects.length + 2);

        for (int[] project : projects) {
            cols.add(structType.fields().get(project[0]));
        }

        cols.add(structType.field(OFFSET_COLUMN_NAME));
        cols.add(structType.field(TIMESTAMP_COLUMN_NAME));
        return tableScan.project(new Schema(cols));
    }

    /** Iterator for iceberg record as fluss record. */
    public static class IcebergRecordAsFlussRecordIterator implements CloseableIterator<LogRecord> {

        private final org.apache.iceberg.io.CloseableIterator<Record> icebergRecordIterator;

        private final ProjectedRow projectedRow;
        private final IcebergRecordAsFlussRow icebergRecordAsFlussRow;

        private final int logOffsetColIndex;
        private final int timestampColIndex;
        private final @Nullable Catalog catalog;

        public IcebergRecordAsFlussRecordIterator(
                CloseableIterable<Record> icebergRecordIterator, Types.StructType struct) {
            this(icebergRecordIterator, struct, null);
        }

        public IcebergRecordAsFlussRecordIterator(
                CloseableIterable<Record> icebergRecordIterator,
                Types.StructType struct,
                @Nullable Catalog catalog) {
            this.icebergRecordIterator = icebergRecordIterator.iterator();
            this.catalog = catalog;
            this.logOffsetColIndex = struct.fields().indexOf(struct.field(OFFSET_COLUMN_NAME));
            this.timestampColIndex = struct.fields().indexOf(struct.field(TIMESTAMP_COLUMN_NAME));

            int[] project = IntStream.range(0, struct.fields().size() - 2).toArray();
            projectedRow = ProjectedRow.from(project);
            icebergRecordAsFlussRow = new IcebergRecordAsFlussRow();
        }

        @Override
        public void close() {
            try {
                icebergRecordIterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close iterator.", e);
            } finally {
                closeCatalogQuietly(catalog);
            }
        }

        @Override
        public boolean hasNext() {
            return icebergRecordIterator.hasNext();
        }

        @Override
        public LogRecord next() {
            Record icebergRecord = icebergRecordIterator.next();
            long offset = icebergRecord.get(logOffsetColIndex, Long.class);
            long timestamp =
                    icebergRecord
                            .get(timestampColIndex, OffsetDateTime.class)
                            .toInstant()
                            .toEpochMilli();

            return new GenericRecord(
                    offset,
                    timestamp,
                    ChangeType.INSERT,
                    projectedRow.replaceRow(
                            icebergRecordAsFlussRow.replaceIcebergRecord(icebergRecord)));
        }
    }

    /** Iterator yielding each row with its physical {@code _pos} and the data file name. */
    public static class IcebergRowWithPosIterator implements CloseableIterator<RowWithPosResult> {

        private final org.apache.iceberg.io.CloseableIterator<Record> icebergRecordIterator;
        private final IcebergRecordAsFlussRow icebergRecordAsFlussRow =
                new IcebergRecordAsFlussRow();
        private final ProjectedRow projectedRow;
        private final RowWithPosResult reusable = new RowWithPosResult();
        private final int posIndex;
        private final String fileName;
        private final @Nullable Catalog catalog;

        public IcebergRowWithPosIterator(
                CloseableIterable<Record> records,
                Types.StructType struct,
                int posIndex,
                String fileName) {
            this(records, struct, posIndex, fileName, null);
        }

        public IcebergRowWithPosIterator(
                CloseableIterable<Record> records,
                Types.StructType struct,
                int posIndex,
                String fileName,
                @Nullable Catalog catalog) {
            this.icebergRecordIterator = records.iterator();
            this.posIndex = posIndex;
            this.fileName = fileName;
            this.catalog = catalog;
            // Project away the trailing _pos column so the caller sees only its own columns.
            int[] project =
                    IntStream.range(0, struct.fields().size()).filter(i -> i != posIndex).toArray();
            this.projectedRow = ProjectedRow.from(project);
        }

        @Override
        public boolean hasNext() {
            return icebergRecordIterator.hasNext();
        }

        @Override
        public RowWithPosResult next() {
            Record icebergRecord = icebergRecordIterator.next();
            long pos = icebergRecord.get(posIndex, Long.class);
            return reusable.set(
                    projectedRow.replaceRow(
                            icebergRecordAsFlussRow.replaceIcebergRecord(icebergRecord)),
                    pos,
                    fileName);
        }

        @Override
        public void close() {
            try {
                icebergRecordIterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close iterator.", e);
            } finally {
                closeCatalogQuietly(catalog);
            }
        }
    }

    /** Close the catalog (releasing its FileIO/S3 connection pool); never throw on close. */
    private static void closeCatalogQuietly(@Nullable Catalog catalog) {
        if (catalog instanceof AutoCloseable) {
            try {
                ((AutoCloseable) catalog).close();
            } catch (Exception ignored) {
                // best-effort; a failed catalog close must not fail the read
            }
        }
    }
}
