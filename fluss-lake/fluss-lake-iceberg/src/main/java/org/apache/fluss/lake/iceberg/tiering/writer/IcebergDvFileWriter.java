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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;

/**
 * Writes Iceberg v3 Puffin deletion vectors (RoaringBitmap) marking deleted row positions in
 * existing data files. Replaces v2 equality/position delete files for DV tables.
 */
public class IcebergDvFileWriter implements Closeable {

    private final Table table;
    private final DVFileWriter dvWriter;

    public IcebergDvFileWriter(Table table, int partitionId) {
        this(table, partitionId, path -> null);
    }

    /**
     * @param loadPreviousDvs given a data-file path, returns that file's current DV positions (or
     *     null). Required for cross-round re-materialization: the writer merges the new positions
     *     with the existing DV and supersedes the old delete file, preserving Iceberg v3's
     *     one-DV-per-data-file invariant.
     */
    public IcebergDvFileWriter(
            Table table, int partitionId, Function<String, PositionDeleteIndex> loadPreviousDvs) {
        this.table = table;
        OutputFileFactory outputFileFactory =
                OutputFileFactory.builderFor(table, partitionId, 0)
                        .format(FileFormat.PUFFIN)
                        .build();
        this.dvWriter = new BaseDVFileWriter(outputFileFactory, loadPreviousDvs);
    }

    /** Marks the given row positions as deleted in the given data file. */
    public void delete(DataFile dataFile, long... positions) {
        PartitionSpec spec = table.specs().get(dataFile.specId());
        for (long position : positions) {
            dvWriter.delete(dataFile.location(), position, spec, dataFile.partition());
        }
    }

    /** Closes the writer and returns the produced Puffin deletion-vector delete files. */
    public DeleteWriteResult complete() throws IOException {
        dvWriter.close();
        return dvWriter.result();
    }

    @Override
    public void close() throws IOException {
        dvWriter.close();
    }
}
