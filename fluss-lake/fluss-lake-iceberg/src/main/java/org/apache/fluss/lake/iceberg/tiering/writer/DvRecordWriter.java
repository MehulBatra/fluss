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

import org.apache.fluss.lake.iceberg.tiering.RecordWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;

import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link RecordWriter} for Iceberg deletion-vector tables. In DV mode the writer appends
 * surviving {@code +I}/{@code +U} rows (with {@code __rowid}) as data files only; {@code -U}/{@code
 * -D} are skipped since deletes are materialized as Puffin deletion vectors at commit time.
 */
public class DvRecordWriter extends RecordWriter {

    @Nullable private final Roaring64Bitmap logDvBitmap;

    public DvRecordWriter(
            Table icebergTable,
            WriterInitContext writerInitContext,
            TaskWriter<Record> taskWriter) {
        super(
                taskWriter,
                icebergTable.schema(),
                writerInitContext.tableInfo().getRowType(),
                writerInitContext.tableBucket(),
                true);
        this.logDvBitmap = deserializeLogDvBitmap(writerInitContext.logDvBitmap());
    }

    @Override
    public void write(LogRecord record) throws Exception {
        ChangeType changeType = record.getChangeType();

        // -U/-D are not written in DV mode; their effect is captured by deletion vectors.
        if (changeType == ChangeType.UPDATE_BEFORE || changeType == ChangeType.DELETE) {
            return;
        }

        // +I/+U superseded by a later -U/-D within this tiering round are skipped.
        if (logDvBitmap != null && logDvBitmap.contains(record.logOffset())) {
            return;
        }

        flussRecordAsIcebergRecord.setFlussRecord(record);
        taskWriter.write(flussRecordAsIcebergRecord);
    }

    @Nullable
    private static Roaring64Bitmap deserializeLogDvBitmap(@Nullable byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        try {
            bitmap.deserialize(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize LogDv bitmap", e);
        }
        return bitmap;
    }
}
