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

package org.apache.fluss.flink.lake.reader;

import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/** A scanner for reading lake split {@link LakeSnapshotSplit}. */
public class LakeSnapshotScanner implements BatchScanner {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSnapshotScanner.class);

    private final LakeSource<LakeSplit> lakeSource;
    private final LakeSnapshotSplit lakeSnapshotSplit;

    private CloseableIterator<InternalRow> rowsIterator;

    public LakeSnapshotScanner(
            LakeSource<LakeSplit> lakeSource,
            LakeSnapshotSplit lakeSnapshotSplit,
            @Nullable int[][] projection) {
        this.lakeSource = lakeSource;
        this.lakeSnapshotSplit = lakeSnapshotSplit;
        // readWithPos()/read() only emit the projected columns; the DV path in particular needs a
        // non-null projection, else the reader emits an empty row (only _pos, then stripped).
        if (projection != null) {
            lakeSource.withProject(projection);
        }
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (rowsIterator == null) {
            Map<String, byte[]> lakeDvMap = lakeSnapshotSplit.getLakeDvMap();
            RecordReader reader =
                    lakeSource.createRecordReader(
                            (LakeSource.ReaderContext<LakeSplit>) lakeSnapshotSplit::getLakeSplit);

            if (lakeDvMap != null && !lakeDvMap.isEmpty()) {
                // DV path: use readWithPos() and filter by file name + position
                LOG.info(
                        "Using DV-filtered lake read for split {}, lakeDvMap files: {}",
                        lakeSnapshotSplit.splitId(),
                        lakeDvMap.keySet());
                CloseableIterator<RowWithPosResult> posIter = reader.readWithPos();
                rowsIterator = new DvFilteredIterator(posIter, lakeDvMap);
            } else {
                // Original path: read directly
                LOG.info(
                        "Using direct lake read (no DV) for split {}", lakeSnapshotSplit.splitId());
                rowsIterator = InternalRowIterator.wrap(reader.read());
            }
        }
        return rowsIterator.hasNext() ? rowsIterator : null;
    }

    @Override
    public void close() throws IOException {
        if (rowsIterator != null) {
            rowsIterator.close();
        }
    }

    /**
     * Filters lake rows by looking up each row's file name in the lakeDv map and skipping rows
     * whose positions are marked as deleted. Supports multi-file splits where each file has its own
     * deletion bitmap.
     */
    private static class DvFilteredIterator implements CloseableIterator<InternalRow> {

        private final CloseableIterator<RowWithPosResult> delegate;
        private final Map<String, byte[]> lakeDvMap;
        private final Map<String, Roaring64Bitmap> bitmapCache = new HashMap<>();
        private InternalRow nextRow;

        DvFilteredIterator(
                CloseableIterator<RowWithPosResult> delegate, Map<String, byte[]> lakeDvMap) {
            this.delegate = delegate;
            this.lakeDvMap = lakeDvMap;
        }

        @Override
        public boolean hasNext() {
            while (nextRow == null && delegate.hasNext()) {
                RowWithPosResult result = delegate.next();
                String fileName = result.getFileName();
                if (fileName != null && lakeDvMap.containsKey(fileName)) {
                    Roaring64Bitmap bitmap = getOrDeserializeBitmap(fileName);
                    if (bitmap.contains(result.getPos())) {
                        continue;
                    }
                }
                nextRow = result.getRow();
            }
            return nextRow != null;
        }

        @Override
        public InternalRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            InternalRow row = nextRow;
            nextRow = null;
            return row;
        }

        @Override
        public void close() {
            delegate.close();
        }

        private Roaring64Bitmap getOrDeserializeBitmap(String fileName) {
            Roaring64Bitmap bitmap = bitmapCache.get(fileName);
            if (bitmap == null) {
                bitmap = new Roaring64Bitmap();
                try {
                    bitmap.deserialize(ByteBuffer.wrap(lakeDvMap.get(fileName)));
                } catch (IOException e) {
                    throw new RuntimeException(
                            "Failed to deserialize lakeDv bitmap for file: " + fileName, e);
                }
                bitmapCache.put(fileName, bitmap);
            }
            return bitmap;
        }
    }

    private static class InternalRowIterator implements CloseableIterator<InternalRow> {

        private final CloseableIterator<LogRecord> recordCloseableIterator;

        private static InternalRowIterator wrap(
                CloseableIterator<LogRecord> recordCloseableIterator) {
            return new InternalRowIterator(recordCloseableIterator);
        }

        private InternalRowIterator(CloseableIterator<LogRecord> recordCloseableIterator) {
            this.recordCloseableIterator = recordCloseableIterator;
        }

        @Override
        public void close() {
            recordCloseableIterator.close();
        }

        @Override
        public boolean hasNext() {
            return recordCloseableIterator.hasNext();
        }

        @Override
        public InternalRow next() {
            return recordCloseableIterator.next().getRow();
        }
    }
}
