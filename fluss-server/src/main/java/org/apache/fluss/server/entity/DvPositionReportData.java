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

package org.apache.fluss.server.entity;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;

import java.util.List;
import java.util.Map;

/** Data class for DV position report attached to lake table snapshot metadata. */
@Internal
public class DvPositionReportData {

    private final Map<TableBucket, DvBucketOffset> bucketOffsets;
    // Snapshot the RowPos index/SST was uploaded under; may differ from the readable snapshot.
    private final long indexSnapshotId;

    public DvPositionReportData(
            Map<TableBucket, DvBucketOffset> bucketOffsets, long indexSnapshotId) {
        this.bucketOffsets = bucketOffsets;
        this.indexSnapshotId = indexSnapshotId;
    }

    /**
     * Returns bucket offsets keyed by TableBucket (includes partition ID for partitioned tables).
     */
    public Map<TableBucket, DvBucketOffset> getBucketOffsets() {
        return bucketOffsets;
    }

    /** Snapshot the RowPos index/SST was uploaded under (0 if indexed == readable snapshot). */
    public long getIndexSnapshotId() {
        return indexSnapshotId;
    }

    /** Per-bucket DV info including readable offset, file dict entries and old files. */
    public static class DvBucketOffset {

        private final long readableOffset;
        private final Map<Integer, String> newFileDictEntries;
        private final List<String> oldFiles;

        public DvBucketOffset(
                long readableOffset,
                Map<Integer, String> newFileDictEntries,
                List<String> oldFiles) {
            this.readableOffset = readableOffset;
            this.newFileDictEntries = newFileDictEntries;
            this.oldFiles = oldFiles;
        }

        public long getReadableOffset() {
            return readableOffset;
        }

        /** Returns new file dict entries: fileId -> filePath. */
        public Map<Integer, String> getNewFileDictEntries() {
            return newFileDictEntries;
        }

        /** Returns old file paths to be cleaned up. */
        public List<String> getOldFiles() {
            return oldFiles;
        }
    }
}
