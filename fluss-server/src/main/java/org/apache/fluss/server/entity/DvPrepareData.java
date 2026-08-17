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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Data class for DV prepare phase attached to notify lake table offset request. */
@Internal
public class DvPrepareData {

    private final long tableId;
    private final long readableSnapshotId;
    // Snapshot to download the RowPos index/SST from; 0 -> fall back to readableSnapshotId.
    private final long indexSnapshotId;
    private final Map<TableBucket, DvPositionReportData.DvBucketOffset> bucketOffsets;

    public DvPrepareData(
            long tableId,
            long readableSnapshotId,
            long indexSnapshotId,
            Map<TableBucket, DvPositionReportData.DvBucketOffset> bucketOffsets) {
        this.tableId = tableId;
        this.readableSnapshotId = readableSnapshotId;
        this.indexSnapshotId = indexSnapshotId;
        this.bucketOffsets = bucketOffsets;
    }

    public long getTableId() {
        return tableId;
    }

    public long getReadableSnapshotId() {
        return readableSnapshotId;
    }

    /** Snapshot to download the RowPos index from; 0 -> use {@link #getReadableSnapshotId()}. */
    public long getIndexSnapshotId() {
        return indexSnapshotId;
    }

    /**
     * Returns bucket offsets keyed by TableBucket (includes partition ID for partitioned tables).
     */
    public Map<TableBucket, DvPositionReportData.DvBucketOffset> getBucketOffsets() {
        return bucketOffsets;
    }

    /**
     * Returns a new {@link DvPrepareData} containing only the bucket offsets for the given {@link
     * TableBucket}s.
     */
    public DvPrepareData filterByBuckets(Set<TableBucket> tableBuckets) {
        Map<TableBucket, DvPositionReportData.DvBucketOffset> filtered = new HashMap<>();
        for (TableBucket tb : tableBuckets) {
            DvPositionReportData.DvBucketOffset offset = bucketOffsets.get(tb);
            if (offset != null) {
                filtered.put(tb, offset);
            }
        }
        return new DvPrepareData(tableId, readableSnapshotId, indexSnapshotId, filtered);
    }
}
