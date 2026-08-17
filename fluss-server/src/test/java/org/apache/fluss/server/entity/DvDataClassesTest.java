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

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for DV data classes. */
class DvDataClassesTest {

    @Test
    void testDvPositionReportData() {
        Map<Integer, String> dictEntries0 = new HashMap<>();
        dictEntries0.put(1, "/sst/f1.sst");
        dictEntries0.put(2, "/sst/f2.sst");

        Map<TableBucket, DvPositionReportData.DvBucketOffset> bucketOffsets = new HashMap<>();
        bucketOffsets.put(
                new TableBucket(1L, 0),
                new DvPositionReportData.DvBucketOffset(
                        100L, dictEntries0, Arrays.asList("/sst/old1.sst")));
        bucketOffsets.put(
                new TableBucket(1L, 1),
                new DvPositionReportData.DvBucketOffset(
                        200L, Collections.emptyMap(), Collections.emptyList()));

        DvPositionReportData data = new DvPositionReportData(bucketOffsets, 0L);

        assertThat(data.getBucketOffsets()).hasSize(2);
        assertThat(data.getBucketOffsets().get(new TableBucket(1L, 0)).getReadableOffset())
                .isEqualTo(100L);
        assertThat(data.getBucketOffsets().get(new TableBucket(1L, 0)).getNewFileDictEntries())
                .hasSize(2);
        assertThat(
                        data.getBucketOffsets()
                                .get(new TableBucket(1L, 0))
                                .getNewFileDictEntries()
                                .get(1))
                .isEqualTo("/sst/f1.sst");
        assertThat(data.getBucketOffsets().get(new TableBucket(1L, 0)).getOldFiles())
                .contains("/sst/old1.sst");
        assertThat(data.getBucketOffsets().get(new TableBucket(1L, 1)).getReadableOffset())
                .isEqualTo(200L);
        assertThat(data.getBucketOffsets().get(new TableBucket(1L, 1)).getNewFileDictEntries())
                .isEmpty();
        assertThat(data.getBucketOffsets().get(new TableBucket(1L, 1)).getOldFiles()).isEmpty();
    }

    @Test
    void testDvBucketOffset() {
        Map<Integer, String> dictEntries = new HashMap<>();
        dictEntries.put(5, "/sst/f5.sst");

        DvPositionReportData.DvBucketOffset offset =
                new DvPositionReportData.DvBucketOffset(
                        500L, dictEntries, Arrays.asList("/sst/old.sst"));

        assertThat(offset.getReadableOffset()).isEqualTo(500L);
        assertThat(offset.getNewFileDictEntries()).hasSize(1);
        assertThat(offset.getNewFileDictEntries().get(5)).isEqualTo("/sst/f5.sst");
        assertThat(offset.getOldFiles()).hasSize(1).contains("/sst/old.sst");
    }

    @Test
    void testDvPrepareData() {
        Map<Integer, String> dictEntries = new HashMap<>();
        dictEntries.put(10, "/dict/f10.sst");

        Map<TableBucket, DvPositionReportData.DvBucketOffset> bucketOffsets = new HashMap<>();
        bucketOffsets.put(
                new TableBucket(42L, 3),
                new DvPositionReportData.DvBucketOffset(
                        500L, dictEntries, Arrays.asList("/dict/old.sst")));

        DvPrepareData data = new DvPrepareData(42L, 7L, 0L, bucketOffsets);

        assertThat(data.getTableId()).isEqualTo(42L);
        assertThat(data.getReadableSnapshotId()).isEqualTo(7L);
        assertThat(data.getBucketOffsets()).hasSize(1);
        assertThat(data.getBucketOffsets().get(new TableBucket(42L, 3)).getReadableOffset())
                .isEqualTo(500L);
        assertThat(
                        data.getBucketOffsets()
                                .get(new TableBucket(42L, 3))
                                .getNewFileDictEntries()
                                .get(10))
                .isEqualTo("/dict/f10.sst");
        assertThat(data.getBucketOffsets().get(new TableBucket(42L, 3)).getOldFiles())
                .contains("/dict/old.sst");
    }

    @Test
    void testDvPrepareDataFilterByBuckets() {
        Map<TableBucket, DvPositionReportData.DvBucketOffset> bucketOffsets = new HashMap<>();
        bucketOffsets.put(
                new TableBucket(42L, 0),
                new DvPositionReportData.DvBucketOffset(
                        100L, Collections.singletonMap(1, "/sst/f1"), Arrays.asList("/old1")));
        bucketOffsets.put(
                new TableBucket(42L, 1),
                new DvPositionReportData.DvBucketOffset(
                        200L, Collections.singletonMap(2, "/sst/f2"), Arrays.asList("/old2")));
        bucketOffsets.put(
                new TableBucket(42L, 2),
                new DvPositionReportData.DvBucketOffset(
                        300L, Collections.emptyMap(), Collections.emptyList()));

        DvPrepareData data = new DvPrepareData(42L, 7L, 0L, bucketOffsets);

        DvPrepareData filtered =
                data.filterByBuckets(
                        new HashSet<>(
                                Arrays.asList(new TableBucket(42L, 0), new TableBucket(42L, 2))));

        assertThat(filtered.getTableId()).isEqualTo(42L);
        assertThat(filtered.getReadableSnapshotId()).isEqualTo(7L);
        assertThat(filtered.getBucketOffsets()).hasSize(2);
        assertThat(filtered.getBucketOffsets()).containsKey(new TableBucket(42L, 0));
        assertThat(filtered.getBucketOffsets()).containsKey(new TableBucket(42L, 2));
        assertThat(filtered.getBucketOffsets()).doesNotContainKey(new TableBucket(42L, 1));
        assertThat(
                        filtered.getBucketOffsets()
                                .get(new TableBucket(42L, 0))
                                .getNewFileDictEntries()
                                .get(1))
                .isEqualTo("/sst/f1");
        assertThat(filtered.getBucketOffsets().get(new TableBucket(42L, 0)).getOldFiles())
                .contains("/old1");
    }

    @Test
    void testDvReadableSwitchData() {
        List<TableBucket> tableBuckets =
                Arrays.asList(
                        new TableBucket(100L, 0),
                        new TableBucket(100L, 1),
                        new TableBucket(100L, 3));
        DvReadableSwitchData data = new DvReadableSwitchData(5, 100L, 10L, tableBuckets);

        assertThat(data.getCoordinatorEpoch()).isEqualTo(5);
        assertThat(data.getTableId()).isEqualTo(100L);
        assertThat(data.getReadableSnapshotId()).isEqualTo(10L);
        assertThat(data.getTableBuckets())
                .containsExactly(
                        new TableBucket(100L, 0),
                        new TableBucket(100L, 1),
                        new TableBucket(100L, 3));
    }

    @Test
    void testCommitLakeTableSnapshotsDataWithDvReport() {
        Map<TableBucket, DvPositionReportData.DvBucketOffset> bucketOffsets = new HashMap<>();
        bucketOffsets.put(
                new TableBucket(42L, 0),
                new DvPositionReportData.DvBucketOffset(
                        100L,
                        Collections.singletonMap(1, "/sst/f1.sst"),
                        Arrays.asList("/sst/old.sst")));

        DvPositionReportData dvReport = new DvPositionReportData(bucketOffsets, 0L);

        CommitLakeTableSnapshotsData.Builder builder = CommitLakeTableSnapshotsData.builder();
        builder.addTableSnapshot(42L, null, null, null, null, dvReport);

        CommitLakeTableSnapshotsData data = builder.build();
        CommitLakeTableSnapshotsData.CommitLakeTableSnapshot snapshot =
                data.getCommitLakeTableSnapshotByTableId().get(42L);

        assertThat(snapshot).isNotNull();
        assertThat(snapshot.getDvPositionReport()).isNotNull();
        assertThat(snapshot.getDvPositionReport().getBucketOffsets()).hasSize(1);
    }

    @Test
    void testCommitLakeTableSnapshotsDataWithoutDvReport() {
        CommitLakeTableSnapshotsData.Builder builder = CommitLakeTableSnapshotsData.builder();
        builder.addTableSnapshot(42L, null, null, null, null);

        CommitLakeTableSnapshotsData data = builder.build();
        CommitLakeTableSnapshotsData.CommitLakeTableSnapshot snapshot =
                data.getCommitLakeTableSnapshotByTableId().get(42L);

        assertThat(snapshot).isNotNull();
        assertThat(snapshot.getDvPositionReport()).isNull();
    }

    @Test
    void testNotifyLakeTableOffsetDataWithDvPrepare() {
        Map<TableBucket, DvPositionReportData.DvBucketOffset> bucketOffsets = new HashMap<>();
        bucketOffsets.put(
                new TableBucket(42L, 0),
                new DvPositionReportData.DvBucketOffset(
                        100L, Collections.emptyMap(), Collections.emptyList()));

        DvPrepareData dvPrepare = new DvPrepareData(42L, 7L, 0L, bucketOffsets);

        NotifyLakeTableOffsetData data =
                new NotifyLakeTableOffsetData(5, new HashMap<>(), dvPrepare);

        assertThat(data.getCoordinatorEpoch()).isEqualTo(5);
        assertThat(data.getDvPrepare()).isNotNull();
        assertThat(data.getDvPrepare().getTableId()).isEqualTo(42L);
        assertThat(data.getDvPrepare().getReadableSnapshotId()).isEqualTo(7L);
    }

    @Test
    void testNotifyLakeTableOffsetDataWithoutDvPrepare() {
        NotifyLakeTableOffsetData data = new NotifyLakeTableOffsetData(5, new HashMap<>());

        assertThat(data.getCoordinatorEpoch()).isEqualTo(5);
        assertThat(data.getDvPrepare()).isNull();
    }
}
