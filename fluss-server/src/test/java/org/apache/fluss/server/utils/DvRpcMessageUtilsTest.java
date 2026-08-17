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

package org.apache.fluss.server.utils;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.DvReadableSwitchRequest;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import org.apache.fluss.rpc.messages.PbDvBucketOffset;
import org.apache.fluss.rpc.messages.PbDvPrepare;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotMetadata;
import org.apache.fluss.server.entity.CommitLakeTableSnapshotsData;
import org.apache.fluss.server.entity.DvPositionReportData;
import org.apache.fluss.server.entity.DvPrepareData;
import org.apache.fluss.server.entity.DvReadableSwitchData;
import org.apache.fluss.server.entity.NotifyLakeTableOffsetData;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for DV-related methods in {@link ServerRpcMessageUtils}. */
class DvRpcMessageUtilsTest {

    @Test
    void testGetCommitLakeTableSnapshotDataWithDvReport() {
        CommitLakeTableSnapshotRequest request = new CommitLakeTableSnapshotRequest();
        PbLakeTableSnapshotMetadata metadata = request.addLakeTableSnapshotMetadata();
        metadata.setTableId(42L);
        metadata.setSnapshotId(5L);
        metadata.setTieredBucketOffsetsFilePath("/path/tiered");

        // Attach DV position report with per-bucket file dict and old files
        PbDvBucketOffset bucketOffset =
                metadata.setDvPositionReport()
                        .addBucketOffset()
                        .setBucketId(0)
                        .setReadableOffset(100L);
        bucketOffset.addNewFileDictEntry().setFileId(1).setFilePath("/sst/f1");
        bucketOffset.addOldFile("/sst/old.sst");

        CommitLakeTableSnapshotsData data =
                ServerRpcMessageUtils.getCommitLakeTableSnapshotData(request);
        CommitLakeTableSnapshotsData.CommitLakeTableSnapshot snapshot =
                data.getCommitLakeTableSnapshotByTableId().get(42L);

        assertThat(snapshot).isNotNull();
        assertThat(snapshot.getDvPositionReport()).isNotNull();

        DvPositionReportData dvReport = snapshot.getDvPositionReport();
        assertThat(dvReport.getBucketOffsets()).hasSize(1);

        DvPositionReportData.DvBucketOffset bo =
                dvReport.getBucketOffsets().get(new TableBucket(42L, 0));
        assertThat(bo.getReadableOffset()).isEqualTo(100L);
        assertThat(bo.getNewFileDictEntries()).hasSize(1);
        assertThat(bo.getNewFileDictEntries().get(1)).isEqualTo("/sst/f1");
        assertThat(bo.getOldFiles()).hasSize(1).contains("/sst/old.sst");
    }

    @Test
    void testGetCommitLakeTableSnapshotDataWithoutDvReport() {
        CommitLakeTableSnapshotRequest request = new CommitLakeTableSnapshotRequest();
        PbLakeTableSnapshotMetadata metadata = request.addLakeTableSnapshotMetadata();
        metadata.setTableId(42L);
        metadata.setSnapshotId(5L);
        metadata.setTieredBucketOffsetsFilePath("/path/tiered");

        CommitLakeTableSnapshotsData data =
                ServerRpcMessageUtils.getCommitLakeTableSnapshotData(request);
        CommitLakeTableSnapshotsData.CommitLakeTableSnapshot snapshot =
                data.getCommitLakeTableSnapshotByTableId().get(42L);

        assertThat(snapshot).isNotNull();
        assertThat(snapshot.getDvPositionReport()).isNull();
    }

    @Test
    void testGetNotifyLakeTableOffsetWithDvPrepare() {
        NotifyLakeTableOffsetRequest request = new NotifyLakeTableOffsetRequest();
        request.setCoordinatorEpoch(3);

        PbDvPrepare dvPrepare = request.setDvPrepare();
        dvPrepare.setTableId(42L);
        dvPrepare.setReadableSnapshotId(7L);
        PbDvBucketOffset bo = dvPrepare.addBucketOffset().setBucketId(0).setReadableOffset(100L);
        bo.addNewFileDictEntry().setFileId(1).setFilePath("/sst/f1");
        bo.addOldFile("/sst/old.sst");

        NotifyLakeTableOffsetData data = ServerRpcMessageUtils.getNotifyLakeTableOffset(request);

        assertThat(data.getCoordinatorEpoch()).isEqualTo(3);
        assertThat(data.getDvPrepare()).isNotNull();
        assertThat(data.getDvPrepare().getTableId()).isEqualTo(42L);
        assertThat(data.getDvPrepare().getReadableSnapshotId()).isEqualTo(7L);

        DvPositionReportData.DvBucketOffset parsedBo =
                data.getDvPrepare().getBucketOffsets().get(new TableBucket(42L, 0));
        assertThat(parsedBo.getReadableOffset()).isEqualTo(100L);
        assertThat(parsedBo.getNewFileDictEntries().get(1)).isEqualTo("/sst/f1");
        assertThat(parsedBo.getOldFiles()).contains("/sst/old.sst");
    }

    @Test
    void testGetNotifyLakeTableOffsetWithoutDvPrepare() {
        NotifyLakeTableOffsetRequest request = new NotifyLakeTableOffsetRequest();
        request.setCoordinatorEpoch(3);

        NotifyLakeTableOffsetData data = ServerRpcMessageUtils.getNotifyLakeTableOffset(request);

        assertThat(data.getCoordinatorEpoch()).isEqualTo(3);
        assertThat(data.getDvPrepare()).isNull();
    }

    @Test
    void testGetDvReadableSwitchData() {
        DvReadableSwitchRequest request = new DvReadableSwitchRequest();
        request.setCoordinatorEpoch(5);
        request.setTableId(100L);
        request.setReadableSnapshotId(10L);
        request.addBucketId(0);
        request.addBucketId(2);
        request.addBucketId(5);

        DvReadableSwitchData data = ServerRpcMessageUtils.getDvReadableSwitchData(request);

        assertThat(data.getCoordinatorEpoch()).isEqualTo(5);
        assertThat(data.getTableId()).isEqualTo(100L);
        assertThat(data.getReadableSnapshotId()).isEqualTo(10L);
        assertThat(data.getTableBuckets())
                .containsExactly(
                        new TableBucket(100L, 0),
                        new TableBucket(100L, 2),
                        new TableBucket(100L, 5));
    }

    @Test
    void testBuildDvPrepareMessage() {
        Map<Integer, String> dictEntries0 = new HashMap<>();
        dictEntries0.put(1, "/sst/f1");
        dictEntries0.put(2, "/sst/f2");

        Map<TableBucket, DvPositionReportData.DvBucketOffset> bucketOffsets = new HashMap<>();
        bucketOffsets.put(
                new TableBucket(42L, 0),
                new DvPositionReportData.DvBucketOffset(
                        100L, dictEntries0, Arrays.asList("/sst/old1.sst")));
        bucketOffsets.put(
                new TableBucket(42L, 1),
                new DvPositionReportData.DvBucketOffset(
                        200L, Collections.emptyMap(), Collections.emptyList()));

        DvPrepareData data = new DvPrepareData(42L, 7L, 88L, bucketOffsets);

        PbDvPrepare pb = ServerRpcMessageUtils.buildDvPrepareMessage(data);

        assertThat(pb.getTableId()).isEqualTo(42L);
        assertThat(pb.getReadableSnapshotId()).isEqualTo(7L);
        assertThat(pb.getIndexSnapshotId()).isEqualTo(88L);
        assertThat(pb.getBucketOffsetsCount()).isEqualTo(2);

        // Verify round-trip: serialize and parse back
        byte[] bytes = pb.toByteArray();
        PbDvPrepare parsed = new PbDvPrepare();
        parsed.parseFrom(bytes);

        assertThat(parsed.getTableId()).isEqualTo(42L);
        assertThat(parsed.getReadableSnapshotId()).isEqualTo(7L);
        assertThat(parsed.getIndexSnapshotId()).isEqualTo(88L);
        assertThat(parsed.getBucketOffsetsCount()).isEqualTo(2);
    }

    @Test
    void testBuildAndParseDvPrepareRoundTrip() {
        // Build data -> proto -> bytes -> proto -> data
        Map<Integer, String> dictEntries = new HashMap<>();
        dictEntries.put(5, "/path/to/sst");

        Map<TableBucket, DvPositionReportData.DvBucketOffset> bucketOffsets = new HashMap<>();
        bucketOffsets.put(
                new TableBucket(100L, 3),
                new DvPositionReportData.DvBucketOffset(
                        999L, dictEntries, Arrays.asList("/old/path")));

        DvPrepareData original = new DvPrepareData(100L, 50L, 60L, bucketOffsets);

        // Data -> Proto
        PbDvPrepare pb = ServerRpcMessageUtils.buildDvPrepareMessage(original);

        // Proto -> bytes -> Proto
        byte[] bytes = pb.toByteArray();
        PbDvPrepare parsedPb = new PbDvPrepare();
        parsedPb.parseFrom(bytes);

        // Proto -> Data
        DvPrepareData parsed = ServerRpcMessageUtils.parseDvPrepare(parsedPb);

        assertThat(parsed.getTableId()).isEqualTo(100L);
        assertThat(parsed.getReadableSnapshotId()).isEqualTo(50L);
        assertThat(parsed.getIndexSnapshotId()).isEqualTo(60L);
        assertThat(parsed.getBucketOffsets()).hasSize(1);

        DvPositionReportData.DvBucketOffset parsedBo =
                parsed.getBucketOffsets().get(new TableBucket(100L, 3));
        assertThat(parsedBo.getReadableOffset()).isEqualTo(999L);
        assertThat(parsedBo.getNewFileDictEntries()).hasSize(1);
        assertThat(parsedBo.getNewFileDictEntries().get(5)).isEqualTo("/path/to/sst");
        assertThat(parsedBo.getOldFiles()).hasSize(1).contains("/old/path");
    }
}
