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

package org.apache.fluss.flink.lake;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.exception.FlussException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.messages.PbLakeDvEntry;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.flink.source.split.LogSplit.NO_STOPPING_OFFSET;
import static org.apache.fluss.metadata.ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR;

/** A generator for lake splits. */
public class LakeSplitGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSplitGenerator.class);

    private static final int MAX_OUTER_RETRIES = 3;
    private static final int MAX_DV_FETCH_RETRIES = 10;
    private static final long INITIAL_BACKOFF_MS = 500;
    private static final long MAX_BACKOFF_MS = 10000;

    private final TableInfo tableInfo;
    private final Admin flussAdmin;
    private final OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final int bucketCount;
    private final Supplier<Set<PartitionInfo>> listPartitionSupplier;
    private final boolean dvEnabled;

    private final LakeSource<LakeSplit> lakeSource;

    public LakeSplitGenerator(
            TableInfo tableInfo,
            Admin flussAdmin,
            LakeSource<LakeSplit> lakeSource,
            OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever,
            OffsetsInitializer stoppingOffsetInitializer,
            int bucketCount,
            Supplier<Set<PartitionInfo>> listPartitionSupplier) {
        this.tableInfo = tableInfo;
        this.flussAdmin = flussAdmin;
        this.lakeSource = lakeSource;
        this.bucketOffsetsRetriever = bucketOffsetsRetriever;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.bucketCount = bucketCount;
        this.listPartitionSupplier = listPartitionSupplier;
        this.dvEnabled = tableInfo.getTableConfig().isDeletionVectorsEnabled();
    }

    /**
     * Return A list of hybrid lake snapshot {@link LakeSnapshotSplit}, {@link
     * LakeSnapshotAndFlussLogSplit} and the corresponding Fluss {@link LogSplit} based on the lake
     * snapshot. Return null if no lake snapshot exists.
     *
     * <p>If DV is enabled, fetches DV snapshots from TabletServers with retry:
     *
     * <ul>
     *   <li>Inner retry: per-bucket backoff for "not ready" (server hasn't completed Switch yet)
     *   <li>Outer retry: refresh LakeSnapshot when snapshot has been superseded
     * </ul>
     */
    @Nullable
    public List<SourceSplitBase> generateHybridLakeFlussSplits() throws Exception {
        for (int outerRetry = 0; outerRetry < MAX_OUTER_RETRIES; outerRetry++) {
            LakeSnapshot lakeSnapshotInfo;
            try {
                lakeSnapshotInfo =
                        flussAdmin.getReadableLakeSnapshot(tableInfo.getTablePath()).get();
            } catch (Exception exception) {
                if (ExceptionUtils.stripExecutionException(exception)
                        instanceof LakeTableSnapshotNotExistException) {
                    return null;
                }
                throw exception;
            }

            long snapshotId = lakeSnapshotInfo.getSnapshotId();

            boolean isLogTable = !tableInfo.hasPrimaryKey();
            boolean isPartitioned = tableInfo.isPartitioned();

            Map<String, Map<Integer, List<LakeSplit>>> lakeSplits =
                    groupLakeSplits(
                            lakeSource
                                    .createPlanner((LakeSource.PlannerContext) () -> snapshotId)
                                    .plan());

            Map<TableBucket, Long> tableBucketsOffset = lakeSnapshotInfo.getTableBucketsOffset();

            // Pre-compute stopping offsets and partition info
            Map<Long, String> partitionNameById = null;
            Map<TableBucket, Long> allStoppingOffsets = new HashMap<>();
            List<Integer> bucketIds =
                    IntStream.range(0, bucketCount).boxed().collect(Collectors.toList());

            if (isPartitioned) {
                Set<PartitionInfo> partitionInfos = listPartitionSupplier.get();
                partitionNameById =
                        partitionInfos.stream()
                                .collect(
                                        Collectors.toMap(
                                                PartitionInfo::getPartitionId,
                                                PartitionInfo::getPartitionName));
                for (Map.Entry<Long, String> entry : partitionNameById.entrySet()) {
                    Map<Integer, Long> offsets =
                            stoppingOffsetInitializer.getBucketOffsets(
                                    entry.getValue(), bucketIds, bucketOffsetsRetriever);
                    for (Map.Entry<Integer, Long> offsetEntry : offsets.entrySet()) {
                        allStoppingOffsets.put(
                                new TableBucket(
                                        tableInfo.getTableId(),
                                        entry.getKey(),
                                        offsetEntry.getKey()),
                                offsetEntry.getValue());
                    }
                }
            } else {
                Map<Integer, Long> offsets =
                        stoppingOffsetInitializer.getBucketOffsets(
                                null, bucketIds, bucketOffsetsRetriever);
                for (Map.Entry<Integer, Long> offsetEntry : offsets.entrySet()) {
                    allStoppingOffsets.put(
                            new TableBucket(tableInfo.getTableId(), null, offsetEntry.getKey()),
                            offsetEntry.getValue());
                }
            }

            // Fetch DV data if enabled, only for buckets with data gap
            Map<TableBucket, DvSnapshotInfo> bucketDvSnapshots = null;
            if (dvEnabled) {
                Set<TableBucket> bucketsNeedingDv =
                        findBucketsNeedingDv(tableBucketsOffset, allStoppingOffsets);
                if (!bucketsNeedingDv.isEmpty()) {
                    try {
                        bucketDvSnapshots = fetchDvForAllBuckets(snapshotId, bucketsNeedingDv);
                    } catch (Exception e) {
                        Throwable cause = ExceptionUtils.stripExecutionException(e);
                        if (cause instanceof StaleSnapshotException) {
                            StaleSnapshotException stale = (StaleSnapshotException) cause;
                            if (stale.getRequestedSnapshotId() < stale.getCurrentSnapshotId()) {
                                // Snapshot superseded, refresh and retry
                                LOG.info(
                                        "DV snapshot {} superseded (current: {}), refreshing.",
                                        snapshotId,
                                        stale.getCurrentSnapshotId());
                                continue;
                            }
                        }
                        throw e;
                    }
                }
            }

            if (isPartitioned) {
                return generatePartitionTableSplit(
                        lakeSplits,
                        isLogTable,
                        tableBucketsOffset,
                        partitionNameById,
                        allStoppingOffsets,
                        bucketDvSnapshots);
            } else {
                Map<Integer, List<LakeSplit>> nonPartitionLakeSplits =
                        lakeSplits.isEmpty() ? null : lakeSplits.values().iterator().next();
                // non-partitioned table
                return generateNoPartitionedTableSplit(
                        nonPartitionLakeSplits,
                        isLogTable,
                        tableBucketsOffset,
                        allStoppingOffsets,
                        bucketDvSnapshots);
            }
        }
        throw new FlussException(
                "Failed to fetch DV snapshots after "
                        + MAX_OUTER_RETRIES
                        + " retries due to snapshot superseding");
    }

    private Map<String, Map<Integer, List<LakeSplit>>> groupLakeSplits(List<LakeSplit> lakeSplits) {
        Map<String, Map<Integer, List<LakeSplit>>> result = new HashMap<>();
        for (LakeSplit split : lakeSplits) {
            String partition = String.join(PARTITION_SPEC_SEPARATOR, split.partition());
            int bucket = split.bucket();
            // Get or create the partition group
            Map<Integer, List<LakeSplit>> bucketMap =
                    result.computeIfAbsent(partition, k -> new HashMap<>());
            List<LakeSplit> splitList = bucketMap.computeIfAbsent(bucket, k -> new ArrayList<>());
            splitList.add(split);
        }
        return result;
    }

    private List<SourceSplitBase> generatePartitionTableSplit(
            Map<String, Map<Integer, List<LakeSplit>>> lakeSplits,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<Long, String> partitionNameById,
            Map<TableBucket, Long> allStoppingOffsets,
            @Nullable Map<TableBucket, DvSnapshotInfo> bucketDvSnapshots) {
        List<SourceSplitBase> splits = new ArrayList<>();
        Map<String, Long> flussPartitionIdByName =
                partitionNameById.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getValue,
                                        Map.Entry::getKey,
                                        (existing, replacement) -> existing,
                                        LinkedHashMap::new));
        long lakeSplitPartitionId = -1L;

        // iterate lake splits
        for (Map.Entry<String, Map<Integer, List<LakeSplit>>> lakeSplitEntry :
                lakeSplits.entrySet()) {
            String partitionName = lakeSplitEntry.getKey();
            Map<Integer, List<LakeSplit>> lakeSplitsOfPartition = lakeSplitEntry.getValue();
            Long partitionId = flussPartitionIdByName.remove(partitionName);
            if (partitionId != null) {
                // mean the partition also exist in fluss partition
                splits.addAll(
                        generateSplit(
                                lakeSplitsOfPartition,
                                partitionId,
                                partitionName,
                                isLogTable,
                                tableBucketSnapshotLogOffset,
                                allStoppingOffsets,
                                bucketDvSnapshots));

            } else {
                // only lake data
                splits.addAll(
                        toLakeSnapshotSplits(
                                lakeSplitsOfPartition,
                                partitionName,
                                // now, we can't get partition id for the partition only
                                // in lake, set them to a arbitrary partition id, but
                                // make sure different partition have different partition id
                                // to enable different partition can be distributed to different
                                // tasks
                                lakeSplitPartitionId--));
            }
        }

        // iterate remain fluss splits
        for (Map.Entry<String, Long> partitionIdByNameEntry : flussPartitionIdByName.entrySet()) {
            String partitionName = partitionIdByNameEntry.getKey();
            Long partitionId = partitionIdByNameEntry.getValue();
            splits.addAll(
                    generateSplit(
                            null,
                            partitionId,
                            partitionName,
                            isLogTable,
                            // pass empty map since we won't read lake splits
                            Collections.emptyMap(),
                            allStoppingOffsets,
                            bucketDvSnapshots));
        }
        return splits;
    }

    private List<SourceSplitBase> generateSplit(
            @Nullable Map<Integer, List<LakeSplit>> lakeSplits,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<TableBucket, Long> allStoppingOffsets,
            @Nullable Map<TableBucket, DvSnapshotInfo> bucketDvSnapshots) {
        List<SourceSplitBase> splits = new ArrayList<>();
        if (isLogTable) {
            if (lakeSplits != null) {
                splits.addAll(toLakeSnapshotSplits(lakeSplits, partitionName, partitionId));
            }
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(tableBucket);
                Long stoppingOffset = allStoppingOffsets.get(tableBucket);
                if (stoppingOffset == null) {
                    stoppingOffset = NO_STOPPING_OFFSET;
                }
                if (snapshotLogOffset == null) {
                    // no data committed to lake for this bucket, scan from fluss log
                    if (stoppingOffset == NO_STOPPING_OFFSET || stoppingOffset > 0) {
                        splits.add(
                                new LogSplit(
                                        tableBucket,
                                        partitionName,
                                        EARLIEST_OFFSET,
                                        stoppingOffset));
                    }
                } else {
                    // need to read remain fluss log
                    if (stoppingOffset == NO_STOPPING_OFFSET
                            || snapshotLogOffset < stoppingOffset) {
                        splits.add(
                                new LogSplit(
                                        tableBucket,
                                        partitionName,
                                        snapshotLogOffset,
                                        stoppingOffset));
                    }
                }
            }
        } else {
            // it's primary key table
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(tableBucket);
                Long stoppingOffset = allStoppingOffsets.get(tableBucket);
                if (stoppingOffset == null) {
                    stoppingOffset = NO_STOPPING_OFFSET;
                }
                DvSnapshotInfo dvSnapshot =
                        bucketDvSnapshots != null ? bucketDvSnapshots.get(tableBucket) : null;
                splits.addAll(
                        generateSplitForPrimaryKeyTableBucket(
                                lakeSplits != null ? lakeSplits.get(bucket) : null,
                                tableBucket,
                                partitionName,
                                snapshotLogOffset,
                                stoppingOffset,
                                dvSnapshot));
            }
        }

        return splits;
    }

    private List<SourceSplitBase> toLakeSnapshotSplits(
            Map<Integer, List<LakeSplit>> lakeSplits,
            @Nullable String partitionName,
            @Nullable Long partitionId) {
        List<SourceSplitBase> splits = new ArrayList<>();
        // we may have multiple table buckets; so we need to
        // introduce an index to make split unique
        int index = 0;
        for (LakeSplit lakeSplit :
                lakeSplits.values().stream().flatMap(List::stream).collect(Collectors.toList())) {
            TableBucket tableBucket =
                    new TableBucket(tableInfo.getTableId(), partitionId, lakeSplit.bucket());
            splits.add(new LakeSnapshotSplit(tableBucket, partitionName, lakeSplit, index++));
        }
        return splits;
    }

    private List<SourceSplitBase> generateSplitForPrimaryKeyTableBucket(
            @Nullable List<LakeSplit> lakeSplits,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable Long snapshotLogOffset,
            long stoppingOffset,
            @Nullable DvSnapshotInfo dvSnapshot) {
        // no snapshot data for this bucket or no a corresponding log offset in this bucket,
        // can only scan from change log
        if (snapshotLogOffset == null || snapshotLogOffset < 0) {
            return Collections.singletonList(
                    new LakeSnapshotAndFlussLogSplit(
                            tableBucket, partitionName, null, EARLIEST_OFFSET, stoppingOffset));
        }

        // No DV available: fall back to sort-merge via LakeSnapshotAndFlussLogSplit
        if (dvSnapshot == null) {
            return Collections.singletonList(
                    new LakeSnapshotAndFlussLogSplit(
                            tableBucket,
                            partitionName,
                            lakeSplits,
                            snapshotLogOffset,
                            stoppingOffset,
                            0,
                            0,
                            lakeSplits == null,
                            null));
        }

        // DV available: split into independent lake + log splits with DV filtering
        LOG.info(
                "Using DV-based split for bucket {}: lakeDvSize={}, logDvPresent={}, "
                        + "snapshotLogOffset={}, stoppingOffset={}, dvLogEndOffset={}",
                tableBucket,
                dvSnapshot.getLakeDv().size(),
                dvSnapshot.getLogDvBitmap() != null,
                snapshotLogOffset,
                stoppingOffset,
                dvSnapshot.getLogEndOffset());
        List<SourceSplitBase> splits = new ArrayList<>();

        // Truncate stoppingOffset to DV coverage range
        if (stoppingOffset > 0) {
            stoppingOffset = Math.min(stoppingOffset, dvSnapshot.getLogEndOffset());
        }

        // Generate LakeSnapshotSplit(s) with lakeDv map
        if (lakeSplits != null) {
            Map<String, byte[]> lakeDvMap = dvSnapshot.getLakeDv();
            int index = 0;
            for (LakeSplit lakeSplit : lakeSplits) {
                splits.add(
                        new LakeSnapshotSplit(
                                tableBucket, partitionName, lakeSplit, index++, 0, lakeDvMap));
            }
        }

        // Generate LogSplit with logDv bitmap.
        // Use empty byte[] when logDvBitmap is null to indicate DV batch read mode
        // (enables DELETE/UPDATE_BEFORE filtering even when no specific offsets to skip).
        if (stoppingOffset == NO_STOPPING_OFFSET || snapshotLogOffset < stoppingOffset) {
            byte[] logDvBitmap = dvSnapshot.getLogDvBitmap();
            if (logDvBitmap == null) {
                logDvBitmap = new byte[0];
            }
            splits.add(
                    new LogSplit(
                            tableBucket,
                            partitionName,
                            snapshotLogOffset,
                            stoppingOffset,
                            logDvBitmap));
        }

        return splits;
    }

    private List<SourceSplitBase> generateNoPartitionedTableSplit(
            @Nullable Map<Integer, List<LakeSplit>> lakeSplits,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<TableBucket, Long> allStoppingOffsets,
            @Nullable Map<TableBucket, DvSnapshotInfo> bucketDvSnapshots) {
        return generateSplit(
                lakeSplits,
                null,
                null,
                isLogTable,
                tableBucketSnapshotLogOffset,
                allStoppingOffsets,
                bucketDvSnapshots);
    }

    // --------- DV fetch helpers ---------

    /**
     * Finds buckets that need DV data. A DV-enabled bucket needs its DV snapshot when either it has
     * lake snapshot data (whose stale rows must be masked by LakeDv) or there is a hot-log gap
     * (readable offset &lt; latest, needing LogDv). Buckets with lake data but no log gap must
     * still be included; otherwise they fall back to the sort-merge path, which has no comparator
     * for Iceberg (non-sorted lake reader) and throws.
     */
    private Set<TableBucket> findBucketsNeedingDv(
            Map<TableBucket, Long> tableBucketsOffset, Map<TableBucket, Long> allStoppingOffsets) {
        Set<TableBucket> result = new HashSet<>();
        for (Map.Entry<TableBucket, Long> entry : tableBucketsOffset.entrySet()) {
            TableBucket tb = entry.getKey();
            long snapshotLogOffset = entry.getValue();
            Long stoppingOffset = allStoppingOffsets.get(tb);
            // Include buckets with lake snapshot data (need LakeDv), plus the log-gap cases.
            if (snapshotLogOffset >= 0
                    || stoppingOffset == null
                    || stoppingOffset == NO_STOPPING_OFFSET
                    || snapshotLogOffset != stoppingOffset) {
                result.add(tb);
            }
        }
        return result;
    }

    /**
     * Fetches DV snapshot for the specified table buckets. Per-bucket retry for "not ready" errors.
     * Throws {@link StaleSnapshotException} (superseded) to caller for outer retry.
     */
    private Map<TableBucket, DvSnapshotInfo> fetchDvForAllBuckets(
            long snapshotId, Set<TableBucket> tableBuckets) throws Exception {
        TablePath tablePath = tableInfo.getTablePath();
        Map<TableBucket, DvSnapshotInfo> results = new HashMap<>();
        for (TableBucket tb : tableBuckets) {
            results.put(tb, fetchDvForBucketWithRetry(tablePath, tb, snapshotId));
        }
        return results;
    }

    private DvSnapshotInfo fetchDvForBucketWithRetry(
            TablePath tablePath, TableBucket tableBucket, long snapshotId) throws Exception {
        long backoffMs = INITIAL_BACKOFF_MS;
        for (int attempt = 0; attempt < MAX_DV_FETCH_RETRIES; attempt++) {
            try {
                GetDvSnapshotResponse resp =
                        flussAdmin
                                .getDvSnapshot(
                                        tablePath,
                                        tableBucket.getTableId(),
                                        tableBucket.getPartitionId(),
                                        tableBucket.getBucket(),
                                        snapshotId)
                                .get();
                return toDvSnapshotInfo(resp);
            } catch (Exception e) {
                Throwable cause = ExceptionUtils.stripExecutionException(e);
                if (cause instanceof StaleSnapshotException) {
                    StaleSnapshotException stale = (StaleSnapshotException) cause;
                    if (stale.getRequestedSnapshotId() > stale.getCurrentSnapshotId()) {
                        // Server not ready yet, backoff and retry
                        LOG.debug(
                                "Bucket {} not ready for snapshot {} (current: {}), retry {}/{}",
                                tableBucket,
                                snapshotId,
                                stale.getCurrentSnapshotId(),
                                attempt + 1,
                                MAX_DV_FETCH_RETRIES);
                        Thread.sleep(backoffMs);
                        backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
                        continue;
                    }
                    // Snapshot superseded - re-throw to trigger outer retry
                    throw e;
                }
                throw e;
            }
        }
        throw new FlussException(
                String.format(
                        "Failed to fetch DV snapshot for bucket %s after %d retries",
                        tableBucket, MAX_DV_FETCH_RETRIES));
    }

    private static DvSnapshotInfo toDvSnapshotInfo(GetDvSnapshotResponse resp) {
        Map<String, byte[]> lakeDv = new HashMap<>();
        for (int i = 0; i < resp.getLakeDvEntriesCount(); i++) {
            PbLakeDvEntry entry = resp.getLakeDvEntryAt(i);
            lakeDv.put(entry.getFilePath(), entry.getDeletedPositionsBitmap());
        }
        byte[] logDvBitmap = resp.hasLogDvBitmap() ? resp.getLogDvBitmap() : null;
        return new DvSnapshotInfo(
                lakeDv, logDvBitmap, resp.getLogEndOffset(), resp.getSnapshotStartOffset());
    }
}
