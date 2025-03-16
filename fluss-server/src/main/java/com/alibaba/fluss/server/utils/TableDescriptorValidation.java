/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.config.ConfigOption;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.ReadableConfig;
import com.alibaba.fluss.exception.InvalidConfigException;
import com.alibaba.fluss.exception.InvalidTableException;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.MergeEngineType;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeRoot;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.AutoPartitionStrategy;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import static com.alibaba.fluss.config.FlussConfigUtils.TABLE_OPTIONS;
import static com.alibaba.fluss.utils.PartitionUtils.PARTITION_KEY_SUPPORTED_TYPES;

/** Validator of {@link TableDescriptor}. */
public class TableDescriptorValidation {

    private static final List<String> RESERVED_COLUMN_NAMES =
            Collections.unmodifiableList(
                    Arrays.asList("_change_type", "_log_offset", "_commit_timestamp"));

    /** Validate table descriptor to create is valid and contain all necessary information. */
    public static void validateTableDescriptor(TableDescriptor tableDescriptor) {
        boolean hasPrimaryKey = tableDescriptor.getSchema().getPrimaryKey().isPresent();
        RowType schema = tableDescriptor.getSchema().getRowType();
        Configuration tableConf = Configuration.fromMap(tableDescriptor.getProperties());
        // Validate column names against reserved names
        for (String columnName : tableDescriptor.getSchema().getColumnNames()) {
            if (RESERVED_COLUMN_NAMES.contains(columnName)) {
                throw new InvalidTableException(
                        String.format("Column name '%s' is reserved for system use.", columnName));
            }
        }
        // check properties should only contain table.* options,
        // and this cluster know it,
        // and value is valid
        for (String key : tableConf.keySet()) {
            if (!TABLE_OPTIONS.containsKey(key)) {
                throw new InvalidConfigException(
                        String.format(
                                "'%s' is not a Fluss table property. Please use '.customProperty(..)' to set custom properties.",
                                key));
            }
            ConfigOption<?> option = TABLE_OPTIONS.get(key);
            validateOptionValue(tableConf, option);
        }

        // check distribution
        checkDistribution(tableDescriptor);

        // check individual options
        checkReplicationFactor(tableConf);
        checkLogFormat(tableConf, hasPrimaryKey);
        checkArrowCompression(tableConf);
        checkMergeEngine(tableConf, hasPrimaryKey, schema);
        checkTieredLog(tableConf);
        checkPartition(tableConf, tableDescriptor.getPartitionKeys(), schema);
    }

    private static void checkDistribution(TableDescriptor tableDescriptor) {
        if (!tableDescriptor.getTableDistribution().isPresent()) {
            throw new InvalidTableException("Table distribution is required.");
        }
        if (!tableDescriptor.getTableDistribution().get().getBucketCount().isPresent()) {
            throw new InvalidTableException("Bucket number must be set.");
        }
    }

    private static void checkReplicationFactor(Configuration tableConf) {
        if (!tableConf.containsKey(ConfigOptions.TABLE_REPLICATION_FACTOR.key())) {
            throw new InvalidConfigException(
                    String.format(
                            "Missing required table property '%s'.",
                            ConfigOptions.TABLE_REPLICATION_FACTOR.key()));
        }
        if (tableConf.get(ConfigOptions.TABLE_REPLICATION_FACTOR) <= 0) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' must be greater than 0.",
                            ConfigOptions.TABLE_REPLICATION_FACTOR.key()));
        }
    }

    private static void checkLogFormat(Configuration tableConf, boolean hasPrimaryKey) {
        KvFormat kvFormat = tableConf.get(ConfigOptions.TABLE_KV_FORMAT);
        LogFormat logFormat = tableConf.get(ConfigOptions.TABLE_LOG_FORMAT);
        if (hasPrimaryKey && kvFormat == KvFormat.COMPACTED && logFormat != LogFormat.ARROW) {
            throw new InvalidConfigException(
                    "Currently, Primary Key Table only supports ARROW log format if kv format is COMPACTED.");
        }
    }

    private static void checkArrowCompression(Configuration tableConf) {
        if (tableConf.containsKey(ConfigOptions.TABLE_LOG_ARROW_COMPRESSION_ZSTD_LEVEL.key())) {
            int compressionLevel =
                    tableConf.get(ConfigOptions.TABLE_LOG_ARROW_COMPRESSION_ZSTD_LEVEL);
            if (compressionLevel < 1 || compressionLevel > 22) {
                throw new InvalidConfigException(
                        "Invalid ZSTD compression level: "
                                + compressionLevel
                                + ". Expected a value between 1 and 22.");
            }
        }
    }

    private static void checkMergeEngine(
            Configuration tableConf, boolean hasPrimaryKey, RowType schema) {
        MergeEngineType mergeEngine = tableConf.get(ConfigOptions.TABLE_MERGE_ENGINE);
        if (mergeEngine != null) {
            if (!hasPrimaryKey) {
                throw new InvalidConfigException(
                        "Merge engine is only supported in primary key table.");
            }
            if (mergeEngine == MergeEngineType.VERSIONED) {
                Optional<String> versionColumn =
                        tableConf.getOptional(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN);
                if (!versionColumn.isPresent()) {
                    throw new InvalidConfigException(
                            String.format(
                                    "'%s' must be set for versioned merge engine.",
                                    ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key()));
                }
                int columnIndex = schema.getFieldIndex(versionColumn.get());
                if (columnIndex < 0) {
                    throw new InvalidConfigException(
                            String.format(
                                    "The version column '%s' for versioned merge engine doesn't exist in schema.",
                                    versionColumn.get()));
                }
                EnumSet<DataTypeRoot> supportedTypes =
                        EnumSet.of(
                                DataTypeRoot.INTEGER,
                                DataTypeRoot.BIGINT,
                                DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                                DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
                DataType columnType = schema.getTypeAt(columnIndex);
                if (!supportedTypes.contains(columnType.getTypeRoot())) {
                    throw new InvalidConfigException(
                            String.format(
                                    "The version column '%s' for versioned merge engine must be one type of "
                                            + "[INT, BIGINT, TIMESTAMP, TIMESTAMP_LTZ]"
                                            + ", but got %s.",
                                    versionColumn.get(), columnType));
                }
            }
        }
    }

    private static void checkTieredLog(Configuration tableConf) {
        if (tableConf.get(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS) <= 0) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' must be greater than 0.",
                            ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS.key()));
        }
    }

    private static void checkPartition(
            Configuration tableConf, List<String> partitionKeys, RowType rowType) {
        boolean isPartitioned = !partitionKeys.isEmpty();
        AutoPartitionStrategy autoPartition = AutoPartitionStrategy.from(tableConf);

        if (!isPartitioned && autoPartition.isAutoPartitionEnabled()) {
            throw new InvalidConfigException(
                    String.format(
                            "Currently, auto partition is only supported for partitioned table, please set table property '%s' to false.",
                            ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key()));
        }

        if (isPartitioned) {
            // TODO Currently, we only support one partition key, multi-partition keys will be
            // supported in next pr.
            if (partitionKeys.size() > 1) {
                throw new InvalidTableException(
                        String.format(
                                "Currently, partitioned table only supports one partition key, but got partition keys %s.",
                                partitionKeys));
            }

            for (String partitionKey : partitionKeys) {
                int partitionIndex = rowType.getFieldIndex(partitionKey);
                DataType partitionDataType = rowType.getTypeAt(partitionIndex);
                if (!PARTITION_KEY_SUPPORTED_TYPES.contains(partitionDataType.getTypeRoot())) {
                    throw new InvalidTableException(
                            String.format(
                                    "Currently, partitioned table supported partition key type are %s, "
                                            + "but got partition key '%s' with data type %s.",
                                    PARTITION_KEY_SUPPORTED_TYPES,
                                    partitionKey,
                                    partitionDataType));
                }
            }

            if (autoPartition.isAutoPartitionEnabled()) {
                if (autoPartition.timeUnit() == null) {
                    throw new InvalidTableException(
                            String.format(
                                    "Currently, auto partitioned table must set auto partition time unit when auto "
                                            + "partition is enabled, please set table property '%s'.",
                                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key()));
                }
            }
        }
    }

    private static void validateOptionValue(ReadableConfig options, ConfigOption<?> option) {
        try {
            options.get(option);
        } catch (Throwable t) {
            throw new InvalidConfigException(
                    String.format(
                            "Invalid value for config '%s'. Reason: %s",
                            option.key(), t.getMessage()));
        }
    }
}
