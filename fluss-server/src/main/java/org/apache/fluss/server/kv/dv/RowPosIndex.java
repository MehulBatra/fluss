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

package org.apache.fluss.server.kv.dv;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Operations wrapper for the RowPosIndex Column Family.
 *
 * <p>The RowPosIndex maps RowId (8-byte BigEndian long) to FilePos (varint-encoded file_id +
 * row_position). This index enables O(1) lookup of a row's physical location given its logical
 * RowId.
 */
public class RowPosIndex {

    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;
    private final WriteOptions writeOptions;

    RowPosIndex(RocksDB db, ColumnFamilyHandle cfHandle, WriteOptions writeOptions) {
        this.db = db;
        this.cfHandle = cfHandle;
        this.writeOptions = writeOptions;
    }

    /** Point-get: looks up the FilePos for the given rowId. Returns null if not found. */
    @Nullable
    public FilePos get(long rowId) throws IOException {
        try {
            byte[] value = db.get(cfHandle, encodeRowId(rowId));
            return value == null ? null : FilePos.decode(value);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get RowPosIndex entry for rowId " + rowId, e);
        }
    }

    /** Writes a rowId to filePos mapping. */
    public void put(long rowId, FilePos filePos) throws IOException {
        try {
            db.put(cfHandle, writeOptions, encodeRowId(rowId), filePos.encode());
        } catch (RocksDBException e) {
            throw new IOException("Failed to put RowPosIndex entry for rowId " + rowId, e);
        }
    }

    /** Deletes the mapping for the given rowId. */
    public void delete(long rowId) throws IOException {
        try {
            db.delete(cfHandle, writeOptions, encodeRowId(rowId));
        } catch (RocksDBException e) {
            throw new IOException("Failed to delete RowPosIndex entry for rowId " + rowId, e);
        }
    }

    /** Adds a put operation to the given WriteBatch. */
    public void put(WriteBatch batch, long rowId, FilePos filePos) throws IOException {
        try {
            batch.put(cfHandle, encodeRowId(rowId), filePos.encode());
        } catch (RocksDBException e) {
            throw new IOException(
                    "Failed to add RowPosIndex put to WriteBatch for rowId " + rowId, e);
        }
    }

    /** Adds a delete operation to the given WriteBatch. */
    public void delete(WriteBatch batch, long rowId) throws IOException {
        try {
            batch.delete(cfHandle, encodeRowId(rowId));
        } catch (RocksDBException e) {
            throw new IOException(
                    "Failed to add RowPosIndex delete to WriteBatch for rowId " + rowId, e);
        }
    }

    /**
     * Deletes every RowId-&gt;FilePos entry whose FilePos points to one of the given fileIds.
     * Called when files are removed (compaction/cleanup) so stale mappings can't later resolve a
     * PendingDelete onto a position that a surviving/live row now occupies.
     */
    public void pruneFiles(Set<Integer> fileIds) throws IOException {
        if (fileIds.isEmpty()) {
            return;
        }
        List<byte[]> keysToDelete = new ArrayList<>();
        try (RocksIterator it = db.newIterator(cfHandle)) {
            it.seekToFirst();
            while (it.isValid()) {
                if (fileIds.contains(FilePos.decode(it.value()).fileId())) {
                    keysToDelete.add(it.key());
                }
                it.next();
            }
        }
        try {
            for (byte[] key : keysToDelete) {
                db.delete(cfHandle, writeOptions, key);
            }
        } catch (RocksDBException e) {
            throw new IOException("Failed to prune RowPosIndex entries for removed files", e);
        }
    }

    /** Ingests external SST files into the RowPosIndex column family. */
    public void ingestExternalFile(List<String> sstPaths) throws IOException {
        try (IngestExternalFileOptions options = new IngestExternalFileOptions()) {
            options.setMoveFiles(true);
            db.ingestExternalFile(cfHandle, sstPaths, options);
        } catch (RocksDBException e) {
            throw new IOException("Failed to ingest SST files into RowPosIndex", e);
        }
    }

    /** Encodes a rowId as an 8-byte BigEndian key. */
    static byte[] encodeRowId(long rowId) {
        byte[] key = new byte[8];
        key[0] = (byte) (rowId >>> 56);
        key[1] = (byte) (rowId >>> 48);
        key[2] = (byte) (rowId >>> 40);
        key[3] = (byte) (rowId >>> 32);
        key[4] = (byte) (rowId >>> 24);
        key[5] = (byte) (rowId >>> 16);
        key[6] = (byte) (rowId >>> 8);
        key[7] = (byte) rowId;
        return key;
    }

    /** Decodes a rowId from an 8-byte BigEndian key. */
    static long decodeRowId(byte[] key) {
        return ((long) (key[0] & 0xFF) << 56)
                | ((long) (key[1] & 0xFF) << 48)
                | ((long) (key[2] & 0xFF) << 40)
                | ((long) (key[3] & 0xFF) << 32)
                | ((long) (key[4] & 0xFF) << 24)
                | ((long) (key[5] & 0xFF) << 16)
                | ((long) (key[6] & 0xFF) << 8)
                | ((long) (key[7] & 0xFF));
    }
}
