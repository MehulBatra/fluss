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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.apache.fluss.record.ChangeType.INSERT;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the DV-aware behavior (__rowid handling) of {@link FlussRecordAsIcebergRecord}. */
class FlussRecordAsIcebergRecordDvTest {

    // 1 business column + __bucket + __offset + __timestamp + __rowid.
    private static final Types.StructType DV_STRUCT =
            Types.StructType.of(
                    Types.NestedField.required(0, "id", Types.IntegerType.get()),
                    Types.NestedField.required(1, "__bucket", Types.IntegerType.get()),
                    Types.NestedField.required(2, "__offset", Types.LongType.get()),
                    Types.NestedField.required(3, "__timestamp", Types.TimestampType.withZone()),
                    Types.NestedField.required(4, "__rowid", Types.LongType.get()));

    private static final RowType FLUSS_ROW_TYPE = RowType.of(DataTypes.INT());

    @Test
    void rowidReturnsLogOffsetInDvMode() {
        FlussRecordAsIcebergRecord record =
                new FlussRecordAsIcebergRecord(3, DV_STRUCT, FLUSS_ROW_TYPE, true);
        long logOffset = 42L;
        GenericRow row = new GenericRow(1);
        row.setField(0, 7);
        LogRecord logRecord = new GenericRecord(logOffset, System.currentTimeMillis(), INSERT, row);
        record.setFlussRecord(logRecord);

        // positions: 0=id, 1=__bucket, 2=__offset, 3=__timestamp, 4=__rowid
        assertThat(record.get(1)).isEqualTo(3);
        assertThat(record.get(2)).isEqualTo(logOffset);
        assertThat(record.get(4)).isEqualTo(logOffset);
        assertThat(record.getField("__rowid")).isEqualTo(logOffset);
    }
}
