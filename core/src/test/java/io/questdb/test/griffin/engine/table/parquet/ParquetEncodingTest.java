/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.table.parquet.ParquetEncoding;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.griffin.engine.table.parquet.ParquetEncoding.*;

public class ParquetEncodingTest {

    private static final int[] ALL_ENCODINGS = {
            ENCODING_DEFAULT,
            ENCODING_PLAIN,
            ENCODING_RLE_DICTIONARY,
            ENCODING_DELTA_LENGTH_BYTE_ARRAY,
            ENCODING_DELTA_BINARY_PACKED,
            ENCODING_BYTE_STREAM_SPLIT,
    };

    private static final int[] COLUMN_TYPES = {
            ColumnType.BOOLEAN,
            ColumnType.BYTE,
            ColumnType.SHORT,
            ColumnType.CHAR,
            ColumnType.INT,
            ColumnType.LONG,
            ColumnType.DATE,
            ColumnType.TIMESTAMP,
            ColumnType.FLOAT,
            ColumnType.DOUBLE,
            ColumnType.STRING,
            ColumnType.SYMBOL,
            ColumnType.BINARY,
            ColumnType.VARCHAR,
            ColumnType.LONG128,
            ColumnType.UUID,
            ColumnType.LONG256,
            ColumnType.IPv4,
            ColumnType.GEOBYTE,
            ColumnType.GEOSHORT,
            ColumnType.GEOINT,
            ColumnType.GEOLONG,
    };

    @Test
    public void testDefaultValidForAllTypes() {
        for (int colType : COLUMN_TYPES) {
            Assert.assertTrue(
                    "DEFAULT should be valid for " + ColumnType.nameOf(colType),
                    ParquetEncoding.isValidForColumnType(ENCODING_DEFAULT, colType)
            );
        }
    }

    @Test
    public void testPlainValidForAllExceptSymbolAndVarchar() {
        for (int colType : COLUMN_TYPES) {
            int tag = ColumnType.tagOf(colType);
            boolean expected = tag != ColumnType.SYMBOL && tag != ColumnType.VARCHAR;
            Assert.assertEquals(
                    "PLAIN for " + ColumnType.nameOf(colType),
                    expected,
                    ParquetEncoding.isValidForColumnType(ENCODING_PLAIN, colType)
            );
        }
    }

    @Test
    public void testRleDictionaryOnlyForSymbolAndVarchar() {
        for (int colType : COLUMN_TYPES) {
            int tag = ColumnType.tagOf(colType);
            boolean expected = tag == ColumnType.SYMBOL || tag == ColumnType.VARCHAR;
            Assert.assertEquals(
                    "RLE_DICTIONARY for " + ColumnType.nameOf(colType),
                    expected,
                    ParquetEncoding.isValidForColumnType(ENCODING_RLE_DICTIONARY, colType)
            );
        }
    }

    @Test
    public void testDeltaLengthByteArrayOnlyForStringBinaryVarchar() {
        for (int colType : COLUMN_TYPES) {
            int tag = ColumnType.tagOf(colType);
            boolean expected = tag == ColumnType.STRING
                    || tag == ColumnType.BINARY
                    || tag == ColumnType.VARCHAR;
            Assert.assertEquals(
                    "DELTA_LENGTH_BYTE_ARRAY for " + ColumnType.nameOf(colType),
                    expected,
                    ParquetEncoding.isValidForColumnType(ENCODING_DELTA_LENGTH_BYTE_ARRAY, colType)
            );
        }
    }

    @Test
    public void testDeltaBinaryPackedForIntegerAndGeoTypes() {
        for (int colType : COLUMN_TYPES) {
            int tag = ColumnType.tagOf(colType);
            boolean expected = tag == ColumnType.BYTE
                    || tag == ColumnType.SHORT
                    || tag == ColumnType.CHAR
                    || tag == ColumnType.INT
                    || tag == ColumnType.LONG
                    || tag == ColumnType.DATE
                    || tag == ColumnType.TIMESTAMP
                    || tag == ColumnType.IPv4
                    || tag == ColumnType.GEOBYTE
                    || tag == ColumnType.GEOSHORT
                    || tag == ColumnType.GEOINT
                    || tag == ColumnType.GEOLONG;
            Assert.assertEquals(
                    "DELTA_BINARY_PACKED for " + ColumnType.nameOf(colType),
                    expected,
                    ParquetEncoding.isValidForColumnType(ENCODING_DELTA_BINARY_PACKED, colType)
            );
        }
    }

    @Test
    public void testByteStreamSplitRejectedForAllTypes() {
        for (int colType : COLUMN_TYPES) {
            Assert.assertFalse(
                    "BYTE_STREAM_SPLIT should be rejected for " + ColumnType.nameOf(colType),
                    ParquetEncoding.isValidForColumnType(ENCODING_BYTE_STREAM_SPLIT, colType)
            );
        }
    }

    @Test
    public void testExhaustiveMatrix() {
        for (int encoding : ALL_ENCODINGS) {
            for (int colType : COLUMN_TYPES) {
                boolean actual = ParquetEncoding.isValidForColumnType(encoding, colType);
                boolean expected = expectedValidity(encoding, colType);
                Assert.assertEquals(
                        encodingName(encoding) + " for " + ColumnType.nameOf(colType),
                        expected,
                        actual
                );
            }
        }
    }

    private static boolean expectedValidity(int encoding, int columnType) {
        int tag = ColumnType.tagOf(columnType);
        return switch (encoding) {
            case ENCODING_DEFAULT -> true;
            case ENCODING_PLAIN -> tag != ColumnType.SYMBOL && tag != ColumnType.VARCHAR;
            case ENCODING_RLE_DICTIONARY -> tag == ColumnType.SYMBOL || tag == ColumnType.VARCHAR;
            case ENCODING_DELTA_LENGTH_BYTE_ARRAY -> tag == ColumnType.STRING
                    || tag == ColumnType.BINARY
                    || tag == ColumnType.VARCHAR;
            case ENCODING_DELTA_BINARY_PACKED -> tag == ColumnType.BYTE
                    || tag == ColumnType.SHORT
                    || tag == ColumnType.CHAR
                    || tag == ColumnType.INT
                    || tag == ColumnType.LONG
                    || tag == ColumnType.DATE
                    || tag == ColumnType.TIMESTAMP
                    || tag == ColumnType.IPv4
                    || tag == ColumnType.GEOBYTE
                    || tag == ColumnType.GEOSHORT
                    || tag == ColumnType.GEOINT
                    || tag == ColumnType.GEOLONG;
            default -> false;
        };
    }

    private static String encodingName(int encoding) {
        CharSequence name = ParquetEncoding.getEncodingName(encoding);
        return name != null ? name.toString() : "DEFAULT";
    }
}
