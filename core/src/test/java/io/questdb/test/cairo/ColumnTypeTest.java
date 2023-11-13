/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Long128;
import io.questdb.std.Long256;
import io.questdb.std.Uuid;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.ColumnType.*;

public class ColumnTypeTest {

    @Test
    public void testIsBuiltInWideningCast() {
        for (byte type = BOOLEAN; type < NULL; type++) {
            Assert.assertTrue(isBuiltInWideningCast(NULL, type));
        }
        Assert.assertTrue(isBuiltInWideningCast(CHAR, SHORT));
        Assert.assertTrue(isBuiltInWideningCast(TIMESTAMP, LONG));
        for (byte fromType = BYTE; fromType < STRING; fromType++) {
            for (byte toType = BYTE; toType < STRING; toType++) {
                if (fromType < toType) {
                    Assert.assertTrue(isBuiltInWideningCast(fromType, toType));
                }
            }
        }
    }

    @Test
    public void testIsVariableLength() {
        for (int type = UNDEFINED; type <= NULL; type++) {
            boolean expectedVariableLen = true;
            switch (type) {
                case STRING:
                    Assert.assertEquals(Integer.BYTES, variableColumnLengthBytes(type));
                    break;
                case BINARY:
                    Assert.assertEquals(Long.BYTES, variableColumnLengthBytes(type));
                    break;
                default:
                    expectedVariableLen = false;
            }
            Assert.assertEquals(expectedVariableLen, isVariableLength(type));
        }
    }

    @Test
    public void testNameOf() {
        for (byte type = UNDEFINED; type <= NULL; type++) {
            Assert.assertEquals(type, tagOf(nameOf(type)));
        }

        StringSink sink = new StringSink();
        for (int b = 1; b <= GEOLONG_MAX_BITS; b++) {
            sink.clear();
            sink.put("GEOHASH(");
            if (b % 5 != 0) {
                sink.put(b).put("b)");
            } else {
                sink.put(b / 5).put("c)");
            }
            Assert.assertEquals(nameOf(getGeoHashTypeWithBits(b)), sink.toString());
        }
    }

    @Test
    public void testOverloadPriorityMatrix() {
        Matrix matrix = new Matrix();
        Assert.assertEquals(
                "             | UNDEFINED | BOOLEAN | BYTE | SHORT | CHAR | INT | LONG | DATE | TIMESTAMP | FLOAT | DOUBLE | STRING | SYMBOL | LONG256 | GEOBYTE | GEOSHORT | GEOINT | GEOLONG | BINARY | UUID | CURSOR | VARARG | RECORD | GEOHASH | LONG128 | IPv4 | regclass | regprocedure | text[] | NULL | unknown | unknown | \n" +
                        "================================================================================================================================================================================================================================================================================================================= | \n" +
                        "UNDEFINED    |           | 10      | 9    | 8     | 7    | 6   | 3    | 5    | 4         | 1     | 0      | 2      |        |         |         |          |        |         |        |      |        |        |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "BOOLEAN      |           | 0       |      |       |      |     |      |      |           |       |        |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "BYTE         |           |         | 0    | 1     |      | 2   | 3    |      |           | 4     | 5      |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "SHORT        |           |         |      | 0     |      | 1   | 2    |      |           | 3     | 4      |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "CHAR         |           |         |      |       | 0    |     |      |      |           |       |        | 1      |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "INT          |           |         |      |       |      | 0   | 1    | 5    | 4         | 2     | 3      |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "LONG         |           |         |      |       |      |     | 0    | 3    | 2         |       | 1      |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "DATE         |           |         |      |       |      |     | 2    | 0    | 1         |       |        |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "TIMESTAMP    |           |         |      |       |      |     | 1    | 2    | 0         |       |        |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "FLOAT        |           |         |      |       |      |     |      |      |           | 0     | 1      |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "DOUBLE       |           |         |      |       |      |     |      |      |           |       | 0      |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "STRING       |           |         | 7    | 6     | 1    | 4   | 3    |      |           | 5     | 2      | 0      |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "SYMBOL       |           |         |      |       |      |     |      |      |           |       |        | 1      | 0      |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "LONG256      |           |         |      |       |      |     |      |      |           |       |        |        |        | 0       |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "GEOBYTE      |           |         |      |       |      |     |      |      |           |       |        |        |        |         | 0       | 1        | 2      | 3       |        |      |        | -1     |        | 4       |         |      |          |              |        |      | 0       | 0       | \n" +
                        "GEOSHORT     |           |         |      |       |      |     |      |      |           |       |        |        |        |         |         | 0        | 1      | 2       |        |      |        | -1     |        | 3       |         |      |          |              |        |      | 0       | 0       | \n" +
                        "GEOINT       |           |         |      |       |      |     |      |      |           |       |        |        |        |         |         |          | 0      | 1       |        |      |        | -1     |        | 2       |         |      |          |              |        |      | 0       | 0       | \n" +
                        "GEOLONG      |           |         |      |       |      |     |      |      |           |       |        |        |        |         |         |          |        | 0       |        |      |        | -1     |        | 1       |         |      |          |              |        |      | 0       | 0       | \n" +
                        "BINARY       |           |         |      |       |      |     |      |      |           |       |        |        |        |         |         |          |        |         | 0      |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "UUID         |           |         |      |       |      |     |      |      |           |       |        | 1      |        |         |         |          |        |         |        | 0    |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "CURSOR       |           |         |      |       |      |     |      |      |           |       |        |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "VARARG       |           |         |      |       |      |     |      |      |           |       |        |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "RECORD       |           |         |      |       |      |     |      |      |           |       |        |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "GEOHASH      |           |         |      |       |      |     |      |      |           |       |        |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "LONG128      |           |         |      |       |      |     |      |      |           |       |        |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "IPv4         |           |         |      |       |      |     |      |      |           |       |        |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "regclass     |           |         |      |       |      |     |      |      |           |       |        |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "regprocedure |           |         |      |       |      |     |      |      |           |       |        |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "text[]       |           |         |      |       |      |     |      |      |           |       |        |        |        |         |         |          |        |         |        |      |        | -1     |        |         |         |      |          |              |        |      | 0       | 0       | \n" +
                        "NULL         | 0         | 0       | 0    | 0     | 0    | 0   | 0    | 0    | 0         | 0     | 0      | -1     | -1     | 0       | 0       | 0        | 0      | 0       | 0      | 0    | 0      | 0      | 0      | 0       | 0       | 0    | 0        | 0            | 0      | 0    | 0       | 0       | \n" +
                        "unknown      | 0         | 0       | 0    | 0     | 0    | 0   | 0    | 0    | 0         | 0     | 0      | 0      | 0      | 0       | 0       | 0        | 0      | 0       | 0      | 0    | 0      | 0      | 0      | 0       | 0       | 0    | 0        | 0            | 0      | 0    | 0       | 0       | \n" +
                        "unknown      | 0         | 0       | 0    | 0     | 0    | 0   | 0    | 0    | 0         | 0     | 0      | 0      | 0      | 0       | 0       | 0        | 0      | 0       | 0      | 0    | 0      | 0      | 0      | 0       | 0       | 0    | 0        | 0            | 0      | 0    | 0       | 0       | \n" +
                        "================================================================================================================================================================================================================================================================================================================= | \n",
                matrix.toString()
        );
    }

    @Test
    public void testSizeOf() {
        for (int type = UNDEFINED; type <= NULL; type++) {
            int size = sizeOf(type);
            switch (type) {
                case CURSOR:
                case VAR_ARG:
                case RECORD:
                case UNDEFINED:
                    Assert.assertEquals(NOT_STORED_TYPE_SIZE, size);
                    break;
                case BOOLEAN:
                case BYTE:
                case GEOBYTE:
                    Assert.assertEquals(Byte.BYTES, size);
                    break;
                case SHORT:
                case GEOSHORT:
                    Assert.assertEquals(Short.BYTES, size);
                    break;
                case CHAR:
                    Assert.assertEquals(Character.BYTES, size);
                    break;
                case INT:
                case SYMBOL:
                case GEOINT:
                case IPv4:
                    Assert.assertEquals(Integer.BYTES, size);
                    break;
                case LONG:
                case DATE:
                case TIMESTAMP:
                case GEOLONG:
                    Assert.assertEquals(Long.BYTES, size);
                    break;
                case FLOAT:
                    Assert.assertEquals(Float.BYTES, size);
                    break;
                case DOUBLE:
                    Assert.assertEquals(Double.BYTES, size);
                    break;
                case STRING:
                case BINARY:
                case NULL:
                    Assert.assertEquals(DYNAMIC_TYPE_SIZE, size);
                    break;
                case LONG256:
                    Assert.assertEquals(Long256.BYTES, size);
                    break;
                case UUID:
                    Assert.assertEquals(Uuid.BYTES, size);
                case LONG128:
                    Assert.assertEquals(Long128.BYTES, size);
                    break;
            }
        }
    }

    private static class Matrix {
        private static final IntObjHashMap<String> CACHE = new IntObjHashMap<>();
        private static final String CELL_SEP = " | ";
        private static final int CELL_WIDTH;
        private final static int[] TYPE_WIDTH = new int[OVERLOAD_PRIORITY_N];
        private final StringSink sink = new StringSink();
        private final StringSink sink2 = new StringSink();

        private Matrix() {
            // heather (column name is toTag)
            appendCell("", CELL_WIDTH);
            for (short i = 0; i < OVERLOAD_PRIORITY_N; i++) {
                appendCell(ColumnType.nameOf(i), 0);
            }
            sink.put('\n');
            int lineLen = sink.length() - CELL_SEP.length() - 1;
            appendLine(lineLen);

            // body
            for (byte fromTag = 0; fromTag < OVERLOAD_PRIORITY_N; fromTag++) {
                appendCell(ColumnType.nameOf(fromTag), CELL_WIDTH); // first cell is fromTag
                for (byte toTag = 0; toTag < OVERLOAD_PRIORITY_N; toTag++) {
                    appendWeight(
                            OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * fromTag + toTag],
                            TYPE_WIDTH[toTag]
                    );
                }
                sink.put('\n');
            }
            appendLine(lineLen);
        }

        @Override
        public String toString() {
            return sink.toString();
        }

        private void appendCell(String str, int width) {
            appendCell(str, width, ' ');
        }

        private void appendCell(String str, int width, char fill) {
            int gap = width - str.length();
            String gapFill = CACHE.get(gap);
            if (gapFill == null) {
                sink2.clear();
                for (int i = 0; i < gap; i++) {
                    sink2.put(fill);
                }
                CACHE.put(gap, gapFill = sink2.toString());
            }
            sink.put(str).put(gapFill).put(CELL_SEP);
        }

        private void appendLine(int lineLen) {
            appendCell("", lineLen, '=');
            sink.put('\n');
        }

        private void appendWeight(int weight, int gap) {
            appendCell(weight != ColumnType.OVERLOAD_NONE ? Integer.toString(weight) : "", gap);
        }

        static {
            int maxLen = Integer.MIN_VALUE;
            for (short i = 0; i < OVERLOAD_PRIORITY_N; i++) {
                String name = ColumnType.nameOf(i);
                int len = name.length();
                TYPE_WIDTH[i] = len;
                maxLen = Math.max(maxLen, len);
            }
            CELL_WIDTH = maxLen;
        }
    }
}
