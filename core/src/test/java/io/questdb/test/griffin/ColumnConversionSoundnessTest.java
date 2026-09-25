/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * ALTER TABLE ... ALTER COLUMN ... TYPE admits a conversion by the rows of
 * {@code SqlCompilerImpl.columnConversionRow} and then runs it through
 * {@code ColumnTypeConverter}, whose own rows (the text parsers, the text renderers, the native
 * fixed-to-fixed kernel, the decimal converter) decide what it can actually do. The two must
 * agree: every cell the compiler admits must have a converter path, or the statement is
 * accepted and then fails with "unsupported conversion" half-way through the table.
 * <p>
 * The test converts a two-row table (one value, one NULL) between every admitted pair of
 * column types, both timestamp precisions and one decimal per storage width included, and
 * lists every pair that fails. It does not check the converted values: the per-type tests in
 * {@code AlterTableChangeColumnTypeTest} do that. A cell whose value is not convertible
 * (a number as an IPv4) converts to NULL, which is a path, not a failure.
 */
public class ColumnConversionSoundnessTest extends AbstractCairoTest {

    @Test
    public void testEveryAdmittedConversionHasAConverter() throws Exception {
        final boolean[][] support = columnConversionSupport();
        assertMemoryLeak(() -> {
            final StringSink failures = new StringSink();
            int cells = 0;
            for (ColumnTypeTag from : ColumnTypeTag.values()) {
                for (ColumnTypeTag to : ColumnTypeTag.values()) {
                    if (from.code() < 0 || to.code() < 0 || !support[from.code()][to.code()]) {
                        continue;
                    }
                    for (String fromDdl : ddlTypesOf(from)) {
                        for (String toDdl : ddlTypesOf(to)) {
                            if (fromDdl.equals(toDdl)) {
                                continue; // "cannot convert to the same type"
                            }
                            cells++;
                            convert(fromDdl, toDdl, failures);
                        }
                    }
                }
            }
            Assert.assertTrue("no admitted cells found", cells > 0);
            Assert.assertEquals("admitted conversions with no converter path:\n" + failures, 0, failures.length());
        });
    }

    private static boolean[][] columnConversionSupport() throws Exception {
        final Field field = SqlCompilerImpl.class.getDeclaredField("columnConversionSupport");
        field.setAccessible(true);
        return (boolean[][]) field.get(null);
    }

    private static void convert(String fromDdl, String toDdl, StringSink failures) throws Exception {
        execute("CREATE TABLE t (x " + fromDdl + ")");
        try {
            execute("INSERT INTO t VALUES (" + sampleValueOf(fromDdl) + "), (NULL)");
            execute("ALTER TABLE t ALTER COLUMN x TYPE " + toDdl);
            try (TableReader reader = getReader("t")) {
                final int columnType = reader.getMetadata().getColumnType(0);
                if (!ColumnType.nameOf(columnType).equalsIgnoreCase(toDdl)) {
                    failures.put(fromDdl).put(" -> ").put(toDdl).put(": column type is ").put(ColumnType.nameOf(columnType)).put('\n');
                }
            }
            // read the converted column back: a converter that wrote garbage sizes fails here
            printSql("SELECT x FROM t");
            if (sink.length() == 0) {
                failures.put(fromDdl).put(" -> ").put(toDdl).put(": no rows read back\n");
            }
        } catch (Throwable e) {
            failures.put(fromDdl).put(" -> ").put(toDdl).put(": ").put(e.getMessage()).put('\n');
        } finally {
            execute("DROP TABLE t");
        }
    }

    /**
     * The column types a tag is declared as; a tag with more than one encoding lists one type
     * per encoding, so that the converter sees every variant on both sides.
     */
    private static String[] ddlTypesOf(ColumnTypeTag tag) {
        return switch (tag) {
            case BOOLEAN -> new String[]{"BOOLEAN"};
            case BYTE -> new String[]{"BYTE"};
            case SHORT -> new String[]{"SHORT"};
            case CHAR -> new String[]{"CHAR"};
            case INT -> new String[]{"INT"};
            case LONG -> new String[]{"LONG"};
            case DATE -> new String[]{"DATE"};
            case TIMESTAMP -> new String[]{"TIMESTAMP", "TIMESTAMP_NS"};
            case FLOAT -> new String[]{"FLOAT"};
            case DOUBLE -> new String[]{"DOUBLE"};
            case STRING -> new String[]{"STRING"};
            case SYMBOL -> new String[]{"SYMBOL"};
            case UUID -> new String[]{"UUID"};
            case IPv4 -> new String[]{"IPv4"};
            case VARCHAR -> new String[]{"VARCHAR"};
            case DECIMAL8 -> new String[]{"DECIMAL(2,1)"};
            case DECIMAL16 -> new String[]{"DECIMAL(4,1)"};
            case DECIMAL32 -> new String[]{"DECIMAL(9,1)"};
            case DECIMAL64 -> new String[]{"DECIMAL(18,1)"};
            case DECIMAL128 -> new String[]{"DECIMAL(38,1)"};
            case DECIMAL256 -> new String[]{"DECIMAL(76,1)"};
            case LONG256 -> new String[]{"LONG256"};
            case BINARY -> new String[]{"BINARY"};
            case LONG128 -> new String[]{"LONG128"};
            case GEOBYTE -> new String[]{"GEOHASH(1c)"};
            case GEOSHORT -> new String[]{"GEOHASH(2c)"};
            case GEOINT -> new String[]{"GEOHASH(6c)"};
            case GEOLONG -> new String[]{"GEOHASH(12c)"};
            case INTERVAL -> new String[]{"INTERVAL"};
            case ARRAY -> new String[]{"DOUBLE[]"};
            case UNDEFINED, CURSOR, VAR_ARG, RECORD, GEOHASH, DECIMAL, REGCLASS, REGPROCEDURE, ARRAY_STRING, PARAMETER,
                 VARCHAR_SLICE, NULL, UNKNOWN -> throw new AssertionError("not a column type: " + tag);
        };
    }

    private static String sampleValueOf(String ddl) {
        return switch (ddl) {
            case "BOOLEAN" -> "true";
            case "CHAR" -> "'1'";
            case "STRING", "SYMBOL", "VARCHAR" -> "'1'";
            case "UUID" -> "'11111111-1111-1111-1111-111111111111'";
            case "IPv4" -> "'1.1.1.1'";
            case "LONG256" -> "'0x01'";
            case "GEOHASH(1c)", "GEOHASH(2c)", "GEOHASH(6c)", "GEOHASH(12c)" -> "'sp052w92p1p8'";
            case "DOUBLE[]" -> "ARRAY[1.0]";
            case "BINARY", "LONG128", "INTERVAL" -> "NULL";
            default -> "1"; // numbers, dates, timestamps, decimals
        };
    }
}
