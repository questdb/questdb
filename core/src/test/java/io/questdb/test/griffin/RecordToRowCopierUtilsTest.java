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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class RecordToRowCopierUtilsTest extends AbstractCairoTest {
    @Test
    public void testCopierByteToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.BYTE, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.BYTE, 123), getLongAsserter(123000));
    }

    @Test(expected = ImplicitCastException.class)
    public void testCopierByteToDecimalOverflow() {
        RecordToRowCopier copier = generateCopier(ColumnType.BYTE, ColumnType.getDecimalType(4, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.BYTE, 99), new RowAsserter());
    }

    @Test
    public void testCopierDecimal8ToDecimal32() {
        // Converting 1.2 from a Decimal8 with a scale of 1 (1.2) to a Decimal32 with a scale of 3 (1.200)
        int fromType = ColumnType.getDecimalType(2, 1);
        Assert.assertEquals("Unexpected from type", ColumnType.DECIMAL8, ColumnType.tagOf(fromType));
        int toType = ColumnType.getDecimalType(8, 3);
        Assert.assertEquals("Unexpected to type", ColumnType.DECIMAL32, ColumnType.tagOf(toType));

        RecordToRowCopier copier = generateCopier(fromType, toType);

        Record rec = new Record() {
            @Override
            public byte getDecimal8(int col) {
                return 12;
            }
        };

        copier.copy(sqlExecutionContext, rec, getIntAsserter(1200));
    }

    @Test
    public void testCopierDecimalCases() {
        Decimal256 d = new Decimal256();
        Decimal256 c = new Decimal256();
        Decimal256 dnull = new Decimal256();
        BytecodeAssembler asm = new BytecodeAssembler();
        dnull.ofNull();
        boolean[] isNull = new boolean[]{false, true};
        int[] fromPrecisions = new int[]{1, 2, 3, 4, 5, 8, 15, 25, 30, 50, 75};
        int[] toPrecisions = new int[]{1, 2, 3, 4, 5, 8, 15, 25, 30, 50, 75};
        for (int fromPrecision : fromPrecisions) {
            for (int toPrecision : toPrecisions) {
                for (boolean nulled : isNull) {
                    if (nulled) {
                        testDecimalCast(asm, fromPrecision, toPrecision, 0, dnull, true);
                        continue;
                    }

                    for (int fromScale = 1; fromScale <= fromPrecision; fromScale <<= 1) {
                        for (int toScale = 1; toScale <= toPrecision; toScale <<= 1) {
                            boolean fit;
                            generateValue(d, fromPrecision, fromScale);
                            try {
                                c.copyFrom(d);
                                c.rescale(toScale);
                                fit = c.comparePrecision(toPrecision);
                            } catch (NumericException ignored) {
                                fit = false;
                            }
                            testDecimalCast(asm, fromPrecision, toPrecision, toScale, d, fit);
                            d.negate();
                            testDecimalCast(asm, fromPrecision, toPrecision, toScale, d, fit);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testCopierIntToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.INT, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.INT, 123456), getLongAsserter(123456000));
    }

    @Test(expected = ImplicitCastException.class)
    public void testCopierIntToDecimalOverflow() {
        RecordToRowCopier copier = generateCopier(ColumnType.INT, ColumnType.getDecimalType(8, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.INT, 999999), new RowAsserter());
    }

    @Test
    public void testCopierLongToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.LONG, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.LONG, 123456L), getLongAsserter(123456000L));
    }

    @Test(expected = ImplicitCastException.class)
    public void testCopierLongToDecimalOverflow() {
        RecordToRowCopier copier = generateCopier(ColumnType.LONG, ColumnType.getDecimalType(8, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.LONG, 999999L), new RowAsserter());
    }

    @Test
    public void testCopierNullIntToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.INT, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.INT, Numbers.INT_NULL), getLongAsserter(Decimals.DECIMAL64_NULL));
    }

    @Test
    public void testCopierNullLongToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.LONG, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.LONG, Numbers.LONG_NULL), getLongAsserter(Decimals.DECIMAL64_NULL));
    }

    @Test
    public void testCopierShortToDecimal() {
        RecordToRowCopier copier = generateCopier(ColumnType.SHORT, ColumnType.getDecimalType(16, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.SHORT, 1234), getLongAsserter(1234000));
    }

    @Test(expected = ImplicitCastException.class)
    public void testCopierShortToDecimalOverflow() {
        RecordToRowCopier copier = generateCopier(ColumnType.SHORT, ColumnType.getDecimalType(5, 3));
        copier.copy(sqlExecutionContext, getLongRecord(ColumnType.SHORT, 9999), new RowAsserter());
    }

    private static @NotNull Record getLongRecord(int fromType, long value) {
        return new Record() {
            @Override
            public byte getByte(int col) {
                Assert.assertEquals(ColumnType.BYTE, fromType);
                return (byte) value;
            }

            @Override
            public int getInt(int col) {
                Assert.assertEquals(ColumnType.INT, fromType);
                return (int) value;
            }

            @Override
            public long getLong(int col) {
                Assert.assertEquals(ColumnType.LONG, fromType);
                return value;
            }

            @Override
            public short getShort(int col) {
                Assert.assertEquals(ColumnType.SHORT, fromType);
                return (short) value;
            }
        };
    }

    private RecordToRowCopier generateCopier(int fromType, int toType) {
        ArrayColumnTypes from = new ArrayColumnTypes();
        from.add(fromType);
        GenericRecordMetadata to = new GenericRecordMetadata();
        TableColumnMetadata toCol = new TableColumnMetadata("x", toType);
        to.add(toCol);
        ListColumnFilter mapping = new ListColumnFilter();
        mapping.add(1);
        return RecordToRowCopierUtils.generateCopier(new BytecodeAssembler(), from, to, mapping);
    }

    private void generateValue(Decimal256 d, int precision, int scale) {
        String maxValue = "98765432109876543210987654321098765432109876543210987654321098765432109876543210";
        BigDecimal value = new BigDecimal(maxValue.substring(0, precision));
        Decimal256 v = Decimal256.fromBigDecimal(value);
        d.of(v.getHh(), v.getHl(), v.getLh(), v.getLl(), scale);
    }

    private TableWriter.Row getIntAsserter(int expected) {
        return new RowAsserter() {
            @Override
            public void putInt(int col, int value) {
                Assert.assertEquals(expected, value);
            }
        };
    }

    private TableWriter.Row getLongAsserter(long expected) {
        return new RowAsserter() {
            @Override
            public void putLong(int col, long value) {
                Assert.assertEquals(expected, value);
            }
        };
    }

    private void testDecimalCast(BytecodeAssembler asm, int fromPrecision, int toPrecision, int toScale, Decimal256 value, boolean fitInTargetType) {
        int fromType = ColumnType.getDecimalType(fromPrecision, value.getScale());
        int toType = ColumnType.getDecimalType(toPrecision, toScale);
        ArrayColumnTypes from = new ArrayColumnTypes();
        from.add(fromType);
        GenericRecordMetadata to = new GenericRecordMetadata();
        TableColumnMetadata toCol = new TableColumnMetadata("x", toType);
        to.add(toCol);
        ListColumnFilter mapping = new ListColumnFilter();
        mapping.add(1);
        RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(asm, from, to, mapping);

        Decimal256 expectedValue;
        if (fitInTargetType) {
            expectedValue = new Decimal256();
            expectedValue.copyFrom(value);
            expectedValue.rescale(toScale);
        } else {
            expectedValue = null;
        }

        Record rec = DecimalUtilTest.getDecimalRecord(fromType, value);
        TableWriter.Row row = DecimalUtilTest.getRowAsserter(toType, -1, expectedValue);
        try {
            copier.copy(sqlExecutionContext, rec, row);
            if (!fitInTargetType) {
                Assert.fail(String.format("Expected cast to fail from (%s - p:%s - s:%s) to (%s - p:%s - s:%s) for %s",
                        ColumnType.nameOf(ColumnType.tagOf(fromType)), ColumnType.getDecimalPrecision(fromType), ColumnType.getDecimalScale(fromType),
                        ColumnType.nameOf(ColumnType.tagOf(toType)), ColumnType.getDecimalPrecision(toType), ColumnType.getDecimalScale(toType),
                        value));
            }
        } catch (ImplicitCastException e) {
            if (fitInTargetType) {
                throw e;
            }
        } catch (AssertionError e) {
            System.err.printf("Cast failed from (%s - p:%s - s:%s) to (%s - p:%s - s:%s) for '%s'\n",
                    ColumnType.nameOf(ColumnType.tagOf(fromType)), ColumnType.getDecimalPrecision(fromType), ColumnType.getDecimalScale(fromType),
                    ColumnType.nameOf(ColumnType.tagOf(toType)), ColumnType.getDecimalPrecision(toType), ColumnType.getDecimalScale(toType),
                    value);
            throw e;
        }
    }
}
