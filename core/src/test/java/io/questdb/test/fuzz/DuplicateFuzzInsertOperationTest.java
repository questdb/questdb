/*+*****************************************************************************
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

package io.questdb.test.fuzz;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Rnd;
import io.questdb.std.Uuid;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * DedupInsertFuzzTest relies on the copies that {@link DuplicateFuzzInsertOperation} writes to be duplicates: a copy
 * repeats the dedup key values of the original row, and a copy that is not identical differs from the original in
 * one column outside the keys, or in all the columns outside the keys that the row writes.
 */
public class DuplicateFuzzInsertOperationTest extends AbstractCairoTest {
    // a column of each type that FuzzInsertOperation writes, and a dropped one
    private static final String COLUMNS = """
            (
                ts TIMESTAMP, c_bool BOOLEAN, c_byte BYTE, c_short SHORT, c_char CHAR, c_int INT, c_long LONG,
                c_date DATE, c_ts TIMESTAMP, c_ts_ns TIMESTAMP_NS, c_float FLOAT, c_double DOUBLE, c_sym SYMBOL,
                c_str STRING, c_varchar VARCHAR, c_bin BINARY, c_long128 LONG128, c_long256 LONG256, c_uuid UUID,
                c_ipv4 IPV4, c_geo_byte GEOHASH(5b), c_geo_short GEOHASH(10b), c_geo_int GEOHASH(25b),
                c_geo_long GEOHASH(35b), c_array DOUBLE[][], c_dropped INT
            ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL""";
    private static final int ROW_COUNT = 2_000;
    private int allChangedRowCount;
    private int oneChangedRowCount;

    @Test
    public void testArrayOnlyColumnOutsideKeys() throws Exception {
        // An ARRAY column cannot be a dedup key. With every other column in the keys, every copy changes the array.
        final Setup setup = new Setup();
        setup.isArrayOnlyColumnOutsideKeys = true;
        setup.notSet = 0;
        assertCopies(setup, (row, original, copy, metadata, changedColumns, otherColumnCount) -> {
            Assert.assertEquals("row " + row, 1, otherColumnCount);
            Assert.assertEquals("row " + row, "[c_array]", columnNames(metadata, changedColumns));
        });
    }

    @Test
    public void testCancelledRows() throws Exception {
        // A copy of a row that FuzzInsertOperation cancels is cancelled too. A row cancel costs more than an
        // append, hence fewer rows.
        final Setup setup = new Setup();
        setup.cancelRows = 1;
        setup.rowCount = 20;
        setup.expectedRowCount = 0;
        assertCopies(setup, (row, original, copy, metadata, changedColumns, otherColumnCount) -> Assert.fail("row " + row));
    }

    @Test
    public void testChangedCopy() throws Exception {
        final Setup setup = new Setup();
        setup.notSet = 0;
        assertOneOrAllColumnsChange(setup);
    }

    @Test
    public void testChangedCopyEmptyVarSizeValues() throws Exception {
        // With strLen = 0, FuzzInsertOperation writes empty STRING, VARCHAR and BINARY values
        final Setup setup = new Setup();
        setup.notSet = 0;
        setup.strLen = 0;
        assertOneOrAllColumnsChange(setup);
    }

    @Test
    public void testChangedCopyNoNullValues() throws Exception {
        // With nullSet = 0, a copy does not change a value to NULL either
        final Setup setup = new Setup();
        setup.notSet = 0;
        setup.nullSet = 0;
        assertOneOrAllColumnsChange(setup, (row, original, copy, metadata, changedColumns, otherColumnCount) -> {
            for (int i = 0, n = changedColumns.size(); i < n; i++) {
                final int columnIndex = changedColumns.getQuick(i);
                final int columnType = metadata.getColumnType(columnIndex);
                if (!isNullValue(original, columnIndex, columnType)) {
                    Assert.assertFalse(
                            "row " + row + ", column " + metadata.getColumnName(columnIndex),
                            isNullValue(copy, columnIndex, columnType)
                    );
                }
            }
        });
    }

    @Test
    public void testChangedCopyNullValues() throws Exception {
        // With nullSet = 1, FuzzInsertOperation writes NULL to every column type that has a NULL
        final Setup setup = new Setup();
        setup.notSet = 0;
        setup.nullSet = 1;
        assertOneOrAllColumnsChange(setup);
    }

    @Test
    public void testChangedCopyOneSymbol() throws Exception {
        // The only change of the only symbol is NULL, and of NULL the symbol
        final Setup setup = new Setup();
        setup.notSet = 0;
        setup.symbols = new String[]{"s"};
        assertOneOrAllColumnsChange(setup, (row, original, copy, metadata, changedColumns, otherColumnCount) -> {
            final int columnIndex = metadata.getColumnIndex("c_sym");
            if (changedColumns.contains(columnIndex)) {
                final CharSequence originalValue = original.getSymA(columnIndex);
                final CharSequence copyValue = copy.getSymA(columnIndex);
                if (originalValue == null) {
                    TestUtils.assertEquals("row " + row, "s", copyValue);
                } else {
                    Assert.assertNull("row " + row, copyValue);
                }
            }
        });
    }

    @Test
    public void testChangedCopyUnsetColumns() throws Exception {
        // The copy leaves the columns that the original row leaves unset unset, and the dedup keys stay the same
        final Setup setup = new Setup();
        assertCopies(setup, (row, original, copy, metadata, changedColumns, otherColumnCount) -> {
            Assert.assertTrue("row " + row, changedColumns.size() <= otherColumnCount);
            if (changedColumns.size() == 0) {
                // only a row that writes no column outside the keys leaves them all as they are
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    if (!setup.dedupKeyNames.contains(metadata.getColumnName(i))) {
                        Assert.assertTrue(
                                "row " + row + ", column " + metadata.getColumnName(i),
                                isUnsetValue(original, i, metadata.getColumnType(i))
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testCopyWithoutSymbols() throws Exception {
        // Without symbols, FuzzInsertOperation writes NULL symbols, which a copy cannot change: it changes
        // another column
        final Setup setup = new Setup();
        setup.notSet = 0;
        setup.symbols = new String[0];
        assertCopies(setup, (row, original, copy, metadata, changedColumns, otherColumnCount) -> {
            final int symbolColumnIndex = metadata.getColumnIndex("c_sym");
            Assert.assertFalse("row " + row, changedColumns.contains(symbolColumnIndex));
            final int changeableColumnCount = setup.dedupKeyNames.contains("c_sym") ? otherColumnCount : otherColumnCount - 1;
            assertOneOrAllColumnsChanged(row, metadata, changedColumns, changeableColumnCount);
        });
        assertBothChangeKinds();
    }

    @Test
    public void testCopyWritesSameRowEachTime() throws Exception {
        // DedupInsertFuzzTest writes each copy to the reference table and to the dedup table
        final Setup setup = new Setup();
        setup.isWrittenTwice = true;
        assertCopies(setup, (row, original, copy, metadata, changedColumns, otherColumnCount) -> Assert.assertTrue(changedColumns.size() <= otherColumnCount));
    }

    @Test
    public void testIdenticalCopy() throws Exception {
        final Setup setup = new Setup();
        setup.isIdentical = true;
        assertCopies(setup, (row, original, copy, metadata, changedColumns, otherColumnCount) ->
                Assert.assertEquals("row " + row + ", changed " + columnNames(metadata, changedColumns), 0, changedColumns.size())
        );
    }

    @Test
    public void testNoColumnWritten() throws Exception {
        // A row that writes no column has nothing for a copy to change
        final Setup setup = new Setup();
        setup.notSet = 1;
        assertCopies(setup, (row, original, copy, metadata, changedColumns, otherColumnCount) ->
                Assert.assertEquals("row " + row + ", changed " + columnNames(metadata, changedColumns), 0, changedColumns.size())
        );
    }

    private static String columnNames(RecordMetadata metadata, IntList columnIndexes) {
        final StringBuilder names = new StringBuilder("[");
        for (int i = 0, n = columnIndexes.size(); i < n; i++) {
            if (i > 0) {
                names.append(", ");
            }
            names.append(metadata.getColumnName(columnIndexes.getQuick(i)));
        }
        return names.append(']').toString();
    }

    private static boolean isNullValue(Record record, int columnIndex, int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR -> false;
            case ColumnType.INT -> record.getInt(columnIndex) == Numbers.INT_NULL;
            case ColumnType.IPv4 -> record.getIPv4(columnIndex) == Numbers.IPv4_NULL;
            case ColumnType.LONG -> record.getLong(columnIndex) == Numbers.LONG_NULL;
            case ColumnType.DATE -> record.getDate(columnIndex) == Numbers.LONG_NULL;
            case ColumnType.TIMESTAMP -> record.getTimestamp(columnIndex) == Numbers.LONG_NULL;
            case ColumnType.FLOAT -> Float.isNaN(record.getFloat(columnIndex));
            case ColumnType.DOUBLE -> Double.isNaN(record.getDouble(columnIndex));
            case ColumnType.SYMBOL -> record.getSymA(columnIndex) == null;
            case ColumnType.STRING -> record.getStrA(columnIndex) == null;
            case ColumnType.VARCHAR -> record.getVarcharA(columnIndex) == null;
            case ColumnType.BINARY -> record.getBin(columnIndex) == null;
            case ColumnType.LONG128, ColumnType.UUID ->
                    Uuid.isNull(record.getLong128Lo(columnIndex), record.getLong128Hi(columnIndex));
            case ColumnType.LONG256 -> {
                final Long256 value = record.getLong256A(columnIndex);
                yield Long256Impl.isNull(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3());
            }
            case ColumnType.GEOBYTE -> record.getGeoByte(columnIndex) == GeoHashes.BYTE_NULL;
            case ColumnType.GEOSHORT -> record.getGeoShort(columnIndex) == GeoHashes.SHORT_NULL;
            case ColumnType.GEOINT -> record.getGeoInt(columnIndex) == GeoHashes.INT_NULL;
            case ColumnType.GEOLONG -> record.getGeoLong(columnIndex) == GeoHashes.NULL;
            case ColumnType.ARRAY -> record.getArray(columnIndex, columnType).isNull();
            default -> throw new UnsupportedOperationException(ColumnType.nameOf(columnType));
        };
    }

    // a NULL array view can keep the flat view of the previous value, which arrayEquals() would compare
    private static boolean isSameArray(ArrayView left, ArrayView right) {
        if (left.isNull() || right.isNull()) {
            return left.isNull() == right.isNull();
        }
        return left.arrayEquals(right);
    }

    private static boolean isSameValue(Record original, Record copy, int columnIndex, int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN -> original.getBool(columnIndex) == copy.getBool(columnIndex);
            case ColumnType.BYTE -> original.getByte(columnIndex) == copy.getByte(columnIndex);
            case ColumnType.SHORT -> original.getShort(columnIndex) == copy.getShort(columnIndex);
            case ColumnType.CHAR -> original.getChar(columnIndex) == copy.getChar(columnIndex);
            case ColumnType.INT -> original.getInt(columnIndex) == copy.getInt(columnIndex);
            case ColumnType.IPv4 -> original.getIPv4(columnIndex) == copy.getIPv4(columnIndex);
            case ColumnType.LONG -> original.getLong(columnIndex) == copy.getLong(columnIndex);
            case ColumnType.DATE -> original.getDate(columnIndex) == copy.getDate(columnIndex);
            case ColumnType.TIMESTAMP -> original.getTimestamp(columnIndex) == copy.getTimestamp(columnIndex);
            case ColumnType.FLOAT ->
                    Float.floatToIntBits(original.getFloat(columnIndex)) == Float.floatToIntBits(copy.getFloat(columnIndex));
            case ColumnType.DOUBLE ->
                    Double.doubleToLongBits(original.getDouble(columnIndex)) == Double.doubleToLongBits(copy.getDouble(columnIndex));
            case ColumnType.SYMBOL -> Chars.equalsNullable(original.getSymA(columnIndex), copy.getSymA(columnIndex));
            case ColumnType.STRING -> Chars.equalsNullable(original.getStrA(columnIndex), copy.getStrA(columnIndex));
            case ColumnType.VARCHAR -> Utf8s.equals(original.getVarcharA(columnIndex), copy.getVarcharA(columnIndex));
            case ColumnType.BINARY -> TestUtils.areEqual(original.getBin(columnIndex), copy.getBin(columnIndex));
            case ColumnType.LONG128, ColumnType.UUID ->
                    original.getLong128Lo(columnIndex) == copy.getLong128Lo(columnIndex)
                            && original.getLong128Hi(columnIndex) == copy.getLong128Hi(columnIndex);
            case ColumnType.LONG256 -> {
                final Long256 originalValue = original.getLong256A(columnIndex);
                final Long256 copyValue = copy.getLong256A(columnIndex);
                yield originalValue.getLong0() == copyValue.getLong0()
                        && originalValue.getLong1() == copyValue.getLong1()
                        && originalValue.getLong2() == copyValue.getLong2()
                        && originalValue.getLong3() == copyValue.getLong3();
            }
            case ColumnType.GEOBYTE -> original.getGeoByte(columnIndex) == copy.getGeoByte(columnIndex);
            case ColumnType.GEOSHORT -> original.getGeoShort(columnIndex) == copy.getGeoShort(columnIndex);
            case ColumnType.GEOINT -> original.getGeoInt(columnIndex) == copy.getGeoInt(columnIndex);
            case ColumnType.GEOLONG -> original.getGeoLong(columnIndex) == copy.getGeoLong(columnIndex);
            case ColumnType.ARRAY ->
                    isSameArray(original.getArray(columnIndex, columnType), copy.getArray(columnIndex, columnType));
            default -> throw new UnsupportedOperationException(ColumnType.nameOf(columnType));
        };
    }

    // the value of a column that a row leaves unset
    private static boolean isUnsetValue(Record record, int columnIndex, int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN -> !record.getBool(columnIndex);
            case ColumnType.BYTE -> record.getByte(columnIndex) == 0;
            case ColumnType.SHORT -> record.getShort(columnIndex) == 0;
            case ColumnType.CHAR -> record.getChar(columnIndex) == 0;
            default -> isNullValue(record, columnIndex, columnType);
        };
    }

    private void assertBothChangeKinds() {
        Assert.assertTrue(oneChangedRowCount > 0);
        Assert.assertTrue(allChangedRowCount > 0);
    }

    private void assertCopies(Setup setup, RowAssertion rowAssertion) throws Exception {
        oneChangedRowCount = 0;
        allChangedRowCount = 0;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE original_rows " + COLUMNS);
            execute("CREATE TABLE copied_rows " + COLUMNS);
            execute("ALTER TABLE original_rows DROP COLUMN c_dropped");
            execute("ALTER TABLE copied_rows DROP COLUMN c_dropped");
            if (setup.isWrittenTwice) {
                execute("CREATE TABLE copied_rows_again " + COLUMNS);
                execute("ALTER TABLE copied_rows_again DROP COLUMN c_dropped");
            }

            final Rnd rnd = TestUtils.generateRandom(LOG);
            // FuzzInsertOperation takes column indexes of the writer metadata, which has the dropped column
            final IntHashSet dedupKeyIndexes = new IntHashSet();
            try (TableWriter writer = getWriter("original_rows")) {
                final RecordMetadata metadata = writer.getMetadata();
                final int timestampIndex = metadata.getTimestampIndex();
                dedupKeyIndexes.add(timestampIndex);
                setup.dedupKeyNames.add(metadata.getColumnName(timestampIndex));
                final int columnCount = metadata.getColumnCount();
                final int keyCount = setup.isArrayOnlyColumnOutsideKeys ? Integer.MAX_VALUE : 1 + rnd.nextInt(4);
                for (int i = 0; i < columnCount && dedupKeyIndexes.size() <= keyCount; ) {
                    final int columnIndex = setup.isArrayOnlyColumnOutsideKeys ? i++ : rnd.nextInt(columnCount);
                    final int columnType = metadata.getColumnType(columnIndex);
                    // an ARRAY column cannot be a dedup key
                    if (columnType > 0 && !ColumnType.isArray(columnType)) {
                        dedupKeyIndexes.add(columnIndex);
                        setup.dedupKeyNames.add(metadata.getColumnName(columnIndex));
                    }
                }
            }

            final double notSet = setup.notSet < 0 ? rnd.nextDouble() : setup.notSet;
            final double nullSet = setup.nullSet < 0 ? rnd.nextDouble() : setup.nullSet;
            final int strLen = setup.strLen < 0 ? 1 + rnd.nextInt(20) : setup.strLen;
            String[] symbols = setup.symbols;
            if (symbols == null) {
                symbols = new String[2 + rnd.nextInt(10)];
                for (int i = 0; i < symbols.length; i++) {
                    symbols[i] = "s" + i;
                }
            }
            LOG.info().$("writing copies [notSet=").$(notSet)
                    .$(", nullSet=").$(nullSet)
                    .$(", strLen=").$(strLen)
                    .$(", symbolCount=").$(symbols.length)
                    .$(", isIdentical=").$(setup.isIdentical)
                    .I$();

            try (
                    TableWriter originalWriter = getWriter("original_rows");
                    TableWriter copyWriter = getWriter("copied_rows");
                    TableWriter copyAgainWriter = setup.isWrittenTwice ? getWriter("copied_rows_again") : null
            ) {
                for (int i = 0; i < setup.rowCount; i++) {
                    final FuzzInsertOperation operation = new FuzzInsertOperation(
                            rnd.nextLong(),
                            rnd.nextLong(),
                            i,
                            notSet,
                            nullSet,
                            setup.cancelRows,
                            strLen,
                            symbols
                    );
                    operation.apply(rnd, engine, originalWriter, -1, null);
                    final FuzzInsertOperation copy = new DuplicateFuzzInsertOperation(operation, dedupKeyIndexes, setup.isIdentical);
                    copy.apply(rnd, engine, copyWriter, -1, null);
                    if (copyAgainWriter != null) {
                        copy.apply(rnd, engine, copyAgainWriter, -1, null);
                    }
                }
                originalWriter.commit();
                copyWriter.commit();
                if (copyAgainWriter != null) {
                    copyAgainWriter.commit();
                }
            }

            if (setup.isWrittenTwice) {
                assertRows("copied_rows", "copied_rows_again", setup, (row, original, copy, metadata, changedColumns, otherColumnCount) ->
                        Assert.assertEquals("row " + row + ", changed " + columnNames(metadata, changedColumns), 0, changedColumns.size())
                );
            }
            assertRows("original_rows", "copied_rows", setup, rowAssertion);
        });
    }

    private void assertOneOrAllColumnsChange(Setup setup) throws Exception {
        assertOneOrAllColumnsChange(setup, null);
    }

    // every row writes every column, so a copy changes one of the columns outside the dedup keys, or all of them
    private void assertOneOrAllColumnsChange(Setup setup, RowAssertion rowAssertion) throws Exception {
        assertCopies(setup, (row, original, copy, metadata, changedColumns, otherColumnCount) -> {
            assertOneOrAllColumnsChanged(row, metadata, changedColumns, otherColumnCount);
            if (rowAssertion != null) {
                rowAssertion.assertRow(row, original, copy, metadata, changedColumns, otherColumnCount);
            }
        });
        assertBothChangeKinds();
    }

    private void assertOneOrAllColumnsChanged(int row, RecordMetadata metadata, IntList changedColumns, int changeableColumnCount) {
        final int changedColumnCount = changedColumns.size();
        if (changedColumnCount == 1 && changeableColumnCount > 1) {
            oneChangedRowCount++;
        } else {
            Assert.assertEquals("row " + row + ", changed " + columnNames(metadata, changedColumns), changeableColumnCount, changedColumnCount);
            allChangedRowCount++;
        }
    }

    private void assertRows(String originalTable, String copyTable, Setup setup, RowAssertion rowAssertion) throws Exception {
        try (
                RecordCursorFactory originalFactory = select(originalTable);
                RecordCursorFactory copyFactory = select(copyTable);
                RecordCursor originalCursor = originalFactory.getCursor(sqlExecutionContext);
                RecordCursor copyCursor = copyFactory.getCursor(sqlExecutionContext)
        ) {
            final RecordMetadata metadata = originalFactory.getMetadata();
            final Record original = originalCursor.getRecord();
            final Record copy = copyCursor.getRecord();
            final IntList changedColumns = new IntList();
            int row = 0;
            while (originalCursor.hasNext()) {
                Assert.assertTrue(copyCursor.hasNext());
                changedColumns.clear();
                int otherColumnCount = 0;
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    final boolean isSame = isSameValue(original, copy, i, metadata.getColumnType(i));
                    if (setup.dedupKeyNames.contains(metadata.getColumnName(i))) {
                        Assert.assertTrue("dedup key " + metadata.getColumnName(i) + " differs in row " + row, isSame);
                    } else {
                        otherColumnCount++;
                        if (!isSame) {
                            changedColumns.add(i);
                        }
                    }
                }
                rowAssertion.assertRow(row, original, copy, metadata, changedColumns, otherColumnCount);
                row++;
            }
            Assert.assertFalse(copyCursor.hasNext());
            Assert.assertEquals(setup.expectedRowCount, row);
        }
    }

    @FunctionalInterface
    private interface RowAssertion {
        void assertRow(int row, Record original, Record copy, RecordMetadata metadata, IntList changedColumns, int otherColumnCount);
    }

    private static class Setup {
        private final ObjHashSet<String> dedupKeyNames = new ObjHashSet<>();
        private double cancelRows;
        private int expectedRowCount = ROW_COUNT;
        private boolean isArrayOnlyColumnOutsideKeys;
        private boolean isIdentical;
        private boolean isWrittenTwice;
        // a negative value picks a random one
        private double notSet = -1;
        private double nullSet = -1;
        private int rowCount = ROW_COUNT;
        private int strLen = -1;
        // null picks distinct symbols
        private String[] symbols;
    }
}
