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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.NoopArrayWriteState;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.join.JoinRecord;
import io.questdb.griffin.engine.join.NullRecordFactory;
import io.questdb.griffin.engine.table.MultiHashJoinRecord;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link MultiHashJoinRecord} reads every column as the nested {@code JoinRecord}s of the serial
 * plan read it, the oracle: for every column type, in both orientations, with every combination of
 * matched stages and stages that read typed NULLs.
 * <p>
 * The sources are rows of one table with every column type. The oracle nests {@code JoinRecord}s
 * over the table's own layout, the left input first and then each stage, and gives a stage without
 * a match the NULL record the serial LEFT join gives it. The flat record reads the same rows through
 * projections that order the columns differently for each source, as the operator's probe
 * projection and payload layouts do, so a column map that pointed at the wrong source or column
 * would read another value.
 */
public class MultiHashJoinRecordTest extends AbstractCairoTest {
    // Source roles: the left input, stage 0's right input and two more stages.
    private static final int LEFT = 0;
    private static final int STAGE_COUNT = 3;

    @Test
    public void testGettersMatchNestedJoinRecords() throws Exception {
        assertMemoryLeak(() -> {
            createAllTypesTable();
            final ObjList<RecordCursorFactory> factories = new ObjList<>();
            final ObjList<RecordCursor> cursors = new ObjList<>();
            try {
                // The oracle's sources: the table's own layout for the left input and each stage.
                final ObjList<Record> natural = new ObjList<>();
                for (int source = 0; source <= STAGE_COUNT; source++) {
                    natural.add(openRecord("t", factories, cursors));
                }
                final RecordMetadata metadata = factories.getQuick(0).getMetadata();
                final int columnCount = metadata.getColumnCount();
                Assert.assertEquals(30, columnCount);

                // The flat record's sources, each in its own column order: the left input as the
                // probe (A) and as stage 0's payload (B), stage 0's right input as its payload (A)
                // and as the probe (B), and the payloads of stages 1 and 2.
                final int[][] permutations = new int[6][];
                final ObjList<Record> projected = new ObjList<>();
                for (int p = 0; p < permutations.length; p++) {
                    permutations[p] = permutation(columnCount, 3 + 5 * p, (p & 1) == 1);
                    projected.add(openRecord(projection(metadata, permutations[p]), factories, cursors));
                }
                final int leftA = 0, rightA = 1, leftB = 2, rightB = 3, stage1 = 4, stage2 = 5;

                // Orientation A: the probe is the left input, stage k's payload is source k + 1.
                final IntList sourcesA = new IntList();
                final IntList indexesA = new IntList();
                addColumns(sourcesA, indexesA, 0, permutations[leftA]);
                addColumns(sourcesA, indexesA, 1, permutations[rightA]);
                addColumns(sourcesA, indexesA, 2, permutations[stage1]);
                addColumns(sourcesA, indexesA, 3, permutations[stage2]);
                // Orientation B: the left columns come from stage 0's payload, source 1, and stage
                // 0's right columns from the probe, source 0.
                final IntList sourcesB = new IntList();
                final IntList indexesB = new IntList();
                addColumns(sourcesB, indexesB, 1, permutations[leftB]);
                addColumns(sourcesB, indexesB, 0, permutations[rightB]);
                addColumns(sourcesB, indexesB, 2, permutations[stage1]);
                addColumns(sourcesB, indexesB, 3, permutations[stage2]);
                final ObjList<ColumnTypes> payloadTypesA = new ObjList<>();
                payloadTypesA.add(factories.getQuick(STAGE_COUNT + 1 + rightA).getMetadata());
                payloadTypesA.add(factories.getQuick(STAGE_COUNT + 1 + stage1).getMetadata());
                payloadTypesA.add(factories.getQuick(STAGE_COUNT + 1 + stage2).getMetadata());
                final ObjList<ColumnTypes> payloadTypesB = new ObjList<>();
                payloadTypesB.add(factories.getQuick(STAGE_COUNT + 1 + leftB).getMetadata());
                payloadTypesB.add(factories.getQuick(STAGE_COUNT + 1 + stage1).getMetadata());
                payloadTypesB.add(factories.getQuick(STAGE_COUNT + 1 + stage2).getMetadata());
                final MultiHashJoinRecord record = new MultiHashJoinRecord(
                        sourcesA,
                        indexesA,
                        payloadTypesA,
                        sourcesB,
                        indexesB,
                        payloadTypesB
                );

                // The oracle: the serial plan's nesting, master columns first.
                final ArrayColumnTypes outputTypes = new ArrayColumnTypes();
                for (int source = 0; source <= STAGE_COUNT; source++) {
                    for (int col = 0; col < columnCount; col++) {
                        outputTypes.add(metadata.getColumnType(col));
                    }
                }
                final ObjList<JoinRecord> joins = new ObjList<>();
                for (int stage = 0; stage < STAGE_COUNT; stage++) {
                    joins.add(new JoinRecord((stage + 1) * columnCount));
                }
                final Record nullRecord = NullRecordFactory.getInstance(metadata);

                // A scan positions records at row ids once it has walked its frames.
                final LongList rowIds = new LongList();
                for (int i = 0, n = cursors.size(); i < n; i++) {
                    final RecordCursor cursor = cursors.getQuick(i);
                    final Record cursorRecord = cursor.getRecord();
                    while (cursor.hasNext()) {
                        if (i == 0) {
                            rowIds.add(cursorRecord.getRowId());
                        }
                    }
                }
                final int rowCount = rowIds.size();
                Assert.assertEquals(7, rowCount);

                final StringSink expected = new StringSink();
                final StringSink actual = new StringSink();
                int comparisons = 0;
                boolean hasEmptyValues = false;
                boolean hasFourByteUtf8 = false;
                boolean hasNullValues = false;
                for (int row = 0; row < rowCount; row++) {
                    // Source s reads row (row + s), so that every source holds another row.
                    for (int source = 0; source <= STAGE_COUNT; source++) {
                        positionAt(cursors.getQuick(source), natural.getQuick(source), rowIds.getQuick((row + source) % rowCount));
                    }
                    positionAt(cursors.getQuick(STAGE_COUNT + 1 + leftA), projected.getQuick(leftA), rowIds.getQuick(row));
                    positionAt(cursors.getQuick(STAGE_COUNT + 1 + leftB), projected.getQuick(leftB), rowIds.getQuick(row));
                    positionAt(cursors.getQuick(STAGE_COUNT + 1 + rightA), projected.getQuick(rightA), rowIds.getQuick((row + 1) % rowCount));
                    positionAt(cursors.getQuick(STAGE_COUNT + 1 + rightB), projected.getQuick(rightB), rowIds.getQuick((row + 1) % rowCount));
                    positionAt(cursors.getQuick(STAGE_COUNT + 1 + stage1), projected.getQuick(stage1), rowIds.getQuick((row + 2) % rowCount));
                    positionAt(cursors.getQuick(STAGE_COUNT + 1 + stage2), projected.getQuick(stage2), rowIds.getQuick((row + 3) % rowCount));

                    for (boolean isSwapped : new boolean[]{false, true}) {
                        record.setOrientation(isSwapped);
                        if (isSwapped) {
                            record.setProbeRecord(projected.getQuick(rightB));
                            record.setStageRecord(0, projected.getQuick(leftB));
                        } else {
                            record.setProbeRecord(projected.getQuick(leftA));
                            record.setStageRecord(0, projected.getQuick(rightA));
                        }
                        record.setStageRecord(1, projected.getQuick(stage1));
                        record.setStageRecord(2, projected.getQuick(stage2));
                        // Every combination of matched stages, toggled from the previous one.
                        for (int mask = (1 << STAGE_COUNT) - 1; mask >= 0; mask--) {
                            for (int stage = 0; stage < STAGE_COUNT; stage++) {
                                record.setHasMatch(stage, isMatched(mask, stage));
                            }
                            // Stage 0 without a match reads NULLs in place of its right input in
                            // A, and in place of the left input, its payload, in B.
                            final Record left = isSwapped && !isMatched(mask, 0) ? nullRecord : natural.getQuick(LEFT);
                            final Record right = !isSwapped && !isMatched(mask, 0) ? nullRecord : natural.getQuick(1);
                            joins.getQuick(0).of(left, right);
                            for (int stage = 1; stage < STAGE_COUNT; stage++) {
                                joins.getQuick(stage).of(
                                        joins.getQuick(stage - 1),
                                        isMatched(mask, stage) ? natural.getQuick(stage + 1) : nullRecord
                                );
                            }
                            expected.clear();
                            actual.clear();
                            final boolean hasLeftRow = !isSwapped || isMatched(mask, 0);
                            printRecord(joins.getQuick(STAGE_COUNT - 1), outputTypes, hasLeftRow, expected);
                            printRecord(record, outputTypes, hasLeftRow, actual);
                            TestUtils.assertEquals("row " + row + ", swapped " + isSwapped + ", mask " + mask, expected, actual);
                            comparisons++;
                            // Column 12 is the left input's STRING column.
                            hasFourByteUtf8 |= Chars.contains(expected, "#12='a\uD83D\uDE00b'/'a\uD83D\uDE00b'/4 sameView=false\n");
                            hasEmptyValues |= Chars.contains(expected, "#12=''/''/0 sameView=false\n");
                            hasNullValues |= Chars.contains(expected, "#12=null/null/-1 sameView=false\n");
                        }
                    }
                }
                Assert.assertEquals(rowCount * 2 * (1 << STAGE_COUNT), comparisons);
                // The left input's STRING column read 4-byte UTF-8, empty values and NULLs.
                Assert.assertTrue(hasFourByteUtf8);
                Assert.assertTrue(hasEmptyValues);
                Assert.assertTrue(hasNullValues);
            } finally {
                Misc.freeObjList(cursors);
                Misc.freeObjList(factories);
            }
        });
    }

    @Test
    public void testIntervalsRecordsAndRowIdsComeFromTheirSources() {
        // Records hand out intervals and nested records, which no table column holds, and row ids:
        // marker records answer each with an object or value of their own.
        final MarkerRecord left = new MarkerRecord(1);
        final MarkerRecord right = new MarkerRecord(2);
        // Two columns per side: an interval and a long, in that order in both layouts.
        final ArrayColumnTypes types = new ArrayColumnTypes()
                .add(ColumnType.INTERVAL_TIMESTAMP_MICRO)
                .add(ColumnType.LONG);
        final ObjList<ColumnTypes> payloadTypes = new ObjList<>();
        payloadTypes.add(types);
        final MultiHashJoinRecord record = new MultiHashJoinRecord(
                intList(0, 0, 1, 1),
                intList(0, 1, 0, 1),
                payloadTypes,
                intList(1, 1, 0, 0),
                intList(0, 1, 0, 1),
                payloadTypes
        );
        final JoinRecord oracle = new JoinRecord(2);
        for (boolean isSwapped : new boolean[]{false, true}) {
            record.setOrientation(isSwapped);
            record.setProbeRecord(isSwapped ? right : left);
            record.setStageRecord(0, isSwapped ? left : right);
            oracle.of(left, right);
            for (int col = 0; col < 4; col++) {
                Assert.assertSame(oracle.getInterval(col), record.getInterval(col));
                Assert.assertSame(oracle.getRecord(col), record.getRecord(col));
                Assert.assertEquals(oracle.getLong(col), record.getLong(col));
            }
            // The left input's row ids, the probe's in A and stage 0's payload's in B.
            Assert.assertEquals(100, record.getRowId());
            Assert.assertEquals(101, record.getUpdateRowId());

            // Without a match, stage 0's payload reads typed NULLs: the right input's columns in A,
            // the left input's in B.
            record.setHasMatch(0, false);
            final Record nullRecord = NullRecordFactory.getInstance(types);
            oracle.of(isSwapped ? nullRecord : left, isSwapped ? right : nullRecord);
            for (int col = 0; col < 4; col += 2) {
                Assert.assertEquals(oracle.getInterval(col), record.getInterval(col));
                Assert.assertEquals(oracle.getLong(col + 1), record.getLong(col + 1));
            }
            Assert.assertEquals(Numbers.LONG_NULL, record.getLong(isSwapped ? 1 : 3));
            Assert.assertEquals(Interval.NULL, record.getInterval(isSwapped ? 0 : 2));
            // A match again reads the payload record.
            record.setHasMatch(0, true);
            Assert.assertSame(right.intervals.getQuick(0), record.getInterval(2));
        }
    }

    @Test
    public void testSwapNeedsTheSwappedColumnMap() {
        final ObjList<ColumnTypes> payloadTypes = new ObjList<>();
        payloadTypes.add(new ArrayColumnTypes().add(ColumnType.LONG));
        final MultiHashJoinRecord record = new MultiHashJoinRecord(intList(0, 1), intList(0, 0), payloadTypes, null, null, null);
        final AssertionError error = Assert.assertThrows(AssertionError.class, () -> record.setOrientation(true));
        TestUtils.assertContains(error.getMessage(), "no column map for the swapped orientation");
        // The record stays in orientation A.
        final MarkerRecord left = new MarkerRecord(1);
        final MarkerRecord right = new MarkerRecord(2);
        record.setProbeRecord(left);
        record.setStageRecord(0, right);
        Assert.assertEquals(left.getLong(0), record.getLong(0));
        Assert.assertEquals(right.getLong(0), record.getLong(1));
        Assert.assertEquals(100, record.getRowId());
    }

    private static void addColumns(IntList sources, IntList indexes, int source, int[] permutation) {
        // Output column c of the source reads the projection column that holds table column c.
        final int[] inverse = new int[permutation.length];
        for (int j = 0; j < permutation.length; j++) {
            inverse[permutation[j]] = j;
        }
        for (int col = 0; col < permutation.length; col++) {
            sources.add(source);
            indexes.add(inverse[col]);
        }
    }

    private static IntList intList(int... values) {
        final IntList list = new IntList();
        for (int value : values) {
            list.add(value);
        }
        return list;
    }

    private static boolean isMatched(int mask, int stage) {
        return (mask & (1 << stage)) != 0;
    }

    // Opens a cursor of its own for a source, so that each source holds a row of its own.
    private static Record openRecord(String sql, ObjList<RecordCursorFactory> factories, ObjList<RecordCursor> cursors) throws Exception {
        final RecordCursorFactory factory = select(sql);
        factories.add(factory);
        Assert.assertTrue(factory.recordCursorSupportsRandomAccess());
        final RecordCursor cursor = factory.getCursor(sqlExecutionContext);
        cursors.add(cursor);
        return cursor.getRecordB();
    }

    // Projection order p: the table's columns rotated by shift, reversed when asked.
    private static int[] permutation(int columnCount, int shift, boolean isReversed) {
        final int[] permutation = new int[columnCount];
        for (int j = 0; j < columnCount; j++) {
            final int rotated = (j + shift) % columnCount;
            permutation[isReversed ? columnCount - 1 - j : j] = rotated;
        }
        return permutation;
    }

    private static void positionAt(RecordCursor cursor, Record record, long rowId) {
        cursor.recordAt(record, rowId);
    }

    private static void printArray(Record record, int col, int type, StringSink sink) {
        final ArrayView array = record.getArray(col, type);
        ArrayTypeDriver.arrayToJson(array, sink, NoopArrayWriteState.INSTANCE);
        final int dims = ColumnType.decodeArrayDimensionality(type);
        for (int dim = 1; dim <= dims; dim++) {
            sink.put(" dim").put(dim).put('=').put(record.getArrayDimLen(col, type, dim));
        }
        sink.put(" at00=").put(record.getArrayDouble1d2d(col, type, 0, 0));
        sink.put(" at11=").put(record.getArrayDouble1d2d(col, type, 1, 1));
    }

    private static void printBin(BinarySequence bin, StringSink sink) {
        if (bin == null) {
            sink.put("null");
            return;
        }
        sink.put('[');
        for (long i = 0, n = bin.length(); i < n; i++) {
            sink.put(bin.byteAt(i)).put(' ');
        }
        sink.put(']');
    }

    // Every getter that applies to the column's type, the A and B variants and the lengths included.
    private static void printColumn(Record record, int col, int type, StringSink sink) {
        sink.put('#').put(col).put('=');
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN -> sink.put(record.getBool(col));
            case ColumnType.BYTE -> sink.put(record.getByte(col));
            case ColumnType.SHORT -> sink.put(record.getShort(col));
            case ColumnType.CHAR -> sink.put((int) record.getChar(col));
            case ColumnType.INT -> sink.put(record.getInt(col));
            case ColumnType.IPv4 -> sink.put(record.getIPv4(col)).put('/').put(record.getLongIPv4(col));
            case ColumnType.LONG -> sink.put(record.getLong(col));
            case ColumnType.DATE -> sink.put(record.getDate(col)).put('/').put(record.getLong(col));
            case ColumnType.TIMESTAMP -> sink.put(record.getTimestamp(col)).put('/').put(record.getLong(col));
            case ColumnType.FLOAT -> sink.put(record.getFloat(col));
            case ColumnType.DOUBLE -> sink.put(record.getDouble(col));
            case ColumnType.STRING -> {
                final CharSequence a = record.getStrA(col);
                final CharSequence b = record.getStrB(col);
                putQuoted(a, sink);
                sink.put('/');
                putQuoted(b, sink);
                // A and B are separate views, so that comparing them compares two values.
                sink.put('/').put(record.getStrLen(col)).put(" sameView=").put(a != null && a == b);
            }
            case ColumnType.SYMBOL -> {
                putQuoted(record.getSymA(col), sink);
                sink.put('/');
                putQuoted(record.getSymB(col), sink);
                sink.put('/').put(record.getInt(col));
            }
            case ColumnType.VARCHAR -> {
                final Utf8Sequence a = record.getVarcharA(col);
                final Utf8Sequence b = record.getVarcharB(col);
                if (a == null) {
                    sink.put("null");
                } else {
                    sink.put('\'').put(a).put('\'');
                }
                sink.put('/');
                if (b == null) {
                    sink.put("null");
                } else {
                    sink.put('\'').put(b).put('\'');
                }
                sink.put('/').put(record.getVarcharSize(col)).put('/');
                if (a != null) {
                    record.getVarchar(col, sink);
                }
                sink.put(" sameView=").put(a != null && a == b);
            }
            case ColumnType.LONG256 -> {
                record.getLong256(col, sink);
                final Long256 a = record.getLong256A(col);
                final Long256 b = record.getLong256B(col);
                sink.put('/').put(a.getLong0()).put(',').put(a.getLong1()).put(',').put(a.getLong2()).put(',').put(a.getLong3());
                sink.put('/').put(b.getLong0()).put(',').put(b.getLong1()).put(',').put(b.getLong2()).put(',').put(b.getLong3());
                sink.put(" sameView=").put(a == b);
            }
            case ColumnType.UUID -> sink.put(record.getLong128Hi(col)).put('/').put(record.getLong128Lo(col));
            case ColumnType.GEOBYTE -> sink.put(record.getGeoByte(col));
            case ColumnType.GEOSHORT -> sink.put(record.getGeoShort(col));
            case ColumnType.GEOINT -> sink.put(record.getGeoInt(col));
            case ColumnType.GEOLONG -> sink.put(record.getGeoLong(col));
            case ColumnType.BINARY -> {
                printBin(record.getBin(col), sink);
                sink.put('/').put(record.getBinLen(col));
            }
            case ColumnType.ARRAY -> printArray(record, col, type, sink);
            case ColumnType.DECIMAL8 -> sink.put(record.getDecimal8(col));
            case ColumnType.DECIMAL16 -> sink.put(record.getDecimal16(col));
            case ColumnType.DECIMAL32 -> sink.put(record.getDecimal32(col));
            case ColumnType.DECIMAL64 -> sink.put(record.getDecimal64(col));
            case ColumnType.DECIMAL128 -> {
                final Decimal128 decimal = new Decimal128();
                record.getDecimal128(col, decimal);
                sink.put(decimal.getHigh()).put(',').put(decimal.getLow()).put(',').put(decimal.isNull());
            }
            case ColumnType.DECIMAL256 -> {
                final Decimal256 decimal = new Decimal256();
                record.getDecimal256(col, decimal);
                sink.put(decimal.getHh()).put(',').put(decimal.getHl()).put(',')
                        .put(decimal.getLh()).put(',').put(decimal.getLl()).put(',').put(decimal.isNull());
            }
            default -> Assert.fail("the table has no column of type " + ColumnType.nameOf(type));
        }
        sink.put('\n');
    }

    // Every column, then the left input's row ids when the left input has a row: in orientation B
    // a stage 0 without a match leaves the left input's columns to a NULL record, which has none.
    private static void printRecord(Record record, ColumnTypes types, boolean hasLeftRow, StringSink sink) {
        for (int col = 0, n = types.getColumnCount(); col < n; col++) {
            printColumn(record, col, types.getColumnType(col), sink);
        }
        if (hasLeftRow) {
            sink.put("rowId=").put(record.getRowId()).put(" updateRowId=");
            try {
                sink.put(record.getUpdateRowId());
            } catch (UnsupportedOperationException e) {
                sink.put("unsupported");
            }
            sink.put('\n');
        }
    }

    private static String projection(RecordMetadata metadata, int[] permutation) {
        final StringSink sql = new StringSink();
        sql.put("SELECT ");
        for (int j = 0; j < permutation.length; j++) {
            if (j > 0) {
                sql.put(", ");
            }
            sql.put(metadata.getColumnName(permutation[j]));
        }
        sql.put(" FROM t");
        return sql.toString();
    }

    private static void putQuoted(CharSequence value, StringSink sink) {
        if (value == null) {
            sink.put("null");
        } else {
            sink.put('\'').put(value).put('\'');
        }
    }

    private void createAllTypesTable() throws Exception {
        execute("""
                CREATE TABLE t (
                    bool BOOLEAN, bt BYTE, sh SHORT, ch CHAR, i INT, ip IPV4, l LONG, dt DATE,
                    ts TIMESTAMP, tn TIMESTAMP_NS, f FLOAT, d DOUBLE, s STRING, sym SYMBOL, v VARCHAR,
                    l256 LONG256, u UUID, g1 GEOHASH(5b), g2 GEOHASH(2c), g4 GEOHASH(4c), g8 GEOHASH(8c),
                    bin BINARY, a1 DOUBLE[], a2 DOUBLE[][], d8 DECIMAL(2,1), d16 DECIMAL(4,2),
                    d32 DECIMAL(9,3), d64 DECIMAL(18,4), d128 DECIMAL(38,5), d256 DECIMAL(76,6)
                )
                """);
        execute("""
                INSERT INTO t SELECT
                    rnd_boolean(), rnd_byte(), rnd_short(), rnd_char(), rnd_int(), rnd_ipv4(), rnd_long(),
                    rnd_date(0, 100_000_000_000L, 0), (x * 1_000_003)::TIMESTAMP, (x * 1_000_000_007)::TIMESTAMP_NS,
                    rnd_float(), rnd_double(), rnd_str(1, 8, 0), rnd_symbol(4, 2, 4, 0), rnd_varchar(1, 8, 0),
                    rnd_long256(), rnd_uuid4(), rnd_geohash(5), rnd_geohash(10), rnd_geohash(20), rnd_geohash(40),
                    rnd_bin(1, 8, 0), rnd_double_array(1), rnd_double_array(2), rnd_decimal(2, 1, 0),
                    rnd_decimal(4, 2, 0), rnd_decimal(9, 3, 0), rnd_decimal(18, 4, 0), rnd_decimal(38, 5, 0),
                    rnd_decimal(76, 6, 0)
                FROM long_sequence(4)
                """);
        // Empty values, 4-byte UTF-8, and a row of NULLs.
        execute("INSERT INTO t (bool, s, v, sym) VALUES (true, '', '', '')");
        execute("INSERT INTO t (s, v, sym) VALUES ('a\uD83D\uDE00b', 'c\uD83D\uDE00d', '\uD83D\uDE00')");
        execute("INSERT INTO t (bool) VALUES (false)");
    }

    /**
     * Answers every getter the flat record must route: a long per column, an interval and a nested
     * record per column, and row ids, all distinct per marker.
     */
    private static final class MarkerRecord implements Record {
        private final int id;
        private final ObjList<Interval> intervals = new ObjList<>();
        private final ObjList<Record> records = new ObjList<>();

        private MarkerRecord(int id) {
            this.id = id;
            for (int col = 0; col < 2; col++) {
                intervals.add(new Interval(id * 10L + col, id * 10L + col + 1));
                records.add(new Record() {
                });
            }
        }

        @Override
        public Interval getInterval(int col) {
            return intervals.getQuick(col);
        }

        @Override
        public long getLong(int col) {
            return id * 1_000L + col;
        }

        @Override
        public Record getRecord(int col) {
            return records.getQuick(col);
        }

        @Override
        public long getRowId() {
            return id * 100L;
        }

        @Override
        public long getUpdateRowId() {
            return id * 100L + 1;
        }
    }
}
