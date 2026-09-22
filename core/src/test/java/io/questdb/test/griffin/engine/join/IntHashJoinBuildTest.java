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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Hash;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class IntHashJoinBuildTest extends AbstractCairoTest {
    private static final SqlExecutionCircuitBreaker NOOP = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
    private static final Symbols POPULATED_SYMBOLS = new Symbols(
            "symbol-0", "symbol-1", "symbol-2", "symbol-3", "symbol-4", "symbol-5",
            "symbol-6", "symbol-7", "symbol-8", "symbol-9", "symbol-10", "symbol-11"
    );

    @Test
    public void testCompressedIncrementalHeapBoundAndGrowthCap() throws Exception {
        assertMemoryLeak(() -> {
            final long limit = CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE;
            Record source = new Record() {
                @Override
                public int getInt(int column) {
                    return 42;
                }
            };
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64);
                 IntHashJoinBuild build = newBuild(2, 8, ColumnType.INT)) {
                build.open(tracker, NOOP);
                build.append(17, source);
                Field heapField = IntHashJoinBuild.class.getDeclaredField("heap");
                heapField.setAccessible(true);
                Object heap = heapField.get(build);
                Field rowsField = heap.getClass().getDeclaredField("rows");
                rowsField.setAccessible(true);
                Object rows = rowsField.get(heap);
                Field capacityField = rows.getClass().getDeclaredField("capacity");
                capacityField.setAccessible(true);
                Method ensure = rows.getClass().getDeclaredMethod("ensure", long.class, long.class);
                ensure.setAccessible(true);
                // Exercise the incremental path directly, without the known-size hint guard.
                for (long required : new long[]{-1, limit + 1, Long.MAX_VALUE}) {
                    InvocationTargetException error = Assert.assertThrows(InvocationTargetException.class,
                            () -> ensure.invoke(rows, required, 8L));
                    Assert.assertTrue(error.getCause() instanceof CairoException);
                    TestUtils.assertContains(((CairoException) error.getCause()).getFlyweightMessage(), "buffer overflow");
                    Assert.assertEquals(32, tracker.getUsed());
                }
                final long realCapacity = capacityField.getLong(rows);
                try {
                    capacityField.setLong(rows, limit / 2 + 8);
                    InvocationTargetException error = Assert.assertThrows(InvocationTargetException.class,
                            () -> ensure.invoke(rows, limit / 2 + 16, 8L));
                    Assert.assertTrue(error.getCause() instanceof CairoException);
                    // The tiny tracker rejects before allocation/copy. The requested
                    // destination must be capped even though doubling exceeds the limit.
                    TestUtils.assertContains(((CairoException) error.getCause()).getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(((CairoException) error.getCause()).getFlyweightMessage(), ", size=" + limit + ",");
                } finally {
                    capacityField.setLong(rows, realCapacity);
                }
                build.append(17, source);
                FrozenHashJoinBuild.IntProbe probe = build.freeze().newProbe();
                probe.find(17);
                probe.next();
                Assert.assertEquals(42, probe.getRecord().getInt(0));
                probe.next();
                Assert.assertEquals(42, probe.getRecord().getInt(0));
                Assert.assertFalse(probe.hasNext());
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testCompressedUnsignedReferencesSurviveRehashCollisions() throws Exception {
        assertMemoryLeak(() -> {
            int[] keys = new int[3];
            for (int key = 0, count = 0; count < keys.length; key++) {
                if (((int) Hash.hashInt64(key) & 7) == 7) {
                    keys[count++] = key;
                }
            }
            AtomicInteger value = new AtomicInteger();
            Record source = new Record() {
                @Override
                public int getInt(int col) {
                    return value.get();
                }
            };
            try (IntHashJoinBuild build = newBuild(4, 24, ColumnType.INT)) {
                build.open(null, NOOP);
                build.append(keys[0], source);
                value.set(1);
                build.append(keys[1], source);
                Field keysField = IntHashJoinBuild.class.getDeclaredField("keys");
                keysField.setAccessible(true);
                Object table = keysField.get(build);
                Field addressField = table.getClass().getDeclaredField("address");
                addressField.setAccessible(true);
                long address = addressField.getLong(table);
                long offset = 0x80000000L << 3;
                Unsafe.putInt(address + 3 * 8 + 4, CompressedOffsets.compressBiased8(offset));
                Unsafe.putInt(address + 4, CompressedOffsets.compressBiased8(offset + 16));
                value.set(2);
                build.append(keys[2], source); // Rehash both negative references into colliding destination slots.
                FrozenHashJoinBuild.IntProbe probe = build.freeze().newProbe();
                Field rowsField = probe.getClass().getSuperclass().getDeclaredField("payloadRowsAddress");
                rowsField.setAccessible(true);
                long realRows = rowsField.getLong(probe);
                for (int i = 0; i < keys.length; i++) {
                    rowsField.setLong(probe, i < 2 ? realRows - offset : realRows);
                    probe.findUnchecked(keys[i]);
                    Assert.assertTrue(probe.hasNext());
                    Assert.assertEquals(i < 2 ? offset + i * 16L : 32, probe.next());
                    Assert.assertEquals(i, probe.getRecord().getInt(0));
                    Assert.assertFalse(probe.hasNext());
                    Assert.assertTrue(probe.findSingleUnchecked(keys[i]));
                    Assert.assertEquals(i, probe.getRecord().getInt(0));
                }
                rowsField.setLong(probe, realRows);
            }
        });
    }

    @Test
    public void testCompressedUnsignedProbeReferences() throws Exception {
        assertMemoryLeak(() -> {
            for (int count : new int[]{1, 2}) {
                try (IntHashJoinBuild build = newBuild(2, 8, ColumnType.INT)) {
                    build.open(null, NOOP);
                    Record source = new Record() {
                        @Override
                        public int getInt(int col) {
                            return 42;
                        }
                    };
                    for (int i = 0; i < count; i++) {
                        build.append(17, source);
                    }
                    FrozenHashJoinBuild.IntProbe probe = build.freeze().newProbe();
                    Field keysField = IntHashJoinBuild.class.getDeclaredField("keys");
                    keysField.setAccessible(true);
                    Object keys = keysField.get(build);
                    Field addressField = keys.getClass().getDeclaredField("address");
                    addressField.setAccessible(true);
                    long slot = addressField.getLong(keys) + ((int) Hash.hashInt64(17) & 1) * 8L;
                    Field rowsField = probe.getClass().getSuperclass().getDeclaredField("payloadRowsAddress");
                    rowsField.setAccessible(true);
                    long realRows = rowsField.getLong(probe);
                    // Simulate a large relative heap. Every dereference still lands
                    // in the real, owned payload rows; no 16/32 GiB allocation is needed.
                    for (long offset : new long[]{0, 0x80000000L << 3, CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE - count * 16}) {
                        Unsafe.putInt(slot + 4, CompressedOffsets.compressBiased8(offset + (count - 1) * 16));
                        if (count == 2) {
                            Unsafe.putLong(realRows + 16, offset + 8);
                        }
                        rowsField.setLong(probe, realRows - offset);
                        probe.findUnchecked(17);
                        for (int i = count - 1; i >= 0; i--) {
                            Assert.assertTrue(probe.hasNext());
                            Assert.assertEquals(offset + i * 16L, probe.next());
                            Assert.assertEquals(42, probe.getRecord().getInt(0));
                        }
                        Assert.assertFalse(probe.hasNext());
                        if (count == 1) {
                            Assert.assertTrue(probe.findSingleUnchecked(17));
                            Assert.assertEquals(42, probe.getRecord().getInt(0));
                            Assert.assertFalse(probe.hasNext());
                            Assert.assertFalse(probe.findSingleUnchecked(-17));
                        }
                    }
                    Unsafe.putInt(slot + 4, CompressedOffsets.compressBiased8((count - 1) * 16L));
                    if (count == 2) {
                        Unsafe.putLong(realRows + 16, 8);
                    }
                    rowsField.setLong(probe, realRows);
                }
            }
        });
    }

    @Test
    public void testCompressedHeapBoundBeforeAllocationAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            final long limit = CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE;
            Assert.assertThrows(IllegalArgumentException.class, () -> newBuild(2, limit + 1, ColumnType.INT));
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64);
                 IntHashJoinBuild build = newBuild(2, 8, ColumnType.INT);
                 RecordCursorFactory factory = select("SELECT 17::INT k, 42::INT v FROM long_sequence(1)");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                for (long hint : new long[]{limit / 16 + 1, Long.MAX_VALUE}) {
                    build.open(tracker, NOOP);
                    CairoException error = Assert.assertThrows(CairoException.class, () -> build.build(cursor, 0, hint, -1));
                    TestUtils.assertContains(error.getFlyweightMessage(), "hash join build buffer overflow");
                    Assert.assertEquals(0, tracker.getUsed());
                    cursor.toTop();
                    build.open(tracker, NOOP);
                    FrozenHashJoinBuild.IntProbe probe = build.build(cursor, 0, 1, -1).newProbe();
                    Assert.assertTrue(probe.findSingleUnchecked(17));
                    Assert.assertEquals(17, probe.getRecord().getInt(0));
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                    cursor.toTop();
                }
                // The largest legal hint reaches tracked allocation and is rejected
                // by this tiny memory limit, rather than wrapping its compressed offset.
                build.open(tracker, NOOP);
                CairoException error = Assert.assertThrows(CairoException.class, () -> build.build(cursor, 0, limit / 16, -1));
                TestUtils.assertContains(error.getFlyweightMessage(), "query memory limit exceeded");
                TestUtils.assertContains(error.getFlyweightMessage(), ", size=" + limit + ",");
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testUniqueDuplicateAndEmptyBuildReuseAcrossPayloadWidths() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger row = new AtomicInteger();
            Symbols symbols = new Symbols();
            symbols.put(1, "Aa", "BB");
            Record source = new Record() {
                @Override
                public int getInt(int column) {
                    // Column 1 is the SYMBOL payload, stored as its source key.
                    return column != 1 ? row.get() : row.get() % 3 == 2 ? SymbolTable.VALUE_IS_NULL : row.get() % 3;
                }

                @Override
                public double getDouble(int column) {
                    return row.get() * 0.5;
                }

                @Override
                public CharSequence getSymA(int column) {
                    return symbols.valueOf(column, getInt(column));
                }
            };
            // Empty, narrow and wide layouts, with growth through page/cache-line boundaries.
            for (int width : new int[]{0, 1, 3, 17}) {
                ArrayColumnTypes types = new ArrayColumnTypes();
                IntList columns = new IntList();
                for (int i = 0; i < width; i++) {
                    types.add(i == 0 ? ColumnType.INT : i == 1 ? ColumnType.SYMBOL : ColumnType.DOUBLE);
                    columns.add(i);
                }
                try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(32 * 1024 * 1024);
                     IntHashJoinBuild build = new IntHashJoinBuild(types, columns, 2, 8, true)) {
                    FrozenHashJoinBuild.IntProbe reusable = null;
                    // Recompute uniqueness on empty -> unique -> late duplicate -> skew -> unique reuse.
                    for (int execution = 0; execution < 5; execution++) {
                        build.open(tracker, NOOP);
                        Map<Integer, List<Integer>> expected = new HashMap<>();
                        int count = execution == 0 ? 0 : 4097;
                        for (int r = 0; r < count; r++) {
                            row.set(r);
                            int key = execution == 3 ? (r % 7) : execution == 2 && r == count - 1 ? 0 : r;
                            key = key == 1 ? Numbers.INT_NULL : key == 2 ? Integer.MAX_VALUE : key == 3 ? -1 : key;
                            expected.computeIfAbsent(key, ignored -> new ArrayList<>()).add(r);
                            build.append(key, source);
                        }
                        FrozenHashJoinBuild.IntKeyed frozen = build.freeze(symbols);
                        Assert.assertEquals(count, frozen.getRowCount());
                        Assert.assertEquals(expected.size(), frozen.getKeyCount());
                        Assert.assertEquals(tracker.getUsed(), frozen.getSizeInBytes());
                        if (reusable == null) {
                            reusable = frozen.newProbe();
                        } else {
                            reusable.reopen();
                        }
                        List<Long> handles = new ArrayList<>();
                        List<Integer> payloadRows = new ArrayList<>();
                        for (Map.Entry<Integer, List<Integer>> entry : expected.entrySet()) {
                            reusable.findUnchecked(entry.getKey());
                            List<Integer> matches = new ArrayList<>();
                            while (reusable.hasNext()) {
                                long handle = reusable.next();
                                int value = width == 0 ? 0 : reusable.getRecord().getInt(0);
                                matches.add(value);
                                handles.add(handle);
                                payloadRows.add(value);
                            }
                            Assert.assertEquals(entry.getValue().size(), matches.size());
                            if (width > 0) {
                                matches.sort(Integer::compare);
                                Assert.assertEquals(entry.getValue(), matches);
                            }
                            if (count == expected.size()) {
                                Assert.assertTrue(reusable.findSingleUnchecked(entry.getKey()));
                                if (width > 0) {
                                    Assert.assertEquals(entry.getValue().get(0).intValue(), reusable.getRecord().getInt(0));
                                }
                                Assert.assertFalse(reusable.hasNext());
                            }
                        }
                        reusable.findUnchecked(-42);
                        Assert.assertFalse(reusable.hasNext());
                        if (count == expected.size()) {
                            Assert.assertFalse(reusable.findSingleUnchecked(-42));
                        }
                        for (int h = 0; h < handles.size(); h++) {
                            reusable.recordAt(handles.get(h));
                            int value = payloadRows.get(h);
                            if (width > 0) {
                                Assert.assertEquals(value, reusable.getRecord().getInt(0));
                            }
                            for (int column = 2; column < width; column++) {
                                Assert.assertEquals(value * 0.5, reusable.getRecord().getDouble(column), 0);
                            }
                            if (width > 1) {
                                row.set(value);
                                TestUtils.assertEquals(source.getSymA(1), reusable.getRecord().getSymA(1));
                            }
                        }
                        build.close();
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                }
            }
        });
    }

    @Test
    public void testAllPayloadTypesAndPrunedMapping() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table payload (b boolean, by byte, sh short, ch char, i int, l long, d date, ts timestamp, "
                    + "ns timestamp_ns, f float, dbl double, s symbol, unused string)");
            execute("insert into payload values (true, 12, 123, 'Q', 42, 9876543210L, 100, 123456789, "
                    + "123456789123456789L, 1.25, 2.5, 'ES', 'ignored'), "
                    + "(null, null, null, null, null, null, null, null, null, null, null, null, 'ignored')");
            String[] expressions = {"s", "dbl", "f", "ns", "ts", "d", "l", "i", "ch", "sh", "by", "b"};
            ArrayColumnTypes types = new ArrayColumnTypes();
            IntList mapping = new IntList();
            try (RecordCursorFactory source = select("payload")) {
                for (String name : expressions) {
                    int index = source.getMetadata().getColumnIndex(name);
                    types.add(source.getMetadata().getColumnType(index));
                    mapping.add(index);
                }
                try (IntHashJoinBuild build = new IntHashJoinBuild(types, mapping, 2, 16);
                     RecordCursor cursor = source.getCursor(sqlExecutionContext)) {
                    build.open(null, NOOP);
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(ColumnType.FLOAT, types.getColumnType(2));
                    Assert.assertEquals(1.25f, cursor.getRecord().getFloat(mapping.getQuick(2)), 0);
                    build.append(42, cursor.getRecord());
                    Assert.assertTrue(cursor.hasNext());
                    build.append(43, cursor.getRecord());
                    Assert.assertFalse(cursor.hasNext());
                    // SYMBOL payloads resolve through the open source cursor, whose position no longer matters.
                    FrozenHashJoinBuild.IntProbe probe = build.freeze(cursor).newProbe();
                    probe.find(42);
                    Assert.assertTrue(probe.hasNext());
                    probe.next();
                    Record record = probe.getRecord();
                    TestUtils.assertEquals("ES", record.getSymA(0));
                    Assert.assertEquals(2.5, record.getDouble(1), 0);
                    Assert.assertEquals(1.25f, record.getFloat(2), 0);
                    Assert.assertEquals(123456789123456789L, record.getTimestamp(3));
                    Assert.assertEquals(123456789L, record.getTimestamp(4));
                    Assert.assertEquals(100L, record.getDate(5));
                    Assert.assertEquals(9876543210L, record.getLong(6));
                    Assert.assertEquals(42, record.getInt(7));
                    Assert.assertEquals('Q', record.getChar(8));
                    Assert.assertEquals(123, record.getShort(9));
                    Assert.assertEquals(12, record.getByte(10));
                    Assert.assertTrue(record.getBool(11));
                    Assert.assertFalse(probe.hasNext());
                    Assert.assertTrue(probe.getSymbolTable(0) instanceof StaticSymbolTable);
                    Assert.assertEquals(0, ((StaticSymbolTable) probe.getSymbolTable(0)).keyOf("ES"));
                    probe.find(43);
                    probe.next();
                    Assert.assertNull(record.getSymA(0));
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, record.getInt(0));
                    Assert.assertTrue(Double.isNaN(record.getDouble(1)));
                    Assert.assertTrue(Float.isNaN(record.getFloat(2)));
                    Assert.assertEquals(Numbers.LONG_NULL, record.getTimestamp(3));
                    Assert.assertEquals(Numbers.LONG_NULL, record.getTimestamp(4));
                    Assert.assertEquals(Numbers.LONG_NULL, record.getDate(5));
                    Assert.assertEquals(Numbers.LONG_NULL, record.getLong(6));
                    Assert.assertEquals(Numbers.INT_NULL, record.getInt(7));
                    Assert.assertEquals(0, record.getChar(8));
                    Assert.assertEquals(0, record.getShort(9));
                    Assert.assertEquals(0, record.getByte(10));
                    Assert.assertFalse(record.getBool(11));
                    Assert.assertFalse(probe.hasNext());
                }
            }
        });
    }

    @Test
    public void testWideFixedSizePayloadTypesRoundTripAndNullExtend() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wide (ip IPV4, u UUID, l256 LONG256, "
                    + "g1 GEOHASH(1c), g3 GEOHASH(3c), g6 GEOHASH(6c), g12 GEOHASH(12c), "
                    + "dec8 DECIMAL(2,1), dec16 DECIMAL(4,1), dec32 DECIMAL(9,2), "
                    + "dec64 DECIMAL(18,2), dec128 DECIMAL(38,2), dec256 DECIMAL(50,2))");
            execute("""
                    INSERT INTO wide VALUES
                    ('10.0.0.7', '11111111-2222-3333-4444-555555555555',
                     '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                     'q', 'sp0', 'sp052w', 'sp052w92p1p8',
                     1.5::DECIMAL(2,1), 12.5::DECIMAL(4,1), 1234.56::DECIMAL(9,2),
                     1234567890.12::DECIMAL(18,2), 123456789012345678.90::DECIMAL(38,2),
                     12345678901234567890123456.78::DECIMAL(50,2)),
                    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)""");
            // The payload order reverses the table's, so an offset that ignored alignment shows up.
            String[] names = {"dec256", "dec8", "dec128", "dec16", "l256", "dec32", "u", "dec64",
                    "g1", "g3", "g6", "g12", "ip"};
            ArrayColumnTypes types = new ArrayColumnTypes();
            IntList mapping = new IntList();
            try (RecordCursorFactory source = select("wide")) {
                for (String name : names) {
                    int index = source.getMetadata().getColumnIndex(name);
                    types.add(source.getMetadata().getColumnType(index));
                    mapping.add(index);
                }
                try (IntHashJoinBuild build = new IntHashJoinBuild(types, mapping, 2, 16);
                     RecordCursor cursor = source.getCursor(sqlExecutionContext)) {
                    build.open(null, NOOP);
                    Assert.assertTrue(cursor.hasNext());
                    build.append(7, cursor.getRecord());
                    Assert.assertTrue(cursor.hasNext());
                    build.append(8, cursor.getRecord());
                    FrozenHashJoinBuild.IntProbe probe = build.freeze(cursor).newProbe();
                    probe.find(7);
                    probe.next();
                    Record record = probe.getRecord();
                    Decimal256 decimal256 = new Decimal256();
                    Decimal128 decimal128 = new Decimal128();
                    // The heap stores raw words only, so the reader applies the scale the payload
                    // column's type carries, exactly as every other fixed-size record does.
                    record.getDecimal256(0, decimal256);
                    decimal256.of(decimal256.getHh(), decimal256.getHl(), decimal256.getLh(), decimal256.getLl(),
                            ColumnType.getDecimalScale(types.getColumnType(0)));
                    TestUtils.assertEquals("12345678901234567890123456.78", decimal256.toString());
                    Assert.assertEquals(15, record.getDecimal8(1));
                    record.getDecimal128(2, decimal128);
                    decimal128.of(decimal128.getHigh(), decimal128.getLow(),
                            ColumnType.getDecimalScale(types.getColumnType(2)));
                    TestUtils.assertEquals("123456789012345678.90", decimal128.toString());
                    Assert.assertEquals(125, record.getDecimal16(3));
                    TestUtils.assertEquals("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                            record.getLong256A(4).toString());
                    // getLong256B() must survive a getLong256A() on the same column.
                    Long256 a = record.getLong256A(4);
                    Long256 b = record.getLong256B(4);
                    Assert.assertNotSame(a, b);
                    Assert.assertEquals(a.getLong0(), b.getLong0());
                    Assert.assertEquals(123456, record.getDecimal32(5));
                    Assert.assertEquals(0x1111111122223333L, record.getLong128Hi(6));
                    Assert.assertEquals(0x4444555555555555L, record.getLong128Lo(6));
                    Assert.assertEquals(123456789012L, record.getDecimal64(7));
                    Assert.assertEquals(ColumnType.GEOBYTE, ColumnType.tagOf(types.getColumnType(8)));
                    Assert.assertEquals(ColumnType.GEOSHORT, ColumnType.tagOf(types.getColumnType(9)));
                    Assert.assertEquals(ColumnType.GEOINT, ColumnType.tagOf(types.getColumnType(10)));
                    Assert.assertEquals(ColumnType.GEOLONG, ColumnType.tagOf(types.getColumnType(11)));
                    Assert.assertEquals(GeoHashes.fromString("q", 0, 1), record.getGeoByte(8));
                    Assert.assertEquals(GeoHashes.fromString("sp0", 0, 3), record.getGeoShort(9));
                    Assert.assertEquals(GeoHashes.fromString("sp052w", 0, 6), record.getGeoInt(10));
                    Assert.assertEquals(GeoHashes.fromString("sp052w92p1p8", 0, 12), record.getGeoLong(11));
                    Assert.assertEquals(Numbers.parseIPv4("10.0.0.7"), record.getIPv4(12));
                    Assert.assertFalse(probe.hasNext());
                    // The NULL row keeps every type's own sentinel, which is what a LEFT join's
                    // null-extended row has to match.
                    probe.find(8);
                    probe.next();
                    record.getDecimal256(0, decimal256);
                    Assert.assertTrue(decimal256.isNull());
                    Assert.assertEquals(Decimals.DECIMAL8_NULL, record.getDecimal8(1));
                    record.getDecimal128(2, decimal128);
                    Assert.assertTrue(decimal128.isNull());
                    Assert.assertEquals(Decimals.DECIMAL16_NULL, record.getDecimal16(3));
                    Assert.assertEquals(Long256Impl.NULL_LONG256, record.getLong256A(4));
                    Assert.assertEquals(Decimals.DECIMAL32_NULL, record.getDecimal32(5));
                    Assert.assertEquals(Numbers.LONG_NULL, record.getLong128Hi(6));
                    Assert.assertEquals(Numbers.LONG_NULL, record.getLong128Lo(6));
                    Assert.assertEquals(Decimals.DECIMAL64_NULL, record.getDecimal64(7));
                    Assert.assertEquals(GeoHashes.BYTE_NULL, record.getGeoByte(8));
                    Assert.assertEquals(GeoHashes.SHORT_NULL, record.getGeoShort(9));
                    Assert.assertEquals(GeoHashes.INT_NULL, record.getGeoInt(10));
                    Assert.assertEquals(GeoHashes.NULL, record.getGeoLong(11));
                    Assert.assertEquals(Numbers.IPv4_NULL, record.getIPv4(12));
                    Assert.assertFalse(probe.hasNext());
                }
            }
        });
    }

    @Test
    public void testKnownBuildSizeReservesTrackedRowsAndOverflowReuses() throws Exception {
        assertMemoryLeak(() -> {
            // Enough for the exact payload and two key slots, but not a doubling copy.
            final long capacity = 10_000 * 16L + 16;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(capacity);
                 IntHashJoinBuild build = new IntHashJoinBuild(new ArrayColumnTypes().add(ColumnType.DOUBLE), indexes(1), 2, 16);
                 RecordCursorFactory factory = select("select 1::int k, x*0.5 v from long_sequence(10000)");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertEquals(10_000, cursor.size());
                for (int execution = 0; execution < 2; execution++) {
                    cursor.toTop();
                    build.open(tracker, NOOP);
                    FrozenHashJoinBuild.IntKeyed frozen = build.build(cursor, 0, cursor.size(), -1);
                    Assert.assertEquals(10_000, frozen.getRowCount());
                    Assert.assertEquals(capacity, tracker.getUsed());
                    Assert.assertEquals(capacity, frozen.getSizeInBytes());
                    FrozenHashJoinBuild.IntProbe probe = frozen.newProbe();
                    probe.find(1);
                    probe.next();
                    Assert.assertEquals(5_000, probe.getRecord().getDouble(0), 0);
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                    build.open(tracker, NOOP);
                    CairoException error = Assert.assertThrows(CairoException.class,
                            () -> build.build(cursor, 0, Long.MAX_VALUE, -1));
                    TestUtils.assertContains(error.getFlyweightMessage(), "hash join build buffer overflow");
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testKeyCountHintBoundsWithoutCapping() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1 << 20);
                 IntHashJoinBuild build = new IntHashJoinBuild(new ArrayColumnTypes(), new IntList(), 64, 64);
                 RecordCursorFactory factory = select("SELECT x::INT k FROM long_sequence(1_000)");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                // A hint below the distinct keys presizes that far, to 256 slots, and the table
                // grows past it to 2_048 slots; the heap doubles from 64 bytes to 8_192 for 8_000.
                build.open(tracker, NOOP);
                FrozenHashJoinBuild.IntKeyed frozen = build.build(cursor, 0, -1, 100);
                Assert.assertEquals(1_000, frozen.getKeyCount());
                Assert.assertEquals(2_048 * 8 + 8_192, frozen.getSizeInBytes());
                FrozenHashJoinBuild.IntProbe probe = frozen.newProbe();
                for (int key = 0; key <= 1_001; key++) {
                    Assert.assertEquals(key > 0 && key <= 1_000, probe.findSingleUnchecked(key));
                }
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
                // A hint past the largest table asks for all of it, 8 GiB, which the limit rejects
                // before any allocation; the failure releases the build, which then reopens.
                cursor.toTop();
                build.open(tracker, NOOP);
                CairoException error = Assert.assertThrows(CairoException.class, () -> build.build(cursor, 0, -1, Long.MAX_VALUE));
                TestUtils.assertContains(error.getFlyweightMessage(), "query memory limit exceeded");
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, build.getSizeInBytes());
                cursor.toTop();
                build.open(tracker, NOOP);
                Assert.assertEquals(1_000, build.build(cursor, 0, 1_000, 1_000).getKeyCount());
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testKeyCountHintPresizesKeyTableAndLowersPeak() throws Exception {
        assertMemoryLeak(() -> {
            final int rows = 100_000;
            // 100_000 distinct keys fill 262_144 eight-byte slots up to half, and an empty payload
            // leaves an eight-byte link a row, presized from the row count in both builds below.
            final long keyTableBytes = 262_144 * 8L;
            final long heapBytes = rows * 8L;
            // The presize replaces the 64 initial slots, which stay charged while it allocates.
            final long initialKeyTableBytes = 64 * 8L;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(keyTableBytes + heapBytes + initialKeyTableBytes);
                 IntHashJoinBuild build = new IntHashJoinBuild(new ArrayColumnTypes(), new IntList(), 64, 64);
                 RecordCursorFactory factory = select("SELECT x::INT k FROM long_sequence(" + rows + ")");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                for (int execution = 0; execution < 2; execution++) {
                    cursor.toTop();
                    SiteBreaker breaker = new SiteBreaker(null);
                    build.open(tracker, breaker);
                    FrozenHashJoinBuild.IntKeyed frozen = build.build(cursor, 0, rows, rows);
                    Assert.assertEquals(rows, frozen.getKeyCount());
                    Assert.assertEquals(keyTableBytes + heapBytes, frozen.getSizeInBytes());
                    Assert.assertEquals(keyTableBytes + heapBytes, tracker.getUsed());
                    // A single rehash, of the 64 empty initial slots, before the first row.
                    Assert.assertEquals(1, breaker.keyRehashChecks);
                    FrozenHashJoinBuild.IntProbe probe = frozen.newProbe();
                    Assert.assertTrue(probe.findSingleUnchecked(1));
                    Assert.assertTrue(probe.findSingleUnchecked(rows));
                    Assert.assertFalse(probe.findSingleUnchecked(rows + 1));
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
                // Growing into the same table rehashes the previous one, half its size, while both
                // are charged, which the same limit does not hold.
                cursor.toTop();
                build.open(tracker, NOOP);
                CairoException error = Assert.assertThrows(CairoException.class, () -> build.build(cursor, 0, rows, -1));
                TestUtils.assertContains(error.getFlyweightMessage(), "query memory limit exceeded");
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testRowSizeMatchesHeapLayout() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE layout (k INT, b BOOLEAN, sh SHORT, i INT, d DOUBLE, u UUID, l256 LONG256)");
            execute("""
                    INSERT INTO layout VALUES
                        (1, true, 1, 1, 1.5, NULL, NULL),
                        (1, false, 2, 2, 2.5, NULL, NULL),
                        (1, true, 3, 3, 3.5, NULL, NULL)
                    """);
            // An eight-byte link, then each payload aligned to its own size up to eight bytes,
            // and the row rounded up to eight bytes.
            final int[][] layouts = {{}, {4}, {3}, {1, 2, 3}, {3, 4}, {1, 5}, {6, 1}, {2, 5, 1, 3}};
            final long[] rowSizes = {8, 16, 16, 16, 24, 32, 48, 40};
            try (RecordCursorFactory source = select("layout")) {
                for (int l = 0; l < layouts.length; l++) {
                    ArrayColumnTypes types = new ArrayColumnTypes();
                    for (int column : layouts[l]) {
                        types.add(source.getMetadata().getColumnType(column));
                    }
                    Assert.assertEquals(rowSizes[l], FrozenHashJoinBuild.getRowSize(types));
                    try (IntHashJoinBuild build = new IntHashJoinBuild(types, indexes(layouts[l]), 2, 8);
                         RecordCursor cursor = source.getCursor(sqlExecutionContext)) {
                        build.open(null, NOOP);
                        FrozenHashJoinBuild.IntKeyed frozen = build.build(cursor, 0, cursor.size(), -1);
                        Assert.assertEquals(3, frozen.getRowCount());
                        // Two eight-byte key slots, and a heap presized to exactly three rows.
                        Assert.assertEquals(16 + 3 * rowSizes[l], frozen.getSizeInBytes());
                    }
                }
            }
        });
    }

    @Test
    public void testCursorBuildLeavesRowChecksToCursorFrames() throws Exception {
        assertMemoryLeak(() -> {
            final int frameRows = 1024;
            AtomicInteger clockReads = new AtomicInteger();
            AtomicInteger consumed = new AtomicInteger();
            AtomicInteger interruptMode = new AtomicInteger();
            AtomicBoolean cancelled = new AtomicBoolean();
            AtomicBoolean expired = new AtomicBoolean();
            DefaultSqlExecutionCircuitBreakerConfiguration config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                @Override
                public int getCircuitBreakerThrottle() {
                    // Every consultation performs a real check, so clock reads count build checks.
                    return 1;
                }

                @Override
                public MillisecondClock getClock() {
                    return () -> {
                        clockReads.incrementAndGet();
                        return expired.get() ? 1002 : 1000;
                    };
                }
            };
            Record record = new Record() {
                @Override
                public double getDouble(int col) {
                    return consumed.get();
                }

                @Override
                public int getInt(int col) {
                    return 1;
                }
            };
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(8 * 1024 * 1024);
                 NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine, config);
                 IntHashJoinBuild build = newBuild(2, 1_600_000, ColumnType.DOUBLE);
                 RecordCursor cursor = new RecordCursor() {
                     @Override
                     public void close() {
                     }

                     @Override
                     public Record getRecord() {
                         return record;
                     }

                     @Override
                     public Record getRecordB() {
                         throw new UnsupportedOperationException();
                     }

                     @Override
                     public boolean hasNext() {
                         if (consumed.get() == 100_000) {
                             return false;
                         }
                         final int row = consumed.incrementAndGet();
                         if (row == 32) {
                             cancelled.set(interruptMode.get() == 1);
                             expired.set(interruptMode.get() == 2);
                         }
                         if (interruptMode.get() == 1 || interruptMode.get() == 2) {
                             // Table cursors check the breaker once per page frame.
                             if (row % frameRows == 0) {
                                 breaker.statefulThrowExceptionIfTrippedTimeThrottled();
                             }
                         }
                         return true;
                     }

                     @Override
                     public long preComputedStateSize() {
                         return 0;
                     }

                     @Override
                     public void recordAt(Record record, long rowId) {
                         throw new UnsupportedOperationException();
                     }

                     @Override
                     public long size() {
                         return 100_000;
                     }

                     @Override
                     public void toTop() {
                         consumed.set(0);
                     }
                 }) {
                breaker.setCancelledFlag(cancelled);
                breaker.setTimeout(1);
                for (int mode = 0; mode < 4; mode++) {
                    interruptMode.set(mode);
                    cancelled.set(false);
                    expired.set(false);
                    cursor.toTop();
                    breaker.resetTimer();
                    clockReads.set(0);
                    build.open(tracker, breaker);
                    if (mode == 1 || mode == 2) {
                        CairoException error = Assert.assertThrows(CairoException.class, () -> build.build(cursor, 0));
                        Assert.assertEquals(mode == 1, error.isCancellation());
                        Assert.assertEquals("the build observes interruption at the cursor's frame check",
                                frameRows, consumed.get());
                    } else {
                        FrozenHashJoinBuild.IntKeyed frozen = build.build(cursor, 0);
                        Assert.assertEquals(100_000, frozen.getRowCount());
                        // Only open, the initial slot clear and freeze check; appended rows do not.
                        Assert.assertTrue("build checks must not scale with rows: " + clockReads.get(),
                                clockReads.get() <= 3);
                        FrozenHashJoinBuild.IntProbe probe = frozen.newProbe();
                        probe.find(1);
                        probe.next();
                        Assert.assertEquals(100_000, probe.getRecord().getDouble(0), 0);
                    }
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testCancellationAtEveryBuildCheckAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1_000_000);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL, ColumnType.DOUBLE)) {
                Source record = new Source();
                CountingSqlExecutionCircuitBreaker counting = new CountingSqlExecutionCircuitBreaker(NOOP);
                populate(build, record, tracker, counting);
                long checks = counting.getCheckCount();
                build.close();
                // Includes initialization, copied rows, hash/row growth and freeze.
                for (long failAt = 1; failAt <= checks; failAt++) {
                    final long failureCheck = failAt;
                    CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(NOOP) {
                        @Override
                        public void statefulThrowExceptionIfTripped() {
                            super.statefulThrowExceptionIfTripped();
                            failAtCheck();
                        }

                        @Override
                        public void statefulThrowExceptionIfTrippedNoThrottle() {
                            super.statefulThrowExceptionIfTrippedNoThrottle();
                            failAtCheck();
                        }

                        @Override
                        public void statefulThrowExceptionIfTrippedTimeThrottled() {
                            super.statefulThrowExceptionIfTrippedTimeThrottled();
                            failAtCheck();
                        }

                        private void failAtCheck() {
                            if (getCheckCount() == failureCheck) {
                                throw CairoException.queryCancelled(1);
                            }
                        }
                    };
                    try {
                        populate(build, record, tracker, breaker);
                        Assert.fail("expected cancellation at check " + failAt);
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isCancellation());
                    }
                    Assert.assertEquals("cancelled build releases all allocations", 0, tracker.getUsed());
                    Assert.assertEquals(0, build.getSizeInBytes());
                }
                populate(build, record, tracker, NOOP);
                Assert.assertTrue(tracker.getUsed() > 0);
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testRehashChecksCancellationPerMiBOfSlots() throws Exception {
        assertMemoryLeak(() -> {
            // Row 131_072 rehashes 2 MiB of key slots.
            final int rows = 131_073;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64 * 1024 * 1024);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL)) {
                Source source = new Source();
                Symbols symbols = new Symbols("reused");
                for (String failSite : new String[]{null, "growKeyTable"}) {
                    SiteBreaker breaker = new SiteBreaker(failSite);
                    build.open(tracker, breaker);
                    try {
                        for (int row = 0; row < rows; row++) {
                            breaker.row = row;
                            source.row = row;
                            build.append(row, source);
                        }
                        Assert.assertNull("expected cancellation inside " + failSite, failSite);
                        Assert.assertEquals(rows, build.freeze(symbols).getRowCount());
                        Assert.assertEquals("appended rows must not check", 0, breaker.rowChecks);
                        // One check per rehash plus one per MiB of old slots: 19 key checks.
                        Assert.assertTrue("key rehash checks: " + breaker.keyRehashChecks,
                                breaker.keyRehashChecks > 0 && breaker.keyRehashChecks < 64);
                        build.close();
                    } catch (CairoException e) {
                        Assert.assertNotNull("unexpected interruption: " + e.getFlyweightMessage(), failSite);
                        Assert.assertTrue(e.isCancellation());
                        // The first rehash of more than 1 MiB of old slots checks again inside its loop.
                        Assert.assertEquals(131_072, breaker.failedRow);
                    }
                    Assert.assertEquals("rehash cancellation releases all allocations", 0, tracker.getUsed());
                    Assert.assertEquals(0, build.getSizeInBytes());
                }
                build.open(tracker, NOOP);
                source.row = 0;
                build.append(1, source);
                FrozenHashJoinBuild.IntProbe probe = build.freeze(symbols).newProbe();
                Assert.assertTrue(probe.findSingleUnchecked(1));
                TestUtils.assertEquals("reused", probe.getRecord().getSymA(0));
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testCancellationDuringPayloadCopy() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(8 * 1024 * 1024);
                 IntHashJoinBuild build = newBuild(2, 1024 * 1024, ColumnType.DOUBLE)) {
                CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(NOOP) {
                    @Override
                    public void statefulThrowExceptionIfTripped() {
                        failWhileCopiesCoexist();
                    }

                    @Override
                    public void statefulThrowExceptionIfTrippedTimeThrottled() {
                        // The copy loop checks once per MiB copied.
                        failWhileCopiesCoexist();
                    }

                    private void failWhileCopiesCoexist() {
                        if (tracker.getUsed() > 2 * 1024 * 1024) {
                            throw CairoException.queryCancelled(1);
                        }
                    }
                };
                build.open(tracker, breaker);
                Source record = new Source();
                for (int i = 0; i < 65536; i++) {
                    build.append(1, record);
                }
                // Cancellation after allocating the destination, while source and destination coexist.
                Assert.assertThrows(CairoException.class, () -> build.append(1, record));
                Assert.assertEquals(0, tracker.getUsed());
                build.open(tracker, NOOP);
                build.append(1, record);
                Assert.assertEquals(1, build.freeze().getRowCount());
            }
        });
    }

    @Test
    public void testConcurrentLookupsAndIndependentSymbolFlyweights() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (k INT, s SYMBOL, d DOUBLE)");
            execute("""
                    INSERT INTO src
                    SELECT ((x - 1) % 100)::INT, CASE WHEN x % 2 = 1 THEN 'ES' ELSE 'IT' END, (x - 1) + 0.25
                    FROM long_sequence(10_000)
                    """);
            try (IntHashJoinBuild build = new IntHashJoinBuild(new ArrayColumnTypes().add(ColumnType.SYMBOL).add(ColumnType.DOUBLE), indexes(1, 2), 2, 16);
                 RecordCursorFactory factory = select("src");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                build.open(null, NOOP);
                FrozenHashJoinBuild.IntKeyed frozen = build.build(cursor, 0);
                ExecutorService executor = Executors.newFixedThreadPool(4);
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                try {
                    for (int worker = 0; worker < 4; worker++) {
                        final int shift = worker;
                        // The owner binds probes and their symbol table views before publication.
                        FrozenHashJoinBuild.IntProbe a = frozen.newProbe();
                        FrozenHashJoinBuild.IntProbe b = frozen.newProbe();
                        Assert.assertNotSame(a.getSymbolTable(0), b.getSymbolTable(0));
                        futures.add(executor.submit(() -> {
                            // Task submission and the latch publish the completed build.
                            start.await();
                            for (int i = 0; i < 1000; i++) {
                                int key = (i + shift) % 100;
                                a.find(key);
                                b.find((key + 1) % 100);
                                for (int row = 9900 + key; row >= 0; row -= 100) {
                                    Assert.assertTrue(a.hasNext());
                                    a.next();
                                    CharSequence first = a.getRecord().getSymA(0);
                                    b.next();
                                    TestUtils.assertEquals((key & 1) == 0 ? "ES" : "IT", first);
                                    Assert.assertEquals(row + 0.25, a.getRecord().getDouble(1), 0);
                                    TestUtils.assertEquals((key & 1) == 0 ? "IT" : "ES", b.getRecord().getSymB(0));
                                    // Another view's flyweight does not overwrite this one.
                                    TestUtils.assertEquals((key & 1) == 0 ? "ES" : "IT", first);
                                }
                                Assert.assertFalse(a.hasNext());
                                Assert.assertFalse(b.hasNext());
                                a.find(-1);
                                Assert.assertFalse(a.hasNext());
                            }
                            return null;
                        }));
                    }
                    start.countDown();
                    for (Future<?> future : futures) {
                        future.get(30, TimeUnit.SECONDS);
                    }
                } finally {
                    start.countDown();
                    executor.shutdownNow();
                    // Do not release native backing until readers have stopped even on failure.
                    Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
                }
            }
        });
    }

    @Test
    public void testEmptyBuildAndLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(4096);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL)) {
                Symbols symbols = new Symbols("fresh");
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, build.getSizeInBytes());
                Assert.assertThrows(IllegalStateException.class, build::freeze);
                build.open(tracker, NOOP);
                // A SYMBOL payload cannot resolve without a source; the failure closes the build.
                Assert.assertThrows(IllegalArgumentException.class, build::freeze);
                Assert.assertEquals(0, tracker.getUsed());
                build.open(tracker, NOOP);
                FrozenHashJoinBuild.IntKeyed frozen = build.freeze(symbols);
                Assert.assertEquals(0, frozen.getRowCount());
                Assert.assertEquals(0, frozen.getKeyCount());
                Assert.assertEquals(tracker.getUsed(), frozen.getSizeInBytes());
                FrozenHashJoinBuild.IntProbe probe = frozen.newProbe();
                probe.find(Numbers.INT_NULL);
                Assert.assertFalse(probe.hasNext());
                Assert.assertNull(probe.getSymbolTable(0).valueOf(SymbolTable.VALUE_IS_NULL));
                Assert.assertNull(probe.newSymbolTable(0).valueBOf(SymbolTable.VALUE_IS_NULL));
                Assert.assertTrue(probe.getSymbolTable(0).supportsKeyValueAccess());
                Assert.assertThrows(IllegalStateException.class, () -> build.append(1, new Source()));
                Assert.assertThrows(IllegalStateException.class, () -> build.open(tracker, NOOP));
                build.close();
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertThrows(IllegalStateException.class, () -> frozen.newProbe());
                build.open(tracker, NOOP);
                build.append(1, new Source());
                probe = build.freeze(symbols).newProbe();
                probe.find(1);
                probe.next();
                Assert.assertEquals(0, probe.getRecord().getInt(0));
                TestUtils.assertEquals("fresh", probe.getRecord().getSymA(0));
            }
        });
    }

    @Test
    public void testEmptyPayloadPreservesDuplicateCounts() throws Exception {
        assertMemoryLeak(() -> {
            try (IntHashJoinBuild build = newBuild(2, 8)) {
                build.open(null, NOOP);
                for (int i = 0; i < 100; i++) {
                    build.append(Numbers.INT_NULL, new Source());
                }
                FrozenHashJoinBuild.IntProbe probe = build.freeze().newProbe();
                probe.find(Numbers.INT_NULL);
                for (int i = 0; i < 100; i++) {
                    Assert.assertTrue(probe.hasNext());
                    probe.next();
                }
                Assert.assertFalse(probe.hasNext());
            }
        });
    }

    @Test
    public void testHashCollisionsZeroNegativeAndNullKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (IntHashJoinBuild build = newBuild(16, 16, ColumnType.DOUBLE)) {
                build.open(null, NOOP);
                Source source = new Source();
                IntList keys = new IntList();
                keys.add(0);
                keys.add(-1);
                keys.add(Numbers.INT_NULL);
                keys.add(Integer.MAX_VALUE);
                // Force collision and wraparound at the last bucket, below the resize threshold.
                for (int key = 1; keys.size() < 8; key++) {
                    if ((Hash.hashInt64(key) & 15) == 15) {
                        keys.add(key);
                    }
                }
                for (int i = 0; i < keys.size(); i++) {
                    for (int j = 0; j < 3; j++) {
                        source.row = i * 10 + j;
                        build.append(keys.getQuick(i), source);
                    }
                }
                FrozenHashJoinBuild.IntKeyed frozen = build.freeze();
                Assert.assertEquals(8, frozen.getKeyCount());
                Assert.assertEquals(24, frozen.getRowCount());
                FrozenHashJoinBuild.IntProbe probe = frozen.newProbe();
                for (int i = 0; i < keys.size(); i++) {
                    probe.find(keys.getQuick(i));
                    long handle = -1;
                    for (int j = 2; j >= 0; j--) {
                        Assert.assertTrue(probe.hasNext());
                        handle = probe.next();
                        Assert.assertEquals(i * 10 + j + 0.25, probe.getRecord().getDouble(0), 0);
                    }
                    Assert.assertFalse(probe.hasNext());
                    probe.find(-123456);
                    Assert.assertFalse(probe.hasNext());
                    probe.recordAt(handle);
                    Assert.assertEquals(i * 10 + 0.25, probe.getRecord().getDouble(0), 0);
                }
            }
        });
    }

    @Test
    public void testInvalidLayout() {
        Assert.assertThrows(IllegalArgumentException.class, () -> newBuild(3, 16, ColumnType.INT));
        Assert.assertThrows(IllegalArgumentException.class, () -> newBuild(2, 0, ColumnType.INT));
        Assert.assertThrows(IllegalArgumentException.class, () -> newBuild(2, 16, ColumnType.STRING));
        Assert.assertThrows(IllegalArgumentException.class, () -> new IntHashJoinBuild(new ArrayColumnTypes(), indexes(0), 2, 16));
    }

    @Test
    public void testHighSymbolCardinalityAndDuplicateGrowthAccounting() throws Exception {
        assertMemoryLeak(() -> {
            String[] values = new String[8192];
            for (int i = 0; i < values.length; i++) {
                values[i] = "country-with-a-long-name-" + i;
            }
            Symbols symbols = new Symbols(values);
            Source source = new Source();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL);
                 IntHashJoinBuild intBuild = newBuild(2, 16, ColumnType.INT)) {
                long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
                for (long limit : new long[]{4096, 16384, 65536, 0}) {
                    tracker.setLimit(limit);
                    try {
                        build.open(tracker, NOOP);
                        for (int i = 0; i < values.length; i++) {
                            source.symbol = i;
                            build.append(i % 257, source);
                            Assert.assertEquals(build.getSizeInBytes(), tracker.getUsed());
                            Assert.assertEquals(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP) - baseline, tracker.getUsed());
                        }
                        Assert.assertEquals(0, limit);
                        FrozenHashJoinBuild.IntProbe probe = build.freeze(symbols).newProbe();
                        probe.find(0);
                        int matches = 0;
                        while (probe.hasNext()) {
                            probe.next();
                            TestUtils.assertEquals("country-with-a-long-name-" + (31 - matches) * 257, probe.getRecord().getSymA(0));
                            matches++;
                        }
                        Assert.assertEquals(32, matches);
                        // Distinct symbols cost exactly what distinct INT payloads cost: no dictionary copy.
                        intBuild.open(null, NOOP);
                        for (int i = 0; i < values.length; i++) {
                            source.symbol = i;
                            intBuild.append(i % 257, source);
                        }
                        Assert.assertEquals(intBuild.freeze().getSizeInBytes(), build.getSizeInBytes());
                        intBuild.close();
                    } catch (CairoException ex) {
                        Assert.assertTrue(ex.isOutOfMemory());
                        Assert.assertNotEquals(0, limit);
                    } finally {
                        build.close();
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
                }
            }
        });
    }

    @Test
    public void testMemoryLimitAtEveryAllocationAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL, ColumnType.DOUBLE)) {
                // Sweep byte limits through all small hash and row allocations.
                int failures = 0;
                long peak = 0;
                for (long limit = 1; limit <= 16_384; limit++) {
                    tracker.setLimit(limit);
                    try {
                        populate(build, new Source(), tracker, NOOP);
                        Assert.assertEquals(build.getSizeInBytes(), tracker.getUsed());
                        build.close();
                        peak = limit;
                        break;
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        failures++;
                        Assert.assertEquals(0, build.getSizeInBytes());
                    }
                    Assert.assertEquals("all partial build allocations must be released", 0, tracker.getUsed());
                }
                // Every limit below the peak fails, and the peak covers growth of both buffers.
                Assert.assertEquals(peak - 1, failures);
                Assert.assertTrue("peak: " + peak, peak > 256);
                tracker.setLimit(1_000_000);
                populate(build, new Source(), tracker, NOOP);
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testMemoryLimitIncludesHashRehashPeak() throws Exception {
        assertMemoryLeak(() -> {
            // Two slots (16 bytes), 32-byte row capacity. Rehash needs old 16 + new 32 + rows 32 = 80.
            // The final map and two rows fit in 64 bytes, which must still fail during rehash.
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64);
                 IntHashJoinBuild build = newBuild(2, 32, ColumnType.DOUBLE)) {
                build.open(tracker, NOOP);
                build.append(1, new Source());
                Assert.assertEquals(48, tracker.getUsed());
                Assert.assertThrows(CairoException.class, () -> build.append(2, new Source()));
                Assert.assertEquals(0, tracker.getUsed());
                tracker.setLimit(80);
                build.open(tracker, NOOP);
                build.append(1, new Source());
                build.append(2, new Source());
                Assert.assertEquals(64, build.freeze().getSizeInBytes());
            }
        });
    }

    @Test
    public void testMemoryLimitIncludesPayloadGrowthPeak() throws Exception {
        assertMemoryLeak(() -> {
            // Unique key count stays at one: only the duplicate payload buffer grows.
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(48);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.DOUBLE)) {
                build.open(tracker, NOOP);
                build.append(1, new Source());
                // Final 16-byte hash + 32-byte payload fits, old 16-byte payload is still live.
                Assert.assertThrows(CairoException.class, () -> build.append(1, new Source()));
                Assert.assertEquals(0, tracker.getUsed());
                tracker.setLimit(64);
                build.open(tracker, NOOP);
                build.append(1, new Source());
                build.append(1, new Source());
                Assert.assertEquals(48, build.freeze().getSizeInBytes());
            }
        });
    }

    @Test
    public void testNullKeyMatchesExistingHashJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table l (k int)");
            execute("create table r (k int, v double)");
            execute("insert into l values (null), (0), (-1)");
            execute("insert into r values (null, 1.0), (null, 2.0), (0, 3.0), (-1, 4.0)");
            assertQuery("select l.k, r.v from l join r on l.k = r.k order by l.k, r.v")
                    .noLeakCheck().returns("k\tv\nnull\t1.0\nnull\t2.0\n-1\t4.0\n0\t3.0\n");
            try (RecordCursorFactory factory = select("r");
                 IntHashJoinBuild build = new IntHashJoinBuild(new ArrayColumnTypes().add(ColumnType.DOUBLE), indexes(1), 2, 16)) {
                build.open(null, NOOP);
                FrozenHashJoinBuild.IntKeyed frozen;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    frozen = build.build(cursor, 0);
                }
                FrozenHashJoinBuild.IntProbe probe = frozen.newProbe();
                probe.find(Numbers.INT_NULL);
                probe.next();
                Assert.assertEquals(2.0, probe.getRecord().getDouble(0), 0);
                probe.next();
                Assert.assertEquals(1.0, probe.getRecord().getDouble(0), 0);
                Assert.assertFalse(probe.hasNext());
            }
        });
    }

    @Test
    public void testRandomizedGrowthAgainstMultimap() throws Exception {
        assertMemoryLeak(() -> {
            try (IntHashJoinBuild build = newBuild(2, 16, ColumnType.DOUBLE)) {
                Map<Integer, List<Integer>> expected = new HashMap<>();
                build.open(null, NOOP);
                Source record = new Source();
                Rnd rnd = new Rnd(130, 131);
                for (int i = 0; i < 20_000; i++) {
                    int key = rnd.nextInt(2000) - 1000;
                    record.row = i;
                    build.append(key, record);
                    expected.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
                }
                FrozenHashJoinBuild.IntKeyed frozen = build.freeze();
                Assert.assertEquals(expected.size(), frozen.getKeyCount());
                Assert.assertEquals(20_000, frozen.getRowCount());
                FrozenHashJoinBuild.IntProbe probe = frozen.newProbe();
                for (int key = -1100; key <= 1100; key++) {
                    probe.find(key);
                    List<Integer> values = expected.get(key);
                    if (values != null) {
                        for (int i = values.size() - 1; i >= 0; i--) {
                            Assert.assertTrue(probe.hasNext());
                            probe.next();
                            Assert.assertEquals(values.get(i) + 0.25, probe.getRecord().getDouble(0), 0);
                        }
                    }
                    Assert.assertFalse(probe.hasNext());
                }
            }
        });
    }

    @Test
    public void testSourceGetterFailureClosesPartialBuild() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(4096);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL, ColumnType.DOUBLE)) {
                build.open(tracker, NOOP);
                Source source = new Source();
                build.append(1, source);
                Record broken = new Record() {
                    @Override
                    public double getDouble(int col) {
                        throw new IllegalStateException("source getter failed");
                    }

                    @Override
                    public int getInt(int col) {
                        return 0;
                    }
                };
                Assert.assertThrows(IllegalStateException.class, () -> build.append(2, broken));
                Assert.assertEquals(0, tracker.getUsed());
                build.open(tracker, NOOP);
                build.append(2, source);
                Assert.assertEquals(1, build.freeze(new Symbols("ES")).getRowCount());
            }
        });
    }

    @Test
    public void testSymbolPayloadsResolveThroughSourceViews() throws Exception {
        assertMemoryLeak(() -> {
            // Payload columns 0 and 1 read source columns 3 and 5, whose dictionaries differ.
            ArrayColumnTypes types = new ArrayColumnTypes().add(ColumnType.SYMBOL).add(ColumnType.SYMBOL).add(ColumnType.DOUBLE);
            try (IntHashJoinBuild build = new IntHashJoinBuild(types, indexes(3, 5, 0), 2, 16)) {
                build.open(null, NOOP);
                Symbols symbols = new Symbols();
                symbols.put(3, "", "Aa", "BB", "ES", "国家🌞");
                symbols.put(5, "IT", "ES");
                int[][] rows = {{0, 1}, {1, 0}, {3, SymbolTable.VALUE_IS_NULL}, {SymbolTable.VALUE_IS_NULL, 1}, {4, 0}, {3, 1}};
                int[] row = new int[2];
                Record source = new Record() {
                    @Override
                    public double getDouble(int col) {
                        return Double.NaN;
                    }

                    @Override
                    public int getInt(int col) {
                        return col == 3 ? row[0] : col == 5 ? row[1] : -1;
                    }
                };
                for (int i = 0; i < rows.length; i++) {
                    row[0] = rows[i][0];
                    row[1] = rows[i][1];
                    build.append(i, source);
                }
                // Mutating the source after the copy does not change stored keys.
                row[0] = row[1] = 2;
                FrozenHashJoinBuild.IntKeyed frozen = build.freeze(symbols);
                Assert.assertEquals(0, symbols.newSymbolTableCalls);
                FrozenHashJoinBuild.IntProbe probe = frozen.newProbe();
                FrozenHashJoinBuild.IntProbe peer = frozen.newProbe();
                // Each probe takes one view per SYMBOL payload column, from its mapped source column.
                Assert.assertEquals(4, symbols.newSymbolTableCalls);
                Assert.assertEquals("[3,5,3,5]", symbols.requestedColumns.toString());
                for (int i = 0; i < rows.length; i++) {
                    probe.find(i);
                    probe.next();
                    Record record = probe.getRecord();
                    Assert.assertEquals(rows[i][0], record.getInt(0));
                    Assert.assertEquals(rows[i][1], record.getInt(1));
                    TestUtils.assertEquals(symbols.valueOf(3, rows[i][0]), record.getSymA(0));
                    TestUtils.assertEquals(symbols.valueOf(5, rows[i][1]), record.getSymB(1));
                    Assert.assertTrue(Double.isNaN(record.getDouble(2)));
                }
                probe.find(4);
                probe.next();
                CharSequence a = probe.getRecord().getSymA(0);
                peer.find(2);
                peer.next();
                TestUtils.assertEquals("ES", peer.getRecord().getSymA(0));
                TestUtils.assertEquals("国家🌞", a);
                SymbolTable view = probe.getSymbolTable(0);
                Assert.assertSame(view, probe.getSymbolTable(0));
                Assert.assertNotSame(view, peer.getSymbolTable(0));
                Assert.assertNotSame(view, probe.getSymbolTable(1));
                SymbolTable fresh = probe.newSymbolTable(1);
                Assert.assertNotSame(fresh, probe.getSymbolTable(1));
                Assert.assertEquals(5, symbols.newSymbolTableCalls);
                TestUtils.assertEquals("ES", fresh.valueOf(1));
                // Keys outside a dictionary resolve as the source resolves them.
                for (int key : new int[]{-1, SymbolTable.VALUE_NOT_FOUND, 5, Integer.MAX_VALUE}) {
                    Assert.assertNull(view.valueOf(key));
                    Assert.assertNull(fresh.valueBOf(key));
                }
                build.close();
                Assert.assertThrows(AssertionError.class, () -> probe.getSymbolTable(0));
                Assert.assertThrows(AssertionError.class, () -> probe.newSymbolTable(0));
            }
        });
    }

    @Test
    public void testReusableBuildGrowthDoesNotAllocateHeap() throws Exception {
        assertMemoryLeak(() -> {
            com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
            org.junit.Assume.assumeTrue(bean.isThreadAllocatedMemorySupported());
            bean.setThreadAllocatedMemoryEnabled(true);
            ArrayColumnTypes types = new ArrayColumnTypes().add(ColumnType.SYMBOL);
            String[] values = new String[65_536];
            for (int i = 0; i < values.length; i++) {
                values[i] = Integer.toString(i);
            }
            // The source hands out one retained view, so only the build itself can allocate.
            Symbols symbols = new Symbols(values);
            symbols.isViewShared = true;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64 * 1024 * 1024);
                 IntHashJoinBuild build = new IntHashJoinBuild(types, indexes(0), 2, 16, true)) {
                Source source = new Source();
                FrozenHashJoinBuild.IntProbe probe = null;
                long allocated = 0;
                // Keep the setup cardinality fixed while warming the JVM's loop backedges.
                // A short setup can charge VM String/byte[] allocations
                // at the probe-loop backedge to the first measured native growth.
                final int warmupExecutions = 200;
                for (int execution = 0; execution < warmupExecutions + 2; execution++) {
                    long before = bean.getCurrentThreadAllocatedBytes();
                    build.open(tracker, NOOP);
                    int rows = execution < warmupExecutions ? 512 : 65_536;
                    for (int row = 0; row < rows; row++) {
                        source.symbol = row;
                        build.append(row, source);
                    }
                    FrozenHashJoinBuild.IntKeyed snapshot = build.freeze(symbols);
                    if (probe == null) probe = snapshot.newProbe();
                    else probe.reopen();
                    for (int row = 0; row < rows; row++) {
                        Assert.assertTrue(probe.findSingleUnchecked(row));
                        probe.getRecord().getSymA(0).length();
                        probe.find(row);
                        probe.next();
                        probe.getRecord().getSymA(0).length();
                    }
                    build.close();
                    long bytes = bean.getCurrentThreadAllocatedBytes() - before;
                    if (execution >= warmupExecutions) allocated += bytes;
                    Assert.assertEquals(0, tracker.getUsed());
                }
                Assert.assertEquals("fresh builds, unseen symbols and forced native growth", 0, allocated);
            }
        });
    }

    @Test
    public void testReusableSnapshotRequiresExplicitProbeRebinding() throws Exception {
        assertMemoryLeak(() -> {
            ArrayColumnTypes types = new ArrayColumnTypes().add(ColumnType.SYMBOL);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1 << 20);
                 IntHashJoinBuild build = new IntHashJoinBuild(types, indexes(0), 2, 16, true)) {
                Source source = new Source();
                Symbols oldSymbols = new Symbols("old");
                build.open(tracker, NOOP);
                build.append(1, source);
                FrozenHashJoinBuild.IntKeyed snapshot = build.freeze(oldSymbols);
                FrozenHashJoinBuild.IntProbe probe = snapshot.newProbe();
                FrozenHashJoinBuild.IntProbe peer = snapshot.newProbe();
                probe.find(1);
                long oldHandle = probe.next();
                TestUtils.assertEquals("old", probe.getRecord().getSymA(0));
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertThrows(IllegalStateException.class, probe::reopen);

                build.open(tracker, NOOP);
                Symbols newSymbols = new Symbols("new");
                for (int row = 0; row < 4096; row++) {
                    build.append(row, source);
                }
                Assert.assertSame(snapshot, build.freeze(newSymbols));
                Assert.assertThrows(AssertionError.class, () -> probe.find(1));
                Assert.assertThrows(AssertionError.class, probe::next);
                Assert.assertThrows(AssertionError.class, () -> probe.findUnchecked(1));
                Assert.assertThrows(AssertionError.class, () -> probe.findSingleUnchecked(1));
                Assert.assertThrows(AssertionError.class, () -> probe.getSymbolTable(0));
                Assert.assertThrows(AssertionError.class, () -> probe.newSymbolTable(0));
                probe.reopen();
                Assert.assertThrows(AssertionError.class, () -> probe.recordAt(oldHandle));
                // Both table capacity and native backing grew in the new execution.
                probe.findUnchecked(4095);
                Assert.assertTrue(probe.hasNext());
                probe.next();
                TestUtils.assertEquals("new", probe.getRecord().getSymA(0));
                probe.find(4095);
                probe.next();
                TestUtils.assertEquals("new", probe.getRecord().getSymA(0));
                TestUtils.assertEquals("new", probe.newSymbolTable(0).valueOf(0));
                // Only the two original bindings used the previous execution's source.
                Assert.assertEquals(2, oldSymbols.newSymbolTableCalls);
                Assert.assertEquals(2, newSymbols.newSymbolTableCalls);
                // Rebinding one acquired slot must not revive another slot's view.
                Assert.assertThrows(AssertionError.class, () -> peer.find(1));
                peer.reopen();
                peer.find(1);
                peer.next();
                TestUtils.assertEquals("new", peer.getRecord().getSymA(0));
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testSymbolKeyTranslationCachesDistinctKeysAndKeepsEveryBuildRow() throws Exception {
        assertMemoryLeak(() -> {
            // Equal text has different keys in the two dictionaries; IT and FR are absent from the
            // probe, and PT from the build. The build keeps every row: it stores its own keys now.
            Symbols buildSymbols = new Symbols("ES", "IT", "FR", "DE");
            Symbols probeSymbols = new Symbols("DE", "ES", "PT");
            final int nil = SymbolTable.VALUE_IS_NULL;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1 << 20);
                 IntHashJoinBuild build = new IntHashJoinBuild(new ArrayColumnTypes().add(ColumnType.SYMBOL), indexes(0), 2, 16, true);
                 SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                FrozenHashJoinBuild.IntProbe probe = null;
                // The second execution has no duplicate key, leaving a unique build.
                int[][] executions = {{0, 1, 0, nil, 3, 2, 0, 3, nil}, {1, 0, 2, 3, nil}};
                for (int[] keys : executions) {
                    buildSymbols.resetCounts();
                    probeSymbols.resetCounts();
                    build.open(tracker, NOOP);
                    FrozenHashJoinBuild.IntKeyed frozen = build.build(new KeyCursor(buildSymbols, keys), 0, keys.length, -1);
                    Assert.assertEquals(build.getSizeInBytes(), tracker.getUsed());
                    // The build translates nothing, so no lookup happened while it read its rows.
                    Assert.assertEquals(0, buildSymbols.keyOfCalls);
                    Assert.assertEquals(0, probeSymbols.valueOfCalls);
                    // Every row is kept, including the ones whose symbol the probe lacks.
                    Assert.assertEquals(keys.length, frozen.getRowCount());
                    Assert.assertEquals(5, frozen.getKeyCount());
                    Assert.assertEquals(keys.length == 5, frozen.getRowCount() == frozen.getKeyCount());

                    translator.of(3, tracker, NOOP);
                    Assert.assertEquals(3 * Integer.BYTES, translator.getSizeInBytes());
                    view.of(translator, probeSymbols.newTable(0), buildSymbols.newTable(0));
                    if (probe == null) {
                        probe = frozen.newProbe();
                    } else {
                        probe.reopen();
                    }
                    // Probe keys: DE is 0 against the build's 3, ES is 1 against 0, PT is absent
                    // from the build, a probe key past the dictionary reads as null, and null
                    // matches null. Two passes, so the second reads the cache.
                    for (int pass = 0; pass < 2; pass++) {
                        for (int probeKey : new int[]{0, 1, 2, 3, nil}) {
                            final int buildKey = view.translate(probeKey);
                            Assert.assertEquals(probeKey == 0 ? 3 : probeKey == 1 ? 0
                                    : probeKey == 2 ? SymbolTable.VALUE_NOT_FOUND : nil, buildKey);
                            probe.find(buildKey);
                            int matches = 0;
                            while (probe.hasNext()) {
                                probe.next();
                                // The payload keeps the build key, which resolves to the same text.
                                Assert.assertEquals(buildKey, probe.getRecord().getInt(0));
                                TestUtils.assertEquals(buildSymbols.valueOf(0, buildKey), probe.getRecord().getSymA(0));
                                matches++;
                            }
                            int expected = 0;
                            for (int key : keys) {
                                if (key == buildKey) {
                                    expected++;
                                }
                            }
                            Assert.assertEquals(expected, matches);
                        }
                    }
                    // One lookup per distinct cached probe key, none per probed row and none for
                    // null; the key past the dictionary is the only one that resolves every time.
                    Assert.assertEquals(5, buildSymbols.keyOfCalls);
                    Assert.assertEquals(5, probeSymbols.valueOfCalls);
                    view.close();
                    translator.close();
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testSymbolKeyTranslationFailureLeavesTheEntryUnresolved() throws Exception {
        assertMemoryLeak(() -> {
            Symbols buildSymbols = new Symbols("ES", "IT");
            Symbols probeSymbols = new Symbols("IT", "ES");
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1 << 20);
                 SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                translator.of(2, tracker, NOOP);
                view.of(translator, probeSymbols.newTable(0), buildSymbols.newTable(0));
                buildSymbols.failKeyOfAt = 1;
                Assert.assertThrows(IllegalStateException.class, () -> view.translate(0));
                // The entry stays unresolved, so a later probe resolves it rather than reading
                // whatever the failed lookup would have left behind.
                buildSymbols.failKeyOfAt = 0;
                Assert.assertEquals(1, view.translate(0));
                Assert.assertEquals(1, view.translate(0));
                Assert.assertEquals(2, buildSymbols.keyOfCalls);
                view.close();
                translator.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testSymbolKeyTranslatorBoundsMemoryAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(15);
                 SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                Symbols buildSymbols = new Symbols("a", "b", "c", "d");
                Symbols probeSymbols = new Symbols("d", "c", "b", "a");
                // The cache holds four INT keys: 16 bytes exceed the limit and nothing stays charged.
                CairoException error = Assert.assertThrows(CairoException.class, () -> translator.of(4, tracker, NOOP));
                Assert.assertTrue(error.isOutOfMemory());
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, translator.getSizeInBytes());
                translator.close();

                // Cancellation while clearing the cache releases it as well.
                tracker.setLimit(1 << 20);
                CountingSqlExecutionCircuitBreaker cancelled = new CountingSqlExecutionCircuitBreaker(NOOP) {
                    @Override
                    public void statefulThrowExceptionIfTrippedTimeThrottled() {
                        throw CairoException.queryCancelled(1);
                    }
                };
                Assert.assertThrows(CairoException.class, () -> translator.of(4, tracker, cancelled));
                Assert.assertEquals(0, tracker.getUsed());

                translator.of(4, tracker, NOOP);
                Assert.assertEquals(16, tracker.getUsed());
                view.of(translator, probeSymbols.newTable(0), buildSymbols.newTable(0));
                for (int pass = 0; pass < 2; pass++) {
                    for (int key = 0; key < 4; key++) {
                        Assert.assertEquals(3 - key, view.translate(key));
                    }
                }
                Assert.assertEquals(4, buildSymbols.keyOfCalls);
                Assert.assertEquals(SymbolTable.VALUE_IS_NULL, view.translate(SymbolTable.VALUE_IS_NULL));
                Assert.assertEquals(4, buildSymbols.keyOfCalls);
                // A key outside the probe dictionary resolves as the ordinary join resolves it, uncached.
                for (int key : new int[]{4, -1}) {
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, view.translate(key));
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, view.translate(key));
                }
                Assert.assertEquals(8, buildSymbols.keyOfCalls);

                // Rebinding releases the old cache and resolves every key again for the new dictionaries.
                Symbols grownProbe = new Symbols("a", "b", "c", "d", "e", "f", "g", "h");
                Symbols smallBuild = new Symbols("h", "a");
                translator.of(8, tracker, NOOP);
                Assert.assertEquals(32, tracker.getUsed());
                view.of(translator, grownProbe.newTable(0), smallBuild.newTable(0));
                Assert.assertEquals(1, view.translate(0));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, view.translate(3));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, view.translate(3));
                Assert.assertEquals(0, view.translate(7));
                Assert.assertEquals(3, grownProbe.valueOfCalls);

                // An empty probe dictionary needs no cache.
                translator.of(0, tracker, NOOP);
                Assert.assertEquals(0, tracker.getUsed());
                view.of(translator, new Symbols().newTable(0), buildSymbols.newTable(0));
                Assert.assertEquals(SymbolTable.VALUE_IS_NULL, view.translate(SymbolTable.VALUE_IS_NULL));
                view.close();
                view.close();
                translator.close();
                translator.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    private static IntList indexes(int... columns) {
        IntList result = new IntList();
        for (int column : columns) {
            result.add(column);
        }
        return result;
    }

    private static IntHashJoinBuild newBuild(int slots, long rowCapacity, int... columnTypes) {
        ArrayColumnTypes types = new ArrayColumnTypes();
        IntList columns = new IntList();
        for (int i = 0; i < columnTypes.length; i++) {
            types.add(columnTypes[i]);
            columns.add(i);
        }
        return new IntHashJoinBuild(types, columns, slots, rowCapacity);
    }

    private static void populate(IntHashJoinBuild build, Source record, LimitedMemoryTracker tracker, SqlExecutionCircuitBreaker breaker) {
        build.open(tracker, breaker);
        for (int i = 0; i < 12; i++) {
            record.row = i;
            record.symbol = i;
            build.append(i, record);
        }
        FrozenHashJoinBuild.IntKeyed frozen = build.freeze(POPULATED_SYMBOLS);
        Assert.assertEquals(12, frozen.getRowCount());
    }

    /**
     * Attributes each build check to the IntHashJoinBuild method that issued it. It cancels
     * on a second consecutive check from the fail site, i.e. inside a single rehash loop.
     */
    private static class SiteBreaker extends CountingSqlExecutionCircuitBreaker {
        // The build spans three classes of the join package - the build itself, its row heap
        // and their shared native buffer - so attribution takes the first frame of any of them.
        private static final String BUILD_PACKAGE = "io.questdb.griffin.engine.join.";
        private final String failSite;
        private final StackWalker walker = StackWalker.getInstance();
        private int failedRow = -1;
        private int keyRehashChecks;
        private String previousSite = "";
        private int row;
        private int rowChecks;

        private SiteBreaker(String failSite) {
            super(NOOP);
            this.failSite = failSite;
        }

        @Override
        public void statefulThrowExceptionIfTripped() {
            super.statefulThrowExceptionIfTripped();
            onCheck();
        }

        @Override
        public void statefulThrowExceptionIfTrippedNoThrottle() {
            super.statefulThrowExceptionIfTrippedNoThrottle();
            onCheck();
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            super.statefulThrowExceptionIfTrippedTimeThrottled();
            onCheck();
        }

        private void onCheck() {
            final String site = walker.walk(frames -> frames
                    .filter(frame -> frame.getClassName().startsWith(BUILD_PACKAGE))
                    .findFirst()
                    .map(StackWalker.StackFrame::getMethodName)
                    .orElse(""));
            switch (site) {
                case "append", "appendRow", "build" -> rowChecks++;
                case "growKeyTable" -> keyRehashChecks++;
                default -> {
                }
            }
            final boolean isRepeatedLoopCheck = site.equals(previousSite);
            previousSite = site;
            if (isRepeatedLoopCheck && site.equals(failSite)) {
                failedRow = row;
                throw CairoException.queryCancelled(1);
            }
        }
    }

    /** Replays INT build keys from column 0 and resolves SYMBOL columns through the given source. */
    private static class KeyCursor implements RecordCursor {
        private final int[] keys;
        private final Record record = new Record() {
            @Override
            public int getInt(int col) {
                return keys[row];
            }
        };
        private final Symbols symbols;
        private int row = -1;

        private KeyCursor(Symbols symbols, int[] keys) {
            this.symbols = symbols;
            this.keys = keys;
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return symbols.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            return ++row < keys.length;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return symbols.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return keys.length;
        }

        @Override
        public void toTop() {
            row = -1;
        }
    }

    private static class Source implements Record {
        private int row;
        private int symbol;

        @Override
        public double getDouble(int col) {
            return row + 0.25;
        }

        @Override
        public int getInt(int col) {
            return symbol;
        }
    }

    /**
     * Static dictionaries per source column. Column 0 is the default. Counts lookups and view
     * requests, and returns independent views unless a test asks for one retained view.
     */
    private static class Symbols implements SymbolTableSource {
        private final ObjList<ObjList<String>> dictionaries = new ObjList<>();
        private final IntList requestedColumns = new IntList();
        private int failKeyOfAt;
        private boolean isViewShared;
        private int keyOfCalls;
        private int newSymbolTableCalls;
        private Table sharedView;
        private int valueOfCalls;

        private Symbols(String... values) {
            put(0, values);
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return newTable(columnIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            newSymbolTableCalls++;
            requestedColumns.add(columnIndex);
            if (isViewShared) {
                if (sharedView == null) {
                    sharedView = newTable(columnIndex);
                }
                return sharedView;
            }
            return newTable(columnIndex);
        }

        private Table newTable(int columnIndex) {
            return new Table(dictionaries.getQuick(columnIndex));
        }

        private void put(int columnIndex, String... values) {
            ObjList<String> dictionary = new ObjList<>();
            for (String value : values) {
                dictionary.add(value);
            }
            dictionaries.extendAndSet(columnIndex, dictionary);
        }

        private void resetCounts() {
            keyOfCalls = 0;
            valueOfCalls = 0;
        }

        private String valueOf(int columnIndex, int key) {
            ObjList<String> dictionary = dictionaries.getQuick(columnIndex);
            return key >= 0 && key < dictionary.size() ? dictionary.getQuick(key) : null;
        }

        private class Table implements StaticSymbolTable {
            private final ObjList<String> dictionary;

            private Table(ObjList<String> dictionary) {
                this.dictionary = dictionary;
            }

            @Override
            public boolean containsNullValue() {
                return false;
            }

            @Override
            public int getSymbolCount() {
                return dictionary.size();
            }

            @Override
            public int keyOf(CharSequence value) {
                if (++keyOfCalls == failKeyOfAt) {
                    throw new IllegalStateException("injected symbol lookup failure");
                }
                if (value == null) {
                    return SymbolTable.VALUE_IS_NULL;
                }
                for (int i = 0, n = dictionary.size(); i < n; i++) {
                    if (Chars.equals(value, dictionary.getQuick(i))) {
                        return i;
                    }
                }
                return SymbolTable.VALUE_NOT_FOUND;
            }

            @Override
            public CharSequence valueBOf(int key) {
                return valueOf(key);
            }

            @Override
            public CharSequence valueOf(int key) {
                valueOfCalls++;
                return key >= 0 && key < dictionary.size() ? dictionary.getQuick(key) : null;
            }
        }
    }
}
