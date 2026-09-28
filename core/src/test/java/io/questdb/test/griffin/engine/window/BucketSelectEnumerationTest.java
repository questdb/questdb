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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.PerQueryMemoryTrackerProvider;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;

public class BucketSelectEnumerationTest extends AbstractCairoTest {
    @Test
    public void testMappingMatchesScalarOracle() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            execute("CREATE TABLE tab (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts)");
            final Rnd rnd = new Rnd(42, 79);
            final LongList nonNullRows = new LongList();
            final LongList expected = new LongList();
            try (RecordCursorFactory factory = select("SELECT ts, v FROM tab SUBSAMPLE lttb(v, 2)");
                 DirectLongList dest = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT)) {
                final WindowFunction function = findSelector(factory);
                for (int rows : new int[]{0, 1, 2, 63, 64, 65, 127, 128, 129, 1023, 1024, 1025, 4095, 4096, 4097, 8193}) {
                    for (int pattern = 0; pattern < 8; pattern++) {
                        final boolean[] isNull = new boolean[rows];
                        for (int row = 0; row < rows; row++) {
                            isNull[row] = switch (pattern) {
                                case 0 -> true;
                                case 1 -> row == 0;
                                case 2 -> row < rows - 1;
                                case 3 -> row != 1;
                                case 4 -> (row & 1) == 0;
                                case 5 -> ((row >>> 6) & 1) == 0;
                                case 6 -> row == 0 || row == 63 || row == 64 || row == 65 || row == rows - 1;
                                default -> row == 0 || rnd.nextInt(4) == 0;
                            };
                        }
                        try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                            nonNullRows.clear();
                            final InputRecord record = new InputRecord(isNull);
                            for (int row = 0; row < rows; row++) {
                                record.row = row;
                                function.pass1(record, row, null);
                                if (!isNull[row]) {
                                    nonNullRows.add(row);
                                }
                            }
                            function.preparePass2();
                            // Isolate ordinal translation from the algorithms. Every injected list
                            // contains valid, strictly ascending non-NULL buffer ordinals. Prefix
                            // and suffix selections cluster keeps inside individual mixed words.
                            final DirectLongList selected = (DirectLongList) field(function, "selected").get(function);
                            final int cutoff = rows >>> 6;
                            final int[] randomOrdinals = new int[nonNullRows.size()];
                            for (int distribution = 0; distribution < 4; distribution++) {
                                for (int requested : new int[]{0, 1, 2, Math.max(0, cutoff - 1), cutoff, cutoff + 1, nonNullRows.size()}) {
                                    final int size = Math.min(requested, nonNullRows.size());
                                    // A singleton NULL-prefix fixture has no NULL at all. Its
                                    // identity hint requires every row, irrespective of selected.
                                    if (function.isSelectionAllRows() && size != nonNullRows.size()) {
                                        continue;
                                    }
                                    if (distribution == 3) {
                                        for (int i = 0; i < randomOrdinals.length; i++) {
                                            randomOrdinals[i] = i;
                                        }
                                        for (int i = 0; i < size; i++) {
                                            final int other = i + rnd.nextInt(randomOrdinals.length - i);
                                            final int ordinal = randomOrdinals[i];
                                            randomOrdinals[i] = randomOrdinals[other];
                                            randomOrdinals[other] = ordinal;
                                        }
                                        Arrays.sort(randomOrdinals, 0, size);
                                    }
                                    selected.clear();
                                    expected.clear();
                                    for (int i = 0; i < size; i++) {
                                        final int ordinal = switch (distribution) {
                                            case 0 -> i;
                                            case 1 -> nonNullRows.size() - size + i;
                                            case 2 -> (int) (i * (nonNullRows.size() - 1L) / Math.max(1, size - 1));
                                            default -> randomOrdinals[i];
                                        };
                                        selected.add(ordinal);
                                        expected.add(nonNullRows.getQuick(ordinal));
                                    }
                                    final long reads = assertEnumeration(function, dest, expected);
                                    final long lastRow = size == 0 ? -1 : expected.getQuick(size - 1);
                                    final long expectedReads = size == 0 || nonNullRows.size() == rows
                                            ? 0 : size <= cutoff ? (lastRow >>> 6) + 1 : lastRow + 1;
                                    Assert.assertEquals("rows=" + rows + ", pattern=" + pattern + ", size=" + size
                                            + ", distribution=" + distribution, expectedReads, reads);
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSparseEnumerationReadsBitmapWords() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            for (int rows : new int[]{16_384, 1_000_003}) {
                final String table = "tab" + rows;
                execute("CREATE TABLE " + table + " AS (SELECT x::TIMESTAMP ts, CASE WHEN x = "
                        + (rows / 2 + 1) + " THEN NULL ELSE x END v FROM long_sequence(" + rows + ")) TIMESTAMP(ts)");
                final LongList expectedRows = new LongList();
                expectedRows.add(0);
                expectedRows.add(rows - 1);
                final StringSink expected = new StringSink();
                expected.put("ts\tv\n1970-01-01T00:00:00.000001Z\t1\n");
                MicrosFormatUtils.appendDateTimeUSec(expected, rows);
                expected.put('\t').put(rows).put('\n');
                final ObjList<String> methods = new ObjList<>("lttb", "m4", "minmax");
                for (int method = 0; method < methods.size(); method++) {
                    final String sql = "SELECT ts, v FROM " + table + " SUBSAMPLE " + methods.getQuick(method) + "(v, 2)";
                    assertQuery(sql).assertsPlanContaining("CachedWindowLightSelect");
                    try (RecordCursorFactory factory = select(sql);
                         DirectLongList dest = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT)) {
                        final WindowFunction function = findSelector(factory);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            // Count actual bitmap reads, not checkpoints: the sparse path must
                            // retain the scalar path's cancellation interval of 1024 input rows.
                            Assert.assertEquals((rows + 63L) >>> 6, assertEnumeration(function, dest, expectedRows));
                        }
                        assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns(expected);
                    }
                }
            }
        });
    }

    @Test
    public void testSparseEnumerationMemoryLimitThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            execute("""
                    CREATE TABLE tab AS (
                      SELECT x::TIMESTAMP ts, CASE WHEN x = 2048 THEN NULL ELSE x END v
                      FROM long_sequence(4096)
                    ) TIMESTAMP(ts)
                    """);
            try (RecordCursorFactory factory = select("SELECT ts, v FROM tab SUBSAMPLE lttb(v, 2)")) {
                final WindowFunction function = findSelector(factory);
                MemoryTracker queryTracker;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    queryTracker = sqlExecutionContext.getMemoryTracker();
                    Assert.assertTrue(cursor.hasNext());
                    // Give only the destination a tiny budget, so allocation fails during
                    // enumeration rather than during buffering or algorithm selection.
                    try (PerQueryMemoryTrackerProvider provider = new PerQueryMemoryTrackerProvider(new DefaultCairoConfiguration(root) {
                        @Override
                        public long getQueryMemoryLimitBytes() {
                            return Long.BYTES;
                        }
                    }); MemoryTracker tracker = provider.acquire(AllowAllSecurityContext.INSTANCE, 1, MemoryTrackerWorkload.QUERY)) {
                        try (DirectLongList dest = new DirectLongList(1, MemoryTag.NATIVE_DEFAULT, true)) {
                            dest.setMemoryTracker(tracker);
                            dest.reopen();
                            try {
                                function.getSelectedRows(dest);
                                Assert.fail("expected output-list growth to exceed the memory limit");
                            } catch (CairoException e) {
                                Assert.assertTrue(e.isOutOfMemory());
                                Assert.assertEquals(1, dest.size());
                                Assert.assertEquals(0, dest.get(0));
                                Assert.assertEquals(Long.BYTES, tracker.getUsed());
                            }
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                    try (DirectLongList dest = new DirectLongList(2, MemoryTag.NATIVE_DEFAULT)) {
                        final LongList expected = new LongList();
                        expected.add(0);
                        expected.add(4095);
                        Assert.assertEquals(64, assertEnumeration(function, dest, expected));
                    }
                }
                Assert.assertEquals(0, queryTracker.getUsed());
                Assert.assertEquals(0, engine.getBusyReaderCount());
                assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns("""
                        ts\tv
                        1970-01-01T00:00:00.000001Z\t1
                        1970-01-01T00:00:00.004096Z\t4096
                        """);
            }
        });
    }

    @Test
    public void testSqlIdentityAndFactoryReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab AS (
                      SELECT CASE WHEN x % 127 = 0 THEN NULL
                             ELSE (x / 3 + CASE WHEN x > 2048 THEN 2_000_000 ELSE 0 END)::TIMESTAMP END ts,
                        CASE WHEN x % 7 = 0 OR x BETWEEN 192 AND 256 THEN NULL
                             ELSE 9_007_199_254_740_993 + x % 17 END v,
                        CASE WHEN x % 11 = 0 THEN NULL ELSE ('row-' || x) END::STRING s,
                        CASE WHEN x % 13 = 0 THEN NULL ELSE ('value-' || x) END::VARCHAR vc,
                        x id, x * 7919 % 4097 k
                      FROM long_sequence(4097)
                    )
                    """);
            final ObjList<String> methods = new ObjList<>("lttb(v, $1)", "m4(v, $1)", "minmax(v, $1)", "lttb(v, $1, '1s')");
            for (int method = 0; method < methods.size(); method++) {
                for (int source = 0; source < 3; source++) {
                    final String from = switch (source) {
                        case 0 -> "tab TIMESTAMP(ts)";
                        case 1 -> "(SELECT * FROM tab TIMESTAMP(ts) ORDER BY ts DESC)";
                        default -> "(SELECT * FROM tab TIMESTAMP(ts) ORDER BY k)";
                    };
                    for (int lane = 0; lane < 2; lane++) {
                        final String sql = "SELECT ts, " + (lane == 0 ? "v" : "v::DOUBLE")
                                + " AS v, s, vc, id FROM " + from + " SUBSAMPLE " + methods.getQuick(method);
                        bindVariableService.setLong(0, 2);
                        setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
                        assertQuery(sql).assertsPlanContaining("CachedWindowLightSelect");
                        try (RecordCursorFactory factory = select(sql)) {
                            for (int target : new int[]{2, 17, 63, 64, 65, 128, 5000, 17}) {
                                bindVariableService.setLong(0, target);
                                // The non-LIGHT executor materializes per-row keep flags using
                                // pass2(), not getSelectedRows(). Compare exact rows and payloads,
                                // including tied/NULL timestamps and LONG values beyond 2^53.
                                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "false");
                                final StringSink expected = new StringSink();
                                TestUtils.printSql(engine, sqlExecutionContext, sql, expected);
                                withTimestamp(assertQuery(sql).withPlanNotContaining("CachedWindowLightSelect"), source).returns(expected);
                                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
                                withTimestamp(assertFactory(factory).withContext(sqlExecutionContext), source).returns(expected);
                            }
                        }
                    }
                }
            }
        });
    }

    private static long assertEnumeration(WindowFunction function, DirectLongList dest, LongList expected) throws Exception {
        final Field bitsField = field(function, "nullBits");
        final DirectLongList original = (DirectLongList) bitsField.get(function);
        // Observe reads without adding a production testing hook or changing the stored bitmap.
        try (CountingNullBits bits = new CountingNullBits(original)) {
            bitsField.set(function, bits);
            try {
                dest.ensureCapacity(expected.size() + 1L);
                long previousReads = -1;
                for (int run = 0; run < 2; run++) {
                    dest.add(-1);
                    bits.reads = 0;
                    final long mallocs = Unsafe.getMallocCount();
                    final long reallocs = Unsafe.getReallocCount();
                    function.getSelectedRows(dest);
                    Assert.assertEquals("enumeration needs no scratch allocation", mallocs, Unsafe.getMallocCount());
                    Assert.assertEquals(reallocs, Unsafe.getReallocCount());
                    Assert.assertEquals(expected.size(), dest.size());
                    for (int i = 0; i < expected.size(); i++) {
                        Assert.assertEquals("selection " + i, expected.getQuick(i), dest.get(i));
                    }
                    if (run > 0) {
                        Assert.assertEquals(previousReads, bits.reads);
                    }
                    previousReads = bits.reads;
                }
                return bits.reads;
            } finally {
                bitsField.set(function, original);
            }
        }
    }

    private static Field field(Object object, String name) throws Exception {
        for (Class<?> type = object.getClass(); type != null; type = type.getSuperclass()) {
            try {
                final Field field = type.getDeclaredField(name);
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException ignored) {
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static WindowFunction findSelector(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof CachedWindowLightRecordCursorFactory light) {
                final WindowFunction function = light.getSingleRowSelectingFunction();
                Assert.assertNotNull(function);
                return function;
            }
        }
        throw new AssertionError("expected a cached LIGHT window factory");
    }

    private static QueryAssertion withTimestamp(QueryAssertion assertion, int source) {
        return switch (source) {
            case 0 -> assertion.timestamp("ts");
            case 1 -> assertion.timestampDesc("ts");
            default -> assertion;
        };
    }

    private static class CountingNullBits extends DirectLongList {
        private final DirectLongList source;
        private long reads;

        private CountingNullBits(DirectLongList source) {
            super(0, MemoryTag.NATIVE_DEFAULT, true);
            this.source = source;
        }

        @Override
        public long get(long index) {
            Assert.assertTrue("bitmap read out of bounds: " + index, index >= 0 && index < source.size());
            reads++;
            return source.get(index);
        }
    }

    private static class InputRecord implements Record {
        private final boolean[] isNull;
        private int row;

        private InputRecord(boolean[] isNull) {
            this.isNull = isNull;
        }

        @Override
        public double getDouble(int col) {
            return isNull[row] ? Double.NaN : row + 1;
        }

        @Override
        public long getTimestamp(int col) {
            return isNull[row] && (row & 1) == 0 ? Numbers.LONG_NULL : row + 1;
        }
    }
}
