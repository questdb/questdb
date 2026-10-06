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


package io.questdb.test.cairo.types;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.PhysicalDescriptor;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.idx.CoveringCompressor;
import io.questdb.cairo.lv.LiveViewSnapshotKeyCodec;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.map.RecordValueSink;
import io.questdb.cairo.map.RecordValueSinkFactory;
import io.questdb.cairo.sql.CoveredColumnDecoder;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.UpdateOperatorImpl;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.groupby.GroupByColumnSink;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.std.BinarySequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Coverage of the per-type record-access code: the code generated per type and the opcode functions
 * whose consumers javac cannot check. Every type of the conformance kit takes part, the types
 * registered later included ({@link TypeConformanceTypes}), so a type registered later fails here
 * unless every generator runs for it and every opcode function handles it.
 * <p>
 * The generators ({@link RecordSinkFactory}, {@link RecordValueSinkFactory}, {@link
 * RecordToRowCopierUtils}, {@link RecordComparatorCompiler}) build code for one column of the type,
 * in every sink and copier kind, and the test runs it over a record that answers every getter. The
 * opcode functions are called by reflection, as {@code TypeRelationGoldenTest} does; one that
 * throws or returns its "unhandled" value fails the test, unless the type is on that function's
 * list below. The lists name the existing types a site does not handle, each for a reason; a type
 * registered later is on none of them, and a listed type that becomes handled fails too, so the
 * lists cannot drift.
 */
public class GeneratedAccessorCoverageTest extends AbstractCairoTest {
    private static final Set<String> INTERVALS = Set.of("INTERVAL", "INTERVAL(us)", "INTERVAL(ns)");
    // the generators build code in each of these kinds (0 picks by size)
    private static final int[] KINDS = {
            0,
            RecordSinkFactory.SINK_TYPE_SINGLE_METHOD,
            RecordSinkFactory.SINK_TYPE_CHUNKED,
            RecordSinkFactory.SINK_TYPE_LOOPING
    };
    private static final String NOT_STORED = "VARCHAR_SLICE";
    private final ValueRecord record = new ValueRecord();

    @Test
    public void testComparatorRunsForEveryType() throws Exception {
        // no order: BINARY, arrays, intervals
        final Set<String> unhandled = with(Set.of("BINARY", "DOUBLE[]", "DOUBLE[][]", NOT_STORED), INTERVALS);
        assertMemoryLeak(() -> assertCoverage("comparator", unhandled, entry -> {
            final GenericRecordMetadata metadata = metadataOf(entry.columnType);
            final IntList keys = new IntList();
            keys.add(1);
            final RecordComparator comparator = new RecordComparatorCompiler(new BytecodeAssembler()).newInstance(metadata, keys);
            comparator.setLeft(record);
            Assert.assertEquals(entry.label, 0, comparator.compare(record));
        }));
    }

    @Test
    public void testCopierRunsForEveryType() throws Exception {
        // not column types: intervals and VARCHAR_SLICE have no same-type arm
        final Set<String> unhandled = with(Set.of(NOT_STORED), INTERVALS);
        assertMemoryLeak(() -> {
            for (int kind : KINDS) {
                assertCoverage("copier kind " + kind, unhandled, entry -> {
                    final GenericRecordMetadata metadata = metadataOf(entry.columnType);
                    final EntityColumnFilter filter = new EntityColumnFilter();
                    filter.of(1);
                    final RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(
                            new BytecodeAssembler(),
                            metadata,
                            metadata,
                            filter,
                            configurationOf(kind)
                    );
                    final CountingRow row = new CountingRow();
                    copier.copy(sqlExecutionContext, record, row);
                    Assert.assertTrue(entry.label, row.count > 0);
                });
            }
        });
    }

    @Test
    public void testOpcodeFunctionsHandleEveryType() throws Exception {
        final Map<String, Set<String>> unhandled = new HashMap<>();
        unhandled.put("sinkOpcode", Set.of(NOT_STORED));
        // the fixed-width types a covered read writes, and the four var-size types
        unhandled.put("coveredOpcode", with(Set.of(NOT_STORED), INTERVALS));
        // the SQL event carries no SYMBOL, LONG256, LONG128 or INTERVAL bind variable
        unhandled.put("bindValueOpcode", with(Set.of("SYMBOL", "LONG256", "LONG128", NOT_STORED), INTERVALS));
        unhandled.put("copyOpcode", with(Set.of(NOT_STORED), INTERVALS));
        // UPDATE rejects a LONG256 column at its first row
        unhandled.put("updateOpcode", with(Set.of("LONG256", NOT_STORED), INTERVALS));
        unhandled.put("comparatorOpcode", with(Set.of("BINARY", "DOUBLE[]", "DOUBLE[][]", NOT_STORED), INTERVALS));
        unhandled.put("keyKind", with(Set.of("BINARY", "DOUBLE[]", "DOUBLE[][]", NOT_STORED), INTERVALS));
        // sort-key materialization holds fixed-width values only, IPv4 and the 16- and 32-byte
        // integers excepted
        unhandled.put("materializeOpcode", with(Set.of("STRING", "SYMBOL", "LONG256", "BINARY", "UUID", "LONG128",
                "IPv4", "VARCHAR", "DOUBLE[]", "DOUBLE[][]", NOT_STORED), INTERVALS));
        // covering sidecars hold fixed-width values only
        unhandled.put("codecKind", with(Set.of("STRING", "BINARY", "VARCHAR", "DOUBLE[]", "DOUBLE[][]", NOT_STORED), INTERVALS));
        // the checkpoint key codec has slots up to 8 bytes and no decimal arm
        unhandled.put("byteSizeOfType", with(Set.of("STRING", "LONG256", "BINARY", "UUID", "LONG128", "VARCHAR",
                "DOUBLE[]", "DOUBLE[][]", "DECIMAL8", "DECIMAL16", "DECIMAL32", "DECIMAL64", "DECIMAL128", "DECIMAL256",
                "DECIMAL(5,2)", "DECIMAL(18,3)", NOT_STORED), INTERVALS));

        final Method sink = method(RecordSinkFactory.class, "sinkOpcode", int.class, String.class);
        final Method covered = method(CoveredColumnDecoder.class, "coveredOpcode", int.class);
        final Class<?> walEventWriter = Class.forName("io.questdb.cairo.wal.WalEventWriter");
        final Method bind = method(walEventWriter, "bindValueOpcode", int.class);
        final Method copy = method(RecordToRowCopierUtils.class, "copyOpcode", int.class, int.class);
        final Method update = method(UpdateOperatorImpl.class, "updateOpcode", int.class);
        final Method comparator = method(RecordComparatorCompiler.class, "comparatorOpcode", int.class);
        final Method keyKind = method(SortKeyEncoder.class, "keyKind", int.class);
        final Method materialize = method(Class.forName("io.questdb.griffin.engine.orderby.SortKeyMaterializingRecordCursor"), "materializeOpcode", int.class);
        final Method codec = method(CoveringCompressor.class, "codecKind", int.class);
        final Method slot = method(LiveViewSnapshotKeyCodec.class, "byteSizeOfType", int.class);

        final StringBuilder failures = new StringBuilder();
        for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
            final TypeConformanceTypes.Entry entry = TypeConformanceTypes.ALL.getQuick(i);
            final int type = entry.columnType;
            check(failures, unhandled, "sinkOpcode", entry, () -> (int) sink.invoke(null, type, "column") == constant(RecordSinkFactory.class, "SINK_NONE"));
            check(failures, unhandled, "coveredOpcode", entry, () -> (int) covered.invoke(null, type) == CoveredColumnDecoder.COVERED_NONE);
            check(failures, unhandled, "bindValueOpcode", entry, () -> (int) bind.invoke(null, type) == constant(walEventWriter, "BIND_VALUE_NONE"));
            check(failures, unhandled, "copyOpcode", entry, () -> (int) copy.invoke(null, type, type) == constant(RecordToRowCopierUtils.class, "COPY_NONE"));
            check(failures, unhandled, "updateOpcode", entry, () -> (int) update.invoke(null, type) == constant(UpdateOperatorImpl.class, "UPDATE_NONE"));
            check(failures, unhandled, "comparatorOpcode", entry, () -> {
                comparator.invoke(null, type);
                return false;
            });
            // a later type that does not order like its family has no key kind by design (the
            // guard in keyKind): ORDER BY takes the comparator, which comparatorOpcode checks
            check(failures, unhandled, "keyKind", entry, () -> (int) keyKind.invoke(null, type) == constant(SortKeyEncoder.class, "KIND_NONE")
                    && !(entry.isLater() && !PhysicalDescriptor.isOrderedLikeFamily(PhysicalDescriptor.storedTypeDriverOf(type))));
            check(failures, unhandled, "materializeOpcode", entry, () -> {
                materialize.invoke(null, type);
                return false;
            });
            check(failures, unhandled, "codecKind", entry, () -> {
                codec.invoke(null, type);
                return false;
            });
            check(failures, unhandled, "byteSizeOfType", entry, () -> (int) slot.invoke(null, type) < 0);
            // the group-by column sink appends nothing for a type without an arm: its tag must be
            // the one the type's accessor family is named after, so the type has an arm exactly
            // when its family has one
            if (!NOT_STORED.equals(entry.label) && GroupByColumnSink.argTag(type) != PhysicalDescriptor.accessorOpcodeOf(type)) {
                failures.append("argTag: ").append(entry.label).append(" is not its accessor family's\n");
            }
        }
        Assert.assertEquals("", failures.toString());
    }

    @Test
    public void testRecordSinkRunsForEveryType() throws Exception {
        assertMemoryLeak(() -> {
            for (int kind : KINDS) {
                assertCoverage("sink kind " + kind, Set.of(NOT_STORED), entry -> {
                    final ArrayColumnTypes types = new ArrayColumnTypes();
                    types.add(entry.columnType);
                    final EntityColumnFilter filter = new EntityColumnFilter();
                    filter.of(1);
                    // the column arm, and the key-function arm for every type a column function reads
                    ObjList<Function> functions = null;
                    if (PhysicalDescriptor.accessorOf(entry.columnType) != PhysicalDescriptor.Accessor.SYMBOL) {
                        functions = new ObjList<>();
                        functions.add(ColumnType.getTypeDriver(entry.columnType).newColumnFunction(0, entry.columnType));
                    }
                    final RecordSink sink = RecordSinkFactory.getInstance(configurationOf(kind), new BytecodeAssembler(), types, filter, functions, null);
                    final CountingSinkSpi spi = new CountingSinkSpi();
                    sink.copy(record, spi);
                    Assert.assertEquals(entry.label, functions != null ? 2 : 1, spi.count);
                });
            }
        });
    }

    @Test
    public void testValueSinkRunsForEveryType() throws Exception {
        // a map value holds fixed-width values only
        final Set<String> unhandled = with(Set.of("STRING", "BINARY", "VARCHAR", "DOUBLE[]", "DOUBLE[][]", NOT_STORED), INTERVALS);
        assertMemoryLeak(() -> assertCoverage("value sink", unhandled, entry -> {
            final ArrayColumnTypes types = new ArrayColumnTypes();
            types.add(entry.columnType);
            final EntityColumnFilter filter = new EntityColumnFilter();
            filter.of(1);
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.LONG);
            Assert.assertTrue(entry.label, RecordValueSinkFactory.isSupportedColumnType(entry.columnType));
            final RecordValueSink sink = RecordValueSinkFactory.getInstance(new BytecodeAssembler(), types, filter);
            try (OrderedMap map = (OrderedMap) MapFactory.createOrderedMap(configuration, keyTypes, types)) {
                final MapKey key = map.withKey();
                key.putLong(1);
                final MapValue value = key.createValue();
                sink.copy(record, value);
            }
        }));
    }

    private static void assertCoverage(String site, Set<String> unhandled, EntryCheck check) {
        final StringBuilder failures = new StringBuilder();
        for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
            final TypeConformanceTypes.Entry entry = TypeConformanceTypes.ALL.getQuick(i);
            boolean isHandled;
            String cause = null;
            try {
                check.run(entry);
                isHandled = true;
            } catch (Throwable th) {
                isHandled = false;
                cause = th.getClass().getSimpleName() + ": " + th.getMessage()
                        + (th.getStackTrace().length > 0 ? " at " + th.getStackTrace()[0] : "");
            }
            if (isHandled == unhandled.contains(entry.label)) {
                failures.append(site).append(": ").append(entry.label)
                        .append(isHandled ? " is handled but listed as unhandled" : " is not handled: " + cause)
                        .append('\n');
            }
        }
        Assert.assertEquals("", failures.toString());
    }

    private static void check(StringBuilder failures, Map<String, Set<String>> unhandled, String function, TypeConformanceTypes.Entry entry, UnhandledCheck check) {
        boolean isUnhandled;
        try {
            isUnhandled = check.isUnhandled();
        } catch (InvocationTargetException e) {
            isUnhandled = true;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        if (isUnhandled != unhandled.get(function).contains(entry.label)) {
            failures.append(function).append(": ").append(entry.label)
                    .append(isUnhandled ? " is not handled" : " is handled but listed as unhandled")
                    .append('\n');
        }
    }

    private static CairoConfiguration configurationOf(int kind) {
        return new CairoConfigurationWrapper(configuration) {
            @Override
            public int getCopierType() {
                return kind;
            }
        };
    }

    private static int constant(Class<?> clazz, String name) throws ReflectiveOperationException {
        final Field field = clazz.getDeclaredField(name);
        field.setAccessible(true);
        return field.getInt(null);
    }

    // one column of the type; a SYMBOL column has a symbol table that is not static
    private static GenericRecordMetadata metadataOf(int columnType) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("c", columnType, IndexType.NONE, 0, false, null));
        return metadata;
    }

    private static Method method(Class<?> clazz, String name, Class<?>... parameterTypes) throws NoSuchMethodException {
        final Method method = clazz.getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        return method;
    }

    private static Set<String> with(Set<String> a, Set<String> b) {
        final Set<String> all = new HashSet<>(a);
        all.addAll(b);
        return all;
    }

    @FunctionalInterface
    private interface EntryCheck {
        void run(TypeConformanceTypes.Entry entry) throws Exception;
    }

    @FunctionalInterface
    private interface UnhandledCheck {
        boolean isUnhandled() throws Exception;
    }

    /**
     * Counts every value a sink writes.
     */
    private static class CountingSinkSpi implements RecordSinkSPI {
        int count;

        @Override
        public void putArray(ArrayView view) {
            count++;
        }

        @Override
        public void putBin(BinarySequence value) {
            count++;
        }

        @Override
        public void putBool(boolean value) {
            count++;
        }

        @Override
        public void putByte(byte value) {
            count++;
        }

        @Override
        public void putChar(char value) {
            count++;
        }

        @Override
        public void putDate(long value) {
            count++;
        }

        @Override
        public void putDecimal128(Decimal128 value) {
            count++;
        }

        @Override
        public void putDecimal256(Decimal256 value) {
            count++;
        }

        @Override
        public void putDouble(double value) {
            count++;
        }

        @Override
        public void putFloat(float value) {
            count++;
        }

        @Override
        public void putIPv4(int value) {
            count++;
        }

        @Override
        public void putInt(int value) {
            count++;
        }

        @Override
        public void putInterval(Interval interval) {
            count++;
        }

        @Override
        public void putLong(long value) {
            count++;
        }

        @Override
        public void putLong128(long lo, long hi) {
            count++;
        }

        @Override
        public void putLong256(Long256 value) {
            count++;
        }

        @Override
        public void putLong256(long l0, long l1, long l2, long l3) {
            count++;
        }

        @Override
        public void putRecord(Record value) {
            count++;
        }

        @Override
        public void putShort(short value) {
            count++;
        }

        @Override
        public void putStr(CharSequence value) {
            count++;
        }

        @Override
        public void putStr(CharSequence value, int lo, int hi) {
            count++;
        }

        @Override
        public void putTimestamp(long value) {
            count++;
        }

        @Override
        public void putVarchar(Utf8Sequence value) {
            count++;
        }

        @Override
        public void skip(int bytes) {
            count++;
        }
    }

    /**
     * Counts every value a copier puts.
     */
    private static class CountingRow implements TableWriter.Row {
        int count;

        @Override
        public void append() {
        }

        @Override
        public void cancel() {
        }

        @Override
        public void putArray(int columnIndex, @NotNull ArrayView array) {
            count++;
        }

        @Override
        public void putBin(int columnIndex, long address, long len) {
            count++;
        }

        @Override
        public void putBin(int columnIndex, BinarySequence sequence) {
            count++;
        }

        @Override
        public void putBool(int columnIndex, boolean value) {
            count++;
        }

        @Override
        public void putByte(int columnIndex, byte value) {
            count++;
        }

        @Override
        public void putChar(int columnIndex, char value) {
            count++;
        }

        @Override
        public void putDate(int columnIndex, long value) {
            count++;
        }

        @Override
        public void putDecimal(int columnIndex, Decimal256 value) {
            count++;
        }

        @Override
        public void putDecimal128(int columnIndex, long high, long low) {
            count++;
        }

        @Override
        public void putDecimal256(int columnIndex, long hh, long hl, long lh, long ll) {
            count++;
        }

        @Override
        public void putDecimalChar(int columnIndex, char decimalValue) {
            count++;
        }

        @Override
        public void putDecimalStr(int columnIndex, CharSequence decimalValue) {
            count++;
        }

        @Override
        public void putDecimalVarchar(int columnIndex, Utf8Sequence decimalValue) {
            count++;
        }

        @Override
        public void putDouble(int columnIndex, double value) {
            count++;
        }

        @Override
        public void putFloat(int columnIndex, float value) {
            count++;
        }

        @Override
        public void putGeoHash(int columnIndex, long value) {
            count++;
        }

        @Override
        public void putGeoHashDeg(int columnIndex, double lat, double lon) {
            count++;
        }

        @Override
        public void putGeoStr(int columnIndex, CharSequence value) {
            count++;
        }

        @Override
        public void putGeoVarchar(int columnIndex, Utf8Sequence value) {
            count++;
        }

        @Override
        public void putIPv4(int columnIndex, int value) {
            count++;
        }

        @Override
        public void putInt(int columnIndex, int value) {
            count++;
        }

        @Override
        public void putLong(int columnIndex, long value) {
            count++;
        }

        @Override
        public void putLong128(int columnIndex, long lo, long hi) {
            count++;
        }

        @Override
        public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
            count++;
        }

        @Override
        public void putLong256(int columnIndex, Long256 value) {
            count++;
        }

        @Override
        public void putLong256(int columnIndex, CharSequence hexString) {
            count++;
        }

        @Override
        public void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end) {
            count++;
        }

        @Override
        public void putLong256Utf8(int columnIndex, DirectUtf8Sequence hexString) {
            count++;
        }

        @Override
        public void putLong256Utf8(int columnIndex, Utf8Sequence hexString) {
            count++;
        }

        @Override
        public void putShort(int columnIndex, short value) {
            count++;
        }

        @Override
        public void putStr(int columnIndex, CharSequence value) {
            count++;
        }

        @Override
        public void putStr(int columnIndex, char value) {
            count++;
        }

        @Override
        public void putStr(int columnIndex, CharSequence value, int pos, int len) {
            count++;
        }

        @Override
        public void putStrUtf8(int columnIndex, DirectUtf8Sequence value) {
            count++;
        }

        @Override
        public void putStrUtf8(int columnIndex, Utf8Sequence value) {
            count++;
        }

        @Override
        public void putSym(int columnIndex, CharSequence value) {
            count++;
        }

        @Override
        public void putSym(int columnIndex, char value) {
            count++;
        }

        @Override
        public void putSymIndex(int columnIndex, int key) {
            count++;
        }

        @Override
        public void putSymUtf8(int columnIndex, DirectUtf8Sequence value) {
            count++;
        }

        @Override
        public void putTimestamp(int columnIndex, long value) {
            count++;
        }

        @Override
        public void putUuid(int columnIndex, CharSequence uuid) {
            count++;
        }

        @Override
        public void putUuidUtf8(int columnIndex, Utf8Sequence uuid) {
            count++;
        }

        @Override
        public void putVarchar(int columnIndex, char value) {
            count++;
        }

        @Override
        public void putVarchar(int columnIndex, Utf8Sequence value) {
            count++;
        }
    }

    /**
     * Answers every getter with a value that is not NULL for any type.
     */
    private static class ValueRecord implements Record {
        private final Interval interval = new Interval();
        private final Long256Impl long256 = new Long256Impl();
        private final Utf8String varchar = new Utf8String("v");

        ValueRecord() {
            long256.setAll(1, 2, 3, 4);
            interval.of(1, 2);
        }

        @Override
        public ArrayView getArray(int col, int columnType) {
            return null;
        }

        @Override
        public BinarySequence getBin(int col) {
            return null;
        }

        @Override
        public long getBinLen(int col) {
            return -1;
        }

        @Override
        public boolean getBool(int col) {
            return true;
        }

        @Override
        public byte getByte(int col) {
            return 1;
        }

        @Override
        public char getChar(int col) {
            return 'c';
        }

        @Override
        public long getDate(int col) {
            return 1;
        }

        @Override
        public void getDecimal128(int col, Decimal128 sink) {
            sink.ofRaw(0, 1);
        }

        @Override
        public short getDecimal16(int col) {
            return 1;
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            sink.ofRaw(0, 0, 0, 1);
        }

        @Override
        public int getDecimal32(int col) {
            return 1;
        }

        @Override
        public long getDecimal64(int col) {
            return 1;
        }

        @Override
        public byte getDecimal8(int col) {
            return 1;
        }

        @Override
        public double getDouble(int col) {
            return 1.5;
        }

        @Override
        public float getFloat(int col) {
            return 1.5f;
        }

        @Override
        public byte getGeoByte(int col) {
            return 1;
        }

        @Override
        public int getGeoInt(int col) {
            return 1;
        }

        @Override
        public long getGeoLong(int col) {
            return 1;
        }

        @Override
        public short getGeoShort(int col) {
            return 1;
        }

        @Override
        public int getIPv4(int col) {
            return 1;
        }

        @Override
        public int getInt(int col) {
            return 1;
        }

        @Override
        public Interval getInterval(int col) {
            return interval;
        }

        @Override
        public long getLong(int col) {
            return 1;
        }

        @Override
        public long getLong128Hi(int col) {
            return 1;
        }

        @Override
        public long getLong128Lo(int col) {
            return 1;
        }

        @Override
        public Long256 getLong256A(int col) {
            return long256;
        }

        @Override
        public Long256 getLong256B(int col) {
            return long256;
        }

        @Override
        public short getShort(int col) {
            return 1;
        }

        @Override
        public CharSequence getStrA(int col) {
            return "s";
        }

        @Override
        public CharSequence getStrB(int col) {
            return "s";
        }

        @Override
        public int getStrLen(int col) {
            return 1;
        }

        @Override
        public CharSequence getSymA(int col) {
            return "s";
        }

        @Override
        public CharSequence getSymB(int col) {
            return "s";
        }

        @Override
        public long getTimestamp(int col) {
            return 1;
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            return varchar;
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            return varchar;
        }

        @Override
        public int getVarcharSize(int col) {
            return varchar.size();
        }
    }
}
