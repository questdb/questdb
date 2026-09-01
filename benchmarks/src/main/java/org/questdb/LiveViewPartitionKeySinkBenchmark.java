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


package org.questdb;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.lv.LiveViewSymbolIdTranslator;
import io.questdb.cairo.lv.LiveViewTranslatingRecord;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.BinarySequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8Sequence;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * The per-row cost of writing a live view's SYMBOL partition key, across the key domain the
 * view uses today and the two mechanisms that could give it an LV-private integer one.
 * <p>
 * Four modes, all over the same rows and the same key columns:
 * <ul>
 *     <li>{@code STRING} - what ships. The key column types are SYMBOL, the sink carries a
 *     {@code writeSymbolAsString} bit, and it emits {@code getSymA} then {@code putStr}. This
 *     is the {@code LiveViewWindow.build} shape, which is the anchor map's and the cadence
 *     dirty map's.</li>
 *     <li>{@code RAW_INT} - the standing upper bound. Same SYMBOL key column with no bit and
 *     no translation, so the sink emits {@code getInt} then {@code putInt} over the raw WAL
 *     id. Not a correct key - sibling transactions give the same raw id to different strings -
 *     but it is exactly what a translated key would cost if translation were free.</li>
 *     <li>{@code SINK} - the translator call compiled into the key sink
 *     ({@code RecordSinkFactory.getTranslatingInstance}).</li>
 *     <li>{@code RECORD} - a vanilla {@code getInt}/{@code putInt} sink reading through
 *     {@link LiveViewTranslatingRecord}.</li>
 * </ul>
 * Three key shapes: one SYMBOL column, two SYMBOL columns over separate dictionaries, and a
 * SYMBOL beside a STRING - the mixed shape, which is the one where the flyweight also stands
 * between the sink and a column it does not translate.
 * <p>
 * Two methods, because the two questions want different instruments. {@code sinkOnly} writes
 * into a sink SPI that only accumulates, so the difference between {@code SINK} and
 * {@code RECORD} is not buried under a map. {@code intoMap} writes into the map the key
 * schema actually selects - {@link Unordered4Map} for a lone SYMBOL, {@link OrderedMap} for
 * everything else and for every STRING key - so it also carries the map win that the integer
 * key is worth in the first place.
 * <p>
 * The translator resolves every id from a fully interned forward array, which is the steady
 * state an id reaches after its first use. First-use interning is a hash lookup and is not
 * measured here; it is charged separately once the real translator exists.
 * <p>
 * Run with:
 * <pre>
 * mvn clean package -DskipTests -pl benchmarks -am
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
 *     -jar benchmarks/target/benchmarks.jar LiveViewPartitionKeySinkBenchmark
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G", "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"})
public class LiveViewPartitionKeySinkBenchmark {
    private static final int COL_AMOUNT = 3;
    private static final int COL_KEY1 = 0;
    private static final int COL_KEY2 = 1;
    private static final int COL_TAG = 2;
    private static final int COL_TS = 4;
    // The second key component is deliberately narrow, matching the region_id
    // LiveViewSteadyStateBenchmark's composite shape adds: it keeps the joint key count
    // bounded so the map reaches a steady size within an iteration.
    private static final int KEY2_CARDINALITY = 8;
    private static final int KEY_CARDINALITY = 10_000;
    private static final int ROW_MASK = 0xFFFF;
    private static final int ROWS = ROW_MASK + 1;
    private static final int SEED = 0x5eed;
    private final AccumulatingSink accumulatingSink = new AccumulatingSink();
    private final SteadyStateTranslator translator = new SteadyStateTranslator();
    @Param({"STRING", "RAW_INT", "SINK", "RECORD"})
    public String mode;
    @Param({"SINGLE", "COMPOSITE", "MIXED"})
    public String shape;
    private LiveViewTranslatingRecord flyweight;
    private Map map;
    private int rowCursor;
    private BaseRowRecord row;
    private RecordSink sink;
    private Record sinkInput;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LiveViewPartitionKeySinkBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void clearMap() {
        map.clear();
        rowCursor = 0;
    }

    @TearDown(Level.Trial)
    public void close() {
        map = Misc.free(map);
    }

    @Benchmark
    public long intoMap() {
        row.rowIndex = rowCursor++ & ROW_MASK;
        final MapKey key = map.withKey();
        key.put(sinkInput, sink);
        final MapValue value = key.createValue();
        value.addLong(0, 1);
        return value.getLong(0);
    }

    @Setup(Level.Trial)
    public void setup() {
        final Rnd rnd = new Rnd(SEED, SEED);
        final String[] key1Symbols = new String[KEY_CARDINALITY];
        for (int i = 0; i < KEY_CARDINALITY; i++) {
            key1Symbols[i] = "acct-" + (100_000 + i);
        }
        final String[] key2Symbols = new String[KEY2_CARDINALITY];
        for (int i = 0; i < KEY2_CARDINALITY; i++) {
            key2Symbols[i] = "rgn-" + i;
        }

        final int[] key1Ids = new int[ROWS];
        final int[] key2Ids = new int[ROWS];
        for (int i = 0; i < ROWS; i++) {
            key1Ids[i] = rnd.nextPositiveInt() % KEY_CARDINALITY;
            key2Ids[i] = rnd.nextPositiveInt() % KEY2_CARDINALITY;
        }

        translator.of(new int[][]{lvIds(rnd, KEY_CARDINALITY), lvIds(rnd, KEY2_CARDINALITY)});
        row = new BaseRowRecord(key1Ids, key2Ids, key1Symbols, key2Symbols);

        final boolean isComposite = "COMPOSITE".equals(shape);
        final boolean isMixed = "MIXED".equals(shape);
        final boolean isStringKey = "STRING".equals(mode);

        // The sink reads the base row's own columns through a filter, the way the anchor map's
        // sink does, rather than a pre-projected key record.
        final ArrayColumnTypes sourceTypes = new ArrayColumnTypes();
        sourceTypes.add(ColumnType.SYMBOL);     // COL_KEY1
        sourceTypes.add(ColumnType.SYMBOL);     // COL_KEY2
        sourceTypes.add(ColumnType.STRING);     // COL_TAG
        sourceTypes.add(ColumnType.LONG);       // COL_AMOUNT
        sourceTypes.add(ColumnType.TIMESTAMP);  // COL_TS

        final ListColumnFilter filter = new ListColumnFilter();
        final ArrayColumnTypes mapKeyTypes = new ArrayColumnTypes();
        final IntList slotByColumn = new IntList();
        for (int i = 0, n = sourceTypes.getColumnCount(); i < n; i++) {
            slotByColumn.add(LiveViewTranslatingRecord.NOT_TRANSLATED);
        }

        filter.add(COL_KEY1 + 1);
        mapKeyTypes.add(isStringKey ? ColumnType.STRING : ColumnType.SYMBOL);
        if (isComposite) {
            filter.add(COL_KEY2 + 1);
            mapKeyTypes.add(isStringKey ? ColumnType.STRING : ColumnType.SYMBOL);
        } else if (isMixed) {
            filter.add(COL_TAG + 1);
            mapKeyTypes.add(ColumnType.STRING);
        }

        switch (mode) {
            case "STRING": {
                final io.questdb.std.BitSet writeSymbolAsString = new io.questdb.std.BitSet();
                writeSymbolAsString.set(COL_KEY1);
                if (isComposite) {
                    writeSymbolAsString.set(COL_KEY2);
                }
                sink = RecordSinkFactory.getInstance(
                        configuration(),
                        new BytecodeAssembler(),
                        sourceTypes,
                        filter,
                        writeSymbolAsString
                );
                sinkInput = row;
                break;
            }
            case "RAW_INT":
                sink = RecordSinkFactory.getInstance(
                        configuration(),
                        new BytecodeAssembler(),
                        sourceTypes,
                        filter,
                        null
                );
                sinkInput = row;
                break;
            case "SINK":
                slotByColumn.setQuick(COL_KEY1, 0);
                if (isComposite) {
                    slotByColumn.setQuick(COL_KEY2, 1);
                }
                sink = RecordSinkFactory.getTranslatingInstance(
                        new BytecodeAssembler(),
                        sourceTypes,
                        filter,
                        slotByColumn,
                        translator
                );
                sinkInput = row;
                break;
            case "RECORD":
                slotByColumn.setQuick(COL_KEY1, 0);
                if (isComposite) {
                    slotByColumn.setQuick(COL_KEY2, 1);
                }
                sink = RecordSinkFactory.getInstance(
                        configuration(),
                        new BytecodeAssembler(),
                        sourceTypes,
                        filter,
                        null
                );
                flyweight = new LiveViewTranslatingRecord(slotByColumn);
                flyweight.of(row, translator);
                sinkInput = flyweight;
                break;
            default:
                throw new IllegalArgumentException("unknown mode: " + mode);
        }

        // AnchorMapValueTypes: LONG + BYTE + BYTE + SHORT, the 12 value bytes that leave the
        // narrow anchor entry at exactly the 16-byte embedded unordered-map limit.
        final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);
        valueTypes.add(ColumnType.BYTE);
        valueTypes.add(ColumnType.BYTE);
        valueTypes.add(ColumnType.SHORT);

        if (mapKeyTypes.getColumnCount() == 1 && !isStringKey) {
            map = new Unordered4Map(ColumnType.SYMBOL, valueTypes, 64, 0.7, Integer.MAX_VALUE);
        } else {
            map = new OrderedMap(4 * 1024 * 1024, mapKeyTypes, valueTypes, 64, 0.7, Integer.MAX_VALUE);
        }
    }

    @Benchmark
    public long sinkOnly() {
        row.rowIndex = rowCursor++ & ROW_MASK;
        sink.copy(sinkInput, accumulatingSink);
        return accumulatingSink.checksum;
    }

    private static DefaultCairoConfiguration configuration() {
        return new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));
    }

    /**
     * A forward array holding a permutation of the same id range the raw ids come from. The
     * permutation is what makes a row's LV id differ from its base id; keeping the range is
     * what keeps {@code RAW_INT} a control rather than a second experiment. Ids offset into
     * their own range would have given the map a different key distribution, and the mode
     * would then have been measuring that as much as the translation.
     */
    private static int[] lvIds(Rnd rnd, int cardinality) {
        final int[] ids = new int[cardinality];
        for (int i = 0; i < cardinality; i++) {
            ids[i] = i;
        }
        for (int i = cardinality - 1; i > 0; i--) {
            final int j = rnd.nextPositiveInt() % (i + 1);
            final int swap = ids[i];
            ids[i] = ids[j];
            ids[j] = swap;
        }
        return ids;
    }

    /**
     * Consumes what the sink writes without a map behind it, so the emission mechanisms can be
     * compared without the map's own cost on top. It accumulates rather than discards: a sink
     * whose writes are dead is a sink the JIT is free to delete.
     */
    private static class AccumulatingSink implements RecordSinkSPI {
        long checksum;

        @Override
        public void putArray(ArrayView array) {
        }

        @Override
        public void putBin(BinarySequence binarySequence) {
        }

        @Override
        public void putBool(boolean value) {
        }

        @Override
        public void putByte(byte value) {
        }

        @Override
        public void putChar(char value) {
        }

        @Override
        public void putDate(long value) {
        }

        @Override
        public void putDecimal128(Decimal128 value) {
        }

        @Override
        public void putDecimal256(Decimal256 value) {
        }

        @Override
        public void putDouble(double value) {
        }

        @Override
        public void putFloat(float value) {
        }

        @Override
        public void putIPv4(int value) {
            checksum += value;
        }

        @Override
        public void putInt(int value) {
            checksum += value;
        }

        @Override
        public void putInterval(Interval interval) {
        }

        @Override
        public void putLong(long value) {
            checksum += value;
        }

        @Override
        public void putLong128(long lo, long hi) {
        }

        @Override
        public void putLong256(Long256 value) {
        }

        @Override
        public void putLong256(long l0, long l1, long l2, long l3) {
        }

        @Override
        public void putRecord(Record value) {
        }

        @Override
        public void putShort(short value) {
        }

        @Override
        public void putStr(CharSequence value) {
            checksum += value.length();
            checksum += value.charAt(0);
        }

        @Override
        public void putStr(CharSequence value, int lo, int hi) {
            checksum += hi - lo;
        }

        @Override
        public void putTimestamp(long value) {
            checksum += value;
        }

        @Override
        public void putVarchar(CharSequence value) {
            checksum += value.length();
        }

        @Override
        public void putVarchar(Utf8Sequence value) {
            checksum += value.size();
        }

        @Override
        public void skip(int bytes) {
        }
    }

    /**
     * One base row as a WAL page-frame cursor presents it: raw symbol ids in the SYMBOL
     * columns, and a symbol table behind {@code getSymA} for the STRING path to resolve
     * through.
     */
    private static class BaseRowRecord implements Record {
        private final int[] key1Ids;
        private final String[] key1Symbols;
        private final int[] key2Ids;
        private final String[] key2Symbols;
        int rowIndex;

        private BaseRowRecord(int[] key1Ids, int[] key2Ids, String[] key1Symbols, String[] key2Symbols) {
            this.key1Ids = key1Ids;
            this.key2Ids = key2Ids;
            this.key1Symbols = key1Symbols;
            this.key2Symbols = key2Symbols;
        }

        @Override
        public int getInt(int col) {
            return col == COL_KEY1 ? key1Ids[rowIndex] : key2Ids[rowIndex];
        }

        @Override
        public long getLong(int col) {
            return rowIndex;
        }

        @Override
        public CharSequence getStrA(int col) {
            return key2Symbols[key2Ids[rowIndex]];
        }

        @Override
        public int getStrLen(int col) {
            return getStrA(col).length();
        }

        @Override
        public CharSequence getSymA(int col) {
            return col == COL_KEY1 ? key1Symbols[key1Ids[rowIndex]] : key2Symbols[key2Ids[rowIndex]];
        }

        @Override
        public long getTimestamp(int col) {
            return rowIndex;
        }
    }

    /**
     * The steady-state half of section 5's translator: the epoch guard, the NULL encoding, the
     * clean/dirty band split, and a forward array lookup. Every id here is clean and already
     * interned, so the dirty branch is present but never taken - which is the shape the hot
     * path has once a refresh cycle is warm.
     */
    private static class SteadyStateTranslator implements LiveViewSymbolIdTranslator {
        private int[][] baseIdToLvId;
        private int[] cleanSymbolCount;
        private int epoch = 1;
        private int[] slotEpoch;

        @Override
        public int translate(int slot, int rawId) {
            if (slotEpoch[slot] != epoch) {
                throw new IllegalStateException("slot not armed for this source [slot=" + slot + ']');
            }
            if (rawId == SymbolTable.VALUE_IS_NULL) {
                return SymbolTable.VALUE_IS_NULL;
            }
            if (rawId < 0) {
                throw new IllegalStateException("negative raw symbol id [slot=" + slot + ", rawId=" + rawId + ']');
            }
            if (rawId < cleanSymbolCount[slot]) {
                return baseIdToLvId[slot][rawId];
            }
            throw new IllegalStateException("dirty band is not exercised by this benchmark");
        }

        void of(int[][] dictionaries) {
            this.baseIdToLvId = dictionaries;
            this.cleanSymbolCount = new int[dictionaries.length];
            this.slotEpoch = new int[dictionaries.length];
            for (int i = 0; i < dictionaries.length; i++) {
                cleanSymbolCount[i] = dictionaries[i].length;
                slotEpoch[i] = epoch;
            }
        }
    }
}
