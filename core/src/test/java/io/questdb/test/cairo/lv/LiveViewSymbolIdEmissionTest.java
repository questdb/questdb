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


package io.questdb.test.cairo.lv;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.lv.LiveViewSymbolIdTranslator;
import io.questdb.cairo.lv.LiveViewTranslatingRecord;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The two mechanisms a live view can emit an LV-private partition-key id through, held to
 * one contract: {@link RecordSinkFactory#getTranslatingInstance} compiles the translator
 * call into the key sink, {@link LiveViewTranslatingRecord} interposes it over the record a
 * vanilla sink reads. A benchmark picks between them on cost; this test is what says they
 * are interchangeable, so the pick stays a cost question.
 * <p>
 * The narrow cases key into {@link Unordered4Map} on purpose. That map's key throws from
 * {@code skip} and from every putter but {@code putInt}, so a sink that emitted a resolved
 * string, or a filter that emitted a skip, fails here rather than silently falling back to
 * {@link OrderedMap} the way today's STRING key does.
 */
public class LiveViewSymbolIdEmissionTest extends AbstractCairoTest {

    @Test
    public void testCompositeKeyTranslatesEachTermThroughItsOwnSlot() throws Exception {
        assertMemoryLeak(() -> {
            // acct-7 and rgn-7 are different strings in different dictionaries, so the same raw
            // id must come out as different LV ids.
            final DictionaryTranslator translator = new DictionaryTranslator(
                    new int[]{100, 101, 102, 103},
                    new int[]{200, 201, 202, 203}
            );
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.SYMBOL);
            keyTypes.add(ColumnType.STRING);
            keyTypes.add(ColumnType.SYMBOL);

            final IntList slots = new IntList();
            slots.add(0);
            slots.add(LiveViewTranslatingRecord.NOT_TRANSLATED);
            slots.add(1);

            final TestRecord record = new TestRecord();
            record.ints[0] = 2;
            record.ints[2] = 3;
            record.strings[1] = "eu-west";

            try (OrderedMap map = new OrderedMap(64 * 1024, keyTypes, new SingleColumnType(ColumnType.LONG), 16, 0.7, Integer.MAX_VALUE)) {
                copyThroughSink(map, translator, keyTypes, slots, record);
                copyThroughFlyweight(map, translator, keyTypes, slots, record);

                Assert.assertEquals(1, map.size());
                try (MapRecordCursor cursor = map.getCursor()) {
                    final Record mapRecord = cursor.getRecord();
                    Assert.assertTrue(cursor.hasNext());
                    // key columns follow the single LONG value column
                    Assert.assertEquals(102, mapRecord.getInt(1));
                    Assert.assertEquals("eu-west", mapRecord.getStrA(2).toString());
                    Assert.assertEquals(203, mapRecord.getInt(3));
                }
            }
        });
    }

    @Test
    public void testNarrowKeyIsIdenticalUnderBothMechanisms() throws Exception {
        assertMemoryLeak(() -> {
            final DictionaryTranslator translator = new DictionaryTranslator(new int[]{7, 3, 9, 4});
            final IntList slots = new IntList();
            slots.add(0);
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.SYMBOL);

            final TestRecord record = new TestRecord();
            try (Unordered4Map map = new Unordered4Map(ColumnType.SYMBOL, new SingleColumnType(ColumnType.LONG), 16, 0.7, Integer.MAX_VALUE)) {
                for (int rawId = 0; rawId < 4; rawId++) {
                    record.ints[0] = rawId;
                    copyThroughSink(map, translator, keyTypes, slots, record);
                    copyThroughFlyweight(map, translator, keyTypes, slots, record);
                }

                // 7, 3, 9, 4 are four distinct LV ids, and the two mechanisms landed on the same
                // entry for each of them rather than on eight.
                Assert.assertEquals(4, map.size());
                final IntHashSet keys = new IntHashSet();
                try (MapRecordCursor cursor = map.getCursor()) {
                    final Record mapRecord = cursor.getRecord();
                    while (cursor.hasNext()) {
                        keys.add(mapRecord.getInt(1));
                    }
                }
                Assert.assertEquals(4, keys.size());
                Assert.assertTrue(keys.contains(3) && keys.contains(4) && keys.contains(7) && keys.contains(9));
            }
        });
    }

    @Test
    public void testNullKeepsItsOwnEncoding() throws Exception {
        assertMemoryLeak(() -> {
            final DictionaryTranslator translator = new DictionaryTranslator(new int[]{7});
            final IntList slots = new IntList();
            slots.add(0);
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.SYMBOL);

            final TestRecord record = new TestRecord();
            record.ints[0] = SymbolTable.VALUE_IS_NULL;

            try (Unordered4Map map = new Unordered4Map(ColumnType.SYMBOL, new SingleColumnType(ColumnType.LONG), 16, 0.7, Integer.MAX_VALUE)) {
                copyThroughSink(map, translator, keyTypes, slots, record);
                copyThroughFlyweight(map, translator, keyTypes, slots, record);

                Assert.assertEquals(1, map.size());
                try (MapRecordCursor cursor = map.getCursor()) {
                    final Record mapRecord = cursor.getRecord();
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, mapRecord.getInt(1));
                }
            }
        });
    }

    @Test
    public void testTranslatingSinkReportsNoDirectColumn() {
        // A direct column tells Unordered4Map.probeBatch it may read the column out of
        // page-frame memory and skip the sink, which would key the raw WAL id. A one-column
        // filter is what sets that index, and a one-column key is this optimization's target
        // shape, so the two would meet.
        final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.SYMBOL);
        final IntList slots = new IntList();
        slots.add(0);

        final RecordSink sink = RecordSinkFactory.getTranslatingInstance(
                new BytecodeAssembler(),
                keyTypes,
                filterOf(1),
                slots,
                new DictionaryTranslator(new int[]{7})
        );
        Assert.assertEquals(-1, sink.getDirectColumnIndex());
    }

    @Test
    public void testTwoTermsOverOneSourceShareADictionary() throws Exception {
        assertMemoryLeak(() -> {
            final DictionaryTranslator translator = new DictionaryTranslator(new int[]{40, 41, 42});
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.SYMBOL);
            keyTypes.add(ColumnType.SYMBOL);

            final IntList slots = new IntList();
            slots.add(0);
            slots.add(0);

            final TestRecord record = new TestRecord();
            record.ints[0] = 1;
            record.ints[1] = 2;

            try (OrderedMap map = new OrderedMap(64 * 1024, keyTypes, new SingleColumnType(ColumnType.LONG), 16, 0.7, Integer.MAX_VALUE)) {
                copyThroughSink(map, translator, keyTypes, slots, record);
                copyThroughFlyweight(map, translator, keyTypes, slots, record);

                Assert.assertEquals(1, map.size());
                try (MapRecordCursor cursor = map.getCursor()) {
                    final Record mapRecord = cursor.getRecord();
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(41, mapRecord.getInt(1));
                    Assert.assertEquals(42, mapRecord.getInt(2));
                }
            }
        });
    }

    @Test
    public void testUnboundSinkStillWritesTheRawId() throws Exception {
        assertMemoryLeak(() -> {
            // A sink compiled without a binding vector must emit exactly what it emitted before
            // the translated mode existed. Pooling the translator entries unconditionally would
            // change every generated class in the tree, so this is the guard on that.
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.SYMBOL);
            final ListColumnFilter filter = filterOf(1);
            final RecordSink sink = RecordSinkFactory.getInstance(
                    RecordSinkFactory.getInstanceClass(new BytecodeAssembler(), keyTypes, filter, null, null),
                    keyTypes,
                    filter,
                    null,
                    null,
                    null,
                    null,
                    null
            );

            final TestRecord record = new TestRecord();
            record.ints[0] = 3;
            try (Unordered4Map map = new Unordered4Map(ColumnType.SYMBOL, new SingleColumnType(ColumnType.LONG), 16, 0.7, Integer.MAX_VALUE)) {
                final MapKey key = map.withKey();
                key.put(record, sink);
                key.createValue();
                try (MapRecordCursor cursor = map.getCursor()) {
                    final Record mapRecord = cursor.getRecord();
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(3, mapRecord.getInt(1));
                }
            }
        });
    }

    @Test
    public void testUnboundColumnsReadThroughTheFlyweight() {
        final DictionaryTranslator translator = new DictionaryTranslator(new int[]{7, 8});
        final IntList slots = new IntList();
        slots.add(LiveViewTranslatingRecord.NOT_TRANSLATED);
        slots.add(0);

        final TestRecord record = new TestRecord();
        record.ints[0] = 1;
        record.ints[1] = 1;
        record.symbols[1] = "acct-1";

        final LiveViewTranslatingRecord flyweight = new LiveViewTranslatingRecord(slots);
        flyweight.of(record, translator);

        Assert.assertEquals(1, flyweight.getInt(0));
        Assert.assertEquals(8, flyweight.getInt(1));
        // the string a raw id names does not change under translation
        Assert.assertEquals("acct-1", flyweight.getSymA(1).toString());
    }

    private static void copyThroughFlyweight(
            io.questdb.cairo.map.Map map,
            LiveViewSymbolIdTranslator translator,
            ArrayColumnTypes keyTypes,
            IntList slots,
            Record record
    ) {
        final ListColumnFilter filter = filterOf(keyTypes.getColumnCount());
        // no writeSymbolAsString bit: the sink emits getInt/putInt, exactly as it does for INT
        final RecordSink sink = RecordSinkFactory.getInstance(
                RecordSinkFactory.getInstanceClass(new BytecodeAssembler(), keyTypes, filter, null, null),
                keyTypes,
                filter,
                null,
                null,
                null,
                null,
                null
        );
        final LiveViewTranslatingRecord flyweight = new LiveViewTranslatingRecord(slots);
        flyweight.of(record, translator);
        final MapKey key = map.withKey();
        key.put(flyweight, sink);
        key.createValue();
    }

    private static void copyThroughSink(
            io.questdb.cairo.map.Map map,
            LiveViewSymbolIdTranslator translator,
            ArrayColumnTypes keyTypes,
            IntList slots,
            Record record
    ) {
        final ListColumnFilter filter = filterOf(keyTypes.getColumnCount());
        final RecordSink sink = RecordSinkFactory.getTranslatingInstance(
                new BytecodeAssembler(),
                keyTypes,
                filter,
                slots,
                translator
        );
        final MapKey key = map.withKey();
        key.put(record, sink);
        key.createValue();
    }

    private static ListColumnFilter filterOf(int columnCount) {
        final ListColumnFilter filter = new ListColumnFilter();
        for (int i = 0; i < columnCount; i++) {
            filter.add(i + 1);
        }
        return filter;
    }

    /**
     * A translator with one fully resolved forward array per slot, which is the steady state
     * every id reaches once it has been interned. Interning itself is step 4's.
     */
    private static class DictionaryTranslator implements LiveViewSymbolIdTranslator {
        private final ObjList<int[]> dictionaries = new ObjList<>();

        private DictionaryTranslator(int[]... dictionaries) {
            for (int[] dictionary : dictionaries) {
                this.dictionaries.add(dictionary);
            }
        }

        @Override
        public int translate(int slot, int rawId) {
            if (rawId == SymbolTable.VALUE_IS_NULL) {
                return SymbolTable.VALUE_IS_NULL;
            }
            if (rawId < 0) {
                throw new IllegalStateException("negative raw symbol id [slot=" + slot + ", rawId=" + rawId + ']');
            }
            return dictionaries.getQuick(slot)[rawId];
        }
    }

    private static class TestRecord implements Record {
        final int[] ints = new int[8];
        final String[] strings = new String[8];
        final String[] symbols = new String[8];

        @Override
        public int getInt(int col) {
            return ints[col];
        }

        @Override
        public CharSequence getStrA(int col) {
            return strings[col];
        }

        @Override
        public int getStrLen(int col) {
            return strings[col] != null ? strings[col].length() : -1;
        }

        @Override
        public CharSequence getSymA(int col) {
            return symbols[col];
        }
    }
}
