/********************************************************************************
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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.lv.LiveViewPartitionKeyBinding;
import io.questdb.cairo.lv.LiveViewPartitionKeyClassifier;
import io.questdb.cairo.lv.LiveViewSymbolIdTranslator;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The stage-1 classifier and the per-map binding the six live-view partition-key sites
 * share, held to the classification rule: a term may key as SYMBOL only when it is a direct
 * window-input column reference that resolves to a base-scan SYMBOL column and for which the
 * runtime installs an LV-private translator; a symbol-valued expression stays STRING.
 * <p>
 * What is being pinned is that one rule decides all six, in both of its states: today's,
 * where no dictionary exists and every SYMBOL term keys through its resolved string, and
 * the one a bound translator produces, where a direct base SYMBOL term keys as a 4-byte
 * LV-private id and everything else is untouched. The sites themselves are covered by the
 * live-view suites; this covers the rule they all go through.
 */
public class LiveViewPartitionKeyClassifierTest extends AbstractCairoTest {

    @Test
    public void testByNameSiteCannotInventABinding() {
        // LiveViewWindow.build resolves its terms by name against the window input metadata,
        // and the name is the wrong identity to bind a dictionary on: it names a column of
        // the view rather than the source column the compiler classified. A bound term is
        // one the compile admitted, and nothing else.
        final GenericRecordMetadata metadata = metadata();
        final LiveViewPartitionKeyClassifier classifier =
                new LiveViewPartitionKeyClassifier(new OffsetTranslator());
        Assert.assertEquals(LiveViewPartitionKeyClassifier.NOT_TRANSLATED, classifier.slotOfSourceColumn(0));

        final LiveViewPartitionKeyBinding unclassified = binding(classifier);
        unclassified.addBoundTerm(0, 0, metadata.getColumnType(0));
        assertKeyTypes(unclassified, ColumnType.STRING);
        Assert.assertNull(unclassified.getSymbolIdSlotByColumn());

        // The same column, once the compiler has classified it, binds at the by-name site too.
        classifier.classify(0, metadata.getColumnType(0));
        final LiveViewPartitionKeyBinding bound = binding(classifier);
        bound.addBoundTerm(0, 0, metadata.getColumnType(0));
        assertKeyTypes(bound, ColumnType.SYMBOL);
        Assert.assertEquals(0, bound.getSymbolIdSlotByColumn().getQuick(0));
    }

    @Test
    public void testColumnReferenceIntoAnotherMetadataIsRefused() {
        // A term parsed against different metadata carries an index that names a different
        // column here. Binding it would key the view through the wrong column's dictionary,
        // which is in range for that dictionary and so passes every check downstream.
        final GenericRecordMetadata metadata = metadata();
        final LiveViewPartitionKeyClassifier classifier =
                new LiveViewPartitionKeyClassifier(new OffsetTranslator());
        Assert.assertEquals(
                LiveViewPartitionKeyClassifier.NOT_TRANSLATED,
                classifier.classify(new SymbolColumn(9, true), metadata)
        );
        // In range, but not a SYMBOL there.
        Assert.assertEquals(
                LiveViewPartitionKeyClassifier.NOT_TRANSLATED,
                classifier.classify(new SymbolColumn(2, true), metadata)
        );
        Assert.assertEquals(0, classifier.getSourceColumnCount());
    }

    @Test
    public void testCompositeSymbolKey() {
        // PARTITION BY sym1, sym2: two fixed-width int keys rather than two resolved
        // strings. The map implementation does not change - a two-column key stays on
        // OrderedMap - but the resolution, the writes and the hashing do.
        assertShape(
                new int[]{ColumnType.STRING, ColumnType.STRING},
                new int[]{ColumnType.SYMBOL, ColumnType.SYMBOL},
                0, 1
        );
    }

    @Test
    public void testFunctionBackedTranslationIsRefused() {
        // The expression projector's sink reads compiled functions rather than the record's
        // columns, and the translating generator has no such mode yet. Refusing is what
        // keeps it from quietly writing the raw transaction-local id instead.
        final GenericRecordMetadata metadata = metadata();
        final LiveViewPartitionKeyClassifier classifier =
                new LiveViewPartitionKeyClassifier(new OffsetTranslator());
        final LiveViewPartitionKeyBinding keyBinding = binding(classifier);
        final ObjList<Function> keyFunctions = new ObjList<>();
        keyFunctions.add(new SymbolColumn(0, true));
        keyBinding.addClassifiedTerm(0, keyFunctions.getQuick(0), metadata);

        try {
            keyBinding.compileKeySink(
                    configuration,
                    new BytecodeAssembler(),
                    metadata,
                    new ListColumnFilter(),
                    keyFunctions
            );
            Assert.fail("expected a refusal");
        } catch (CairoException e) {
            Assert.assertTrue(e.getFlyweightMessage().toString(), e.getMessage().contains("function-backed partition key sink"));
        }
    }

    @Test
    public void testMixedKeyKeepsTheExpressionOnString() {
        // PARTITION BY sym1, lower(sym2): the mixed shape both classifiers have to get
        // identical. The direct term translates and the expression beside it does not, and
        // one term being an expression does not make the other one one.
        final GenericRecordMetadata metadata = metadata();
        final LiveViewPartitionKeyClassifier unbound = new LiveViewPartitionKeyClassifier(null);
        final LiveViewPartitionKeyBinding stringKey = binding(unbound);
        stringKey.addClassifiedTerm(0, new SymbolColumn(0, true), metadata);
        stringKey.addClassifiedTerm(1, new StrColumn(2), metadata);
        assertKeyTypes(stringKey, ColumnType.STRING, ColumnType.STRING);

        final LiveViewPartitionKeyClassifier bound = new LiveViewPartitionKeyClassifier(new OffsetTranslator());
        final LiveViewPartitionKeyBinding translatedKey = binding(bound);
        translatedKey.addClassifiedTerm(0, new SymbolColumn(0, true), metadata);
        translatedKey.addClassifiedTerm(1, new StrColumn(2), metadata);
        assertKeyTypes(translatedKey, ColumnType.SYMBOL, ColumnType.STRING);
        Assert.assertEquals(0, translatedKey.getSymbolIdSlotByColumn().getQuick(0));
        Assert.assertEquals(1, bound.getSourceColumnCount());
    }

    @Test
    public void testMixedTranslatedAndResolvedStringIsRefused() {
        // One generated sink writes one vocabulary per column, and the translating generator
        // has no resolved-string mode: a key that needed both would write one of them as a
        // raw id. Unreachable through SQL - an anchored window's PARTITION BY is literals
        // only - and refused rather than left to a future shape.
        final GenericRecordMetadata metadata = metadata();
        final LiveViewPartitionKeyClassifier classifier =
                new LiveViewPartitionKeyClassifier(new OffsetTranslator());
        final LiveViewPartitionKeyBinding keyBinding = binding(classifier);
        keyBinding.addClassifiedTerm(0, new SymbolColumn(0, true), metadata);
        keyBinding.addClassifiedTerm(1, new SymbolConstant("acct-1", 0), metadata);
        assertKeyTypes(keyBinding, ColumnType.SYMBOL, ColumnType.STRING);

        try {
            keyBinding.compileKeySink(configuration, new BytecodeAssembler(), metadata, filterOf(0, 1));
            Assert.fail("expected a refusal");
        } catch (CairoException e) {
            Assert.assertTrue(
                    e.getFlyweightMessage().toString(),
                    e.getMessage().contains("mixes translated and resolved-string SYMBOL terms")
            );
        }
    }

    @Test
    public void testOrdinaryQueryKeepsSymbolVerbatim() {
        // No classifier means no live view, and a SYMBOL key in an ordinary query stays the
        // table-local int it has always been - stable for the one reader that produced it.
        final GenericRecordMetadata metadata = metadata();
        final LiveViewPartitionKeyBinding keyBinding = binding(null);
        keyBinding.addClassifiedTerm(0, new SymbolColumn(0, true), metadata);
        keyBinding.addClassifiedTerm(1, new StrColumn(2), metadata);
        assertKeyTypes(keyBinding, ColumnType.SYMBOL, ColumnType.STRING);
        Assert.assertNull(keyBinding.getSymbolIdSlotByColumn());
        Assert.assertNull(keyBinding.getWriteSymbolAsString());
        Assert.assertFalse(keyBinding.isKeyRewritten());
    }

    @Test
    public void testSingleSymbolKey() {
        // PARTITION BY sym1, the shape that becomes Unordered4Map-eligible once the key is
        // an int rather than a resolved string.
        assertShape(new int[]{ColumnType.STRING}, new int[]{ColumnType.SYMBOL}, 0);
    }

    @Test
    public void testSymbolValuedExpressionStaysString() {
        // A symbol-valued expression has no id space of its own - the ints it hands out
        // index its own map rather than a base column's dictionary - so it keys through its
        // string whether or not a translator is bound, and it claims no dictionary.
        final GenericRecordMetadata metadata = metadata();
        final LiveViewPartitionKeyClassifier classifier =
                new LiveViewPartitionKeyClassifier(new OffsetTranslator());
        final LiveViewPartitionKeyBinding keyBinding = binding(classifier);
        keyBinding.addClassifiedTerm(0, new SymbolConstant("acct-1", 0), metadata);
        assertKeyTypes(keyBinding, ColumnType.STRING);
        Assert.assertNull(keyBinding.getSymbolIdSlotByColumn());
        Assert.assertEquals(0, classifier.getSourceColumnCount());
    }

    @Test
    public void testTranslatedSinkWritesPrivateIds() throws Exception {
        assertMemoryLeak(() -> {
            // The binding's own sink, end to end into the map its key types select. The
            // narrow key keys into a real Unordered4Map, whose key throws from every putter
            // but putInt, so a sink that resolved the string would fail here rather than
            // fall back to OrderedMap the way today's STRING key does.
            final GenericRecordMetadata metadata = metadata();
            final LiveViewPartitionKeyClassifier classifier =
                    new LiveViewPartitionKeyClassifier(new OffsetTranslator());
            final LiveViewPartitionKeyBinding keyBinding = binding(classifier);
            keyBinding.addClassifiedTerm(0, new SymbolColumn(0, true), metadata);
            final RecordSink sink = keyBinding.compileKeySink(
                    configuration,
                    new BytecodeAssembler(),
                    metadata,
                    filterOf(0)
            );
            // A direct column would let Unordered4Map.probeBatch read the raw id straight out
            // of page-frame memory and never call the sink.
            Assert.assertEquals(-1, sink.getDirectColumnIndex());

            final TestRecord record = new TestRecord();
            try (Unordered4Map map = new Unordered4Map(
                    ColumnType.SYMBOL,
                    new SingleColumnType(ColumnType.LONG),
                    16,
                    0.7,
                    Integer.MAX_VALUE
            )) {
                record.ints[0] = 3;
                put(map, record, sink);
                record.ints[0] = SymbolTable.VALUE_IS_NULL;
                put(map, record, sink);

                Assert.assertEquals(2, map.size());
                final IntList keys = new IntList();
                try (MapRecordCursor cursor = map.getCursor()) {
                    final Record mapRecord = cursor.getRecord();
                    while (cursor.hasNext()) {
                        keys.add(mapRecord.getInt(1));
                    }
                }
                // slot 0 offsets by 1000; NULL keeps its own encoding and is never interned
                Assert.assertTrue(keys.toString(), keys.indexOf(1003, 0, keys.size()) >= 0);
                Assert.assertTrue(keys.toString(), keys.indexOf(SymbolTable.VALUE_IS_NULL, 0, keys.size()) >= 0);
            }
        });
    }

    @Test
    public void testTwoTermsOverOneColumnShareASlot() {
        // Reusing a source column across terms - or across windows - reuses its dictionary.
        // Two slots over one column would be two id spaces for one string set, and a key
        // written in one would not compare equal to the same key written in the other.
        final GenericRecordMetadata metadata = metadata();
        final LiveViewPartitionKeyClassifier classifier =
                new LiveViewPartitionKeyClassifier(new OffsetTranslator());
        final int first = classifier.classify(new SymbolColumn(1, true), metadata);
        final int second = classifier.classify(new SymbolColumn(1, true), metadata);
        Assert.assertEquals(first, second);
        Assert.assertEquals(1, classifier.getSourceColumnCount());
        Assert.assertEquals(1, classifier.getSourceColumn(0));
    }

    private static void assertKeyTypes(LiveViewPartitionKeyBinding keyBinding, int... expected) {
        Assert.assertEquals("key width", expected.length, keyBinding.getKeyColumnTypes().getColumnCount());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(
                    "key column " + i,
                    ColumnType.nameOf(expected[i]),
                    ColumnType.nameOf(keyBinding.getKeyColumnTypes().getColumnType(i))
            );
        }
    }

    /**
     * Classifies {@code sourceColumns} of the shared metadata twice - once with no
     * translator, which is every live view today, and once with one bound - and asserts the
     * key each produces.
     */
    private static void assertShape(int[] unboundTypes, int[] boundTypes, int... sourceColumns) {
        final GenericRecordMetadata metadata = metadata();
        final LiveViewPartitionKeyClassifier unbound = new LiveViewPartitionKeyClassifier(null);
        final LiveViewPartitionKeyBinding stringKey = binding(unbound);
        for (int i = 0; i < sourceColumns.length; i++) {
            stringKey.addClassifiedTerm(i, new SymbolColumn(sourceColumns[i], true), metadata);
        }
        assertKeyTypes(stringKey, unboundTypes);
        Assert.assertNull(stringKey.getSymbolIdSlotByColumn());
        Assert.assertTrue(stringKey.isKeyRewritten());
        // The classification is recorded even with nothing to translate through: it is what
        // says which dictionaries the view would need.
        Assert.assertEquals(sourceColumns.length, unbound.getSourceColumnCount());

        final LiveViewPartitionKeyClassifier bound = new LiveViewPartitionKeyClassifier(new OffsetTranslator());
        final LiveViewPartitionKeyBinding translatedKey = binding(bound);
        for (int i = 0; i < sourceColumns.length; i++) {
            translatedKey.addClassifiedTerm(i, new SymbolColumn(sourceColumns[i], true), metadata);
        }
        assertKeyTypes(translatedKey, boundTypes);
        Assert.assertTrue(translatedKey.isTranslated());
        Assert.assertNull(translatedKey.getWriteSymbolAsString());
        for (int i = 0; i < sourceColumns.length; i++) {
            Assert.assertEquals(sourceColumns[i], translatedKey.getSymbolIdSlotByColumn().getQuick(i));
        }
    }

    private static LiveViewPartitionKeyBinding binding(LiveViewPartitionKeyClassifier classifier) {
        return new LiveViewPartitionKeyBinding(classifier, new ArrayColumnTypes());
    }

    private static ListColumnFilter filterOf(int... columnIndexes) {
        final ListColumnFilter filter = new ListColumnFilter();
        for (int columnIndex : columnIndexes) {
            filter.add(columnIndex + 1);
        }
        return filter;
    }

    /**
     * sym1 SYMBOL, sym2 SYMBOL, name STRING, x INT - the window input metadata every site
     * classifying for one view shares.
     */
    private static GenericRecordMetadata metadata() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("sym1", ColumnType.SYMBOL, IndexType.NONE, 0, true, null));
        metadata.add(new TableColumnMetadata("sym2", ColumnType.SYMBOL, IndexType.NONE, 0, true, null));
        metadata.add(new TableColumnMetadata("name", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("x", ColumnType.INT));
        return metadata;
    }

    private static void put(Unordered4Map map, Record record, RecordSink sink) {
        final MapKey key = map.withKey();
        key.put(record, sink);
        key.createValue();
    }

    /**
     * Stands in for the registry step 4 owns: one id space per slot, offset so a key written
     * through the wrong slot is visible rather than plausible.
     */
    private static class OffsetTranslator implements LiveViewSymbolIdTranslator {
        @Override
        public int translate(int slot, int rawId) {
            if (rawId == SymbolTable.VALUE_IS_NULL) {
                return SymbolTable.VALUE_IS_NULL;
            }
            if (rawId < 0) {
                throw new IllegalStateException("negative raw symbol id [slot=" + slot + ", rawId=" + rawId + ']');
            }
            return 1000 * (slot + 1) + rawId;
        }
    }

    private static class TestRecord implements Record {
        final int[] ints = new int[8];

        @Override
        public int getInt(int col) {
            return ints[col];
        }
    }
}
