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


package io.questdb.cairo.lv;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * One map's worth of {@link LiveViewPartitionKeyClassifier} output: the key column types
 * the map is built from, and the per-column vectors the key sink is generated from.
 * <p>
 * A term reaches a key in one of three shapes, and this class is the only place that
 * decides which:
 * <ul>
 *     <li><b>Verbatim.</b> Anything that is not a SYMBOL, and every SYMBOL outside a live
 *     view - a SYMBOL key in an ordinary query is the table-local int, which is stable for
 *     the one reader that produced it.</li>
 *     <li><b>Resolved string.</b> A live view's SYMBOL term that no dictionary backs: the
 *     key type becomes STRING and the sink writes {@code getSymA}/{@code putStr}. A WAL
 *     segment's symbol ints are transaction-local, so the string is the only identity two
 *     refresh cycles agree on. This is what a CREATE-time validation or EXPLAIN compile
 *     keys through, and what a refresh/repair compile falls back to for a term stage 2
 *     could not bind.</li>
 *     <li><b>LV-private id.</b> A translation candidate whose classifier has a translator
 *     bound: the key type stays SYMBOL and the sink writes the id
 *     {@link LiveViewSymbolIdTranslator#translate} returns for the raw one.</li>
 * </ul>
 * The distinction that matters is the second against the third, and it is not the term's
 * own property: two views can spell one term identically and key it differently, because
 * only one of them has a dictionary behind that column. So the choice lives on the
 * classifier the whole compile shares rather than on anything a site can read off its own
 * metadata.
 *
 * <h2>Index spaces</h2>
 * {@code sinkColumnIndex} is the index the generated sink addresses the term by - the
 * position in the {@link ColumnTypes} handed to {@link #compileKeySink}, which is the
 * term's ordinal in a sink over a {@code VirtualRecord} of the partition functions and the
 * source column's own index in a sink over a record's metadata. Both vectors this class
 * produces are in that space, which is the space {@code RecordSinkFactory} reads
 * {@code writeSymbolAsString} and {@code symbolIdSlotByColumn} in. The <i>slot</i> handed
 * to {@link #addTerm} is in the classifier's space instead, and the two are unrelated.
 */
public final class LiveViewPartitionKeyBinding {
    private final @Nullable LiveViewPartitionKeyClassifier classifier;
    private final boolean isTranslationBound;
    private final ArrayColumnTypes keyColumnTypes;
    private IntList symbolIdSlotByColumn;
    private BitSet writeSymbolAsString;

    /**
     * @param classifier     the compile's classifier, or null for a compile that is not a
     *                       live view's - in which case every term keys verbatim
     * @param keyColumnTypes the list this binding appends each term's key type to. Taken
     *                       rather than owned so a caller with a reusable list can pass it
     *                       straight to the map and the window context.
     */
    public LiveViewPartitionKeyBinding(
            @Nullable LiveViewPartitionKeyClassifier classifier,
            @NotNull ArrayColumnTypes keyColumnTypes
    ) {
        this.classifier = classifier;
        this.isTranslationBound = classifier != null && classifier.isTranslationBound();
        this.keyColumnTypes = keyColumnTypes;
    }

    /**
     * Adds a term the compiler has already classified, looking its slot up rather than
     * admitting it. This is what a site that resolves a term by name against the window's
     * input metadata calls: it may locate the column, but whether that column keys through
     * a dictionary was settled at compile time and is not a property of the type it finds.
     *
     * @param sinkColumnIndex   the term's index in the sink's source column space
     * @param sourceColumnIndex the term's column index in the window's input metadata
     * @param columnType        that column's type
     */
    public void addBoundTerm(int sinkColumnIndex, int sourceColumnIndex, int columnType) {
        addTerm(
                sinkColumnIndex,
                columnType,
                classifier == null
                        ? LiveViewPartitionKeyClassifier.NOT_TRANSLATED
                        : classifier.slotOfSourceColumn(sourceColumnIndex)
        );
    }

    /**
     * Adds a term the caller has already resolved to a window-input column, classifying it.
     *
     * @param sinkColumnIndex   the term's index in the sink's source column space
     * @param sourceColumnIndex the term's column index in the window's input metadata, or a
     *                          negative value when the term is not a column reference
     * @param columnType        the type the source hands the sink
     */
    public void addClassifiedTerm(int sinkColumnIndex, int sourceColumnIndex, int columnType) {
        addTerm(
                sinkColumnIndex,
                columnType,
                classifier == null
                        ? LiveViewPartitionKeyClassifier.NOT_TRANSLATED
                        : classifier.classify(sourceColumnIndex, columnType)
        );
    }

    /**
     * Adds a parsed term, classifying it against the metadata it was parsed with.
     *
     * @param sinkColumnIndex the term's index in the sink's source column space
     * @param term            the parsed PARTITION BY term
     * @param inputMetadata   the window input metadata the term was parsed against
     */
    public void addClassifiedTerm(int sinkColumnIndex, @NotNull Function term, @NotNull RecordMetadata inputMetadata) {
        addTerm(
                sinkColumnIndex,
                term.getType(),
                classifier == null
                        ? LiveViewPartitionKeyClassifier.NOT_TRANSLATED
                        : classifier.classify(term, inputMetadata)
        );
    }

    /**
     * Adds one term, appending its key type and recording how the sink must write it.
     *
     * @param sinkColumnIndex the term's index in the sink's source column space
     * @param columnType      the type the source hands the sink
     * @param slot            the term's classifier slot, or
     *                        {@link LiveViewPartitionKeyClassifier#NOT_TRANSLATED}
     */
    public void addTerm(int sinkColumnIndex, int columnType, int slot) {
        if (classifier == null || !ColumnType.isSymbol(columnType)) {
            keyColumnTypes.add(columnType);
            return;
        }
        if (slot != LiveViewPartitionKeyClassifier.NOT_TRANSLATED && isTranslationBound) {
            if (symbolIdSlotByColumn == null) {
                symbolIdSlotByColumn = new IntList();
            }
            // The generator reads the vector positionally, so every column below this one
            // has to carry an entry even when no term names it.
            while (symbolIdSlotByColumn.size() <= sinkColumnIndex) {
                symbolIdSlotByColumn.add(LiveViewPartitionKeyClassifier.NOT_TRANSLATED);
            }
            symbolIdSlotByColumn.setQuick(sinkColumnIndex, slot);
            keyColumnTypes.add(ColumnType.SYMBOL);
            return;
        }
        if (writeSymbolAsString == null) {
            writeSymbolAsString = new BitSet();
        }
        writeSymbolAsString.set(sinkColumnIndex);
        keyColumnTypes.add(ColumnType.STRING);
    }

    /**
     * Generates the key sink for this binding, which is the translating generator when a
     * term keys as an LV-private id and the ordinary one otherwise. Callers with a
     * function-backed sink use {@link #compileKeySink(CairoConfiguration, BytecodeAssembler,
     * ColumnTypes, ColumnFilter, ObjList)} instead.
     *
     * @param sourceTypes the types the sink reads its source by, which is not necessarily
     *                    {@link #getKeyColumnTypes()}: a sink over a record's metadata reads
     *                    the source's own types, and the key types are what the map holds
     */
    public RecordSink compileKeySink(
            @NotNull CairoConfiguration configuration,
            @NotNull BytecodeAssembler asm,
            ColumnTypes sourceTypes,
            @NotNull ColumnFilter columnFilter
    ) {
        if (symbolIdSlotByColumn == null) {
            return RecordSinkFactory.getInstance(configuration, asm, sourceTypes, columnFilter, writeSymbolAsString);
        }
        rejectMixedSymbolWrites();
        final LiveViewSymbolIdTranslator translator = classifier == null ? null : classifier.getTranslator();
        if (translator == null) {
            // Unreachable: a slot is recorded only while a translator is bound. An ordinary
            // branch rather than an assert, because a sink built with a null translator
            // would not fail here - it would fail per row, deep in a refresh.
            throw CairoException.critical(0)
                    .put("live view translated partition key has no translator bound");
        }
        return RecordSinkFactory.getTranslatingInstance(asm, sourceTypes, columnFilter, symbolIdSlotByColumn, translator);
    }

    /**
     * The function-backed form, for a projector whose terms are compiled expressions rather
     * than columns of the record.
     * <p>
     * A translated term is not written by teaching {@code RecordSinkFactory}'s function-backed
     * generator a translating mode - that generator dispatches purely on
     * {@code keyFunctions.getQuick(i).getType()}, so it never needs one. Instead, the term at
     * a translated position is wrapped in a {@link LiveViewTranslatingFunction}, which reports
     * {@code ColumnType.INT} rather than SYMBOL and computes the translated id in its own
     * {@code getInt}; the untouched generator routes that position to the plain
     * {@code getInt}/{@code putInt} pair it already emits for any INT-typed key function. The
     * wrapping happens on a throwaway copy of {@code keyFunctions} - the caller's own list, and
     * the plain terms it still holds, are untouched and keep their existing owner.
     * <p>
     * Callers that need a sink which never translates - reader-local sinks must not, even when
     * this binding would otherwise translate one of their terms - call
     * {@code RecordSinkFactory.getInstance(configuration, asm, sourceTypes, columnFilter,
     * keyFunctions, writeSymbolAsString)} directly instead of this method, exactly as the
     * column-path's own reader-local sink already does.
     */
    public RecordSink compileKeySink(
            @NotNull CairoConfiguration configuration,
            @NotNull BytecodeAssembler asm,
            ColumnTypes sourceTypes,
            @NotNull ColumnFilter columnFilter,
            @NotNull ObjList<Function> keyFunctions
    ) {
        if (symbolIdSlotByColumn == null) {
            return RecordSinkFactory.getInstance(configuration, asm, sourceTypes, columnFilter, keyFunctions, writeSymbolAsString);
        }
        final LiveViewSymbolIdTranslator translator = classifier == null ? null : classifier.getTranslator();
        if (translator == null) {
            // Unreachable: a slot is recorded only while a translator is bound. An ordinary
            // branch rather than an assert, because a sink built with a null translator
            // would not fail here - it would fail per row, deep in a refresh.
            throw CairoException.critical(0)
                    .put("live view translated partition key has no translator bound");
        }
        final int n = keyFunctions.size();
        final ObjList<Function> translatedFunctions = new ObjList<>(n);
        for (int i = 0; i < n; i++) {
            final int slot = i < symbolIdSlotByColumn.size()
                    ? symbolIdSlotByColumn.getQuick(i)
                    : LiveViewPartitionKeyClassifier.NOT_TRANSLATED;
            final Function term = keyFunctions.getQuick(i);
            translatedFunctions.add(
                    slot == LiveViewPartitionKeyClassifier.NOT_TRANSLATED
                            ? term
                            : new LiveViewTranslatingFunction(term, slot, translator)
            );
        }
        return RecordSinkFactory.getInstance(configuration, asm, sourceTypes, columnFilter, translatedFunctions, writeSymbolAsString);
    }

    public @NotNull ArrayColumnTypes getKeyColumnTypes() {
        return keyColumnTypes;
    }

    /**
     * Returns the per-column translator slots, or null when no term translates. This is the
     * vector {@code RecordSinkFactory.getTranslatingInstance} and
     * {@link LiveViewTranslatingRecord} both take.
     */
    public @Nullable IntList getSymbolIdSlotByColumn() {
        return symbolIdSlotByColumn;
    }

    public @Nullable BitSet getWriteSymbolAsString() {
        return writeSymbolAsString;
    }

    /**
     * Returns true when the key this binding describes differs from what the source hands
     * the sink - a resolved string, or an LV-private id, in place of a raw SYMBOL. A
     * projector whose key is not rewritten needs only one sink, because its reader-local
     * and checkpoint forms are then the same key.
     */
    public boolean isKeyRewritten() {
        return writeSymbolAsString != null || symbolIdSlotByColumn != null;
    }

    /**
     * Returns true when at least one term keys as an LV-private id.
     */
    public boolean isTranslated() {
        return symbolIdSlotByColumn != null;
    }

    private void rejectMixedSymbolWrites() {
        if (writeSymbolAsString != null) {
            // The translating generator has one vocabulary per column and no resolved-string
            // mode, so a key mixing the two would write one of them as a raw id.
            throw CairoException.critical(0)
                    .put("live view partition key mixes translated and resolved-string SYMBOL terms");
        }
    }
}
