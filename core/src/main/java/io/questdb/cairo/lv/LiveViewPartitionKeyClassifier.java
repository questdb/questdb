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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The stage-1 classifier every live-view PARTITION BY term passes through, and the
 * inventory of source columns the terms it admits key by.
 * <p>
 * Six places rewrite a live view's SYMBOL partition term - two in
 * {@code SqlCodeGenerator.generateSelectWindow}, {@link LiveViewWindow#build},
 * {@code LiveViewCheckpointFunctionCompiler}'s column and expression projector paths, and
 * the {@link LiveViewCheckpointKeyProjector} that holds what those produce. They must
 * agree on every term: an anchor map, a window function's own map, a checkpoint row bound
 * and a persisted partition map all compare keys written by different sinks, so a term one
 * of them keys as a resolved string and another as an integer id is not a slower view but
 * a wrong one. Before this class each site decided for itself, off its own metadata, and
 * nothing compared the answers.
 *
 * <h2>Why the decision is split in two</h2>
 * {@link LiveViewCompiledPlan#of} takes an already-compiled factory, so
 * {@link LiveViewCompiledPlan#traceWindowInputColumnToBaseScan} - the only thing that can
 * say which base column a term really reads - runs strictly after the key types and the
 * sink are fixed. The decision therefore splits:
 * <ul>
 *     <li><b>Stage 1</b>, here: a term is a translation candidate when its parsed
 *     {@link Function} is a plain column reference into the window's input metadata and
 *     its type is SYMBOL. That is decidable locally, and it is what fixes the key type and
 *     the sink.</li>
 *     <li><b>Stage 2</b>, once the plan exists: trace each admitted column to a base
 *     writer column and bind it to that column's dictionary
 *     ({@code LiveViewRefreshJob.bindPartitionKeyTranslators}). A live view's own
 *     refresh/repair compile arms this class's translator before stage 1 ever runs
 *     ({@code LiveViewRefreshJob.compileViewSelect}), so {@link #isTranslationBound()} is
 *     what the sites read rather than candidacy alone - it is true for that compile from
 *     construction, and stays false for CREATE-time validation and EXPLAIN, which compile
 *     on the caller's own plain context and so still key every admitted term through its
 *     resolved string.</li>
 * </ul>
 *
 * <h2>The view's own decision outranks both stages</h2>
 * Both stages answer with what this build can prove, and what a build can prove widens and
 * narrows between releases. The key type they settle is persisted, so a term that flips
 * makes a checkpoint's key schema disagree with the compiled runtime and costs the view a
 * rebuild. A view therefore carries the decision it was created with
 * ({@link LiveViewPartitionKeyDecision}), and this class admits a term only when that
 * decision names its source column - so the persisted answer narrows what stage 1 would
 * admit and never widens it. A view with no persisted decision classifies from scratch.
 *
 * <h2>The slot namespace</h2>
 * A slot names one source column's dictionary. This class uses the term's <b>window-input
 * column index</b> as that slot rather than a dense ordinal assigned in classification
 * order, and the reason is the agreement problem above: the six sites classify the same
 * terms against the same window-input metadata, so an index makes two sites structurally
 * incapable of handing one column two slots or two columns one slot, whatever order they
 * run in. A dense ordinal would need every site to share one assignment counter, and a
 * site that missed it would key through the wrong column's dictionary - which is in range
 * for that dictionary, so nothing downstream would reject it.
 * <p>
 * Two window-input columns that trace to one base column would each take a slot and so
 * build two dictionaries for it. That costs memory and buys nothing, but it cannot be
 * wrong: each dictionary is self-consistent, and no key is ever compared across slots.
 * <p>
 * Classification is per term and order-independent, so the inventory {@link #classify}
 * builds may still be growing while an earlier term's key type is already fixed. Nothing
 * reads the inventory as a whole until the compile has finished.
 *
 * <h2>Lifetime</h2>
 * One instance per live-view compile, created before the first partition term is parsed
 * and carried on the compiled {@code WindowRecordCursorFactory} so the refresh path can
 * ask it what the compiler decided instead of deciding again. Non-live-view compiles have
 * none, and a null classifier means "nothing translates" everywhere.
 */
public final class LiveViewPartitionKeyClassifier {
    /**
     * Slot value for a term that keys the way it always did.
     */
    public static final int NOT_TRANSLATED = LiveViewTranslatingRecord.NOT_TRANSLATED;
    // The window-input columns the view's persisted decision allows this compile to admit,
    // or null when the view carries no decision and this compile derives its own. Empty is a
    // decision too: it says the view translates nothing.
    private final @Nullable IntList admittedSourceColumns;
    // Distinct window-input columns admitted by stage 1, in first-seen order. The slot IS
    // the column index; this list is the inventory stage 2 walks, not a lookup table.
    private final IntList sourceColumns = new IntList();
    private final LiveViewSymbolIdTranslator translator;

    /**
     * @param translator            the registry admitted terms translate through, or null
     *                              while no dictionary exists - in which case every admitted
     *                              term still keys through its resolved string and this class
     *                              only records what it would have bound
     * @param admittedSourceColumns the window-input columns the view's persisted decision
     *                              allows, as {@link LiveViewPartitionKeyDecision#admittedSourceColumns}
     *                              resolved them, or null to classify from scratch
     */
    public LiveViewPartitionKeyClassifier(
            @Nullable LiveViewSymbolIdTranslator translator,
            @Nullable IntList admittedSourceColumns
    ) {
        this.translator = translator;
        this.admittedSourceColumns = admittedSourceColumns;
    }

    /**
     * Classifies a term the caller has already resolved to a window-input column, recording
     * it in the inventory when it is admitted.
     *
     * @param sourceColumnIndex the term's column index in the window's input metadata, or a
     *                          negative value when the term is not a column reference
     * @param columnType        that column's type
     * @return the term's slot, or {@link #NOT_TRANSLATED}
     */
    public int classify(int sourceColumnIndex, int columnType) {
        if (sourceColumnIndex < 0 || !ColumnType.isSymbol(columnType)) {
            return NOT_TRANSLATED;
        }
        // The view was created keying this term one way, and it keeps keying it that way for
        // as long as its definition stands. A term the decision does not name keys through
        // its resolved string even when this build would happily admit it, because the key
        // type is what the checkpoint's persisted key schema records and a flip there costs
        // the view a full rebuild from the base table.
        if (admittedSourceColumns != null
                && admittedSourceColumns.indexOf(sourceColumnIndex, 0, admittedSourceColumns.size()) < 0) {
            return NOT_TRANSLATED;
        }
        if (sourceColumns.indexOf(sourceColumnIndex, 0, sourceColumns.size()) < 0) {
            sourceColumns.add(sourceColumnIndex);
        }
        return sourceColumnIndex;
    }

    /**
     * Classifies a parsed term. A memoized column reference is a column reference: the
     * memoizer hands out the same raw id the column would have, so peeling it changes
     * nothing about the key.
     *
     * @param term          the parsed PARTITION BY term, or null
     * @param inputMetadata the metadata the term was parsed against, which every site
     *                      classifying for one view must share
     */
    public int classify(@Nullable Function term, @NotNull RecordMetadata inputMetadata) {
        final ColumnFunction column = term == null ? null : ColumnFunction.unwrap(term);
        if (column == null || !ColumnType.isSymbol(term.getType())) {
            return NOT_TRANSLATED;
        }
        final int index = column.getColumnIndex();
        // A column reference whose index or type does not match the metadata this
        // classifier speaks is a term parsed against something else. Its index would name
        // a different column's dictionary, so refuse it rather than bind it.
        if (index < 0 || index >= inputMetadata.getColumnCount() || !ColumnType.isSymbol(inputMetadata.getColumnType(index))) {
            return NOT_TRANSLATED;
        }
        return classify(index, inputMetadata.getColumnType(index));
    }

    /**
     * Returns the {@code n}-th distinct source column admitted, which is also its slot.
     * Stage 2 walks this inventory to bind each slot to a base column's dictionary.
     */
    public int getSourceColumn(int n) {
        return sourceColumns.getQuick(n);
    }

    /**
     * Returns how many distinct source columns the terms this classifier admitted key by,
     * which is how many dictionaries the view needs.
     */
    public int getSourceColumnCount() {
        return sourceColumns.size();
    }

    public @Nullable LiveViewSymbolIdTranslator getTranslator() {
        return translator;
    }

    /**
     * Returns true when an admitted term actually keys as an LV-private id. False for the
     * CREATE-time validation and EXPLAIN compiles, which build no translator: the
     * classification is recorded, the inventory is built, and the key stays the resolved
     * string it has always been.
     */
    public boolean isTranslationBound() {
        return translator != null;
    }

    /**
     * Returns the slot of a source column this compile already admitted, without admitting
     * anything. This is what a site that resolves a term by name reads: it may locate the
     * column, but the compiler - not the projected type it happens to find there - decides
     * whether that column keys through a dictionary.
     *
     * @param sourceColumnIndex a column index in the same window-input metadata the
     *                          classification ran against
     */
    public int slotOfSourceColumn(int sourceColumnIndex) {
        return sourceColumnIndex >= 0 && sourceColumns.indexOf(sourceColumnIndex, 0, sourceColumns.size()) >= 0
                ? sourceColumnIndex
                : NOT_TRANSLATED;
    }
}
