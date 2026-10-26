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

package io.questdb.griffin.codegen;

import io.questdb.griffin.IntervalExtractor;
import io.questdb.griffin.ParquetPushdownExtractor;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.join.DomainKeepingEmptyRecordCursorFactory;
import io.questdb.griffin.engine.join.SharedRecordCursorFactory;
import io.questdb.griffin.engine.table.MasterSuffixLimitFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowMapSpec;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.BitSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * State of one {@link SqlCodeGenerator#generate} call. Function instantiation can compile a
 * sub-query, which re-enters generation before the outer call finishes, so the generator keeps
 * one frame per nesting depth and passes it explicitly to the operator generators, which keep
 * no state across a nested generation. The frame owns the map key and value types, the two
 * column filters, the symbol-as-string set and the join key layout that an operator fills before
 * it instantiates functions and reads after, so a sub-query generated in between, one frame
 * deeper, cannot overwrite them.
 */
final class GenerationFrame implements Closeable, Mutable {
    private static final String FREED_SHARED_FACTORY = "shared cursor reads a factory the generator freed";
    final IntList columnReferenceCounts = new IntList();
    final ObjList<BoundExpression> expressions = new ObjList<>();
    final ObjList<Function> functions = new ObjList<>(2);
    final IntList intListA = new IntList();
    final IntList intListB = new IntList();
    final IntList intListC = new IntList();
    final IntervalExtractor intervals;
    final JoinKeyLayout joinKeys;
    final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
    final ListColumnFilter listColumnFilterA = new ListColumnFilter();
    final ListColumnFilter listColumnFilterB = new ListColumnFilter();
    final GenericRecordMetadata projectionScopeMetadata = new GenericRecordMetadata();
    final ParquetPushdownExtractor pushdown = new ParquetPushdownExtractor(expressions, intListB);
    final OutputSchema schema = new OutputSchema();
    final BitSet symbolAsString = new BitSet();
    final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
    final IntList windowFunctionPositions = new IntList();
    final ObjObjHashMap<IntList, ObjList<WindowFunction>> windowGroups = new ObjObjHashMap<>();
    final ObjList<TableColumnMetadata> windowMetadata = new ObjList<>();
    final ObjList<TableColumnMetadata> windowOutputColumns = new ObjList<>();
    final WindowFactoryGenerator.WindowPartitionKeys windowPartitionKeys = new WindowFactoryGenerator.WindowPartitionKeys();
    final ObjList<WindowFunction> windowSpecFunctions = new ObjList<>();
    final ObjList<WindowMapSpec> windowSpecs = new ObjList<>();
    /**
     * The entries of the decorrelation domains whose factories a read built before the generator reached them, which
     * the frame owns until the generator reaches them, see {@link #claimProducer}.
     */
    private final IntList pendingProducers = new IntList();
    /**
     * The entries of the decorrelation domains the generator placed in the factory tree, in the order it placed them,
     * see {@link #producerMark}.
     */
    private final IntList placedProducers = new IntList();
    private final ObjList<PreparedFilter> preparedFilters = new ObjList<>();
    private final ObjList<TableColumnMetadata> projectionSlotColumns = new ObjList<>();
    /**
     * The next shared id of the factory at each entry.
     */
    private final IntList sharedConsumerCounts = new IntList();
    /**
     * The entry of the factory each of {@link #sharedConsumers} reads.
     */
    private final IntList sharedConsumerEntries = new IntList();
    /**
     * The shared cursors the frame handed out, see {@link #shareCursor}.
     */
    private final ObjList<SharedRecordCursorFactory> sharedConsumers = new ObjList<>();
    /**
     * The factory the frame built for each of {@link #sharedPlans}.
     */
    private final ObjList<RecordCursorFactory> sharedFactories = new ObjList<>();
    /**
     * The plans whose factories the frame built and other consumers may share, one entry each: the input of each join
     * input, the head of each set operation and each decorrelation domain with readers.
     */
    private final ObjList<LogicalPlan> sharedPlans = new ObjList<>();
    Function limitHi;
    Function limitLo;
    /**
     * The LIMIT count the parallel filter of {@link #masterSuffixScan} applies, which the LIMIT over the scan's join
     * sets.
     */
    MasterSuffixLimitFunction masterSuffixLimit;
    /**
     * The master scan of the join a MASTER_SUFFIX LIMIT is generated over, matched by its output schema.
     */
    ScanPlan masterSuffixScan;
    /**
     * The entry of the set operation head whose shared cursor the generator builds in place of {@link #sharedHeadTarget}.
     */
    int sharedHeadEntry;
    /**
     * The leading branch of a decorrelation domain's input that the generator reads through a shared cursor of the
     * head of the domain's shared source's set operation instead of generating it, see
     * {@link AggregatePlan.SharedRead#SET_OPERATION_HEAD}.
     */
    LogicalPlan sharedHeadTarget;
    private int preparedFilterCount;

    GenerationFrame(
            CairoConfiguration configuration,
            StringSink tmpSink,
            LongList tmpLongs,
            IntList symbolKeyFlags,
            IntList masterSymbolColumns,
            IntList slaveSymbolColumns
    ) {
        this.intervals = new IntervalExtractor(configuration, tmpSink, tmpLongs);
        this.joinKeys = new JoinKeyLayout(listColumnFilterA, listColumnFilterB, keyTypes, intListA, intListB, symbolAsString,
                symbolKeyFlags, masterSymbolColumns, slaveSymbolColumns);
    }

    @Override
    public void clear() {
        Throwable failure = Misc.clearBestEffort(null, intervals);
        intListA.clear();
        intListB.clear();
        intListC.clear();
        windowFunctionPositions.clear();
        keyTypes.clear();
        valueTypes.clear();
        listColumnFilterA.clear();
        listColumnFilterB.clear();
        functions.clear();
        symbolAsString.clear();
        joinKeys.clear();
        limitHi = null;
        limitLo = null;
        masterSuffixLimit = null;
        masterSuffixScan = null;
        expressions.clear();
        schema.clear();
        columnReferenceCounts.clear();
        failure = freePendingProducers(failure);
        placedProducers.clear();
        sharedConsumerCounts.clear();
        sharedConsumerEntries.clear();
        sharedConsumers.clear();
        sharedFactories.clear();
        sharedPlans.clear();
        projectionScopeMetadata.clear();
        windowGroups.clear();
        windowMetadata.clear();
        windowOutputColumns.clear();
        windowPartitionKeys.clear();
        windowSpecFunctions.clear();
        windowSpecs.clear();
        sharedHeadEntry = 0;
        sharedHeadTarget = null;
        for (int i = 0; i < preparedFilterCount; i++) {
            failure = Misc.freeBestEffort(failure, preparedFilters.getQuick(i));
        }
        preparedFilterCount = 0;
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        clear();
    }

    /**
     * Records the factory a domain read built for its producer before the generator reached the producer; the frame
     * owns it once this returns, until {@link #claimProducer}, and the caller owns it on failure. Returns its entry.
     */
    int addPendingProducer(LogicalPlan producer, RecordCursorFactory factory) {
        final int entry = addShared(producer, factory);
        pendingProducers.add(entry);
        return entry;
    }

    /**
     * Records the factory the frame built for a plan other consumers may share, once; returns its entry.
     */
    int addShared(LogicalPlan plan, RecordCursorFactory factory) {
        final int entry = sharedPlans.indexOf(plan);
        if (entry > -1) {
            return entry;
        }
        sharedPlans.add(plan);
        sharedFactories.add(factory);
        sharedConsumerCounts.add(1);
        return sharedPlans.size() - 1;
    }

    /**
     * Hands the factory a domain read built for the producer at {@code entry} over to the producer's place in the
     * factory tree, see {@link #placeProducer}.
     */
    RecordCursorFactory claimProducer(int entry) {
        final int index = pendingProducers.indexOf(entry, 0, pendingProducers.size());
        if (index < 0) {
            throw new IllegalStateException("decorrelation domain is generated twice");
        }
        placedProducers.add(entry);
        pendingProducers.removeIndex(index);
        return sharedFactories.getQuick(entry);
    }

    /**
     * Fails a generation that left the factory of a decorrelation domain to no place in the factory tree, freeing it
     * with {@code factory}, the tree, and one that freed a factory a shared cursor the tree keeps reads: a subtree the
     * generator replaced with an empty factory held it.
     */
    RecordCursorFactory claimedTree(RecordCursorFactory factory) {
        if (pendingProducers.size() > 0) {
            final IllegalStateException failure = new IllegalStateException("decorrelation domain read without its domain");
            Misc.free(factory, failure);
            freePendingProducers(failure);
            throw failure;
        }
        for (int i = 0, n = sharedConsumers.size(); i < n; i++) {
            if (!sharedConsumers.getQuick(i).isClosed() && isFreed(sharedFactories.getQuick(sharedConsumerEntries.getQuick(i)))) {
                final IllegalStateException failure = new IllegalStateException(FREED_SHARED_FACTORY);
                Misc.free(factory, failure);
                throw failure;
            }
        }
        return factory;
    }

    /**
     * Replaces the factory, whose rows the generator proves empty, with an empty factory, consuming it, including on
     * failure. When the factory holds decorrelation domains that other consumers share, the ones the generator placed
     * from {@code producerLo} to {@code producerHi}, see {@link #producerMark}, the empty factory keeps the factory and
     * opens those domains whenever it opens, so that their consumers keep reading them. The empty factory copies the
     * metadata container: closeable metadata may own lookup state the copy must not retain once the factory closes.
     */
    RecordCursorFactory emptied(int producerLo, int producerHi, RecordCursorFactory factory) {
        final RecordCursorFactory empty;
        try {
            final GenericRecordMetadata metadata = GenericRecordMetadata.copyOfNew(factory.getMetadata());
            ObjList<RecordCursorFactory> domains = null;
            for (int i = producerLo; i < producerHi; i++) {
                final int entry = placedProducers.getQuick(i);
                if (((AggregatePlan) sharedPlans.getQuick(entry)).getSharedConsumerCount() > 0) {
                    if (domains == null) {
                        domains = new ObjList<>();
                    }
                    domains.add(sharedFactories.getQuick(entry));
                }
            }
            if (domains != null) {
                return new DomainKeepingEmptyRecordCursorFactory(metadata, factory, domains);
            }
            empty = new EmptyTableRecordCursorFactory(metadata);
        } catch (Throwable th) {
            Misc.free(factory, th);
            throw th;
        }
        return SqlCodeGenerator.closeAfter(factory, empty);
    }

    int getReferenceCount(int columnId) {
        return columnId < columnReferenceCounts.size() ? columnReferenceCounts.getQuick(columnId) : 0;
    }

    /**
     * Records the factory the generator built for a decorrelation domain at the domain's place in the factory tree,
     * which domain reads may then share.
     */
    void placeProducer(LogicalPlan producer, RecordCursorFactory factory) {
        placedProducers.add(addShared(producer, factory));
    }

    /**
     * Pops the innermost holder, freeing what its consumer did not adopt.
     */
    void popPreparedFilter() {
        Misc.free(preparedFilters.getQuick(--preparedFilterCount));
    }

    /**
     * Pops the holder {@link #pushPreparedFilter(boolean)} pushed, if any, freeing what its consumer did not adopt.
     */
    void popPreparedFilter(@Nullable PreparedFilter filter) {
        if (filter != null) {
            popPreparedFilter();
        }
    }

    /**
     * Pops the holder {@link #pushPreparedFilter(boolean)} pushed, if any, on the consumer's failure, freeing what it
     * holds.
     */
    void popPreparedFilter(@Nullable PreparedFilter filter, Throwable primary) {
        if (filter != null) {
            popPreparedFilter(primary);
        }
    }

    /**
     * Pops the innermost holder on the consumer's failure, freeing what it holds.
     */
    void popPreparedFilter(Throwable primary) {
        Misc.free(preparedFilters.getQuick(--preparedFilterCount), primary);
    }

    /**
     * The mark of the next decorrelation domain the generator places in the factory tree: a factory it builds from
     * here on holds the domains it places from this mark on, see {@link #emptied}.
     */
    int producerMark() {
        return placedProducers.size();
    }

    /**
     * The scope column a projection reserves for its output column at this index.
     */
    TableColumnMetadata projectionSlotColumn(int index, int type) {
        TableColumnMetadata column = projectionSlotColumns.getQuiet(index);
        if (column == null || column.getColumnType() != type) {
            final String name = column != null ? column.getColumnName() : "$projection" + index;
            column = new TableColumnMetadata(name, type, IndexType.NONE, 0, false, null);
            projectionSlotColumns.extendAndSet(index, column);
        }
        return column;
    }

    /**
     * The holder of the next filter a parallel consumer steals, which the consumer pops once its factory adopted the
     * filter, or on its failure.
     */
    PreparedFilter pushPreparedFilter() {
        PreparedFilter filter = preparedFilters.getQuiet(preparedFilterCount);
        if (filter == null) {
            filter = new PreparedFilter();
            preparedFilters.extendAndSet(preparedFilterCount, filter);
        }
        preparedFilterCount++;
        return filter;
    }

    /**
     * The holder of the filter a parallel consumer steals when {@code isFilterStolen}, see {@link #pushPreparedFilter()};
     * null otherwise.
     */
    @Nullable
    PreparedFilter pushPreparedFilter(boolean isFilterStolen) {
        return isFilterStolen ? pushPreparedFilter() : null;
    }

    void setReferenceCount(int columnId, int count) {
        while (columnReferenceCounts.size() <= columnId) {
            columnReferenceCounts.add(0);
        }
        columnReferenceCounts.setQuick(columnId, count);
    }

    /**
     * Hands out the next shared cursor of the factory the frame built for the shared plan at {@code entry}.
     */
    RecordCursorFactory shareCursor(int entry) {
        final int sharedId = sharedConsumerCounts.getQuick(entry);
        final SharedRecordCursorFactory consumer = new SharedRecordCursorFactory(sharedFactory(entry), sharedId);
        sharedConsumerCounts.setQuick(entry, sharedId + 1);
        sharedConsumers.add(consumer);
        sharedConsumerEntries.add(entry);
        return consumer;
    }

    /**
     * The entry of the factory the frame built for {@code plan}, -1 when it has built none.
     */
    int sharedEntry(LogicalPlan plan) {
        return sharedPlans.indexOf(plan);
    }

    /**
     * The factory the frame built for the shared plan at {@code entry}, which a shared cursor reads; fails when the
     * generator freed it.
     */
    RecordCursorFactory sharedFactory(int entry) {
        final RecordCursorFactory factory = sharedFactories.getQuick(entry);
        if (isFreed(factory)) {
            throw new IllegalStateException(FREED_SHARED_FACTORY);
        }
        return factory;
    }

    private static boolean isFreed(RecordCursorFactory factory) {
        return factory instanceof AbstractRecordCursorFactory abstractFactory && abstractFactory.isClosed();
    }

    private Throwable freePendingProducers(Throwable failure) {
        for (int i = 0, n = pendingProducers.size(); i < n; i++) {
            failure = Misc.freeBestEffort(failure, sharedFactories.getQuick(pendingProducers.getQuick(i)));
        }
        pendingProducers.clear();
        return failure;
    }
}
