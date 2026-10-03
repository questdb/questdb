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

package io.questdb.griffin;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByAllFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByAllIndexedRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByAllSymbolsFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByDeferredListValuesFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByLightRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByValueDeferredFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByValueDeferredIndexedFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByValueDeferredIndexedRowCursorFactory;
import io.questdb.griffin.engine.table.LatestByValueFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByValueIndexedFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByValueIndexedRowCursorFactory;
import io.questdb.griffin.engine.table.LatestByValuesIndexedFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.ColumnType.*;

/**
 * Builds LATEST BY factories: the keyed latest-row scans over a table and the latest-row
 * selection over a derived input.
 */
final class LatestByFactoryGenerator {
    private final BytecodeAssembler asm;
    private final SqlCodeGenerator codeGenerator;
    private final CairoConfiguration configuration;
    private final ArrayColumnTypes keyTypes;
    private final IntList latestByColumnIndexes;
    private final ListColumnFilter listColumnFilterA;
    private final LongList prefixes;

    LatestByFactoryGenerator(
            CairoConfiguration configuration,
            SqlCodeGenerator codeGenerator,
            BytecodeAssembler asm,
            ArrayColumnTypes keyTypes,
            ListColumnFilter listColumnFilterA,
            IntList latestByColumnIndexes,
            LongList prefixes
    ) {
        this.configuration = configuration;
        this.codeGenerator = codeGenerator;
        this.asm = asm;
        this.keyTypes = keyTypes;
        this.listColumnFilterA = listColumnFilterA;
        this.latestByColumnIndexes = latestByColumnIndexes;
        this.prefixes = prefixes;
    }

    /**
     * True when an unfiltered LATEST BY without key values over these keys scans the bitmap index
     * of its single key; only that scan applies within() GeoHash prefixes.
     */
    static boolean isIndexedScan(RecordMetadata metadata, IntList keyIndexes, boolean isIndexAllowed) {
        return isIndexAllowed && keyIndexes.size() == 1 && metadata.getColumnIndexType(keyIndexes.getQuick(0)) == IndexType.BITMAP;
    }

    /**
     * The plain {@code LATEST ON} index scan a covering factory falls back to: the same plan its
     * caller builds when {@code /*+ no_covering *}{@code /} is set.
     * <p>
     * The returned factory OWNS {@code dfcFactory} and {@code filter}. It owns {@code symbolFunc}
     * only when the key is deferred -- the resolved-key variants take the key as an {@code int}
     * and never see the function, so the covering factory keeps owning it in that case. That is
     * what the covering factory's {@code backupOwnsKeyFunctions} flag records.
     */
    private static RecordCursorFactory buildLatestByIndexScan(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            PartitionFrameCursorFactory dfcFactory,
            int latestByIndex,
            int symbolKey,
            Function symbolFunc,
            @Nullable Function filter,
            IntList columnIndexes,
            IntList columnSizeShifts
    ) {
        if (filter == null) {
            final RowCursorFactory rcf = symbolKey == SymbolTable.VALUE_NOT_FOUND
                    ? new LatestByValueDeferredIndexedRowCursorFactory(latestByIndex, symbolFunc)
                    : new LatestByValueIndexedRowCursorFactory(latestByIndex, symbolKey);
            return new PageFrameRecordCursorFactory(
                    configuration,
                    metadata,
                    dfcFactory,
                    rcf,
                    false,
                    null,
                    false,
                    columnIndexes,
                    columnSizeShifts,
                    true,
                    true
            );
        }
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            return new LatestByValueDeferredIndexedFilteredRecordCursorFactory(
                    configuration,
                    metadata,
                    dfcFactory,
                    latestByIndex,
                    symbolFunc,
                    filter,
                    columnIndexes,
                    columnSizeShifts
            );
        }
        return new LatestByValueIndexedFilteredRecordCursorFactory(
                configuration,
                metadata,
                dfcFactory,
                latestByIndex,
                symbolKey,
                filter,
                columnIndexes,
                columnSizeShifts
        );
    }

    /**
     * Generates LATEST BY over a derived input; a table scan input takes the keyed scan path instead.
     */
    int generateLatestBy(GenerationFrame frame, LatestByPlan latest, SqlExecutionContext executionContext) throws SqlException {
        final LogicalPlan input = latest.getInput();
        // LATEST BY locates its timestamp by position; a declaration alone adds nothing.
        final int inputSlot = codeGenerator.generate(frame, SqlCodeGenerator.isTimestampDeclarationOnly(input) ? input.inputAt(0) : input, executionContext);
        final RecordCursorFactory base = frame.resources.factory(inputSlot);
        final OutputSchema schema = input.getOutput();
        final int timestampIndex = schema.getColumnIndexById(latest.getTimestampColumnId());
        final IntList keyIndexes = latestByColumnIndexes;
        keyIndexes.clear();
        for (int i = 0, n = latest.getKeyColumnIds().size(); i < n; i++) {
            keyIndexes.add(schema.getColumnIndexById(latest.getKeyColumnIds().getQuick(i)));
        }
        final boolean orderedByTimestampAsc = latest.isTimestampOrderInherited()
                && timestampIndex == base.getMetadata().getTimestampIndex()
                && base.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_FORWARD;
        final int slot = frame.resources.reserve();
        frame.resources.detach(inputSlot);
        frame.resources.own(slot, generateLatestBy(base, timestampIndex, keyIndexes, orderedByTimestampAsc));
        return slot;
    }

    /**
     * Consumes the source; column indexes are borrowed only during construction.
     */
    RecordCursorFactory generateLatestBy(RecordCursorFactory factory, int timestampIndex, IntList keyIndexes, boolean orderedByTimestampAsc) {
        try {
            final RecordMetadata metadata = factory.getMetadata();
            keyTypes.clear();
            listColumnFilterA.clear();
            for (int i = 0, n = keyIndexes.size(); i < n; i++) {
                final int index = keyIndexes.getQuick(i);
                keyTypes.add(metadata.getColumnType(index));
                listColumnFilterA.add(index + 1);
            }
            final RecordCursorFactory base = factory;
            factory = null;
            return generateLatestByPrepared(base, timestampIndex, orderedByTimestampAsc);
        } catch (Throwable th) {
            Misc.free(factory, th);
            throw th;
        }
    }

    /**
     * Consumes the optional interval model, filter and key function, including on failure.
     */
    RecordCursorFactory generateLatestByScan(
            PartitionFrameCursorFactory frames,
            RecordMetadata queryMetadata,
            @Transient TableReader reader,
            IntList columnIndexes,
            IntList columnSizeShifts,
            IntList keyIndexes,
            boolean isIndexedAllowed,
            @Nullable Function filter,
            ObjList<Function> keys,
            ObjList<Function> excludedKeys,
            LongList geoHashPrefixes,
            @Nullable IntList symbolCounts,
            boolean isIndexAllowed,
            boolean isCoveringAllowed,
            boolean isBackupSuppressed,
            SqlExecutionContext executionContext
    ) {
        try {
            if (filter != null && filter.isConstant() && !filter.getBool(null)) {
                final Function discardedFilter = filter;
                filter = null;
                discardedFilter.close();
                final PartitionFrameCursorFactory discardedFrames = frames;
                frames = null;
                Misc.free(discardedFrames);
                Misc.freeObjListAndClear(keys);
                Misc.freeObjListAndClear(excludedKeys);
                return new EmptyTableRecordCursorFactory(queryMetadata);
            }
            keyTypes.clear();
            listColumnFilterA.clear();
            prefixes.clear();
            prefixes.addAll(geoHashPrefixes);
            for (int i = 0, n = keyIndexes.size(); i < n; i++) {
                final int index = keyIndexes.getQuick(i);
                keyTypes.add(queryMetadata.getColumnType(index));
                listColumnFilterA.add(index + 1);
            }
            final PartitionFrameCursorFactory ownedFrames = frames;
            final Function ownedFilter = filter;
            frames = null;
            filter = null;
            if (keys.size() > 0 || excludedKeys.size() > 0) {
                final int keyIndex = keyIndexes.getQuick(0);
                final boolean isIndexed = isIndexAllowed && IndexType.isIndexed(queryMetadata.getColumnIndexType(keyIndex));
                if (keys.size() == 1 && excludedKeys.size() == 0) {
                    final Function ownedKey = keys.getQuick(0);
                    keys.clear();
                    return generateLatestBySingleKey(reader, queryMetadata, ownedFrames, keyIndex, ownedKey, ownedFilter,
                            columnIndexes, columnSizeShifts, isIndexed, isCoveringAllowed, isBackupSuppressed);
                }
                return generateLatestByKeyList(reader, queryMetadata, ownedFrames, keyIndex, keys, excludedKeys,
                        ownedFilter, columnIndexes, columnSizeShifts, isIndexed, isCoveringAllowed, isBackupSuppressed);
            }
            return generateLatestByAll(executionContext, queryMetadata, ownedFrames, columnIndexes, columnSizeShifts,
                    ownedFilter == null && isIndexedScan(queryMetadata, keyIndexes, isIndexedAllowed && isIndexAllowed),
                    ownedFilter, symbolCounts);
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.free(filter, th);
            Misc.freeObjList(keys, th);
            Misc.freeObjList(excludedKeys, th);
            throw th;
        }
    }

    /**
     * Consumes frames and filter; keyTypes and listColumnFilterA describe the ordered key tuple.
     */
    private RecordCursorFactory generateLatestByAll(
            SqlExecutionContext executionContext,
            RecordMetadata metadata,
            PartitionFrameCursorFactory frames,
            IntList columnIndexes,
            IntList columnSizeShifts,
            boolean isIndexedScan,
            @Nullable Function filter,
            @Nullable IntList symbolCounts
    ) {
        try {
            if (listColumnFilterA.size() == 1) {
                final int keyIndex = listColumnFilterA.getColumnIndexFactored(0);
                if (isIndexedScan) {
                    final PartitionFrameCursorFactory ownedFrames = frames;
                    frames = null;
                    return new LatestByAllIndexedRecordCursorFactory(executionContext.getCairoEngine(), configuration,
                            metadata, ownedFrames, keyIndex, columnIndexes, columnSizeShifts, prefixes);
                }
                if (isSymbol(metadata.getColumnType(keyIndex)) && metadata.isSymbolTableStatic(keyIndex)) {
                    // This constructor borrows until success, unlike the map/index factories below.
                    final RecordCursorFactory result = new LatestByDeferredListValuesFilteredRecordCursorFactory(
                            configuration, metadata, frames, keyIndex, filter, columnIndexes, columnSizeShifts);
                    frames = null;
                    filter = null;
                    return result;
                }
            }
            boolean hasOnlySymbolKeys = true;
            for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
                hasOnlySymbolKeys &= isSymbol(keyTypes.getColumnType(i));
            }
            final RecordSink sink = RecordSinkFactory.getInstance(configuration, asm, metadata, listColumnFilterA);
            if (hasOnlySymbolKeys) {
                final IntList partitionByColumnIndexes = new IntList(listColumnFilterA.size());
                for (int i = 0, n = listColumnFilterA.size(); i < n; i++) {
                    partitionByColumnIndexes.add(listColumnFilterA.getColumnIndexFactored(i));
                }
                final PartitionFrameCursorFactory ownedFrames = frames;
                frames = null;
                // Until its cursor is constructed this factory owns frames, but not the filter.
                final RecordCursorFactory result = new LatestByAllSymbolsFilteredRecordCursorFactory(configuration, metadata, ownedFrames,
                        sink, keyTypes, partitionByColumnIndexes, symbolCounts, filter, columnIndexes, columnSizeShifts);
                filter = null;
                return result;
            }
            final PartitionFrameCursorFactory ownedFrames = frames;
            final Function ownedFilter = filter;
            frames = null;
            filter = null;
            return new LatestByAllFilteredRecordCursorFactory(configuration, metadata, ownedFrames,
                    sink, keyTypes, ownedFilter, columnIndexes, columnSizeShifts);
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.free(filter, th);
            throw th;
        }
    }

    /**
     * Consumes frames, both key lists and the filter.
     */
    private RecordCursorFactory generateLatestByKeyList(
            TableReader reader,
            RecordMetadata metadata,
            PartitionFrameCursorFactory frames,
            int keyIndex,
            ObjList<Function> keys,
            ObjList<Function> excludedKeys,
            @Nullable Function filter,
            IntList columnIndexes,
            IntList columnSizeShifts,
            boolean isIndexed,
            boolean isCoveringAllowed,
            boolean isBackupSuppressed
    ) {
        RecordCursorFactory backup = null;
        try {
            if (isIndexed && excludedKeys.size() == 0) {
                final int readerKeyIndex = columnIndexes.getQuick(keyIndex);
                final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(readerKeyIndex);
                if (isCoveringAllowed) {
                    final int[] coveringMapping = ScanFactoryGenerator.buildCoveringIndexMapping(reader, readerKeyIndex, columnIndexes, metadata);
                    if (coveringMapping != null) {
                        final PartitionFrameCursorFactory sharedFrames = frames;
                        final Function sharedFilter = filter;
                        if (!isBackupSuppressed && ScanFactoryGenerator.canAnyKeyBeNull(keys, symbolMapReader)) {
                            backup = new LatestByValuesIndexedFilteredRecordCursorFactory(configuration, metadata,
                                    sharedFrames, keyIndex, keys, symbolMapReader, sharedFilter, columnIndexes, columnSizeShifts);
                            frames = null;
                            filter = null;
                        }
                        final RecordCursorFactory result = new CoveringIndexRecordCursorFactory(metadata, sharedFrames,
                                readerKeyIndex, SymbolTable.VALUE_NOT_FOUND, null, columnIndexes, coveringMapping, keys,
                                reader, true, sharedFilter, null, backup, true,
                                backup == null && ScanFactoryGenerator.canAnyKeyBeNull(keys, symbolMapReader));
                        backup = null;
                        frames = null;
                        filter = null;
                        keys.clear();
                        return result;
                    }
                }
                final RecordCursorFactory result = new LatestByValuesIndexedFilteredRecordCursorFactory(configuration,
                        metadata, frames, keyIndex, keys, symbolMapReader, filter, columnIndexes, columnSizeShifts);
                frames = null;
                filter = null;
                keys.clear();
                return result;
            }
            final RecordCursorFactory result = new LatestByDeferredListValuesFilteredRecordCursorFactory(configuration,
                    metadata, frames, keyIndex, keys, excludedKeys, filter, columnIndexes, columnSizeShifts);
            frames = null;
            filter = null;
            keys.clear();
            excludedKeys.clear();
            return result;
        } catch (Throwable th) {
            Misc.free(backup, th);
            Misc.free(frames, th);
            Misc.free(filter, th);
            Misc.freeObjList(keys, th);
            Misc.freeObjList(excludedKeys, th);
            throw th;
        }
    }

    private RecordCursorFactory generateLatestByPrepared(RecordCursorFactory factory, int timestampIndex, boolean orderedByTimestampAsc) {
        try {
            final RecordSink recordSink = RecordSinkFactory.getInstance(configuration, asm, factory.getMetadata(), listColumnFilterA);
            if (!factory.recordCursorSupportsRandomAccess()) {
                final RecordCursorFactory base = factory;
                factory = null;
                return new LatestByRecordCursorFactory(configuration, base, recordSink, keyTypes, timestampIndex);
            }
            return new LatestByLightRecordCursorFactory(configuration, factory, recordSink, keyTypes,
                    timestampIndex, orderedByTimestampAsc);
        } catch (Throwable th) {
            Misc.free(factory, th);
            throw th;
        }
    }

    /**
     * Consumes frames, key and filter; the reader is borrowed only for physical selection.
     */
    private RecordCursorFactory generateLatestBySingleKey(
            TableReader reader,
            RecordMetadata metadata,
            PartitionFrameCursorFactory frames,
            int keyIndex,
            Function key,
            @Nullable Function filter,
            IntList columnIndexes,
            IntList columnSizeShifts,
            boolean isIndexed,
            boolean isCoveringAllowed,
            boolean isBackupSuppressed
    ) {
        RecordCursorFactory backup = null;
        RecordCursorFactory result = null;
        try {
            final int readerKeyIndex = columnIndexes.getQuick(keyIndex);
            final SymbolMapReader symbols = reader.getSymbolMapReader(readerKeyIndex);
            final int symbolKey = key.isRuntimeConstant() ? SymbolTable.VALUE_NOT_FOUND : symbols.keyOf(key.getStrA(null));
            final boolean isDeferred = symbolKey == SymbolTable.VALUE_NOT_FOUND;
            if (isIndexed && isCoveringAllowed) {
                final int[] coveringMapping = ScanFactoryGenerator.buildCoveringIndexMapping(reader, readerKeyIndex, columnIndexes, metadata);
                if (coveringMapping != null) {
                    final PartitionFrameCursorFactory sharedFrames = frames;
                    final Function sharedKey = key;
                    final Function sharedFilter = filter;
                    if (!isBackupSuppressed && ScanFactoryGenerator.canKeyBeNull(symbolKey, key)) {
                        backup = buildLatestByIndexScan(configuration, metadata, frames, keyIndex, symbolKey,
                                key, filter, columnIndexes, columnSizeShifts);
                        frames = null;
                        filter = null;
                        if (isDeferred) {
                            key = null;
                        }
                    }
                    result = new CoveringIndexRecordCursorFactory(metadata, sharedFrames, readerKeyIndex, symbolKey,
                            sharedKey, columnIndexes, coveringMapping, null, null, true, sharedFilter, null, backup,
                            isDeferred, backup == null && ScanFactoryGenerator.canKeyBeNull(symbolKey, sharedKey));
                    backup = null;
                    frames = null;
                    filter = null;
                    key = null;
                    return result;
                }
            }
            if (isIndexed) {
                result = buildLatestByIndexScan(configuration, metadata, frames, keyIndex, symbolKey,
                        key, filter, columnIndexes, columnSizeShifts);
            } else if (isDeferred) {
                result = new LatestByValueDeferredFilteredRecordCursorFactory(configuration, metadata, frames,
                        keyIndex, key, filter, columnIndexes, columnSizeShifts);
            } else {
                result = new LatestByValueFilteredRecordCursorFactory(configuration, metadata, frames,
                        keyIndex, symbolKey, filter, columnIndexes, columnSizeShifts);
            }
            frames = null;
            filter = null;
            if (isDeferred) {
                key = null;
            } else {
                final Function resolvedKey = key;
                key = null;
                resolvedKey.close();
            }
            return result;
        } catch (Throwable th) {
            Misc.free(result, th);
            Misc.free(backup, th);
            Misc.free(frames, th);
            Misc.free(filter, th);
            Misc.free(key, th);
            throw th;
        }
    }
}
