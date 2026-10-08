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
    private final IntList latestByColumnIndexes;

    LatestByFactoryGenerator(
            CairoConfiguration configuration,
            SqlCodeGenerator codeGenerator,
            BytecodeAssembler asm,
            IntList latestByColumnIndexes
    ) {
        this.configuration = configuration;
        this.codeGenerator = codeGenerator;
        this.asm = asm;
        this.latestByColumnIndexes = latestByColumnIndexes;
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
    RecordCursorFactory generateLatestBy(GenerationFrame frame, LatestByPlan latest, SqlExecutionContext executionContext) throws SqlException {
        final LogicalPlan input = latest.getInput();
        // LATEST BY locates its timestamp by position; a declaration alone adds nothing.
        final RecordCursorFactory base = codeGenerator.generate(frame, SqlCodeGenerator.isTimestampDeclarationOnly(input) ? input.inputAt(0) : input, executionContext);
        final OutputSchema schema = input.getOutput();
        final int timestampIndex = schema.getColumnIndexById(latest.getTimestampColumnId());
        final IntList keyIndexes = latestByColumnIndexes;
        try {
            keyIndexes.clear();
            for (int i = 0, n = latest.getKeyColumnIds().size(); i < n; i++) {
                keyIndexes.add(schema.getColumnIndexById(latest.getKeyColumnIds().getQuick(i)));
            }
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        final boolean orderedByTimestampAsc = latest.isTimestampOrderInherited()
                && timestampIndex == base.getMetadata().getTimestampIndex()
                && base.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_FORWARD;
        return generateLatestBy(frame, base, timestampIndex, keyIndexes, orderedByTimestampAsc);
    }

    /**
     * Consumes the source; column indexes are borrowed only during construction.
     */
    RecordCursorFactory generateLatestBy(GenerationFrame frame, RecordCursorFactory factory, int timestampIndex, IntList keyIndexes,
                                         boolean orderedByTimestampAsc) {
        final ArrayColumnTypes keyTypes = frame.keyTypes;
        final ListColumnFilter listColumnFilterA = frame.listColumnFilterA;
        try {
            final RecordMetadata metadata = factory.getMetadata();
            keyTypes.clear();
            listColumnFilterA.clear();
            for (int i = 0, n = keyIndexes.size(); i < n; i++) {
                final int index = keyIndexes.getQuick(i);
                keyTypes.add(metadata.getColumnType(index));
                listColumnFilterA.add(index + 1);
            }
        } catch (Throwable th) {
            Misc.free(factory, th);
            throw th;
        }
        return generateLatestByPrepared(frame, factory, timestampIndex, orderedByTimestampAsc);
    }

    /**
     * Consumes the frames, filter and key functions, including on failure.
     */
    RecordCursorFactory generateLatestByScan(
            GenerationFrame frame,
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
            final ArrayColumnTypes keyTypes = frame.keyTypes;
            final ListColumnFilter listColumnFilterA = frame.listColumnFilterA;
            keyTypes.clear();
            listColumnFilterA.clear();
            for (int i = 0, n = keyIndexes.size(); i < n; i++) {
                final int index = keyIndexes.getQuick(i);
                keyTypes.add(queryMetadata.getColumnType(index));
                listColumnFilterA.add(index + 1);
            }
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.free(filter, th);
            Misc.freeObjList(keys, th);
            Misc.freeObjList(excludedKeys, th);
            throw th;
        }
        if (keys.size() > 0 || excludedKeys.size() > 0) {
            final int keyIndex = keyIndexes.getQuick(0);
            final boolean isIndexed = isIndexAllowed && IndexType.isIndexed(queryMetadata.getColumnIndexType(keyIndex));
            if (keys.size() == 1 && excludedKeys.size() == 0) {
                return generateLatestBySingleKey(reader, queryMetadata, frames, keyIndex, keys.getQuick(0), filter,
                        columnIndexes, columnSizeShifts, isIndexed, isCoveringAllowed, isBackupSuppressed);
            }
            return generateLatestByKeyList(reader, queryMetadata, frames, keyIndex, keys, excludedKeys,
                    filter, columnIndexes, columnSizeShifts, isIndexed, isCoveringAllowed, isBackupSuppressed);
        }
        return generateLatestByAll(frame, executionContext, queryMetadata, frames, columnIndexes, columnSizeShifts,
                filter == null && isIndexedScan(queryMetadata, keyIndexes, isIndexedAllowed && isIndexAllowed),
                filter, symbolCounts, geoHashPrefixes);
    }

    /**
     * Consumes frames and filter, including on failure; the frame's keyTypes and listColumnFilterA describe the ordered key tuple.
     */
    private RecordCursorFactory generateLatestByAll(
            GenerationFrame frame,
            SqlExecutionContext executionContext,
            RecordMetadata metadata,
            PartitionFrameCursorFactory frames,
            IntList columnIndexes,
            IntList columnSizeShifts,
            boolean isIndexedScan,
            @Nullable Function filter,
            @Nullable IntList symbolCounts,
            LongList geoHashPrefixes
    ) {
        final ArrayColumnTypes keyTypes = frame.keyTypes;
        final ListColumnFilter listColumnFilterA = frame.listColumnFilterA;
        final int keyIndex = listColumnFilterA.size() == 1 ? listColumnFilterA.getColumnIndexFactored(0) : -1;
        if (keyIndex >= 0 && isIndexedScan) {
            return new LatestByAllIndexedRecordCursorFactory(executionContext.getCairoEngine(), configuration,
                    metadata, frames, keyIndex, columnIndexes, columnSizeShifts, geoHashPrefixes);
        }
        final boolean isStaticSymbolKey;
        RecordSink sink = null;
        IntList partitionByColumnIndexes = null;
        try {
            isStaticSymbolKey = keyIndex >= 0 && isSymbol(metadata.getColumnType(keyIndex)) && metadata.isSymbolTableStatic(keyIndex);
            if (!isStaticSymbolKey) {
                boolean hasOnlySymbolKeys = true;
                for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
                    hasOnlySymbolKeys &= isSymbol(keyTypes.getColumnType(i));
                }
                sink = RecordSinkFactory.getInstance(configuration, asm, metadata, listColumnFilterA);
                if (hasOnlySymbolKeys) {
                    partitionByColumnIndexes = new IntList(listColumnFilterA.size());
                    for (int i = 0, n = listColumnFilterA.size(); i < n; i++) {
                        partitionByColumnIndexes.add(listColumnFilterA.getColumnIndexFactored(i));
                    }
                }
            }
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.free(filter, th);
            throw th;
        }
        if (isStaticSymbolKey) {
            return new LatestByDeferredListValuesFilteredRecordCursorFactory(
                    configuration, metadata, frames, keyIndex, filter, columnIndexes, columnSizeShifts);
        }
        return partitionByColumnIndexes != null
                ? new LatestByAllSymbolsFilteredRecordCursorFactory(configuration, metadata, frames,
                sink, keyTypes, partitionByColumnIndexes, symbolCounts, filter, columnIndexes, columnSizeShifts)
                : new LatestByAllFilteredRecordCursorFactory(configuration, metadata, frames,
                sink, keyTypes, filter, columnIndexes, columnSizeShifts);
    }

    /**
     * Consumes frames, both key lists and the filter, including on failure.
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
        if (!isIndexed || excludedKeys.size() > 0) {
            return new LatestByDeferredListValuesFilteredRecordCursorFactory(configuration,
                    metadata, frames, keyIndex, keys, excludedKeys, filter, columnIndexes, columnSizeShifts);
        }
        final int readerKeyIndex = columnIndexes.getQuick(keyIndex);
        final SymbolMapReader symbolMapReader;
        final int[] coveringMapping;
        final boolean hasNullableKey;
        try {
            symbolMapReader = reader.getSymbolMapReader(readerKeyIndex);
            coveringMapping = isCoveringAllowed ? ScanFactoryGenerator.buildCoveringIndexMapping(reader, readerKeyIndex, columnIndexes, metadata) : null;
            hasNullableKey = coveringMapping != null && ScanFactoryGenerator.canAnyKeyBeNull(keys, symbolMapReader);
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.free(filter, th);
            Misc.freeObjList(keys, th);
            throw th;
        }
        if (coveringMapping == null) {
            return new LatestByValuesIndexedFilteredRecordCursorFactory(configuration,
                    metadata, frames, keyIndex, keys, symbolMapReader, filter, columnIndexes, columnSizeShifts);
        }
        final RecordCursorFactory backup = !isBackupSuppressed && hasNullableKey
                ? new LatestByValuesIndexedFilteredRecordCursorFactory(configuration, metadata,
                frames, keyIndex, keys, symbolMapReader, filter, columnIndexes, columnSizeShifts)
                : null;
        return new CoveringIndexRecordCursorFactory(metadata, frames,
                readerKeyIndex, SymbolTable.VALUE_NOT_FOUND, null, columnIndexes, coveringMapping, keys,
                reader, true, filter, null, backup, true, backup == null && hasNullableKey);
    }

    private RecordCursorFactory generateLatestByPrepared(GenerationFrame frame, RecordCursorFactory factory, int timestampIndex,
                                                         boolean orderedByTimestampAsc) {
        final ArrayColumnTypes keyTypes = frame.keyTypes;
        final RecordSink recordSink;
        try {
            recordSink = RecordSinkFactory.getInstance(configuration, asm, factory.getMetadata(), frame.listColumnFilterA);
        } catch (Throwable th) {
            Misc.free(factory, th);
            throw th;
        }
        return factory.recordCursorSupportsRandomAccess()
                ? new LatestByLightRecordCursorFactory(configuration, factory, recordSink, keyTypes, timestampIndex, orderedByTimestampAsc)
                : new LatestByRecordCursorFactory(configuration, factory, recordSink, keyTypes, timestampIndex);
    }

    /**
     * Consumes frames, key and filter, including on failure; the reader is borrowed only for physical selection.
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
        final int readerKeyIndex = columnIndexes.getQuick(keyIndex);
        final int symbolKey;
        final int[] coveringMapping;
        try {
            final SymbolMapReader symbols = reader.getSymbolMapReader(readerKeyIndex);
            symbolKey = key.isRuntimeConstant() ? SymbolTable.VALUE_NOT_FOUND : symbols.keyOf(key.getStrA(null));
            coveringMapping = isIndexed && isCoveringAllowed
                    ? ScanFactoryGenerator.buildCoveringIndexMapping(reader, readerKeyIndex, columnIndexes, metadata) : null;
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.free(filter, th);
            Misc.free(key, th);
            throw th;
        }
        final boolean isDeferred = symbolKey == SymbolTable.VALUE_NOT_FOUND;
        if (coveringMapping != null) {
            final boolean canKeyBeNull = ScanFactoryGenerator.canKeyBeNull(symbolKey, key);
            RecordCursorFactory backup = null;
            if (!isBackupSuppressed && canKeyBeNull) {
                try {
                    backup = buildLatestByIndexScan(configuration, metadata, frames, keyIndex, symbolKey,
                            key, filter, columnIndexes, columnSizeShifts);
                } catch (Throwable th) {
                    if (!isDeferred) {
                        Misc.free(key, th);
                    }
                    throw th;
                }
            }
            return new CoveringIndexRecordCursorFactory(metadata, frames, readerKeyIndex, symbolKey,
                    key, columnIndexes, coveringMapping, null, null, true, filter, null, backup,
                    isDeferred, backup == null && canKeyBeNull);
        }
        if (isDeferred) {
            return isIndexed
                    ? buildLatestByIndexScan(configuration, metadata, frames, keyIndex, symbolKey, key, filter, columnIndexes, columnSizeShifts)
                    : new LatestByValueDeferredFilteredRecordCursorFactory(configuration, metadata, frames,
                    keyIndex, key, filter, columnIndexes, columnSizeShifts);
        }
        final RecordCursorFactory result;
        try {
            result = isIndexed
                    ? buildLatestByIndexScan(configuration, metadata, frames, keyIndex, symbolKey, key, filter, columnIndexes, columnSizeShifts)
                    : new LatestByValueFilteredRecordCursorFactory(configuration, metadata, frames,
                    keyIndex, symbolKey, filter, columnIndexes, columnSizeShifts);
        } catch (Throwable th) {
            Misc.free(key, th);
            throw th;
        }
        return SqlCodeGenerator.closeAfter(key, result);
    }
}
