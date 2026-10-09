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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
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
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
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
import io.questdb.griffin.plan.logical.ScanPlan;
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
     * Consumes frames and filter, including on failure; the frame's keyTypes and listColumnFilterA describe the ordered key tuple.
     */
    private RecordCursorFactory generateLatestByAll(
            GenerationFrame frame,
            RecordMetadata metadata,
            PartitionFrameCursorFactory frames,
            IntList columnIndexes,
            IntList columnSizeShifts,
            @Nullable Function filter,
            @Nullable IntList symbolCounts,
            boolean hasOnlySymbolKeys
    ) {
        final ArrayColumnTypes keyTypes = frame.keyTypes;
        final ListColumnFilter listColumnFilterA = frame.listColumnFilterA;
        final RecordSink sink;
        IntList partitionByColumnIndexes = null;
        try {
            sink = RecordSinkFactory.getInstance(configuration, asm, metadata, listColumnFilterA);
            if (hasOnlySymbolKeys) {
                partitionByColumnIndexes = new IntList(listColumnFilterA.size());
                for (int i = 0, n = listColumnFilterA.size(); i < n; i++) {
                    partitionByColumnIndexes.add(listColumnFilterA.getColumnIndexFactored(i));
                }
            }
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.free(filter, th);
            throw th;
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
            ScanPlan.IndexRead indexRead,
            boolean hasNullableKey,
            boolean isBackupSuppressed
    ) {
        if (indexRead == ScanPlan.IndexRead.NONE) {
            return new LatestByDeferredListValuesFilteredRecordCursorFactory(configuration,
                    metadata, frames, keyIndex, keys, excludedKeys, filter, columnIndexes, columnSizeShifts);
        }
        final int readerKeyIndex = columnIndexes.getQuick(keyIndex);
        final SymbolMapReader symbolMapReader;
        final int[] coveringMapping;
        try {
            symbolMapReader = reader.getSymbolMapReader(readerKeyIndex);
            coveringMapping = indexRead == ScanPlan.IndexRead.COVERING ? ScanFactoryGenerator.buildCoveringMapping(reader, readerKeyIndex, columnIndexes) : null;
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
                                                         LatestByPlan.Algorithm algorithm) {
        final ArrayColumnTypes keyTypes = frame.keyTypes;
        final RecordSink recordSink;
        try {
            recordSink = RecordSinkFactory.getInstance(configuration, asm, factory.getMetadata(), frame.listColumnFilterA);
        } catch (Throwable th) {
            Misc.free(factory, th);
            throw th;
        }
        return algorithm == LatestByPlan.Algorithm.MATERIALIZED
                ? new LatestByRecordCursorFactory(configuration, factory, recordSink, keyTypes, timestampIndex)
                : new LatestByLightRecordCursorFactory(configuration, factory, recordSink, keyTypes, timestampIndex,
                algorithm == LatestByPlan.Algorithm.ASCENDING_LIGHT);
    }

    /**
     * Consumes frames, key and filter, including on failure; the reader is borrowed.
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
            ScanPlan.IndexRead indexRead,
            boolean canKeyBeNull,
            boolean isBackupSuppressed
    ) {
        final int readerKeyIndex = columnIndexes.getQuick(keyIndex);
        final boolean isIndexed = indexRead != ScanPlan.IndexRead.NONE;
        final int symbolKey;
        final int[] coveringMapping;
        try {
            final SymbolMapReader symbols = reader.getSymbolMapReader(readerKeyIndex);
            symbolKey = key.isRuntimeConstant() ? SymbolTable.VALUE_NOT_FOUND : symbols.keyOf(key.getStrA(null));
            coveringMapping = indexRead == ScanPlan.IndexRead.COVERING ? ScanFactoryGenerator.buildCoveringMapping(reader, readerKeyIndex, columnIndexes) : null;
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.free(filter, th);
            Misc.free(key, th);
            throw th;
        }
        final boolean isDeferred = symbolKey == SymbolTable.VALUE_NOT_FOUND;
        if (coveringMapping != null) {
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

    /**
     * Generates LATEST BY over a derived input; a table scan input takes the keyed scan path instead.
     */
    RecordCursorFactory generateLatestBy(GenerationFrame frame, LatestByPlan latest, SqlExecutionContext executionContext) throws SqlException {
        final LogicalPlan input = latest.getInput();
        // LATEST BY locates its timestamp by position; a declaration alone adds nothing.
        final RecordCursorFactory base = codeGenerator.generate(frame, LogicalPlans.isTimestampDeclarationOnly(input) ? input.inputAt(0) : input, executionContext);
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
        return generateLatestBy(frame, base, timestampIndex, keyIndexes, latest.getAlgorithm());
    }

    /**
     * Consumes the source; column indexes are borrowed only during construction.
     */
    RecordCursorFactory generateLatestBy(GenerationFrame frame, RecordCursorFactory factory, int timestampIndex, IntList keyIndexes,
                                         LatestByPlan.Algorithm algorithm) {
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
        return generateLatestByPrepared(frame, factory, timestampIndex, algorithm);
    }

    /**
     * Consumes the frames, filter and key functions, including on failure; builds the LATEST BY access path the scan
     * records.
     */
    RecordCursorFactory generateLatestByScan(
            GenerationFrame frame,
            ScanPlan scan,
            PartitionFrameCursorFactory frames,
            RecordMetadata queryMetadata,
            @Transient TableReader reader,
            IntList columnIndexes,
            IntList columnSizeShifts,
            IntList keyIndexes,
            @Nullable Function filter,
            ObjList<Function> keys,
            ObjList<Function> excludedKeys,
            LongList geoHashPrefixes,
            @Nullable IntList symbolCounts,
            SqlExecutionContext executionContext
    ) {
        try {
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
        final int keyIndex = keyIndexes.size() == 0 ? -1 : keyIndexes.getQuick(0);
        final boolean isBackupSuppressed = scan.hasHint(ScanPlan.HINT_FORCE_USE_COVERING);
        return switch (scan.getAccessPath()) {
            case LATEST_BY_VALUE ->
                    generateLatestBySingleKey(reader, queryMetadata, frames, keyIndex, keys.getQuick(0), filter,
                            columnIndexes, columnSizeShifts, scan.getIndexRead(), scan.hasNullableKey(), isBackupSuppressed);
            case LATEST_BY_VALUES ->
                    generateLatestByKeyList(reader, queryMetadata, frames, keyIndex, keys, excludedKeys,
                            filter, columnIndexes, columnSizeShifts, scan.getIndexRead(), scan.hasNullableKey(), isBackupSuppressed);
            case LATEST_BY_ALL_INDEXED ->
                    new LatestByAllIndexedRecordCursorFactory(executionContext.getCairoEngine(), configuration,
                            queryMetadata, frames, keyIndex, columnIndexes, columnSizeShifts, geoHashPrefixes);
            case LATEST_BY_STATIC_SYMBOL -> new LatestByDeferredListValuesFilteredRecordCursorFactory(
                    configuration, queryMetadata, frames, keyIndex, filter, columnIndexes, columnSizeShifts);
            case LATEST_BY_SYMBOLS, LATEST_BY_ALL ->
                    generateLatestByAll(frame, queryMetadata, frames, columnIndexes, columnSizeShifts, filter,
                            symbolCounts, scan.getAccessPath() == ScanPlan.AccessPath.LATEST_BY_SYMBOLS);
            default -> {
                final IllegalStateException failure = new IllegalStateException("scan access path is not a LATEST BY scan");
                Misc.free(frames, failure);
                Misc.free(filter, failure);
                Misc.freeObjList(keys, failure);
                Misc.freeObjList(excludedKeys, failure);
                throw failure;
            }
        };
    }
}
