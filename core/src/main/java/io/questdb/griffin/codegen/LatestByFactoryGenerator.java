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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableAccessInfo;
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
import io.questdb.griffin.engine.table.LatestByValuesIndexedFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Builds LATEST BY factories: the keyed latest-row scans over a table and the latest-row
 * selection over a derived input.
 */
final class LatestByFactoryGenerator {
    private final BytecodeAssembler asm;
    private final SqlCodeGenerator codeGenerator;
    private final CairoConfiguration configuration;
    private final LongList tmpLongs;

    LatestByFactoryGenerator(
            CairoConfiguration configuration,
            SqlCodeGenerator codeGenerator,
            BytecodeAssembler asm,
            LongList tmpLongs
    ) {
        this.configuration = configuration;
        this.codeGenerator = codeGenerator;
        this.asm = asm;
        this.tmpLongs = tmpLongs;
    }

    /**
     * The plain {@code LATEST ON} index scan a covering factory falls back to: the same plan its
     * caller builds when {@code /*+ no_covering *}{@code /} is set.
     * <p>
     * The returned factory owns {@code dfcFactory}, {@code filter} and {@code symbolFunc}.
     */
    private static RecordCursorFactory buildLatestByIndexScan(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            PartitionFrameCursorFactory dfcFactory,
            int latestByIndex,
            Function symbolFunc,
            @Nullable Function filter,
            IntList columnIndexes,
            IntList columnSizeShifts
    ) {
        if (filter == null) {
            return new PageFrameRecordCursorFactory(
                    configuration,
                    metadata,
                    dfcFactory,
                    new LatestByValueDeferredIndexedRowCursorFactory(latestByIndex, symbolFunc),
                    false,
                    null,
                    false,
                    columnIndexes,
                    columnSizeShifts,
                    true,
                    true
            );
        }
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
        final ListColumnFilter listColumnFilterA = frame.listColumnFilterA;
        if (hasOnlySymbolKeys) {
            final IntList partitionByColumnIndexes = new IntList(listColumnFilterA.size());
            for (int i = 0, n = listColumnFilterA.size(); i < n; i++) {
                partitionByColumnIndexes.add(listColumnFilterA.getColumnIndexFactored(i));
            }
            return new LatestByAllSymbolsFilteredRecordCursorFactory(configuration, metadata, frames,
                    partitionByColumnIndexes, symbolCounts, filter, columnIndexes, columnSizeShifts);
        }
        final RecordSink sink;
        try {
            sink = RecordSinkFactory.getInstance(configuration, asm, metadata, listColumnFilterA);
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.free(filter, th);
            throw th;
        }
        return new LatestByAllFilteredRecordCursorFactory(configuration, metadata, frames,
                sink, frame.keyTypes, filter, columnIndexes, columnSizeShifts);
    }

    /**
     * Consumes frames, both key lists and the filter, including on failure.
     */
    private RecordCursorFactory generateLatestByKeyList(
            TableAccessInfo table,
            RecordMetadata metadata,
            PartitionFrameCursorFactory frames,
            int keyIndex,
            ObjList<Function> keys,
            ObjList<Function> excludedKeys,
            @Nullable Function filter,
            IntList columnIndexes,
            IntList columnSizeShifts,
            ScanPlan scan
    ) {
        final ScanPlan.IndexRead indexRead = scan.getIndexRead();
        if (indexRead == ScanPlan.IndexRead.NONE) {
            return new LatestByDeferredListValuesFilteredRecordCursorFactory(configuration,
                    metadata, frames, keyIndex, keys, excludedKeys, filter, columnIndexes, columnSizeShifts);
        }
        if (indexRead == ScanPlan.IndexRead.INDEX) {
            return new LatestByValuesIndexedFilteredRecordCursorFactory(configuration,
                    metadata, frames, keyIndex, keys, filter, columnIndexes, columnSizeShifts);
        }
        final int readerKeyIndex = columnIndexes.getQuick(keyIndex);
        final int[] coveringMapping;
        try {
            coveringMapping = ScanFactoryGenerator.buildCoveringMapping(table, readerKeyIndex, columnIndexes);
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.free(filter, th);
            Misc.freeObjList(keys, th);
            throw th;
        }
        final RecordCursorFactory backup = scan.hasCoveringBackup()
                ? new LatestByValuesIndexedFilteredRecordCursorFactory(configuration, metadata,
                frames, keyIndex, keys, filter, columnIndexes, columnSizeShifts)
                : null;
        return new CoveringIndexRecordCursorFactory(metadata, frames, readerKeyIndex, null, columnIndexes,
                coveringMapping, keys, true, filter, null, backup, scan.isCoveringBackupSuppressed());
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
     * Consumes frames, key and filter, including on failure.
     */
    private RecordCursorFactory generateLatestBySingleKey(
            TableAccessInfo table,
            RecordMetadata metadata,
            PartitionFrameCursorFactory frames,
            int keyIndex,
            Function key,
            @Nullable Function filter,
            IntList columnIndexes,
            IntList columnSizeShifts,
            ScanPlan scan
    ) {
        final ScanPlan.IndexRead indexRead = scan.getIndexRead();
        if (indexRead == ScanPlan.IndexRead.NONE) {
            return new LatestByValueDeferredFilteredRecordCursorFactory(configuration, metadata, frames,
                    keyIndex, key, filter, columnIndexes, columnSizeShifts);
        }
        if (indexRead == ScanPlan.IndexRead.INDEX) {
            return buildLatestByIndexScan(configuration, metadata, frames, keyIndex, key, filter, columnIndexes, columnSizeShifts);
        }
        final int readerKeyIndex = columnIndexes.getQuick(keyIndex);
        final int[] coveringMapping;
        try {
            coveringMapping = ScanFactoryGenerator.buildCoveringMapping(table, readerKeyIndex, columnIndexes);
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.free(filter, th);
            Misc.free(key, th);
            throw th;
        }
        final RecordCursorFactory backup = scan.hasCoveringBackup()
                ? buildLatestByIndexScan(configuration, metadata, frames, keyIndex, key, filter, columnIndexes, columnSizeShifts)
                : null;
        return new CoveringIndexRecordCursorFactory(metadata, frames, readerKeyIndex, key, columnIndexes, coveringMapping,
                null, true, filter, null, backup, scan.isCoveringBackupSuppressed());
    }

    /**
     * Generates LATEST BY over a derived input; a table scan input takes the keyed scan path instead.
     */
    RecordCursorFactory generateLatestBy(GenerationFrame frame, LatestByPlan latest, SqlExecutionContext executionContext) throws SqlException {
        final LogicalPlan input = latest.getChild();
        // LATEST BY locates its timestamp by position; a declaration alone adds nothing.
        final RecordCursorFactory base = codeGenerator.generate(frame, PlanToFactoryRules.latestByBase(latest), executionContext);
        final OutputSchema schema = input.getOutput();
        final ArrayColumnTypes keyTypes = frame.keyTypes;
        final ListColumnFilter listColumnFilterA = frame.listColumnFilterA;
        try {
            final RecordMetadata metadata = base.getMetadata();
            keyTypes.clear();
            listColumnFilterA.clear();
            for (int i = 0, n = latest.getKeyColumnIds().size(); i < n; i++) {
                final int index = schema.getColumnIndexById(latest.getKeyColumnIds().getQuick(i));
                keyTypes.add(metadata.getColumnType(index));
                listColumnFilterA.add(index + 1);
            }
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        return generateLatestByPrepared(frame, base, schema.getColumnIndexById(latest.getTimestampColumnId()), latest.getAlgorithm());
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
            TableAccessInfo table,
            IntList columnIndexes,
            IntList columnSizeShifts,
            IntList keyColumnIds,
            @Nullable Function filter,
            ObjList<Function> keys,
            ObjList<Function> excludedKeys,
            @Nullable IntList symbolCounts,
            SqlExecutionContext executionContext
    ) {
        try {
            tmpLongs.clear();
            if (scan.getWithin() != null) {
                PlanToFactoryRules.withinPrefixes(scan.getWithin(), scan.getOutput(), tmpLongs);
            }
            final ArrayColumnTypes keyTypes = frame.keyTypes;
            final ListColumnFilter listColumnFilterA = frame.listColumnFilterA;
            keyTypes.clear();
            listColumnFilterA.clear();
            for (int i = 0, n = keyColumnIds.size(); i < n; i++) {
                final int index = scan.getOutput().getColumnIndexById(keyColumnIds.getQuick(i));
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
        final int keyIndex = keyColumnIds.size() == 0 ? -1 : scan.getOutput().getColumnIndexById(keyColumnIds.getQuick(0));
        return switch (scan.getAccessPath()) {
            case LATEST_BY_VALUE ->
                    generateLatestBySingleKey(table, queryMetadata, frames, keyIndex, keys.getQuick(0), filter,
                            columnIndexes, columnSizeShifts, scan);
            case LATEST_BY_VALUES -> generateLatestByKeyList(table, queryMetadata, frames, keyIndex, keys, excludedKeys,
                    filter, columnIndexes, columnSizeShifts, scan);
            case LATEST_BY_ALL_INDEXED ->
                    new LatestByAllIndexedRecordCursorFactory(executionContext.getCairoEngine(), configuration,
                            queryMetadata, frames, keyIndex, columnIndexes, columnSizeShifts, tmpLongs);
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
