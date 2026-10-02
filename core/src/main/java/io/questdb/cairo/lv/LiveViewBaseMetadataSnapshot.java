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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableReaderMetadataColumn;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

/**
 * The base table metadata a live view's plan was compiled against, less the two things the
 * plan provably does not depend on: the metadata version itself and every SYMBOL column's
 * capacity.
 * <p>
 * {@link LiveViewRefreshSqlExecutionContext#getReader} refuses a base reader whose metadata
 * version moved past the compiled plan's, and each refusal costs the view a recompile and a
 * runtime restore. {@code TableWriter.changeSymbolCapacity} moves that version too, and the
 * writer's auto-scale calls it on every capacity doubling of a growing key set. Nothing the
 * compiled runtime holds depends on the capacity:
 * <ul>
 *     <li>The rebuild keeps every symbol key. It hard-links the values file, copies the
 *     offsets file under a new symbol table name txn and rebuilds only the hash index, so the
 *     window partition maps, the checkpoint timeline and every other structure keyed by
 *     symbol key stay valid.</li>
 *     <li>The base reader reopens its {@code SymbolMapReader} in place on the new name txn and
 *     reads the new capacity from its header, and the compiled plan binds its symbol tables
 *     again at every cursor open.</li>
 *     <li>The plan's column mapping, types and filter do not read it. The only planner path
 *     that compares capacities picks an indexed symbol key column, which a live view compile
 *     never does, and the view's own table does not inherit the base capacity
 *     ({@link LiveViewTableStructure#getSymbolCapacity}).</li>
 * </ul>
 * So a base reader that moved from this snapshot by SYMBOL capacities alone may be served to
 * the plan. Everything else still counts as drift: a column added, dropped, renamed or
 * retyped, a dedup change, an index, a symbol cache flag, a table parameter. Three checks
 * make up "capacities alone":
 * <ul>
 *     <li>The column structure version has not moved. Every structural change moves it, even
 *     one that leaves the metadata as it was, such as a DEDUP DISABLE on a base that never
 *     deduplicated. A capacity change never does.</li>
 *     <li>The metadata equals the snapshot in every field but the capacities.</li>
 *     <li>At least one capacity differs. A metadata version that moved with no visible
 *     change at all is not a capacity change, so it stays drift.</li>
 * </ul>
 * A statement that rewrites the metadata without changing it, landing in the same backlog as
 * a capacity change, passes with it. It changed nothing the plan could read.
 * <p>
 * Both sides of the comparison come from a fresh parse of the reader's own metadata bytes
 * ({@link TableReaderMetadata#loadFrom}), never from the reader's metadata object. A reloaded
 * reader keeps a column's metadata object across a change that leaves the column's name,
 * writer index, index and dedup flag alone, so that object can still carry the previous cache
 * flag, capacity and parquet encoding, and a cache flag change would read as no change at all.
 * <p>
 * The refresh job accesses a snapshot only under the view's refresh latch.
 */
final class LiveViewBaseMetadataSnapshot {
    private final int columnStructureVersion;
    private final long metadataVersion;
    private final String schema;
    private final IntList symbolCapacities;
    // The newest base metadata version this snapshot proved to differ from metadataVersion by
    // SYMBOL capacities alone, or -1. The metadata at a given version never changes, so the
    // answer holds for good, and every cursor open after a capacity change costs a comparison
    // rather than a metadata parse.
    private long capacityOnlyMetadataVersion = -1;

    private LiveViewBaseMetadataSnapshot(
            long metadataVersion,
            int columnStructureVersion,
            String schema,
            IntList symbolCapacities
    ) {
        this.metadataVersion = metadataVersion;
        this.columnStructureVersion = columnStructureVersion;
        this.schema = schema;
        this.symbolCapacities = symbolCapacities;
    }

    /**
     * Snapshots the metadata {@code reader} holds, which is the metadata a plan compiled
     * against that reader records. Returns null when the metadata cannot be read, which
     * leaves the plan counting every metadata change as drift.
     */
    static @Nullable LiveViewBaseMetadataSnapshot of(CairoConfiguration configuration, TableReader reader) {
        try (TableReaderMetadata metadata = parse(configuration, reader)) {
            if (metadata.getMetadataVersion() != reader.getMetadataVersion()) {
                return null;
            }
            final IntList symbolCapacities = new IntList(metadata.getColumnCount());
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                symbolCapacities.add(metadata.getColumnMetadata(i).getSymbolCapacity());
            }
            return new LiveViewBaseMetadataSnapshot(
                    metadata.getMetadataVersion(),
                    reader.getTxFile().getColumnStructureVersion(),
                    encode(metadata),
                    symbolCapacities
            );
        } catch (CairoException e) {
            return null;
        }
    }

    /**
     * Whether a plan compiled at {@code compiledMetadataVersion} may read through
     * {@code reader}, whose metadata version differs from it: true only when this snapshot
     * is the plan's metadata and the reader moved from it by SYMBOL capacities alone. A
     * metadata read that fails answers false, so the caller counts the change as drift, as
     * it did before the exemption.
     */
    boolean isSymbolCapacityOnlyChange(CairoConfiguration configuration, long compiledMetadataVersion, TableReader reader) {
        if (compiledMetadataVersion != metadataVersion) {
            return false;
        }
        final long readerMetadataVersion = reader.getMetadataVersion();
        if (readerMetadataVersion == capacityOnlyMetadataVersion) {
            return true;
        }
        if (reader.getTxFile().getColumnStructureVersion() != columnStructureVersion) {
            return false;
        }
        try (TableReaderMetadata metadata = parse(configuration, reader)) {
            if (metadata.getMetadataVersion() != readerMetadataVersion
                    || !Chars.equals(schema, encode(metadata))
                    || !hasSymbolCapacityChange(metadata)) {
                return false;
            }
        } catch (CairoException e) {
            return false;
        }
        capacityOnlyMetadataVersion = readerMetadataVersion;
        return true;
    }

    // Every field TableReaderMetadata.readFromMem parses, except the metadata version and the
    // SYMBOL capacities. A field the reader learns to parse belongs here too, or a change to it
    // alone would read as a capacity-only change. Column names are length-prefixed, so no name
    // can forge a separator.
    private static String encode(TableReaderMetadata metadata) {
        final StringSink sink = new StringSink();
        sink.put(metadata.getTableId())
                .put(',').put(metadata.getPartitionBy())
                .put(',').put(metadata.getTimestampIndex())
                .put(',').put(metadata.isWalEnabled())
                .put(',').put(metadata.getMaxUncommittedRows())
                .put(',').put(metadata.getO3MaxLag())
                .put(',').put(metadata.getTtlHoursOrMonths())
                .put(',').put(metadata.getTableFormat())
                .put(',').put(metadata.getWriterColumnCount())
                .put(',').put(metadata.getColumnCount());
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final TableColumnMetadata column = metadata.getColumnMetadata(i);
            final String name = column.getColumnName();
            sink.put('|').put(name.length()).put(':').put(name)
                    .put(',').put(column.getColumnType())
                    .put(',').put(column.getWriterIndex())
                    .put(',').put(column.getOriginalWriterIndex())
                    .put(',').put(column.getReplacingIndex())
                    .put(',').put(metadata.getDenseSymbolIndex(i))
                    .put(',').put(column instanceof TableReaderMetadataColumn readerColumn ? readerColumn.getStableIndex() : -1)
                    .put(',').put(column.getIndexType())
                    .put(',').put(column.getIndexValueBlockCapacity())
                    .put(',').put(column.isSymbolTableStatic())
                    .put(',').put(column.isSymbolCacheFlag())
                    .put(',').put(column.isDedupKeyFlag())
                    .put(',').put(column.getParquetEncodingConfig());
            final IntList covering = column.getCoveringColumnIndices();
            if (covering != null) {
                for (int j = 0, m = covering.size(); j < m; j++) {
                    sink.put(';').put(covering.getQuick(j));
                }
            }
        }
        return sink.toString();
    }

    // A private parse of the reader's metadata bytes, with column metadata objects of its own.
    // The caller closes it.
    private static TableReaderMetadata parse(CairoConfiguration configuration, TableReader reader) {
        final TableReaderMetadata source = reader.getMetadata();
        final TableReaderMetadata metadata = new TableReaderMetadata(configuration, source.getTableToken());
        try {
            metadata.loadFrom(source);
            return metadata;
        } catch (Throwable th) {
            metadata.close();
            throw th;
        }
    }

    // The caller has proved the column lists equal, so the capacities line up by position.
    private boolean hasSymbolCapacityChange(TableReaderMetadata metadata) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getColumnMetadata(i).getSymbolCapacity() != symbolCapacities.getQuick(i)) {
                return true;
            }
        }
        return false;
    }
}
