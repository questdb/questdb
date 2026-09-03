/*******************************************************************************
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

package io.questdb.cairo;

import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;

/**
 * Renders a composite cell's on-disk SEGMENT name from its {@code cellKey}, standing the required
 * readers up directly from a table DIRECTORY rather than from a live {@link TableReader}.
 * <p>
 * That mapping -- attached-partition RECORD to the DIRECTORY it describes -- is what blocks any
 * offline tool that walks a table's files. Directories are named {@code <segment>.<nameTxn>} and
 * CONVERT stamps one name-txn across every cell of a day, so the suffix cannot tell {@code E0.5} from
 * {@code E1.5}; only the segment does. Producing it needs the cell registry plus a reader per
 * dimension -- the interner stack {@link TableReader} builds at open.
 * <p>
 * Everything opened here is READ-ONLY and OWNED by this object, so {@link #close()} frees it. That is
 * the opposite of {@code CompositeDictionaries#cellRegistry()} on a live reader, which is non-owning;
 * mixing the two conventions up leaks or double-frees.
 * <p>
 * Two callers, and they are the reason this is a top-level class rather than a private helper:
 * {@code TableSnapshotRestore} (restoring a checkpoint) and the enterprise checkpoint manifest
 * (describing one). Both walk files that no engine reader is open on.
 */
public class CellSegmentResolver implements QuietCloseable {
    private final CairoConfiguration configuration;
    private final ObjList<SymbolMapReader> dimReaders = new ObjList<>();
    private CompositeInternerLayout layout;
    private CellRegistry registry;
    private SymbolMapReader registryReader;
    private int[] tuple;

    public CellSegmentResolver(CairoConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * The dense (metadata) index of a dimension's SOURCE column, from the writer index the dimension
     * records. A dimension names its source by writer index precisely so that it survives a column
     * drop reordering the dense view.
     */
    public static int denseIndexOfDimensionSource(TableReaderMetadata metadata, PartitionDimension dim) {
        final int writerIndex = dim.getColumnIndex();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getWriterIndex(i) == writerIndex) {
                return i;
            }
        }
        throw CairoException.critical(0)
                .put("composite dimension source column not found [writerIndex=").put(writerIndex).put(']');
    }

    @Override
    public void close() {
        registry = Misc.free(registry);
        registryReader = Misc.freeIfCloseable(registryReader);
        for (int i = 0, n = dimReaders.size(); i < n; i++) {
            Misc.freeIfCloseable(dimReaders.getQuick(i));
        }
        dimReaders.clear();
    }

    /**
     * Opens the registry and one reader per dimension against {@code tableDir}.
     *
     * @return false when the table is not a routed composite one, in which case nothing is opened
     */
    public boolean of(Path tableDir, TableReaderMetadata metadata, TxReader txReader, ColumnVersionReader cvReader) {
        close();
        final PartitionSpec spec = metadata.getPartitionSpec();
        final int dimCount = spec.getDimensionCount();
        if (dimCount <= 0) {
            return false;
        }
        layout = CompositeInternerLayout.of(spec);
        tuple = new int[dimCount];

        // Interner slots sit AFTER the real symbol columns: the registry is the LAST symbol column and
        // the dedicated dicts immediately precede it. Same derivation rebuildCompositeInternerFiles
        // uses; layout.registrySlot()/dedicatedDictSlot() are dense within the interner block, so they
        // need this base added. Passing the raw slot tripped SymbolMapReaderImpl's
        // "charSize > 0 || symbolCount == 0" assert on an EXPRESSION dimension -- a count read from
        // the wrong column against an empty char file.
        final int registrySlot = txReader.getSymbolColumnCount() - 1;
        final int dedicatedBase = registrySlot - layout.dedicatedCount();
        registryReader = new SymbolMapReaderImpl(
                configuration,
                tableDir,
                CompositeInternerLayout.REGISTRY_NAME,
                CompositeInternerLayout.REGISTRY_TXN,
                txReader.getSymbolValueCount(registrySlot)
        );
        registry = new CellRegistry(registryReader);

        for (int i = 0; i < dimCount; i++) {
            final PartitionDimension dim = spec.getDimension(i);
            if (dim.getKind() == PartitionDimension.KIND_HASH) {
                // A bucket cannot be un-hashed and does not need to be: the ordinal IS the name.
                dimReaders.add(null);
            } else if (layout.needsDedicatedDict(i)) {
                dimReaders.add(new SymbolMapReaderImpl(
                        configuration,
                        tableDir,
                        layout.dictName(i),
                        layout.dictColumnNameTxn(i),
                        txReader.getSymbolValueCount(dedicatedBase + layout.dedicatedDictSlot(i))
                ));
            } else {
                final int denseIndex = denseIndexOfDimensionSource(metadata, dim);
                dimReaders.add(new SymbolMapReaderImpl(
                        configuration,
                        tableDir,
                        metadata.getColumnName(denseIndex),
                        cvReader.getDefaultColumnNameTxn(metadata.getWriterIndex(denseIndex)),
                        txReader.getSymbolValueCount(metadata.getDenseSymbolIndex(denseIndex))
                ));
            }
        }
        return true;
    }

    /**
     * Byte-identical to {@link TableReader#renderCellSegment}, which is what named the directory.
     */
    public void render(CharSink<?> sink, TableReaderMetadata metadata, int cellKey) {
        final PartitionSpec spec = metadata.getPartitionSpec();
        registry.getTuple(cellKey, tuple);
        final byte namingMode = spec.getNamingMode();
        for (int i = 0, n = spec.getDimensionCount(); i < n; i++) {
            if (i > 0) {
                sink.put('/');
            }
            final PartitionDimension dim = spec.getDimension(i);
            if (namingMode == PartitionSpec.MODE_HIVE) {
                if (dim.getKind() == PartitionDimension.KIND_EXPRESSION) {
                    sink.put(dim.getAlias()).put('=');
                } else {
                    sink.put(metadata.getColumnName(denseIndexOfDimensionSource(metadata, dim))).put('=');
                }
            }
            if (dim.getKind() == PartitionDimension.KIND_HASH) {
                sink.put(tuple[i]);
            } else {
                TableUtils.putCellSegmentPathSafe(sink, tuple[i], dimReaders.getQuick(i).valueOf(tuple[i]));
            }
        }
    }
}
