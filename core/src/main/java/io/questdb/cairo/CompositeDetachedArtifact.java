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

import io.questdb.std.FilesFacade;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

/**
 * Reads a DETACHED composite partition artifact (a {@code <day>.detached} / {@code <day>.attachable}
 * directory) as ATTACH sees it.
 * <p>
 * {@code detachPartition} copies {@code _meta}, {@code _cv} and {@code _txn} into the artifact, and a
 * composite day keeps its data one level down in per-cell directories. The copied {@code _txn} carries
 * one attached-partition entry per cell, each with its cellKey, so the artifact's cells can be
 * enumerated authoritatively from metadata.
 * <p>
 * <b>Never parse a cell directory name to recover this.</b> Segment rendering is deliberately one-way:
 * it is path-safety encoded, has its own NULL token, and several dimension kinds render through
 * {@code putCellSegmentPathSafe}. Cell-qualified DROP already resolves names by RENDERING each attached
 * cellKey and comparing, precisely to avoid a reverse parse. A parser here would be a second, lossier
 * source of truth for the same mapping.
 */
public final class CompositeDetachedArtifact {

    private CompositeDetachedArtifact() {
    }

    /**
     * Collects, into {@code out}, the cellKey of every entry in the artifact's {@code _txn} whose
     * partition timestamp is {@code partitionTimestamp}. A plain artifact yields exactly one entry,
     * with cellKey 0.
     * <p>
     * The artifact's {@code _txn} is the whole table's {@code _txn} as of the detach, so it lists other
     * partitions too -- filtering by timestamp is required, not incidental.
     *
     * @param artifactRoot path to the artifact directory itself; left trimmed back to its original
     *                     length on return
     */
    public static void readCellKeys(
            FilesFacade ff,
            Path artifactRoot,
            int timestampType,
            int partitionBy,
            long partitionTimestamp,
            IntList out
    ) {
        readCells(ff, artifactRoot, timestampType, partitionBy, partitionTimestamp, out, null);
    }

    /**
     * Collects the cellKey AND row count of every entry the artifact holds for {@code partitionTimestamp},
     * in {@code _txn} order. {@code sizesOut} may be null when only the keys are wanted.
     */
    public static void readCells(
            FilesFacade ff,
            Path artifactRoot,
            int timestampType,
            int partitionBy,
            long partitionTimestamp,
            IntList cellKeysOut,
            @Nullable LongList sizesOut
    ) {
        cellKeysOut.clear();
        if (sizesOut != null) {
            sizesOut.clear();
        }
        final int rootLen = artifactRoot.size();
        try (TxReader txReader = new TxReader(ff)) {
            txReader.ofRO(artifactRoot.concat(TableUtils.TXN_FILE_NAME).$(), timestampType, partitionBy);
            txReader.unsafeLoadAll();
            // getPartitionCount()/getPartitionCellKey() are stride-aware: the artifact's _txn carries the
            // self-describing stride marker, so a composite artifact reads back stride 8 and a plain one
            // stride 4 (cellKey always 0) without the caller knowing which it holds.
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.getPartitionTimestampByIndex(i) == partitionTimestamp) {
                    cellKeysOut.add(txReader.getPartitionCellKey(i));
                    if (sizesOut != null) {
                        sizesOut.add(txReader.getPartitionSize(i));
                    }
                }
            }
        } finally {
            artifactRoot.trimTo(rootLen);
        }
    }

    /**
     * Refuses an artifact that did not come from this table.
     * <p>
     * A cellKey is table-local, and the artifact carries {@code _meta}, {@code _cv} and {@code _txn} but
     * NOT the dimension dictionaries or the {@code _cell} registry -- those live at the table root. So a
     * foreign artifact's cellKeys cannot be decoded to dimension values here, and attaching it would
     * bind its cells to whatever local cells happen to share those ordinals: silently wrong data filed
     * under a different dimension value.
     * <p>
     * Cross-table attach needs a self-describing artifact (the interners copied in, or a
     * cellKey-to-values manifest written at detach). Until then this refuses.
     */
    public static void checkSameTable(
            FilesFacade ff,
            CairoConfiguration configuration,
            Path artifactRoot,
            int expectedTableId,
            CharSequence tableName
    ) {
        // WHAT IT WOULD TAKE to lift this, established 2026-08-26 by looking at what DETACH actually
        // copies rather than by argument -- so the next attempt starts from evidence.
        //
        // detachPartition copies exactly three table-level files into the artifact via copyOverwrite:
        // _meta, _cv, _txn. It does NOT copy symbol tables. A composite _txn stores cellKeys, and
        // decoding a cellKey needs the _cell registry plus the dedicated dictionaries -- neither of
        // which travels. Hence this refusal: a FOREIGN artifact's cellKeys are undecodable.
        //
        // The two candidate fixes are NOT equally conventional, which is the useful part:
        //   - COPY THE INTERNERS alongside _meta/_cv/_txn. Reuses the existing mechanism exactly, but
        //     the interners ARE symbol maps, and symbol maps are deliberately NOT copied today (a
        //     plain ATTACH resolves symbol keys against the TARGET table's own tables). So this
        //     reverses an existing, deliberate choice for symbol-like data.
        //   - WRITE A cellKey->values MANIFEST. Decodes without shipping symbol machinery, but
        //     introduces a file format that exists nowhere else in the artifact.
        //
        // So the existing convention does NOT settle it -- both options are new behaviour, and it is
        // a genuine format decision with compatibility consequences (free while unreleased, a break
        // afterwards). It also means invariant 4 ("values, not ordinals, across table boundaries") is
        // not satisfiable by today's artifact at all.
        final int rootLen = artifactRoot.size();
        try (TableReaderMetadata artifactMeta = new TableReaderMetadata(configuration)) {
            final LPSZ metaPath = artifactRoot.concat(TableUtils.META_FILE_NAME).$();
            if (!ff.exists(metaPath)) {
                throw CairoException.critical(0)
                        .put("composite partitioning does not yet support attaching a partition from another table")
                        .put(" [table=").put(tableName).put(", reason=artifact carries no _meta]");
            }
            artifactMeta.loadMetadata(metaPath);
            final int artifactTableId = artifactMeta.getTableId();
            if (artifactTableId != expectedTableId) {
                throw CairoException.critical(0)
                        .put("composite partitioning does not yet support attaching a partition from another table")
                        .put(" [table=").put(tableName)
                        .put(", tableId=").put(expectedTableId)
                        .put(", artifactTableId=").put(artifactTableId).put(']');
            }
        } finally {
            artifactRoot.trimTo(rootLen);
        }
    }

    /**
     * Refuses an artifact whose SOURCE partition spec does not mean the same thing as {@code localSpec}.
     * <p>
     * The safety gate that must pass before a foreign artifact's manifest values may be re-interned
     * locally. {@code "BTC"} under an {@code IDENTITY} dimension and {@code "BTC"} under a
     * {@code TRUNCATE(3)} dimension are not the same cell, and a {@code HASH} bucket number means
     * nothing without the same bucket count -- so equal VALUES are not sufficient, the dimensions they
     * are values OF must agree too.
     * <p>
     * <b>Read from the artifact's own {@code _meta}, not from the manifest.</b> {@code _meta} already
     * persists the whole spec -- naming mode, and per dimension the kind, column index, param, alias
     * and exprText (see {@code TableUtils#writeCompositePartitionBlock}) -- so duplicating any of it
     * into {@code _cell_manifest.d} would create a second source of truth for the same facts. That is
     * why the manifest carries values only, and why it needs no v2 to support this check.
     * <p>
     * <b>Deliberately conservative.</b> Each rule below can be relaxed later with evidence; none can be
     * added later without breaking artifacts that were already accepted. A wrongly accepted attach is
     * silent corruption filed under the wrong dimension value, so the asymmetry favours strictness:
     * <ul>
     *   <li>dimension COUNT must match -- a differing arity cannot be remapped at all;</li>
     *   <li>each dimension's KIND must match;</li>
     *   <li>{@code param} must match for {@code HASH} (bucket count) and {@code TRUNCATE} (prefix
     *       length): both change which value maps to which cell;</li>
     *   <li>{@code exprText} must match for {@code EXPRESSION} -- a different expression computes a
     *       different value from the same row;</li>
     *   <li>for {@code IDENTITY}, the SOURCE COLUMN NAME must match. Strictly this is stronger than
     *       needed -- the values are strings either way -- but attaching {@code exch} data into a
     *       {@code venue} dimension is far more likely a mistake than an intent, and the cost of
     *       being wrong is silent misfiling;</li>
     *   <li>the NAMING MODE must match. This one is not semantic: it decides how cell directories are
     *       rendered, so a mismatch means every directory in the artifact would need renaming. It is
     *       refused rather than handled because renaming is remap work, and this is the gate that runs
     *       before any of it.</li>
     * </ul>
     *
     * @param artifactRoot artifact directory; trimmed back to its original length on return
     */
    public static void checkSpecCompatible(
            FilesFacade ff,
            CairoConfiguration configuration,
            Path artifactRoot,
            PartitionSpec localSpec,
            TableRecordMetadata localMeta,
            CharSequence tableName
    ) {
        final int rootLen = artifactRoot.size();
        try (TableReaderMetadata artifactMeta = new TableReaderMetadata(configuration)) {
            final LPSZ metaPath = artifactRoot.concat(TableUtils.META_FILE_NAME).$();
            if (!ff.exists(metaPath)) {
                throw incompatible(tableName).put(", reason=artifact carries no _meta]");
            }
            artifactMeta.loadMetadata(metaPath);
            final PartitionSpec srcSpec = artifactMeta.getPartitionSpec();

            final int srcDims = srcSpec.getDimensionCount();
            final int dstDims = localSpec.getDimensionCount();
            if (srcDims != dstDims) {
                throw incompatible(tableName)
                        .put(", reason=dimension count differs, artifact=").put(srcDims)
                        .put(", table=").put(dstDims).put(']');
            }
            if (srcSpec.getNamingMode() != localSpec.getNamingMode()) {
                throw incompatible(tableName)
                        .put(", reason=naming mode differs, artifact=").put(srcSpec.getNamingMode())
                        .put(", table=").put(localSpec.getNamingMode()).put(']');
            }
            for (int i = 0; i < srcDims; i++) {
                final PartitionDimension src = srcSpec.getDimension(i);
                final PartitionDimension dst = localSpec.getDimension(i);
                if (src.getKind() != dst.getKind()) {
                    throw incompatible(tableName)
                            .put(", reason=dimension ").put(i).put(" kind differs, artifact=")
                            .put(src.getKind()).put(", table=").put(dst.getKind()).put(']');
                }
                switch (src.getKind()) {
                    case PartitionDimension.KIND_HASH:
                    case PartitionDimension.KIND_TRUNCATE:
                        if (src.getParam() != dst.getParam()) {
                            throw incompatible(tableName)
                                    .put(", reason=dimension ").put(i).put(" param differs, artifact=")
                                    .put(src.getParam()).put(", table=").put(dst.getParam()).put(']');
                        }
                        break;
                    case PartitionDimension.KIND_EXPRESSION:
                        if (!sameText(src.getExprText(), dst.getExprText())) {
                            throw incompatible(tableName)
                                    .put(", reason=dimension ").put(i).put(" expression differs]");
                        }
                        break;
                    case PartitionDimension.KIND_IDENTITY: {
                        final CharSequence srcCol = src.getColumnIndex() > -1
                                ? artifactMeta.getColumnName(src.getColumnIndex()) : null;
                        final CharSequence dstCol = dst.getColumnIndex() > -1
                                ? localMeta.getColumnName(dst.getColumnIndex()) : null;
                        if (!sameText(srcCol, dstCol)) {
                            throw incompatible(tableName)
                                    .put(", reason=dimension ").put(i).put(" source column differs, artifact=")
                                    .put(srcCol == null ? "<none>" : srcCol)
                                    .put(", table=").put(dstCol == null ? "<none>" : dstCol).put(']');
                        }
                        break;
                    }
                    default:
                        throw incompatible(tableName)
                                .put(", reason=unknown dimension kind ").put(src.getKind()).put(']');
                }
            }
        } finally {
            artifactRoot.trimTo(rootLen);
        }
    }

    /**
     * Null-safe text equality. {@code Chars.equalsNc} requires a NON-NULL left operand, and both sides
     * here are legitimately null -- exprText is null for every non-EXPRESSION dimension, and a
     * dimension with no source column has none to name.
     */
    private static boolean sameText(CharSequence a, CharSequence b) {
        if (a == null || b == null) {
            return a == null && b == null;
        }
        return Chars.equals(a, b);
    }

    private static CairoException incompatible(CharSequence tableName) {
        return CairoException.critical(0)
                .put("composite partitioning cannot attach an artifact with an incompatible partition spec")
                .put(" [table=").put(tableName);
    }

    /**
     * Folds the designated-timestamp min and max across the artifact's cells into
     * {@code minMaxOut[0]}/{@code minMaxOut[1]}.
     * <p>
     * Deliberately never reads the container root. A detached composite artifact carries ZERO-BYTE
     * phantom {@code <column>.d} files there -- measured, not assumed -- so a root read returns -1 for
     * both bounds. Worse, if such a file ever held bytes it would yield silently WRONG bounds rather
     * than an error.
     *
     * @param cellSegments rendered cell segment names, in the same order as {@code cellSizes}
     */
    public static void readMinMaxTimestamps(
            FilesFacade ff,
            Path artifactRoot,
            CharSequence tsColumnName,
            int timestampType,
            ObjList<CharSequence> cellSegments,
            LongList cellSizes,
            long[] minMaxOut
    ) {
        final int rootLen = artifactRoot.size();
        try {
            minMaxOut[0] = -1;
            minMaxOut[1] = -1;
            boolean first = true;
            for (int i = 0, n = cellSegments.size(); i < n; i++) {
                artifactRoot.trimTo(rootLen).concat(cellSegments.getQuick(i));
                readBounds(ff, artifactRoot, tsColumnName, timestampType, cellSizes.getQuick(i), minMaxOut, first);
                if (minMaxOut[0] < 0 || minMaxOut[1] < 0) {
                    // One unreadable cell fails the whole fold: a day whose bounds are computed from a
                    // subset of its cells is worse than one that refuses to attach.
                    return;
                }
                first = false;
            }
        } finally {
            artifactRoot.trimTo(rootLen);
        }
    }

    private static void readBounds(
            FilesFacade ff,
            Path dir,
            CharSequence tsColumnName,
            int timestampType,
            long rows,
            long[] minMaxOut,
            boolean reset
    ) {
        final int len = dir.size();
        try {
            final long fd = ff.openRO(TableUtils.dFile(dir, tsColumnName, TableUtils.COLUMN_NAME_TXN_NONE));
            if (fd < 0) {
                minMaxOut[0] = -1;
                minMaxOut[1] = -1;
                return;
            }
            try {
                final long lo = ff.readNonNegativeLong(fd, 0);
                final long hi = ff.readNonNegativeLong(fd, (rows - 1) * ColumnType.sizeOf(timestampType));
                if (reset || lo < 0 || hi < 0) {
                    minMaxOut[0] = lo;
                    minMaxOut[1] = hi;
                } else {
                    minMaxOut[0] = Math.min(minMaxOut[0], lo);
                    minMaxOut[1] = Math.max(minMaxOut[1], hi);
                }
            } finally {
                ff.close(fd);
            }
        } finally {
            dir.trimTo(len);
        }
    }

    /**
     * Returns the total row count the artifact holds for {@code partitionTimestamp}, summed across every
     * cell. A plain artifact has exactly one entry, so the sum degenerates to it.
     */
    public static long readSize(
            FilesFacade ff,
            Path artifactRoot,
            int timestampType,
            int partitionBy,
            long partitionTimestamp
    ) {
        final int rootLen = artifactRoot.size();
        try (TxReader txReader = new TxReader(ff)) {
            txReader.ofRO(artifactRoot.concat(TableUtils.TXN_FILE_NAME).$(), timestampType, partitionBy);
            txReader.unsafeLoadAll();
            // Deliberately NOT getPartitionRowCountByTimestamp: that resolves through
            // findAttachedPartitionRawIndexByLoTimestamp, which hardcodes cellKey = 0, so on a composite
            // artifact it returns the FIRST cell's row count and calls it the day's.
            long size = 0;
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.getPartitionTimestampByIndex(i) == partitionTimestamp) {
                    size += txReader.getPartitionSize(i);
                }
            }
            return size;
        } finally {
            artifactRoot.trimTo(rootLen);
        }
    }
}
