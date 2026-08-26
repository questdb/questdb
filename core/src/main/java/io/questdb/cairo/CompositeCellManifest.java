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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;


/**
 * The {@code _cell_manifest.d} sidecar: what a detached composite partition's cellKeys MEAN, written
 * so the artifact is self-describing.
 * <p>
 * <b>Why this exists.</b> A cellKey is table-LOCAL -- an ordinal into that table's own {@code _cell}
 * registry. A detached artifact carries {@code _meta}, {@code _cv} and {@code _txn}, but the dimension
 * dictionaries and the {@code _cell} registry live at the TABLE ROOT and are deliberately not copied
 * (the same choice {@code detachPartition} makes for symbol tables). So a foreign artifact's cellKeys
 * cannot be decoded by the receiver, and attaching one would bind its cells to whatever local cells
 * happen to share those ordinals -- silently wrong data filed under a different dimension value, which
 * invariant 2 forbids. That is why cross-table ATTACH is refused.
 * <p>
 * <b>Why a manifest rather than copying the dictionaries in.</b> Measured 2026-08-26 on a 2000-value
 * dimension: the table-root dictionaries totalled 2,252,800 bytes against 20,000 bytes for one
 * partition -- a ratio of <b>112x</b>. That cost is dominated by pre-allocated symbol capacity, so it
 * is a near-CONSTANT: detaching a single-cell partition from a wide table would still copy ~2 MiB, and
 * a composite table has two such structures (the dimension's own symbol map and the {@code _cell}
 * registry), both of which would be duplicated into every artifact. A manifest is instead proportional
 * to the cells the partition actually holds, which is the quantity that varies.
 * <p>
 * <b>Format</b> (little-endian; all ints 32-bit):
 * <pre>
 *   int    version      FORMAT_VERSION
 *   int    dimCount     dimensions in the source table's partition spec
 *   int    cellCount    cells this partition holds
 *   repeat cellCount:
 *     int  cellKey      as it appears in the artifact's _txn
 *     repeat dimCount:
 *       int   len       byte length of the UTF-8 value, or NULL_LEN
 *       byte  utf8[len] omitted entirely when len == NULL_LEN
 * </pre>
 * Values are stored, not ordinals, precisely because ordinals are the thing that does not travel.
 * <p>
 * <b>This class only reads and writes the file.</b> Consuming it -- re-interning the values into the
 * receiving table's dictionaries, minting local cellKeys and remapping the artifact -- is the separate
 * step that would let cross-table ATTACH be supported; the refusal in
 * {@link CompositeDetachedArtifact#checkSameTable} stands until then. Writing the manifest first means
 * artifacts detached from now on are already self-describing when that lands.
 */
public final class CompositeCellManifest {

    public static final String FILE_NAME = "_cell_manifest.d";
    public static final int FORMAT_VERSION = 1;
    /** Sentinel length for a NULL dimension value; no bytes follow. */
    public static final int NULL_LEN = -1;
    /**
     * Refuses absurd headers before any allocation. A composite partition's cell count is bounded by
     * the table's registry size in practice; this only needs to reject garbage, not be tight.
     */
    private static final int SANE_MAX = 1 << 24;

    private CompositeCellManifest() {
    }

    /**
     * Reads the manifest into {@code cellKeysOut} and {@code valuesOut}, where {@code valuesOut} holds
     * {@code cellCount * dimCount} entries in row-major order (cell 0's dims first). A NULL dimension
     * value reads back as a null entry.
     *
     * @param artifactRoot artifact directory; trimmed back to its original length on return
     * @return the dimension count the manifest was written with
     * @throws CairoException if the file is missing, truncated, or fails validation
     */
    public static int read(
            FilesFacade ff,
            Path artifactRoot,
            io.questdb.std.IntList cellKeysOut,
            ObjList<String> valuesOut
    ) {
        cellKeysOut.clear();
        valuesOut.clear();
        final int rootLen = artifactRoot.size();
        try {
            final Path p = artifactRoot.concat(FILE_NAME);
            if (!ff.exists(p.$())) {
                throw CairoException.critical(0)
                        .put("composite cell manifest is missing [path=").put(p).put(']');
            }
            final long fileSize = ff.length(p.$());
            if (fileSize < 12) {
                throw CairoException.critical(0)
                        .put("composite cell manifest is truncated [path=").put(p)
                        .put(", size=").put(fileSize).put(']');
            }
            try (MemoryCMR mem = Vm.getCMRInstance()) {
                mem.of(ff, p.$(), fileSize, fileSize, MemoryTag.MMAP_DEFAULT);
                long off = 0;
                final int version = mem.getInt(off);
                off += Integer.BYTES;
                if (version != FORMAT_VERSION) {
                    throw CairoException.critical(0)
                            .put("unsupported composite cell manifest version [expected=").put(FORMAT_VERSION)
                            .put(", actual=").put(version).put(", path=").put(p).put(']');
                }
                final int dimCount = mem.getInt(off);
                off += Integer.BYTES;
                final int cellCount = mem.getInt(off);
                off += Integer.BYTES;
                if (dimCount < 0 || dimCount > SANE_MAX || cellCount < 0 || cellCount > SANE_MAX) {
                    throw CairoException.critical(0)
                            .put("composite cell manifest header is not sane [dimCount=").put(dimCount)
                            .put(", cellCount=").put(cellCount).put(", path=").put(p).put(']');
                }
                for (int c = 0; c < cellCount; c++) {
                    // Every read below is bounds-checked against fileSize BEFORE it happens: a
                    // truncated or corrupted manifest must raise here rather than read past the
                    // mapping, and a length word is attacker-controlled in exactly the same way a
                    // corrupted one is.
                    requireRemaining(p, off, Integer.BYTES, fileSize);
                    cellKeysOut.add(mem.getInt(off));
                    off += Integer.BYTES;
                    for (int d = 0; d < dimCount; d++) {
                        requireRemaining(p, off, Integer.BYTES, fileSize);
                        final int len = mem.getInt(off);
                        off += Integer.BYTES;
                        if (len == NULL_LEN) {
                            valuesOut.add(null);
                            continue;
                        }
                        if (len < 0 || len > SANE_MAX) {
                            throw CairoException.critical(0)
                                    .put("composite cell manifest value length is not sane [len=").put(len)
                                    .put(", path=").put(p).put(']');
                        }
                        requireRemaining(p, off, len, fileSize);
                        // Decoded via byte[] rather than a Utf8StringSink: that sink asserts each byte
                        // is ASCII, and dimension values are not (measured -- a "ETH-€/£" round-trip
                        // tripped "b is ascii"). The format stores UTF-8 BYTES, so it must decode as
                        // UTF-8, not byte-by-byte.
                        final byte[] raw = new byte[len];
                        for (int i = 0; i < len; i++) {
                            raw[i] = mem.getByte(off + i);
                        }
                        valuesOut.add(new String(raw, java.nio.charset.StandardCharsets.UTF_8));
                        off += len;
                    }
                }
                return dimCount;
            }
        } finally {
            artifactRoot.trimTo(rootLen);
        }
    }

    /**
     * Writes the manifest into the artifact directory.
     *
     * @param artifactRoot artifact directory; trimmed back to its original length on return
     * @param cellKeys     one entry per cell this partition holds
     * @param values       {@code cellKeys.size() * dimCount} entries, row-major, nulls permitted
     */
    public static void write(
            FilesFacade ff,
            Path artifactRoot,
            int fileOpenOpts,
            int dimCount,
            io.questdb.std.IntList cellKeys,
            ObjList<String> values
    ) {
        final int cellCount = cellKeys.size();
        if (values.size() != cellCount * dimCount) {
            throw CairoException.critical(0)
                    .put("composite cell manifest value count mismatch [cells=").put(cellCount)
                    .put(", dims=").put(dimCount).put(", values=").put(values.size()).put(']');
        }
        final int rootLen = artifactRoot.size();
        long written = 0;
        try (MemoryCMARW mem = Vm.getCMARWInstance()) {
            final Path p = artifactRoot.concat(FILE_NAME);
            mem.smallFile(ff, p.$(), MemoryTag.MMAP_DEFAULT);
            mem.jumpTo(0);
            mem.putInt(FORMAT_VERSION);
            mem.putInt(dimCount);
            mem.putInt(cellCount);
            int vi = 0;
            for (int c = 0; c < cellCount; c++) {
                mem.putInt(cellKeys.getQuick(c));
                for (int d = 0; d < dimCount; d++) {
                    final String v = values.getQuick(vi++);
                    if (v == null) {
                        mem.putInt(NULL_LEN);
                        continue;
                    }
                    final byte[] bytes = v.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    mem.putInt(bytes.length);
                    for (byte b : bytes) {
                        mem.putByte(b);
                    }
                }
            }
            written = mem.getAppendOffset();
        } finally {
            artifactRoot.trimTo(rootLen);
        }

        // Truncate to EXACTLY what was written, after the mapping is closed.
        //
        // MEASURED: smallFile maps a whole page and setTruncateSize() only calls jumpTo(), so the file
        // stayed 4096 bytes for 72 bytes of content. That is not cosmetic. The reader bounds-checks
        // against the FILE SIZE, so ~4 KiB of zero padding gives a corrupted length word thousands of
        // bytes of slack before the check fires -- and a zero-filled tail parses as a well-formed
        // EMPTY manifest (version 1, cellCount 0) rather than as corruption. Both are silent wrong
        // answers. CompositeCellManifestTest pins the exact byte count so this cannot drift back.
        final int rootLen2 = artifactRoot.size();
        try {
            final Path p = artifactRoot.concat(FILE_NAME);
            final long fd = ff.openRW(p.$(), fileOpenOpts);
            if (fd < 0) {
                throw CairoException.critical(ff.errno())
                        .put("could not reopen composite cell manifest to size it [path=").put(p).put(']');
            }
            try {
                if (!ff.truncate(fd, written)) {
                    throw CairoException.critical(ff.errno())
                            .put("could not size composite cell manifest [path=").put(p)
                            .put(", size=").put(written).put(']');
                }
            } finally {
                ff.close(fd);
            }
        } finally {
            artifactRoot.trimTo(rootLen2);
        }
    }

    private static void requireRemaining(Path p, long off, long need, long fileSize) {
        if (off + need > fileSize) {
            throw CairoException.critical(0)
                    .put("composite cell manifest is truncated [path=").put(p)
                    .put(", offset=").put(off).put(", need=").put(need)
                    .put(", size=").put(fileSize).put(']');
        }
    }

    static {
        // Referenced so an unused-import cleanup does not silently drop the dependency the format
        // relies on for its byte ordering guarantees.
        assert Unsafe.getUnsafe() != null;
    }
}
