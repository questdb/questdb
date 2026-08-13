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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.TableUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;

/**
 * Per-record CRCs for the V1 sequencer transaction log, kept in a sidecar file beside {@code _txnlog}.
 * <p>
 * V2 stores its CRC in a reserved trailing slot inside each record. V1 has no such slot --
 * {@code RECORD_SIZE} is exactly the fields it writes -- so adding one in place would grow the record
 * and stop older binaries reading the file. A sidecar keeps {@code _txnlog}'s bytes untouched, which is
 * the same trick the WAL-e checksum sidecar already uses for {@code _event} (see
 * {@code WalUtils}: "Additive checksum sidecar. _event and _event.i retain their legacy byte layout so
 * an older reader...").
 * <p>
 * On-disk layout:
 * <pre>
 * [0,8)    MAGIC            " TLCHKSM" (little-endian)
 * [8,12)   file version     int, {@link #FILE_VERSION}
 * [12,16)  entry size       int, {@link #ENTRY_SIZE}
 * [16,24)  firstCoveredTxn  long -- the capability watermark
 * [24,..)  body             one 8-byte CRC per txn, indexed (txn - firstCoveredTxn)
 * </pre>
 * <p>
 * The header is <b>write-once</b> and the body is <b>append-only</b>: nothing is ever mutated in place,
 * so there is no A/B generation here and a torn tail cannot invalidate the prefix. That is the whole
 * reason this file needs no double-buffering while the per-partition data sidecar does.
 * <p>
 * {@code firstCoveredTxn} is the capability watermark. Records below it were written before this file
 * existed and carry no CRC, so a zero there means "legacy, read unverified". At or above it a CRC was
 * guaranteed written, so a zero means the record is absent or torn. Reopening an existing sidecar keeps
 * the recorded watermark and ignores the caller's -- the original value is what classifies every record
 * already covered, and lowering it would retroactively claim coverage the file never had.
 */
public class TxnLogCrcSidecar implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(TxnLogCrcSidecar.class);
    /** Body starts here; the header is fixed-size. */
    public static final int BODY_OFFSET = 24;
    public static final int ENTRY_SIZE = Long.BYTES;
    public static final int FILE_VERSION = 1;
    /** Spells " TLCHKSM" on disk (LE), matching the shape of CV_CHECKSUM_MAGIC / TX_CHECKSUM_CAPABILITY_MAGIC. */
    public static final long MAGIC = 0x4D534B48434C5420L;
    private static final int OFFSET_ENTRY_SIZE = 12;
    private static final int OFFSET_FILE_VERSION = 8;
    private static final int OFFSET_FIRST_COVERED_TXN = 16;
    private long firstCoveredTxn = -1;
    private MemoryCMARW mem;

    /**
     * Opens (creating if absent) the sidecar at {@code path}. When the file is new, {@code watermark} is
     * stamped as its {@code firstCoveredTxn}; when it already exists, the recorded watermark wins.
     */
    public void of(FilesFacade ff, Path path, long watermark) {
        of(ff, path, watermark, false);
    }

    /**
     * Opens the sidecar for a BRAND NEW txnlog lineage, rewriting the header unconditionally.
     * <p>
     * {@code create()} lays down a fresh {@code _txnlog} starting at txn 1. If a stale {@code _txnlog.c}
     * survived a previous lineage -- WAL to non-WAL and back, where the WAL-persistence removal only
     * logs when rmdir fails -- adopting its watermark W would leave records 1..W-1 legacy and then, at
     * txn W, match the NEW records against the OLD lineage's CRCs. That is a permanent "torn" verdict on
     * a healthy table, so the header must be reset rather than adopted.
     */
    public void ofNewLineage(FilesFacade ff, Path path, long watermark) {
        of(ff, path, watermark, true);
    }

    private void of(FilesFacade ff, Path path, long watermark, boolean newLineage) {
        close();
        mem = Vm.getCMARWInstance();
        // smallFile, not of(..., -1, ...): it sizes the mapping from ff.length(name), which is what
        // creates the file cleanly when it does not exist yet.
        mem.smallFile(ff, path.$(), MemoryTag.MMAP_TX_LOG);
        final long size = mem.size();
        if (!newLineage && size >= BODY_OFFSET && mem.getLong(0) == MAGIC) {
            // Existing sidecar: adopt its header. Never rewrite it -- see the class javadoc.
            final int fileVersion = mem.getInt(OFFSET_FILE_VERSION);
            final int entrySize = mem.getInt(OFFSET_ENTRY_SIZE);
            if (fileVersion != FILE_VERSION || entrySize != ENTRY_SIZE) {
                // Disable coverage rather than refuse to open the table. This file carries no
                // durability claim, so an unreadable one must cost DETECTION, never ingestion --
                // throwing here would make an unrecognised sidecar block the sequencer entirely.
                LOG.error().$("unsupported txnlog CRC sidecar, checksums disabled [path=").$(path)
                        .$(", version=").$(fileVersion)
                        .$(", entrySize=").$(entrySize)
                        .I$();
                close();
                return;
            }
            firstCoveredTxn = mem.getLong(OFFSET_FIRST_COVERED_TXN);
        } else {
            firstCoveredTxn = watermark;
            mem.jumpTo(0);
            mem.putLong(MAGIC);
            mem.putInt(FILE_VERSION);
            mem.putInt(ENTRY_SIZE);
            mem.putLong(watermark);
        }
    }

    /**
     * Records the CRC of one txnlog record. {@code txn} is 1-based, matching the sequencer.
     */
    public void append(long txn, long recordBaseAddr, long recordSize) {
        // Not open => no coverage. Deliberately silent rather than fatal: this sidecar carries no
        // durability claim and is re-derivable, so a path that never opened it must lose DETECTION,
        // never ingestion. The watermark then keeps those records classified as legacy, so the
        // failure yields absent coverage rather than wrong coverage.
        if (mem == null || txn < firstCoveredTxn) {
            return;
        }
        final long offset = crcOffset(txn);
        mem.jumpTo(offset);
        // calculateCvAreaChecksum never returns 0, so a zero slot is unambiguously "nothing written
        // here" rather than "hashed to zero".
        mem.putLong(TableUtils.calculateCvAreaChecksum(recordBaseAddr, recordSize));
    }

    @Override
    public void close() {
        mem = Misc.free(mem);
        firstCoveredTxn = -1;
    }

    /** The capability watermark: the first txn this file guarantees a CRC for. */
    public long firstCoveredTxn() {
        return firstCoveredTxn;
    }

    /**
     * The stored CRC for {@code txn}, or 0 when this file does not cover it -- either because the txn is
     * below the watermark, or because the slot was never written back to the device.
     */
    public long readCrc(long txn) {
        if (mem == null || txn < firstCoveredTxn) {
            return 0;
        }
        final long offset = crcOffset(txn);
        if (offset + ENTRY_SIZE > mem.size()) {
            return 0;
        }
        return mem.getLong(offset);
    }

    /** Device flush, for the deferred/batched path. No-op when the sidecar is not open. */
    public void fdatasync() {
        if (mem != null) {
            mem.getFilesFacade().fdatasync(mem.getFd());
        }
    }

    /** Makes everything written so far durable. Callers order this against the txnlog header. */
    public void sync(boolean async) {
        if (mem != null) {
            mem.sync(async);
        }
    }

    private long crcOffset(long txn) {
        return BODY_OFFSET + (txn - firstCoveredTxn) * ENTRY_SIZE;
    }
}
