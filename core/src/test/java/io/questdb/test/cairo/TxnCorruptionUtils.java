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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.junit.Assert;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cairo.TableUtils.TX_BASE_OFFSET_A_32;
import static io.questdb.cairo.TableUtils.TX_BASE_OFFSET_B_32;
import static io.questdb.cairo.TableUtils.TX_BASE_OFFSET_GEOMETRY_VERSION_STAMP_A_32;
import static io.questdb.cairo.TableUtils.TX_BASE_OFFSET_GEOMETRY_VERSION_STAMP_B_32;
import static io.questdb.cairo.TableUtils.TX_BASE_OFFSET_VERSION_64;
import static io.questdb.cairo.TableUtils.TX_OFFSET_BODY_CHECKSUM_64;
import static io.questdb.cairo.TableUtils.TX_OFFSET_BODY_CHECKSUM_STAMP_32;
import static io.questdb.cairo.TableUtils.TX_GEOMETRY_VERSION_STAMP_XOR;
import static io.questdb.cairo.TableUtils.TX_OFFSET_TXN_64;

/**
 * Test-only corruption helpers for a table's {@code _txn} file, the counterpart of
 * {@link CvCorruptionUtils}. Everything works positionally on the closed file (see {@link RawFileAccess}),
 * so the on-disk geometry is edited exactly the way a torn page write would leave it: bytes changed in
 * place, nothing truncated, no version bump.
 * <p>
 * The layout is the one documented on {@code TableUtils}: the base header holds the version word (its
 * parity selects area A or B) and each area's offset and sizes.
 */
public final class TxnCorruptionUtils {

    private TxnCorruptionUtils() {
    }

    /**
     * Makes both areas look like records written before the body checksum existed: checksum slot and stamp
     * zeroed, every other byte untouched.
     * <p>
     * Zeroing only the checksum would leave the stamp naming the record, which reads as a checksum that has
     * gone missing: a torn write, not a legacy record. {@link #tearChecksumSlots} is the helper for that case.
     */
    public static void makeAreasLookLegacy(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            long offsetA = RawFileAccess.peekInt(ff, txnPath, TX_BASE_OFFSET_A_32);
            long offsetB = RawFileAccess.peekInt(ff, txnPath, TX_BASE_OFFSET_B_32);
            RawFileAccess.pokeLong(ff, txnPath, offsetA + TX_OFFSET_BODY_CHECKSUM_64, 0L);
            RawFileAccess.pokeLong(ff, txnPath, offsetB + TX_OFFSET_BODY_CHECKSUM_64, 0L);
            RawFileAccess.pokeInt(ff, txnPath, offsetA + TX_OFFSET_BODY_CHECKSUM_STAMP_32, 0);
            RawFileAccess.pokeInt(ff, txnPath, offsetB + TX_OFFSET_BODY_CHECKSUM_STAMP_32, 0);
        }
    }

    /**
     * Evicts every pooled reader/writer for {@code tableName} and then opens a genuinely fresh
     * {@code TableReader}, so the next load reads the bytes now on disk. Without this a pooled
     * {@code TableReader} short-circuits on an unchanged version word -- and corruption injected here never
     * bumps the version -- so the assertion would pass vacuously against cached state.
     */
    public static void forceReload(CairoEngine engine, String tableName) {
        TableToken token = engine.verifyTableName(tableName);
        engine.releaseInactive();
        try (TableReader reader = engine.getReader(token)) {
            // Opening is the point: it drives TxReader.unsafeLoadAll() against the on-disk bytes.
            Assert.assertNotNull(reader);
        }
    }

    /**
     * The txn stored in the version-selected (live) area.
     */
    public static long readLiveAreaTxn(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            return RawFileAccess.peekLong(ff, txnPath, liveAreaOffset(ff, txnPath) + TX_OFFSET_TXN_64);
        }
    }

    /**
     * The checksum stamp of the version-selected (live) area.
     */
    public static int readLiveAreaChecksumStamp(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            return RawFileAccess.peekInt(ff, txnPath, liveAreaOffset(ff, txnPath) + TX_OFFSET_BODY_CHECKSUM_STAMP_32);
        }
    }

    /**
     * The geometry-version stamp in the base-header slot of the version-selected (live) area.
     */
    public static int readLiveAreaGeometryStamp(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            long version = RawFileAccess.peekLong(ff, txnPath, TX_BASE_OFFSET_VERSION_64);
            return RawFileAccess.peekInt(
                    ff,
                    txnPath,
                    (version & 1L) == 0L ? TX_BASE_OFFSET_GEOMETRY_VERSION_STAMP_A_32 : TX_BASE_OFFSET_GEOMETRY_VERSION_STAMP_B_32
            );
        }
    }

    /**
     * The txn stored in the inactive (previous-commit) area, the one {@code unsafeLoadAll} falls back to.
     */
    public static long readOtherAreaTxn(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            long version = RawFileAccess.peekLong(ff, txnPath, TX_BASE_OFFSET_VERSION_64);
            long offset = RawFileAccess.peekInt(ff, txnPath, (version & 1L) == 0L ? TX_BASE_OFFSET_B_32 : TX_BASE_OFFSET_A_32);
            return RawFileAccess.peekLong(ff, txnPath, offset + TX_OFFSET_TXN_64);
        }
    }

    /**
     * The body-checksum slot of area A. Paired with {@link #writeBodyChecksumSlots} to reproduce a commit
     * made by a QuestDB that predates the body checksum.
     */
    public static long readBodyChecksumSlotA(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            long offsetA = RawFileAccess.peekInt(ff, txnPath, TX_BASE_OFFSET_A_32);
            return RawFileAccess.peekLong(ff, txnPath, offsetA + TX_OFFSET_BODY_CHECKSUM_64);
        }
    }

    /**
     * The body-checksum slot of area B. See {@link #readBodyChecksumSlotA}.
     */
    public static long readBodyChecksumSlotB(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            long offsetB = RawFileAccess.peekInt(ff, txnPath, TX_BASE_OFFSET_B_32);
            return RawFileAccess.peekLong(ff, txnPath, offsetB + TX_OFFSET_BODY_CHECKSUM_64);
        }
    }

    /**
     * The checksum stamp of area A -- the txn the checksum beside it was computed for.
     */
    public static int readChecksumStampA(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            long offsetA = RawFileAccess.peekInt(ff, txnPath, TX_BASE_OFFSET_A_32);
            return RawFileAccess.peekInt(ff, txnPath, offsetA + TX_OFFSET_BODY_CHECKSUM_STAMP_32);
        }
    }

    /**
     * The checksum stamp of area B. See {@link #readChecksumStampA}.
     */
    public static int readChecksumStampB(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            long offsetB = RawFileAccess.peekInt(ff, txnPath, TX_BASE_OFFSET_B_32);
            return RawFileAccess.peekInt(ff, txnPath, offsetB + TX_OFFSET_BODY_CHECKSUM_STAMP_32);
        }
    }

    /**
     * Restores both areas' checksum slots and stamps to values captured before a commit, which reproduces a
     * commit made by a QuestDB that predates the body checksum. That binary's commit path is this one minus the
     * {@code storeBodyChecksum} call: it writes the body, the symbol counts and the A/B offset and size ints,
     * then fences and bumps the version word, and never touches {@code [116,128)}. Both the checksum and the
     * stamp keep what they held before.
     * <p>
     * The stamp is restored along with the checksum: a fresh stamp beside a stale checksum would claim that the
     * old checksum belongs to the new record, which no binary produces.
     */
    public static void writeBodyChecksumSlots(
            CairoEngine engine,
            String tableName,
            long slotA,
            long slotB,
            int stampA,
            int stampB
    ) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            long offsetA = RawFileAccess.peekInt(ff, txnPath, TX_BASE_OFFSET_A_32);
            long offsetB = RawFileAccess.peekInt(ff, txnPath, TX_BASE_OFFSET_B_32);
            RawFileAccess.pokeLong(ff, txnPath, offsetA + TX_OFFSET_BODY_CHECKSUM_64, slotA);
            RawFileAccess.pokeLong(ff, txnPath, offsetB + TX_OFFSET_BODY_CHECKSUM_64, slotB);
            RawFileAccess.pokeInt(ff, txnPath, offsetA + TX_OFFSET_BODY_CHECKSUM_STAMP_32, stampA);
            RawFileAccess.pokeInt(ff, txnPath, offsetB + TX_OFFSET_BODY_CHECKSUM_STAMP_32, stampB);
        }
    }

    /**
     * Zeroes the body-checksum slot of both A and B areas, leaving their stamps and every other byte untouched,
     * as a partial page write does: the stamp still names the record, so the reader knows that a checksum
     * existed for it and is gone.
     */
    public static void tearChecksumSlots(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            long offsetA = RawFileAccess.peekInt(ff, txnPath, TX_BASE_OFFSET_A_32);
            long offsetB = RawFileAccess.peekInt(ff, txnPath, TX_BASE_OFFSET_B_32);
            RawFileAccess.pokeLong(ff, txnPath, offsetA + TX_OFFSET_BODY_CHECKSUM_64, 0L);
            RawFileAccess.pokeLong(ff, txnPath, offsetB + TX_OFFSET_BODY_CHECKSUM_64, 0L);
        }
    }

    /**
     * Tears the body-checksum slot of the version-selected (live) area only, leaving its stamp and the
     * inactive area intact so {@code unsafeLoadAll} can still fall back.
     */
    public static void tearLiveAreaChecksumSlot(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            RawFileAccess.pokeLong(ff, txnPath, liveAreaOffset(ff, txnPath) + TX_OFFSET_BODY_CHECKSUM_64, 0L);
        }
    }

    /**
     * Makes the live area's base-header geometry stamp name the txn two commits back, the stale stamp the fast
     * commit path used to leave behind, with the area's body and checksum stamp intact.
     */
    public static void tearLiveAreaGeometryStamp(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            long version = RawFileAccess.peekLong(ff, txnPath, TX_BASE_OFFSET_VERSION_64);
            long liveTxn = RawFileAccess.peekLong(ff, txnPath, liveAreaOffset(ff, txnPath) + TX_OFFSET_TXN_64);
            RawFileAccess.pokeInt(
                    ff,
                    txnPath,
                    (version & 1L) == 0L ? TX_BASE_OFFSET_GEOMETRY_VERSION_STAMP_A_32 : TX_BASE_OFFSET_GEOMETRY_VERSION_STAMP_B_32,
                    (int) (liveTxn - 2) ^ TX_GEOMETRY_VERSION_STAMP_XOR
            );
        }
    }

    private static long liveAreaOffset(FilesFacade ff, LPSZ txnPath) {
        long version = RawFileAccess.peekLong(ff, txnPath, TX_BASE_OFFSET_VERSION_64);
        boolean isA = (version & 1L) == 0L;
        return RawFileAccess.peekInt(ff, txnPath, isA ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32);
    }

    private static LPSZ txnPath(CairoEngine engine, Path path, String tableName) {
        final CairoConfiguration configuration = engine.getConfiguration();
        final TableToken token = engine.verifyTableName(tableName);
        return path.of(configuration.getDbRoot()).concat(token).concat(TXN_FILE_NAME).$();
    }
}
