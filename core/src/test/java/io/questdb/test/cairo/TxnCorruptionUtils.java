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
import static io.questdb.cairo.TableUtils.TX_BASE_OFFSET_CAPABILITY_MAGIC_64;
import static io.questdb.cairo.TableUtils.TX_BASE_OFFSET_CAPABILITY_WATERMARK_64;
import static io.questdb.cairo.TableUtils.TX_BASE_OFFSET_VERSION_64;
import static io.questdb.cairo.TableUtils.TX_OFFSET_BODY_CHECKSUM_64;
import static io.questdb.cairo.TableUtils.TX_OFFSET_TXN_64;

/**
 * Test-only corruption helpers for a table's {@code _txn} file, the counterpart of
 * {@link CvCorruptionUtils}. Everything works positionally on the closed file (see {@link RawFileAccess}),
 * so the on-disk geometry is edited exactly the way a torn page write would leave it: bytes changed in
 * place, nothing truncated, no version bump.
 * <p>
 * The layout is the one documented on {@code TableUtils}: the base header holds the version word (its
 * parity selects area A or B), each area's offset, and -- from the capability change -- the capability
 * magic and watermark.
 */
public final class TxnCorruptionUtils {

    private TxnCorruptionUtils() {
    }

    /**
     * Erases the capability marker, so the file looks as though it was written before the body checksum
     * existed at all. The false-positive control: such a file must still load with its checksum slots zero.
     */
    public static void clearCapabilityMarker(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            RawFileAccess.pokeLong(ff, txnPath, TX_BASE_OFFSET_CAPABILITY_MAGIC_64, 0L);
            RawFileAccess.pokeLong(ff, txnPath, TX_BASE_OFFSET_CAPABILITY_WATERMARK_64, 0L);
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

    public static long readCapabilityMagic(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            return RawFileAccess.peekLong(ff, txnPath(engine, path, tableName), TX_BASE_OFFSET_CAPABILITY_MAGIC_64);
        }
    }

    public static long readCapabilityWatermark(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            return RawFileAccess.peekLong(ff, txnPath(engine, path, tableName), TX_BASE_OFFSET_CAPABILITY_WATERMARK_64);
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
     * Zeroes the body-checksum slot of BOTH A and B areas, leaving every other byte -- including the
     * capability marker and the version word -- untouched. That is precisely the state a partial page write
     * leaves behind, and precisely the state the pre-capability reader mistook for "legacy record, skip the
     * check".
     */
    public static void zeroBodyChecksumSlots(CairoEngine engine, String tableName) {
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
     * Zeroes the body-checksum slot of the version-selected (live) area only, leaving the inactive area
     * intact so {@code unsafeLoadAll} can still fall back to it.
     */
    public static void zeroLiveAreaBodyChecksumSlot(CairoEngine engine, String tableName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            LPSZ txnPath = txnPath(engine, path, tableName);
            RawFileAccess.pokeLong(ff, txnPath, liveAreaOffset(ff, txnPath) + TX_OFFSET_BODY_CHECKSUM_64, 0L);
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
