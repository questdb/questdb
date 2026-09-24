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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cairo.vm.api.MemoryW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public class ColumnVersionReader implements Closeable, Mutable {
    // Test-observable counter: incremented every time readSafe() detects a stable-version body-checksum
    // mismatch on the version-selected area and therefore attempts the A/B fallback to the other area. On a
    // healthy table this MUST stay 0 - the whole _cv area is commit-immutable (fully rewritten per commit,
    // never mutated in place under a stable version), so a lock-free reader that re-checks the version can
    // never observe a covered-byte change without a version change. Not used by production logic.
    static volatile long bodyChecksumFallbackCount = 0;
    public static final int BLOCK_SIZE = 4;
    public static final int BLOCK_SIZE_BYTES = BLOCK_SIZE * Long.BYTES;
    public static final int BLOCK_SIZE_MSB = Numbers.msb(BLOCK_SIZE);
    // PARTITION_TIMESTAMP_OFFSET = 0;
    public static final int COLUMN_INDEX_OFFSET = 1;
    public static final int COLUMN_NAME_TXN_OFFSET = 2;
    public static final int COLUMN_TOP_OFFSET = 3;
    public static final long COL_TOP_DEFAULT_PARTITION = Long.MIN_VALUE;
    public static final int OFFSET_VERSION_64 = 0;
    public static final int OFFSET_OFFSET_A_64 = OFFSET_VERSION_64 + 8;
    public static final int OFFSET_SIZE_A_64 = OFFSET_OFFSET_A_64 + 8;
    public static final int OFFSET_OFFSET_B_64 = OFFSET_SIZE_A_64 + 8;
    public static final int OFFSET_SIZE_B_64 = OFFSET_OFFSET_B_64 + 8;
    public static final int HEADER_SIZE = OFFSET_SIZE_B_64 + 8;
    public static final long SYMBOL_TABLE_VERSION_PARTITION = COL_TOP_DEFAULT_PARTITION + 1;
    static final int TIMESTAMP_ADDED_PARTITION_OFFSET = COLUMN_TOP_OFFSET;
    private final static Log LOG = LogFactory.getLog(ColumnVersionReader.class);
    protected final LongList cachedColumnVersionList = new LongList();
    private MemoryCMR mem;
    private boolean ownMem;
    // The file version whose live area was torn on the last consistent read, i.e. the version this reader
    // fell back FROM; -1 after a clean read. Lets a caller that needs exactly that version name the corruption
    // (see readSafe(MillisecondClock, long, long)) and keeps the fallback ERROR to one line per torn version
    // instead of one per retry.
    private long tornLiveVersion = -1;
    private long version;

    @Override
    public void clear() {
        if (ownMem) {
            mem.close();
        }
        cachedColumnVersionList.clear();
        tornLiveVersion = -1;
        version = -1;
    }

    @Override
    public void close() {
        clear();
    }

    public void dumpTo(MemoryW mem) {
        mem.putLong(OFFSET_VERSION_64, version);
        boolean areaA = (version & 1L) == 0L;
        final long offset = HEADER_SIZE;
        mem.putLong(areaA ? OFFSET_OFFSET_A_64 : OFFSET_OFFSET_B_64, offset);
        final long size = (long) (cachedColumnVersionList.size() / BLOCK_SIZE) * BLOCK_SIZE_BYTES;
        mem.putLong(areaA ? OFFSET_SIZE_A_64 : OFFSET_SIZE_B_64, size);

        int i = 0;
        long p = offset;
        long lim = offset + size;

        while (p < lim) {
            mem.putLong(p, cachedColumnVersionList.getQuick(i));
            mem.putLong(p + COLUMN_INDEX_OFFSET * Long.BYTES, cachedColumnVersionList.getQuick(i + COLUMN_INDEX_OFFSET));
            mem.putLong(p + COLUMN_NAME_TXN_OFFSET * Long.BYTES, cachedColumnVersionList.getQuick(i + COLUMN_NAME_TXN_OFFSET));
            mem.putLong(p + COLUMN_TOP_OFFSET * Long.BYTES, cachedColumnVersionList.getQuick(i + COLUMN_TOP_OFFSET));
            i += BLOCK_SIZE;
            p += BLOCK_SIZE_BYTES;
        }

        // Checkpoint copies use the same version-stamped 16-byte trailer as regular commits.
        long areaAddr = mem.appendAddressFor(offset, size + TableUtils.CV_CHECKSUM_TRAILER_SIZE);
        mem.putLong(offset + size, TableUtils.CV_CHECKSUM_MAGIC ^ version);
        mem.putLong(offset + size + Long.BYTES, TableUtils.calculateCvAreaChecksum(areaAddr, size));
    }

    public LongList getCachedColumnVersionList() {
        return cachedColumnVersionList;
    }

    public long getColumnNameTxn(long partitionTimestamp, int columnIndex) {
        int versionRecordIndex = getRecordIndex(partitionTimestamp, columnIndex);
        return versionRecordIndex > -1 ? cachedColumnVersionList.getQuick(versionRecordIndex + COLUMN_NAME_TXN_OFFSET) : getDefaultColumnNameTxn(columnIndex);
    }

    public long getColumnNameTxnByIndex(int versionRecordIndex) {
        return versionRecordIndex > -1 ? cachedColumnVersionList.getQuick(versionRecordIndex + COLUMN_NAME_TXN_OFFSET) : -1L;
    }

    /**
     * Checks that column exists in the partition and returns the column top
     *
     * @param partitionTimestamp timestamp of the partition
     * @param columnIndex        column index
     * @return column top in the partition or -1 if column does not exist in the partition
     */
    public long getColumnTop(long partitionTimestamp, int columnIndex) {
        // Check if there is explicit record for this partitionTimestamp / columnIndex combination
        int recordIndex = getRecordIndex(partitionTimestamp, columnIndex);
        return getColumnTopByIndexOrDefault(recordIndex, partitionTimestamp, columnIndex, -1L);
    }

    public long getColumnTopByIndex(int versionRecordIndex) {
        return versionRecordIndex > -1 ? cachedColumnVersionList.getQuick(versionRecordIndex + COLUMN_TOP_OFFSET) : 0L;
    }

    public long getColumnTopByIndexOrDefault(int recordIndex, long partitionTimestamp, int columnIndex, long defaultValue) {
        if (recordIndex > -1L) {
            return cachedColumnVersionList.getQuick(recordIndex + COLUMN_TOP_OFFSET);
        }

        // Check if column has been already added before this partition
        long columnTopDefaultPartition = getColumnTopPartitionTimestamp(columnIndex);
        if (columnTopDefaultPartition <= partitionTimestamp) {
            return 0;
        }

        // This column does not exist in the partition
        return defaultValue;
    }

    /**
     * Get partition when the column was added first into the table.
     * All partitions before that one should not have any data in the column
     * All partitions after that will have 0 column top (column fully exists)
     * Exception is when O3 commit can overwrite column top for any partition where the column did not exist
     * with concrete column top value
     *
     * @param columnIndex column index
     * @return the partition timestamp where column added or Long.MIN_VALUE if column was present from table creation
     */
    public long getColumnTopPartitionTimestamp(int columnIndex) {
        int index = getRecordIndex(COL_TOP_DEFAULT_PARTITION, columnIndex);
        return index > -1 ? getColumnTopByIndex(index) : Long.MIN_VALUE;
    }

    /**
     * Returns the column top without checking that column exists in the partition
     *
     * @param partitionTimestamp timestamp of the partition
     * @param columnIndex        column index
     * @return column top in the partition or 0 if column does not exist in the partition or column exists with no column top
     */
    public long getColumnTopQuick(long partitionTimestamp, int columnIndex) {
        int index = getRecordIndex(partitionTimestamp, columnIndex);
        return getColumnTopByIndex(index);
    }

    public long getDefaultColumnNameTxn(int columnIndex) {
        int index = getRecordIndex(COL_TOP_DEFAULT_PARTITION, columnIndex);
        return index > -1 ? getColumnNameTxnByIndex(index) : -1L;
    }

    public long getMaxPartitionVersion(long partitionTimestamp) {
        long maxVersion = -1;
        int index = cachedColumnVersionList.binarySearchBlock(BLOCK_SIZE_MSB, partitionTimestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (index > -1) {
            final int sz = cachedColumnVersionList.size();
            for (; index < sz && cachedColumnVersionList.getQuick(index) == partitionTimestamp; index += BLOCK_SIZE) {
                final long thisTimestamp = cachedColumnVersionList.getQuick(index);
                if (thisTimestamp != partitionTimestamp) {
                    break;
                }
                final long columnVersion = cachedColumnVersionList.getQuick(index + COLUMN_NAME_TXN_OFFSET);
                maxVersion = Math.max(maxVersion, columnVersion);
            }
        }
        return maxVersion;
    }

    public int getRecordIndex(long partitionTimestamp, int columnIndex) {
        int index = cachedColumnVersionList.binarySearchBlock(BLOCK_SIZE_MSB, partitionTimestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (index > -1) {
            final int sz = cachedColumnVersionList.size();
            for (; index < sz && cachedColumnVersionList.getQuick(index) == partitionTimestamp; index += BLOCK_SIZE) {
                final long thisIndex = cachedColumnVersionList.getQuick(index + COLUMN_INDEX_OFFSET);
                final long thisTimestamp = cachedColumnVersionList.getQuick(index);
                if (thisTimestamp != partitionTimestamp) {
                    break;
                }

                if (thisIndex == columnIndex) {
                    return index;
                }

                if (thisIndex > columnIndex) {
                    break;
                }
            }
        }
        return -1;
    }

    /**
     * Symbol table files name txn - this is name suffix used to version the file group.
     * Whenever symbol table capacity changes, its version is increased. Separate column version
     * entry is used to store this version. Thus, decoupled from version of the columns.
     * <p>
     * Separate column version is optional however, when it is not present, this method will fall back to
     * {@link #getDefaultColumnNameTxn}.
     *
     * @param columnIndex symbol column index
     * @return version suffix
     */
    public long getSymbolTableNameTxn(int columnIndex) {
        int index = getRecordIndex(SYMBOL_TABLE_VERSION_PARTITION, columnIndex);
        return index > -1 ? getColumnNameTxnByIndex(index) : getDefaultColumnNameTxn(columnIndex);
    }

    @TestOnly
    public static long getBodyChecksumFallbackCount() {
        return bodyChecksumFallbackCount;
    }

    @TestOnly
    public static void resetBodyChecksumFallbackCount() {
        bodyChecksumFallbackCount = 0;
    }

    public long getVersion() {
        return version;
    }

    public ColumnVersionReader ofRO(FilesFacade ff, LPSZ fileName) {
        version = -1;
        tornLiveVersion = -1;
        if (this.mem == null || !ownMem) {
            this.mem = Vm.getCMRInstance();
        }
        this.mem.of(ff, fileName, 0, HEADER_SIZE, MemoryTag.MMAP_TABLE_READER);
        ownMem = true;
        return this;
    }

    public void ofRO(MemoryCMR mem) {
        if (this.mem != null && ownMem) {
            this.mem.close();
        }
        this.mem = mem;
        ownMem = false;
        version = -1;
        tornLiveVersion = -1;
    }

    /**
     * Copies column versions from the given reader.
     */
    public void readFrom(ColumnVersionReader columnVersionReader) {
        this.version = columnVersionReader.version;
        tornLiveVersion = -1;
        cachedColumnVersionList.clear();
        cachedColumnVersionList.addAll(columnVersionReader.cachedColumnVersionList);
    }

    /**
     * Spins until the file reads consistently, or until {@code spinLockTimeout} milliseconds
     * have passed since the call. The timeout is a duration measured from here - not an
     * absolute deadline; a caller that hands over {@code clock.getTicks() + timeout} instead
     * buys a budget of roughly the current epoch, which no spin can ever exhaust.
     */
    public void readSafe(MillisecondClock microsecondClock, long spinLockTimeout) {
        final long tick = microsecondClock.getTicks();
        while (true) {
            if (readSafe()) {
                return;
            }

            if (microsecondClock.getTicks() - tick > spinLockTimeout) {
                LOG.error().$("Column Version read timeout [timeout=").$(spinLockTimeout).$("ms]").$();
                throw CairoException.critical(0).put("Column Version read timeout");
            }
            Os.pause();
            LOG.debug().$("read dirty version ").$(version).$(", retrying").$();
        }
    }

    /**
     * {@link #readSafe(MillisecondClock, long)}, then fails fast if the consistent read landed BELOW
     * {@code expectedVersion} - the column version a stable {@code _txn} record names.
     * <p>
     * The writer publishes {@code _cv} before the {@code _txn} record that references it, so a reader that
     * loaded {@code _txn} first can never legitimately find {@code _cv} older. Either the live area is torn
     * and {@link #readSafe()} fell back to the previous commit, or the file itself lags the transaction.
     * Waiting repairs neither, and a caller that loops "until the versions agree" spins for good, at full
     * CPU, re-verifying both areas and logging the fallback on every pass. The diagnosis needs no clock:
     * it is the ordering invariant, not a deadline, that says there is no writer to wait for.
     * <p>
     * A NEWER version is the ordinary race with a commit in flight - published in {@code _cv}, not yet in
     * {@code _txn} - and is returned as-is for the caller to ride out by re-reading {@code _txn}.
     */
    public void readSafe(MillisecondClock microsecondClock, long spinLockTimeout, long expectedVersion) {
        readSafe(microsecondClock, spinLockTimeout);
        if (version < expectedVersion) {
            throw columnVersionBehindException(expectedVersion);
        }
    }

    public boolean readSafe() {
        long version = unsafeGetVersion();
        if (version == this.version) {
            return true;
        }
        Unsafe.loadFence();

        final long offset;
        final long size;

        final boolean areaA = (version & 1L) == 0;
        if (areaA) {
            offset = mem.getLong(OFFSET_OFFSET_A_64);
            size = mem.getLong(OFFSET_SIZE_A_64);
        } else {
            offset = mem.getLong(OFFSET_OFFSET_B_64);
            size = mem.getLong(OFFSET_SIZE_B_64);
        }

        Unsafe.loadFence();
        if (version == unsafeGetVersion()) {
            mem.resize(offset + size);
            readUnsafe(offset, size, cachedColumnVersionList, mem);

            if (unsafeVerifyAreaChecksum(offset, size, version)) {
                Unsafe.loadFence();
                if (version == unsafeGetVersion()) {
                    this.version = version;
                    tornLiveVersion = -1;
                    LOG.debug().$("read clean version ").$(version).$(", offset ").$(offset).$(", size ").$(size).$();
                    return true;
                }
                // Version moved under us: concurrent commit. Retry (return false below).
            } else {
                // The version-selected area's body checksum did not match. Re-read the version: if it is
                // STILL the one we selected, the area is genuinely torn (a partial / reordered msync left a
                // bumped version word over an incomplete area). Only then do we fall back to the other A/B
                // area; otherwise it was a concurrent write and we simply retry.
                Unsafe.loadFence();
                if (version == unsafeGetVersion()) {
                    //noinspection NonAtomicOperationOnVolatileField
                    bodyChecksumFallbackCount++;
                    boolean otherOk = unsafeLoadAndVerifyOtherArea(version);
                    Unsafe.loadFence();
                    if (version == unsafeGetVersion()) {
                        // The whole header + both areas were stable across the attempt.
                        if (otherOk) {
                            // Adopt the prior committed area (published at version - 1). Mirrors the _txn
                            // fallback: the selected area is corrupt, so the previous good area is the best
                            // valid state.
                            this.version = version - 1;
                            if (tornLiveVersion != version) {
                                tornLiveVersion = version;
                                LOG.error().$("read fell back to other _cv area after checksum mismatch [version=").$(version)
                                        .$(", offset=").$(offset).$(", size=").$(size).$(']').$();
                            }
                            return true;
                        }
                        // Neither A nor B verifies. Never return a silently-wrong column-version map -
                        // surface a hard error so the caller fails the read. Reset the cached state (but do
                        // NOT call clear(): it closes mem and the ColumnVersionWriter subclass overrides it
                        // to throw - this path must remain usable from the inherited reader).
                        cachedColumnVersionList.clear();
                        this.version = -1;
                        throw CairoException.critical(0)
                                .put("_cv checksum mismatch in both A and B areas [version=").put(version)
                                .put(", offset=").put(offset)
                                .put(", size=").put(size)
                                .put(']');
                    }
                    // Version changed during the fallback: concurrent write, retry.
                }
                // Version changed: concurrent write, retry.
            }
        }
        return false;
    }

    public long readUnsafe() {
        long version = mem.getLong(OFFSET_VERSION_64);

        boolean areaA = (version & 1L) == 0L;
        long offset = areaA ? mem.getLong(OFFSET_OFFSET_A_64) : mem.getLong(OFFSET_OFFSET_B_64);
        long size = areaA ? mem.getLong(OFFSET_SIZE_A_64) : mem.getLong(OFFSET_SIZE_B_64);
        mem.resize(offset + size);
        readUnsafe(offset, size, cachedColumnVersionList, mem);
        // Verify-or-skip: this is the writer's own single-threaded self-read (e.g. the open-time load and
        // rollback()'s readback), not the lock-free concurrent-reader path. Do NOT throw or fall back here -
        // that would break the writer. A mismatch (or absent/old-format trailing long) is only logged; the
        // critical concurrent path is readSafe(), which performs the A/B fallback.
        if (!unsafeVerifyAreaChecksum(offset, size, version)) {
            LOG.error().$("_cv body checksum mismatch on writer self-read [version=").$(version)
                    .$(", offset=").$(offset).$(", size=").$(size).$(']').$();
        }
        return version;
    }

    @Override
    public String toString() {
        // Used for debugging, don't use Misc.getThreadLocalSink() to not mess with other debugging values
        StringSink sink = new StringSink();
        sink.put("{[");
        for (int i = 0; i < cachedColumnVersionList.size(); i += BLOCK_SIZE) {
            long timestamp = cachedColumnVersionList.getQuick(i);
            int columnIndex = (int) cachedColumnVersionList.getQuick(i + COLUMN_INDEX_OFFSET);
            long columnNameTxn = cachedColumnVersionList.getQuick(i + COLUMN_NAME_TXN_OFFSET);
            long columnTop = cachedColumnVersionList.getQuick(i + COLUMN_TOP_OFFSET);

            if (i > 0) {
                sink.put(",");
            }
            sink.put("\n{columnIndex: ").put(columnIndex).put(", ");
            if (timestamp == COL_TOP_DEFAULT_PARTITION) {
                sink.putAscii("defaultNameTxn: ").put(columnNameTxn).putAscii(", ");
                sink.putAscii("addedPartition: ");
                sink.put(columnTop);
            } else if (timestamp == SYMBOL_TABLE_VERSION_PARTITION) {
                sink.putAscii("symbolTableTxn: ").put(columnNameTxn);
            } else {
                sink.putAscii("nameTxn: ").put(columnNameTxn).putAscii(", ");
                sink.putAscii("partition: ");
                sink.put(timestamp);
                sink.putAscii(", ");
                sink.putAscii("columnTop: ").put(columnTop);
            }
            sink.putAscii('}');
        }
        sink.putAscii("\n]}");
        return sink.toString();
    }

    /**
     * The error for a consistent read that could not reach {@code expectedVersion}. Two shapes, told apart
     * by whether this reader fell back from exactly that version: a torn live area (the file names
     * {@code expectedVersion}, its area does not verify, the previous commit was adopted) or a file that is
     * simply behind the transaction that references it.
     */
    private CairoException columnVersionBehindException(long expectedVersion) {
        if (tornLiveVersion == expectedVersion) {
            return CairoException.critical(0)
                    .put("_cv live area is torn, reader cannot advance past the previous column version [version=").put(version)
                    .put(", expected=").put(expectedVersion)
                    .put(']');
        }
        return CairoException.critical(0)
                .put("_cv is behind the column version _txn references [version=").put(version)
                .put(", expected=").put(expectedVersion)
                .put(']');
    }

    /**
     * Re-points to the OTHER A/B area (the prior committed area, opposite parity, published at
     * {@code selectedVersion - 1}) using its offset/size from the header, loads it into
     * {@code cachedColumnVersionList} and verifies its body checksum. Used as the fallback when the
     * version-selected area is torn under a stable version. MUST be called only after the version has been
     * confirmed stable, so the other area is the settled prior commit (not one the writer is mid-write
     * into). Returns true only if the other area's stored checksum verifies (or is absent: file too short
     * for a trailer / no MAGIC - back-compat). Guards every read against EOF (see
     * {@link #unsafeVerifyAreaChecksum}).
     * <p>
     * Note this fallback is only ever reached when the PRIMARY (version-selected) area had a PRESENT,
     * version-stamped trailer whose checksum mismatched. Legacy areas, including those with a stale
     * trailer left by an older writer, are accepted as unverified on the primary path.
     */
    private boolean unsafeLoadAndVerifyOtherArea(long selectedVersion) {
        // The selected area used slot (selectedVersion & 1); the prior commit lives in the opposite slot.
        boolean otherIsA = (selectedVersion & 1L) != 0L;
        long otherOffset = otherIsA ? mem.getLong(OFFSET_OFFSET_A_64) : mem.getLong(OFFSET_OFFSET_B_64);
        long otherSize = otherIsA ? mem.getLong(OFFSET_SIZE_A_64) : mem.getLong(OFFSET_SIZE_B_64);

        // Geometry sanity: the other area must sit past the header, be a whole number of blocks, and its
        // data + 16-byte trailer must fit within the real file. A bad header here just means "no usable
        // fallback".
        if (otherOffset < HEADER_SIZE || otherSize < 0 || (otherSize % BLOCK_SIZE_BYTES) != 0) {
            return false;
        }
        final FilesFacade ff = mem.getFilesFacade();
        final long realLen = ff.length(mem.getFd());
        if (realLen < otherOffset + otherSize + TableUtils.CV_CHECKSUM_TRAILER_SIZE) {
            // The other area carries no 16-byte trailer (old format) OR the file is too short to even hold
            // its data: in either case we cannot positively verify it, so do not adopt it as a fallback.
            return false;
        }

        mem.resize(otherOffset + otherSize);
        readUnsafe(otherOffset, otherSize, cachedColumnVersionList, mem);
        return unsafeVerifyAreaChecksum(otherOffset, otherSize, selectedVersion - 1);
    }

    /**
     * The 16-byte trailer contains {@code [CV_CHECKSUM_MAGIC ^ areaVersion][checksum]}.
     * A missing or stale stamp is unverified: an old binary can overwrite a body without clearing
     * its former trailer. Only a matching stamp claims checksum coverage. Check the real file length
     * before mapping the trailer to avoid accessing beyond EOF, then classify a matching checksum.
     * The caller brackets this check with stable version reads under concurrent commits.
     */
    private boolean unsafeVerifyAreaChecksum(long offset, long size, long areaVersion) {
        final FilesFacade ff = mem.getFilesFacade();
        final long realLen = ff.length(mem.getFd());
        if (realLen < offset + size + TableUtils.CV_CHECKSUM_TRAILER_SIZE) {
            // Absent: the file is too short to hold a 16-byte trailer (old-format / freshly-created). Skip
            // (back-compatible). Crucially we never resize/read at offset+size here, so no mapping past EOF.
            return true;
        }
        // Safe to map the 16-byte trailer now that the real file is known to cover it.
        mem.resize(offset + size + TableUtils.CV_CHECKSUM_TRAILER_SIZE);
        // The version in the first trailer word ties the checksum to this specific commit.
        // An old writer can reuse the body without clearing a prior trailer; in that case its
        // stale stamp is not evidence about this body, so avoid even hashing it.
        long expectedStamp = TableUtils.CV_CHECKSUM_MAGIC ^ areaVersion;
        if (mem.getLong(offset + size) != expectedStamp) {
            return true;
        }
        return ChecksumTrailer.classify(
                expectedStamp,
                mem.getLong(offset + size + Long.BYTES),
                mem.addressOf(offset),
                size,
                expectedStamp
        ) != ChecksumTrailer.MISMATCH;
    }

    private static void readUnsafe(long offset, long areaSize, LongList cachedList, MemoryR mem) {
        long lim = offset + areaSize;
        mem.extend(lim);
        int i = 0;
        long p = offset;

        assert areaSize % BLOCK_SIZE_BYTES == 0;

        cachedList.setPos((int) ((areaSize / BLOCK_SIZE_BYTES) * BLOCK_SIZE));

        while (p < lim) {
            cachedList.setQuick(i, mem.getLong(p));
            cachedList.setQuick(i + COLUMN_INDEX_OFFSET, mem.getLong(p + COLUMN_INDEX_OFFSET * Long.BYTES));
            cachedList.setQuick(i + COLUMN_NAME_TXN_OFFSET, mem.getLong(p + COLUMN_NAME_TXN_OFFSET * Long.BYTES));
            cachedList.setQuick(i + COLUMN_TOP_OFFSET, mem.getLong(p + COLUMN_TOP_OFFSET * Long.BYTES));
            i += BLOCK_SIZE;
            p += BLOCK_SIZE_BYTES;
        }
    }

    private long unsafeGetVersion() {
        return mem.getLong(OFFSET_VERSION_64);
    }
}
