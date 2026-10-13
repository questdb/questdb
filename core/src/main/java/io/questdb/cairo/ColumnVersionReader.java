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
    // Counts reads that found the selected area torn under a stable version and fell back to the other A/B
    // area. A healthy table keeps it at 0: a commit rewrites the whole _cv area and never changes a published
    // one in place. Tests read it; production logic does not.
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
    private boolean isInitialChecksumVerificationRequired = true;
    private MemoryCMR mem;
    private boolean ownMem;
    // The file version whose live area was torn on the last consistent read, the version this reader fell
    // back from; -1 after a clean read. It lets a caller that needs exactly that version name the corruption
    // (see readSafe(MillisecondClock, long, long)) and limits the fallback error to one line per torn version.
    private long tornLiveVersion = -1;
    private long version;

    @Override
    public void clear() {
        if (ownMem) {
            mem.close();
        }
        cachedColumnVersionList.clear();
        isInitialChecksumVerificationRequired = true;
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

        // Checkpoint copies use the same 16-byte trailer as regular commits.
        long areaAddr = mem.appendAddressFor(offset, size + TableUtils.CV_CHECKSUM_TRAILER_SIZE);
        mem.putLong(offset + size, TableUtils.CV_CHECKSUM_MAGIC);
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

    /**
     * Whether the last {@link #readSafe()} found the live area torn and fell back to the previous one, so that
     * {@link #getVersion()} is one behind the version the file names.
     */
    public boolean hasTornLiveArea() {
        return tornLiveVersion != -1;
    }

    /**
     * Whether the area holding column version {@code areaVersion}, the live one or its predecessor in the
     * other slot, can be loaded: it lies within the file, and its trailer checksum matches when it has a
     * trailer. An area without a trailer passes unverified, as on every load path.
     * Version 0 is the empty state of a file that has never committed. Reads the file only: the loaded column
     * versions stay as they are.
     */
    public boolean isAreaIntact(long areaVersion) {
        if (areaVersion < 1) {
            return true;
        }
        final long liveVersion = unsafeGetVersion();
        if (areaVersion != liveVersion && areaVersion != liveVersion - 1) {
            return false;
        }
        final boolean areaA = (areaVersion & 1L) == 0L;
        final long offset = mem.getLong(areaA ? OFFSET_OFFSET_A_64 : OFFSET_OFFSET_B_64);
        final long size = mem.getLong(areaA ? OFFSET_SIZE_A_64 : OFFSET_SIZE_B_64);
        final long fileLength = mem.getFilesFacade().length(mem.getFd());
        return isAreaWithinFile(offset, size, fileLength) && unsafeVerifyAreaChecksum(offset, size, fileLength);
    }

    public ColumnVersionReader ofRO(FilesFacade ff, LPSZ fileName) {
        isInitialChecksumVerificationRequired = true;
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
        isInitialChecksumVerificationRequired = true;
        version = -1;
        tornLiveVersion = -1;
    }

    /**
     * Loads the area holding column version {@code areaVersion}, the live one or its predecessor in the other
     * slot, into {@code target}, which then reads as that version. This reader's column versions and the file
     * stay as they are. Call it only for an area that {@link #isAreaIntact(long)} accepts.
     */
    public void readAreaTo(long areaVersion, ColumnVersionReader target) {
        final boolean areaA = (areaVersion & 1L) == 0L;
        final long offset = mem.getLong(areaA ? OFFSET_OFFSET_A_64 : OFFSET_OFFSET_B_64);
        final long size = mem.getLong(areaA ? OFFSET_SIZE_A_64 : OFFSET_SIZE_B_64);
        mem.resize(offset + size);
        readUnsafe(offset, size, target.cachedColumnVersionList, mem);
        target.version = areaVersion;
        target.tornLiveVersion = -1;
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
     * {@link #readSafe(MillisecondClock, long)}, then fails fast if the consistent read landed below
     * {@code expectedVersion}, the column version a stable {@code _txn} record names.
     * <p>
     * The writer publishes {@code _cv} before the {@code _txn} record that references it, so a reader that
     * loaded {@code _txn} first never legitimately finds {@code _cv} older. Either the live area is torn and
     * {@link #readSafe()} fell back to the previous commit, or the file lags the transaction. Waiting repairs
     * neither, and a caller that loops until the versions agree would spin for good, so the ordering
     * invariant, not a deadline, decides.
     * <p>
     * A newer version is the ordinary race with a commit in flight (published in {@code _cv}, not yet in
     * {@code _txn}) and is returned as is, for the caller to re-read {@code _txn}.
     */
    public void readSafe(MillisecondClock microsecondClock, long spinLockTimeout, long expectedVersion) {
        readSafe(microsecondClock, spinLockTimeout);
        if (version < expectedVersion) {
            throw columnVersionBehindException(expectedVersion);
        }
    }

    public boolean readSafe() {
        long version = unsafeGetVersion();
        if (version == this.version && !isInitialChecksumVerificationRequired) {
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
            // Verify persisted bytes once per mapping. Live refreshes rely on the publication protocol;
            // copied state, failed reads and fallback must not consume the initial verification.
            final long fileLength = isInitialChecksumVerificationRequired ? mem.getFilesFacade().length(mem.getFd()) : Long.MAX_VALUE;
            final boolean isIntact;
            if (isAreaWithinFile(offset, size, fileLength)) {
                mem.resize(offset + size);
                readUnsafe(offset, size, cachedColumnVersionList, mem);
                isIntact = !isInitialChecksumVerificationRequired || unsafeVerifyAreaChecksum(offset, size, fileLength);
            } else {
                isIntact = false;
            }

            if (isIntact) {
                Unsafe.loadFence();
                if (version == unsafeGetVersion()) {
                    this.version = version;
                    isInitialChecksumVerificationRequired = false;
                    tornLiveVersion = -1;
                    LOG.debug().$("read clean version ").$(version).$(", offset ").$(offset).$(", size ").$(size).$();
                    return true;
                }
                // The version moved: a concurrent commit. Retry (return false below).
            } else {
                // The selected area's checksum did not match. If the version is unchanged, the area is torn (a
                // partial or reordered msync left a bumped version word over an incomplete area), and the read
                // falls back to the other A/B area; otherwise a concurrent write raced the read, and it retries.
                Unsafe.loadFence();
                if (version == unsafeGetVersion()) {
                    isInitialChecksumVerificationRequired = true;
                    //noinspection NonAtomicOperationOnVolatileField
                    bodyChecksumFallbackCount++;
                    boolean otherOk = unsafeLoadAndVerifyOtherArea(version);
                    Unsafe.loadFence();
                    if (version == unsafeGetVersion()) {
                        // The header and both areas stayed stable across the fallback.
                        if (otherOk) {
                            // Adopt the previous committed area (published at version - 1), as the _txn
                            // fallback does.
                            this.version = version - 1;
                            if (tornLiveVersion != version) {
                                tornLiveVersion = version;
                                LOG.error().$("read fell back to other _cv area after checksum mismatch [version=").$(version)
                                        .$(", offset=").$(offset).$(", size=").$(size).$(']').$();
                            }
                            return true;
                        }
                        // Neither area verifies: fail the read rather than return a wrong column-version map.
                        // Reset the cached state without clear(), which closes mem and which the
                        // ColumnVersionWriter subclass overrides to throw.
                        cachedColumnVersionList.clear();
                        this.version = -1;
                        throw CairoException.cvTorn()
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
        return unsafeReadLiveArea(false);
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
     * Loads the version-selected area without the version checks and the A/B fallback of {@link #readSafe()}: a
     * single-threaded self-read, such as the table writer's open-time load and {@code rollback()}'s readback. It
     * neither throws on a checksum mismatch nor falls back, and only logs one; the table writer checks the area
     * it opens with through {@link #isAreaIntact(long)}. A geometry the file does not hold is never mapped: a
     * read-only mapping faults on it, and a writer's read-write mapping would grow the file with zeros that then
     * read as the area. It fails the read, or, with {@code isAreaOutsideFileTolerated}, loads no column versions.
     */
    protected long unsafeReadLiveArea(boolean isAreaOutsideFileTolerated) {
        long version = mem.getLong(OFFSET_VERSION_64);

        boolean areaA = (version & 1L) == 0L;
        long offset = areaA ? mem.getLong(OFFSET_OFFSET_A_64) : mem.getLong(OFFSET_OFFSET_B_64);
        long size = areaA ? mem.getLong(OFFSET_SIZE_A_64) : mem.getLong(OFFSET_SIZE_B_64);
        final long fileLength = mem.getFilesFacade().length(mem.getFd());
        if (!isAreaWithinFile(offset, size, fileLength)) {
            cachedColumnVersionList.clear();
            if (!isAreaOutsideFileTolerated) {
                throw CairoException.cvTorn().put("_cv area lies outside the file [version=").put(version)
                        .put(", offset=").put(offset)
                        .put(", size=").put(size)
                        .put(", fileLength=").put(fileLength)
                        .put(']');
            }
            LOG.error().$("_cv area lies outside the file on self-read [version=").$(version)
                    .$(", offset=").$(offset).$(", size=").$(size).$(", fileLength=").$(fileLength).$(']').$();
            return version;
        }
        mem.resize(offset + size);
        readUnsafe(offset, size, cachedColumnVersionList, mem);
        if (!unsafeVerifyAreaChecksum(offset, size, fileLength)) {
            LOG.error().$("_cv body checksum mismatch on self-read [version=").$(version)
                    .$(", offset=").$(offset).$(", size=").$(size).$(']').$();
        }
        return version;
    }

    /**
     * The error for a consistent read that could not reach {@code expectedVersion}. Two shapes, told apart
     * by whether this reader fell back from exactly that version: a torn live area (the file names
     * {@code expectedVersion}, its area does not verify, the previous commit was adopted) or a file that is
     * simply behind the transaction that references it.
     */
    private CairoException columnVersionBehindException(long expectedVersion) {
        if (tornLiveVersion == expectedVersion) {
            return CairoException.cvTorn()
                    .put("_cv live area is torn, reader cannot advance past the previous column version [version=").put(version)
                    .put(", expected=").put(expectedVersion)
                    .put(']');
        }
        return CairoException.cvTorn()
                .put("_cv is behind the column version _txn references [version=").put(version)
                .put(", expected=").put(expectedVersion)
                .put(']');
    }

    /**
     * Points at the other A/B area (the previous commit, opposite parity, published at
     * {@code selectedVersion - 1}), loads it into {@code cachedColumnVersionList} and verifies its checksum:
     * the fallback when the selected area is torn under a stable version. Call it only after confirming the
     * version is stable, so the other area is the settled previous commit, not one the writer is writing.
     * Returns false when the file is too short for the area and its trailer, and true when the checksum
     * verifies or the trailer carries no magic. Only a selected area whose trailer checksum mismatched
     * reaches this fallback; an area without a trailer loads unverified.
     */
    private boolean unsafeLoadAndVerifyOtherArea(long selectedVersion) {
        // The selected area used slot (selectedVersion & 1); the prior commit lives in the opposite slot.
        boolean otherIsA = (selectedVersion & 1L) != 0L;
        long otherOffset = otherIsA ? mem.getLong(OFFSET_OFFSET_A_64) : mem.getLong(OFFSET_OFFSET_B_64);
        long otherSize = otherIsA ? mem.getLong(OFFSET_SIZE_A_64) : mem.getLong(OFFSET_SIZE_B_64);

        // The other area must sit past the header and be a whole number of blocks; a bad header means there
        // is no usable fallback.
        if (otherOffset < HEADER_SIZE || otherSize < 0 || (otherSize % BLOCK_SIZE_BYTES) != 0) {
            return false;
        }
        final long realLen = mem.getFilesFacade().length(mem.getFd());
        if (realLen < otherOffset + otherSize + TableUtils.CV_CHECKSUM_TRAILER_SIZE) {
            // The file is too short for the other area and its trailer, so it cannot be verified: no fallback.
            return false;
        }

        mem.resize(otherOffset + otherSize);
        readUnsafe(otherOffset, otherSize, cachedColumnVersionList, mem);
        return unsafeVerifyAreaChecksum(otherOffset, otherSize, realLen);
    }

    /**
     * Verifies the 16-byte trailer {@code [CV_CHECKSUM_MAGIC][checksum]} after the area. Without the magic
     * there is no trailer, and the area is unverified. The caller brackets this check with stable version
     * reads and passes the real file length; the trailer is mapped only when the file covers it.
     */
    private boolean unsafeVerifyAreaChecksum(long offset, long size, long realLen) {
        if (realLen < offset + size + TableUtils.CV_CHECKSUM_TRAILER_SIZE) {
            // The file is too short to hold a trailer (an older format or a new file): unverified. Nothing is
            // mapped at offset + size, so no mapping reaches past the end of the file.
            return true;
        }
        // The real file covers the 16-byte trailer, so it is safe to map.
        mem.resize(offset + size + TableUtils.CV_CHECKSUM_TRAILER_SIZE);
        return ChecksumTrailer.classify(
                mem.getLong(offset + size),
                mem.getLong(offset + size + Long.BYTES),
                mem.addressOf(offset),
                size,
                TableUtils.CV_CHECKSUM_MAGIC
        ) != ChecksumTrailer.MISMATCH;
    }

    // Whether the header's geometry for an area describes whole blocks that the file holds. An empty area reads
    // no block: a file that never committed names one at offset 0. Written so that garbage cannot overflow it.
    private static boolean isAreaWithinFile(long offset, long size, long fileLength) {
        if (size == 0) {
            return offset >= 0;
        }
        return offset >= HEADER_SIZE && size > 0 && (size % BLOCK_SIZE_BYTES) == 0 && offset <= fileLength - size;
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
