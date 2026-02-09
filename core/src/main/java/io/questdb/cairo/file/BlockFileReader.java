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

package io.questdb.cairo.file;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.BinarySequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cairo.file.BlockFileUtils.*;

public class BlockFileReader implements Closeable {
    private final BlockCursor blockCursor = new BlockCursor();
    private final @NotNull MillisecondClock clock;
    private final FilesFacade ff;
    private final long spinLockTimeoutMs;
    private MemoryCMR file;
    private MemoryCR memory;

    public BlockFileReader(CairoConfiguration configuration) {
        this.ff = configuration.getFilesFacade();
        this.spinLockTimeoutMs = configuration.getSpinLockTimeout();
        this.clock = configuration.getMillisecondClock();
    }

    @Override
    public void close() {
        Misc.free(memory);
        Misc.free(file);
    }

    public BlockCursor getCursor() {
        return getCursor(-1);
    }

    /**
     * Gets a cursor for reading blocks from the file.
     *
     * @param version the version to read, or -1 to read the current version.
     *                Must be current or current-1, otherwise returns null.
     *                If version changes during read, returns null.
     * @return BlockCursor for reading blocks, or null on failure
     */
    public BlockCursor getCursor(long version) {
        long deadline = clock.getTicks() + spinLockTimeoutMs;
        long currentVersion;
        long regionLength;
        while (true) {
            currentVersion = getVersionVolatile();

            // Specific version must be current or current-1
            if (version >= 0 && version != currentVersion) {
                return null;
            }
            long readVersion = version < 0 ? currentVersion : version;

            final long regionOffset = HEADER_SIZE + file.getLong(getRegionOffsetOffset(readVersion));
            regionLength = file.getLong(getRegionLengthOffset(readVersion));
            if (regionOffset + regionLength > file.size()) {
                if (version >= 0) {
                    return null;
                }
                file.extend(regionOffset + regionLength);
            }

            final long fileBaseAddress = file.getPageAddress(0);
            final long memoryBaseAddress = memory.resize(regionLength);
            Vect.memcpy(memoryBaseAddress, fileBaseAddress + regionOffset, regionLength);

            if (currentVersion == getVersionVolatile()) {
                break;
            }
            if (version >= 0) {
                return null;
            }
            if (clock.getTicks() > deadline) {
                throw CairoException.critical(0)
                        .put("block file read timeout [timeout=")
                        .put(spinLockTimeoutMs)
                        .put("ms, fd=")
                        .put(file.getFd())
                        .put(']');
            }
            Os.pause();
        }

        final long memoryBaseAddress = memory.getPageAddress(0);
        final long checksumAddress = memoryBaseAddress + REGION_HEADER_SIZE + REGION_BLOCK_COUNT_OFFSET;
        final long checksumSize = regionLength - REGION_HEADER_SIZE - REGION_BLOCK_COUNT_OFFSET;
        final int checksum = checksum(checksumAddress, checksumSize);
        final int expectedChecksum = memory.getInt(REGION_CHECKSUM_OFFSET);

        if (checksum != expectedChecksum) {
            if (version >= 0) {
                return null;
            }
            throw CairoException.critical(0)
                    .put("block file checksum mismatch [expected=")
                    .put(expectedChecksum)
                    .put(", actual=")
                    .put(checksum)
                    .put(", fd=")
                    .put(file.getFd())
                    .put(']');
        }

        final int blockCount = memory.getInt(REGION_BLOCK_COUNT_OFFSET);
        assert blockCount > 0;
        final long blocksLength = regionLength - REGION_HEADER_SIZE;
        blockCursor.of(version < 0 ? currentVersion : version, blockCount, REGION_HEADER_SIZE, blocksLength);
        return blockCursor;
    }

    public void of(@Transient LPSZ path) {
        close();
        final long pageSize = ff.getPageSize();
        if (!ff.exists(path)) {
            throw CairoException.fileNotFound()
                    .put("cannot open block file [path=")
                    .put(path)
                    .put(']');
        }

        long fileLen = ff.length(path);
        if (fileLen < HEADER_SIZE) {
            throw CairoException.critical(0)
                    .put("block file too small [expected=").put(HEADER_SIZE)
                    .put(", actual=").put(fileLen)
                    .put(", path=").put(path)
                    .put(']');
        }

        if (file == null) {
            file = new MemoryCMRImpl();
        }
        file.of(ff, path, pageSize, ff.length(path), MemoryTag.MMAP_DEFAULT);
        final long version = getVersionVolatile();
        if (version == 0) {
            throw CairoException.critical(0)
                    .put("cannot read block file, expected at least 1 commited data block [path=")
                    .put(path)
                    .put(']');
        }

        if (memory == null) {
            memory = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    public long getVersion() {
        return getVersionVolatile();
    }

    private long getVersionVolatile() {
        return Unsafe.getUnsafe().getLongVolatile(null, file.getPageAddress(0) + HEADER_VERSION_OFFSET);
    }

    public class BlockCursor {
        private final Block block = new Block();
        private int blockCount;
        private long blocksLimit;
        private long currentBlockOffset;
        private long regionVersion;

        public long getRegionVersion() {
            return regionVersion;
        }

        public boolean hasNext() {
            if (blockCount > 0 && currentBlockOffset < blocksLimit) {
                final int blockLength = memory.getInt(currentBlockOffset + BLOCK_LENGTH_OFFSET);
                return blockLength > 0 && currentBlockOffset + blockLength <= blocksLimit;
            }
            return false;
        }

        public ReadableBlock next() {
            final int blockLength = memory.getInt(currentBlockOffset + BLOCK_LENGTH_OFFSET);
            final int type = memory.getInt(currentBlockOffset + BLOCK_TYPE_OFFSET);

            block.of(blockLength, type, currentBlockOffset);
            currentBlockOffset += blockLength;
            blockCount -= 1;

            return block;
        }

        public void of(long regionVersion, int blockCount, long blocksOffset, long blocksLength) {
            this.regionVersion = regionVersion;
            this.blockCount = blockCount;
            this.blocksLimit = blocksOffset + blocksLength;
            this.currentBlockOffset = blocksOffset;
        }

        private class Block implements ReadableBlock {
            private int length;
            private long payloadOffset;
            private int type;

            @Override
            public long addressOf(long offset) {
                return memory.addressOf(payloadOffset + offset);
            }

            @Override
            public BinarySequence getBin(long offset) {
                return memory.getBin(payloadOffset + offset);
            }

            @Override
            public boolean getBool(long offset) {
                return memory.getBool(payloadOffset + offset);
            }

            @Override
            public byte getByte(long offset) {
                return memory.getByte(payloadOffset + offset);
            }

            @Override
            public char getChar(long offset) {
                return memory.getChar(payloadOffset + offset);
            }

            @Override
            public double getDouble(long offset) {
                return memory.getDouble(payloadOffset + offset);
            }

            @Override
            public float getFloat(long offset) {
                return memory.getFloat(payloadOffset + offset);
            }

            @Override
            public int getInt(long offset) {
                return memory.getInt(payloadOffset + offset);
            }

            @Override
            public long getLong(long offset) {
                return memory.getLong(payloadOffset + offset);
            }

            @Override
            public short getShort(long offset) {
                return memory.getShort(payloadOffset + offset);
            }

            @Override
            public CharSequence getStr(long offset) {
                return memory.getStrA(payloadOffset + offset);
            }

            @Override
            public Utf8Sequence getVarchar(long offset) {
                return VarcharTypeDriver.getPlainValue(memory, payloadOffset + offset);
            }

            @Override
            public long length() {
                return length;
            }

            public void of(int length, int type, long blockOffset) {
                this.length = length;
                this.type = type;
                this.payloadOffset = blockOffset + BLOCK_HEADER_SIZE;
            }

            @Override
            public int type() {
                return type;
            }
        }
    }
}
