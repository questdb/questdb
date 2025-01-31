/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.mv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.BinarySequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.Long256;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8SplitString;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.cairo.mv.DefinitionFileUtils.*;

public class DefinitionFileReader implements Closeable, Mutable {
    private final BlocksCursor blocksCursor = new BlocksCursor();
    private final FilesFacade ff;
    private MemoryCMR file;
    private MemoryCR memory;

    public DefinitionFileReader(FilesFacade ff) {
        this.ff = ff;
    }

    @Override
    public void clear() {
        Misc.free(memory);
        Misc.free(file);
    }

    @Override
    public void close() throws IOException {
        clear();
        memory = null;
        file = null;
    }

    public BlocksCursor getCursor() {
        long regionLength;
        while (true) {
            long currentVersion = getVersionVolatile();
            final long regionOffset = HEADER_SIZE + file.getLong(getRegionOffsetOffset(currentVersion));
            regionLength = file.getLong(getRegionLengthOffset(currentVersion));

            final long fileBaseAddress = file.getPageAddress(0);
            final long memoryBaseAddress = memory.resize(regionLength);
            Vect.memcpy(memoryBaseAddress, fileBaseAddress + regionOffset, regionLength);

            if (currentVersion == getVersionVolatile()) {
                break;
            }
        }

        final long memoryBaseAddress = memory.getPageAddress(0);
        final long checksumAddress = memoryBaseAddress + REGION_HEADER_SIZE + REGION_BLOCK_COUNT_OFFSET;
        final long checksumSize = regionLength - REGION_HEADER_SIZE - REGION_BLOCK_COUNT_OFFSET;
        final int checksum = getChecksum(checksumAddress, checksumSize);
        final int expectedChecksum = memory.getInt(REGION_CHECKSUM_OFFSET);

        if (checksum != expectedChecksum) {
            throw CairoException.critical(0)
                    .put("Checksum mismatch [expected=")
                    .put(expectedChecksum)
                    .put(", actual=")
                    .put(checksum)
                    .put(']');
        }

        final int blockCount = memory.getInt(REGION_BLOCK_COUNT_OFFSET);
        assert blockCount > 0;
        final long blocksLength = regionLength - REGION_HEADER_SIZE;
        blocksCursor.of(blockCount, REGION_HEADER_SIZE, blocksLength);
        return blocksCursor;
    }

    public long getVersionVolatile() {
        return Unsafe.getUnsafe().getLongVolatile(
                null,
                file.getPageAddress(0) + HEADER_VERSION_OFFSET
        );
    }

    public void of(@Transient final LPSZ path) {
        clear();
        final long pageSize = ff.getPageSize();
        if (!ff.exists(path)) {
            throw CairoException.critical(CairoException.ERRNO_FILE_DOES_NOT_EXIST)
                    .put("Cannot find file: ").put(path);
        }

        if (ff.length(path) < HEADER_SIZE) {
            throw CairoException.critical(0).put("Expected at least 1 block");
        }

        if (file == null) {
            file = new MemoryCMRImpl();
        }
        file.of(ff, path, pageSize, ff.length(path), MemoryTag.MMAP_DEFAULT);
        final long version = getVersionVolatile();
        if (version == 0) {
            throw CairoException.critical(0).put("File is empty");
        }

        if (memory == null) {
            memory = new MemoryCARWImpl(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    public class BlocksCursor {
        private final Block block = new Block();
        private int blockCount;
        private long blocksLimit;
        private long currentBlockOffset;

        public boolean hasNext() {
            if (blockCount > 0 && currentBlockOffset < blocksLimit) {
                final int blockLength = memory.getInt(currentBlockOffset + BLOCK_LENGTH_OFFSET);
                return blockLength > 0 && currentBlockOffset + blockLength <= blocksLimit;
            }
            return false;
        }

        public ReadableBlock next() {
            final int blockLength = memory.getInt(currentBlockOffset + BLOCK_LENGTH_OFFSET);
            final short type = memory.getShort(currentBlockOffset + BLOCK_TYPE_OFFSET);
            final byte version = memory.getByte(currentBlockOffset + BLOCK_VERSION_OFFSET);
            final byte flags = memory.getByte(currentBlockOffset + BLOCK_FLAGS_OFFSET);

            block.of(blockLength, version, flags, type, currentBlockOffset);
            currentBlockOffset += blockLength;
            blockCount -= 1;

            return block;
        }

        public void of(int blockCount, long blocksOffset, long blocksLength) {
            this.blockCount = blockCount;
            this.blocksLimit = blocksOffset + blocksLength;
            this.currentBlockOffset = blocksOffset;
        }

        class Block implements ReadableBlock {
            private byte flags;
            private int length;
            private long payloadOffset;
            private short type;
            private byte version;

            @Override
            public long addressOf(long offset) {
                assert offset >= 0 && offset < length;
                return memory.addressOf(payloadOffset + offset);
            }

            @Override
            public byte flags() {
                return flags;
            }

            @Override
            public BinarySequence getBin(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getBin(payloadOffset + offset);
            }

            @Override
            public long getBinLen(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getBinLen(payloadOffset + offset);
            }

            @Override
            public boolean getBool(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getBool(payloadOffset + offset);
            }

            @Override
            public byte getByte(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getByte(payloadOffset + offset);
            }

            @Override
            public char getChar(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getChar(payloadOffset + offset);
            }

            @Override
            public double getDouble(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getDouble(payloadOffset + offset);
            }

            @Override
            public float getFloat(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getFloat(payloadOffset + offset);
            }

            @Override
            public int getIPv4(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getIPv4(payloadOffset + offset);
            }

            @Override
            public int getInt(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getInt(payloadOffset + offset);
            }

            @Override
            public long getLong(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getLong(payloadOffset + offset);
            }

            @Override
            public void getLong256(long offset, CharSink<?> sink) {
                assert offset >= 0 && offset < length;
                memory.getLong256(payloadOffset + offset, sink);
            }

            @Override
            public void getLong256(long offset, Long256Acceptor sink) {
                assert offset >= 0 && offset < length;
                memory.getLong256(payloadOffset + offset, sink);
            }

            @Override
            public Long256 getLong256(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getLong256A(payloadOffset + offset);
            }

            @Override
            public short getShort(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getShort(payloadOffset + offset);
            }

            @Override
            public Utf8SplitString getSplitVarchar(long auxLo, long dataLo, long dataLim, int size, boolean ascii) {
                assert auxLo >= 0 && dataLo >= 0 && dataLim >= 0 && size >= 0;
                return memory.getSplitVarcharA(auxLo, dataLo, dataLim, size, ascii);
            }

            @Override
            public CharSequence getStr(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getStrA(payloadOffset + offset);
            }

            @Override
            public int getStrLen(long offset) {
                assert offset >= 0 && offset < length;
                return memory.getStrLen(payloadOffset + offset);
            }

            @Override
            public DirectUtf8Sequence getVarchar(long offset, int size, boolean ascii) {
                assert offset >= 0 && offset < length;
                return memory.getDirectVarcharA(payloadOffset + offset, size, ascii);
            }


            @Override
            public long length() {
                return length;
            }

            public void of(int length, byte version, byte flags, short type, long blockOffset) {
                this.length = length;
                this.version = version;
                this.flags = flags;
                this.type = type;
                this.payloadOffset = blockOffset + BLOCK_HEADER_SIZE;
            }

            @Override
            public short type() {
                return type;
            }

            @Override
            public byte version() {
                return version;
            }
        }
    }
}