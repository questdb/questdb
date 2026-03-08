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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.BinarySequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cairo.file.BlockFileUtils.*;

public class BlockFileWriter implements Closeable {
    private final BlockMemoryHandleImpl blockMemoryHandle = new BlockMemoryHandleImpl();
    private final int commitMode;
    private final FilesFacade ff;
    private int blockCount;
    private long blockOffset;
    private MemoryCMARW file;
    private boolean isCommitted;
    private MemoryCARW memory;

    public BlockFileWriter(FilesFacade ff, int commitMode) {
        assert commitMode == CommitMode.ASYNC
                || commitMode == CommitMode.NOSYNC
                || commitMode == CommitMode.SYNC;
        this.ff = ff;
        this.commitMode = commitMode;
    }

    public AppendableBlock append() {
        return blockMemoryHandle.reset(0);
    }

    @Override
    public void close() {
        Misc.free(memory);
        if (file != null) {
            file.close(false);
        }
        reset();
        isCommitted = false;
    }

    public void commit() {
        if (!blockMemoryHandle.isCommitted) {
            throw CairoException.critical(0).put("block must be committed before writer commit call");
        }
        if (isCommitted) {
            throw CairoException.critical(0).put("duplicate writer commit call");
        }

        final long memoryBaseAddress = memory.getPageAddress(0);
        final long checksumAddress = memoryBaseAddress + REGION_HEADER_SIZE + REGION_BLOCK_COUNT_OFFSET;
        final long checksumSize = blockOffset - REGION_HEADER_SIZE - REGION_BLOCK_COUNT_OFFSET;
        // Write checksum to detect file corruption on reads.
        final int checksum = checksum(checksumAddress, checksumSize);

        memory.putInt(REGION_CHECKSUM_OFFSET, checksum);
        memory.putInt(REGION_BLOCK_COUNT_OFFSET, blockCount);

        long currentVersion = getVersionVolatile();
        final long regionLength = blockOffset;
        final long currentRegionOffset = getRegionOffset(currentVersion);
        final long regionWriteOffset = (regionLength <= currentRegionOffset)
                ? 0 : currentRegionOffset + getRegionLength(currentVersion);

        file.jumpTo(regionWriteOffset + HEADER_SIZE + regionLength);
        final long fileBaseAddress = file.getPageAddress(0);
        Vect.memcpy(fileBaseAddress + regionWriteOffset + HEADER_SIZE, memoryBaseAddress, regionLength);

        currentVersion += 1;
        setRegionOffset(currentVersion, regionWriteOffset);
        setRegionLength(currentVersion, regionLength);
        setVersionVolatile(currentVersion);

        if (commitMode != CommitMode.NOSYNC) {
            file.sync(commitMode == CommitMode.ASYNC);
        }

        reset();
        isCommitted = true;
    }

    public long getRegionLength(final long version) {
        return file.getLong(getRegionLengthOffset(version));
    }

    public long getRegionOffset(final long version) {
        return file.getLong(getRegionOffsetOffset(version));
    }

    public long getVersionVolatile() {
        return Unsafe.getUnsafe().getLongVolatile(null, file.getPageAddress(0) + HEADER_VERSION_OFFSET);
    }

    public void of(@Transient final LPSZ path) {
        of(path, ff.getPageSize());
    }

    public void of(@Transient final LPSZ path, final long pageSize) {
        close();
        if (file == null) {
            file = Vm.getCMARWInstance();
        }
        file.of(ff, path, pageSize, ff.length(path), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);

        if (memory == null) {
            memory = Vm.getCARWInstance(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
        }
        memory.jumpTo(REGION_HEADER_SIZE);
        blockOffset = REGION_HEADER_SIZE;
        blockCount = 0;
    }

    public WritableBlock reserve(int bytes) {
        return blockMemoryHandle.reset(bytes);
    }

    private void reset() {
        blockOffset = REGION_HEADER_SIZE;
        blockCount = 0;
    }

    private void setRegionLength(final long version, final long length) {
        file.putLong(getRegionLengthOffset(version), length);
    }

    private void setRegionOffset(final long version, final long offset) {
        file.putLong(getRegionOffsetOffset(version), offset);
    }

    private void setVersionVolatile(final long version) {
        Unsafe.getUnsafe().putLongVolatile(null, file.getPageAddress(0) + HEADER_VERSION_OFFSET, version);
    }

    // Depends on the state of memory, blockOffset and blockCount from the outer scope
    private class BlockMemoryHandleImpl implements WritableBlock, AppendableBlock {
        private boolean isCommitted;
        private long payloadOffset;

        @Override
        public void commit(int blockType) {
            if (isCommitted) {
                throw CairoException.critical(0).put("duplicate block commit call");
            }

            final int blockLength = length() + BLOCK_HEADER_SIZE;
            memory.putInt(blockOffset + BLOCK_LENGTH_OFFSET, blockLength);
            memory.putInt(blockOffset + BLOCK_TYPE_OFFSET, blockType);

            blockOffset += blockLength;
            blockCount += 1;

            isCommitted = true;
        }

        @Override
        public int length() {
            long length = memory.getAppendOffset() - payloadOffset;
            assert length <= Integer.MAX_VALUE;
            return (int) length;
        }

        @Override
        public void putBin(long offset, BinarySequence value) {
            assert !isCommitted;
            if (value != null) {
                final long len = value.length();
                long addr = memory.appendAddressFor(payloadOffset + offset, len + Long.BYTES);
                Unsafe.getUnsafe().putLong(addr, len);
                value.copyTo(addr + Long.BYTES, 0, len);
            } else {
                memory.putNullBin();
            }
        }

        @Override
        public long putBin(BinarySequence value) {
            assert !isCommitted;
            return memory.putBin(value);
        }

        @Override
        public void putBool(long offset, boolean value) {
            assert !isCommitted;
            memory.putBool(payloadOffset + offset, value);
        }

        @Override
        public void putBool(boolean value) {
            assert !isCommitted;
            memory.putBool(value);
        }

        @Override
        public void putByte(long offset, byte value) {
            assert !isCommitted;
            memory.putByte(payloadOffset + offset, value);
        }

        @Override
        public void putByte(byte value) {
            assert !isCommitted;
            memory.putByte(value);
        }

        @Override
        public void putChar(long offset, char value) {
            assert !isCommitted;
            memory.putChar(payloadOffset + offset, value);
        }

        @Override
        public void putChar(char value) {
            assert !isCommitted;
            memory.putChar(value);
        }

        @Override
        public void putDouble(long offset, double value) {
            assert !isCommitted;
            memory.putDouble(payloadOffset + offset, value);
        }

        @Override
        public void putDouble(double value) {
            assert !isCommitted;
            memory.putDouble(value);
        }

        @Override
        public void putFloat(long offset, float value) {
            assert !isCommitted;
            memory.putFloat(payloadOffset + offset, value);
        }

        @Override
        public void putFloat(float value) {
            assert !isCommitted;
            memory.putFloat(value);
        }

        @Override
        public void putInt(long offset, int value) {
            assert !isCommitted;
            memory.putInt(payloadOffset + offset, value);
        }

        @Override
        public void putInt(int value) {
            assert !isCommitted;
            memory.putInt(value);
        }

        @Override
        public void putLong(long offset, long value) {
            assert !isCommitted;
            memory.putLong(payloadOffset + offset, value);
        }

        @Override
        public void putLong(long value) {
            assert !isCommitted;
            memory.putLong(value);
        }

        @Override
        public void putShort(long offset, short value) {
            assert !isCommitted;
            memory.putShort(payloadOffset + offset, value);
        }

        @Override
        public void putShort(short value) {
            assert !isCommitted;
            memory.putShort(value);
        }

        @Override
        public void putStr(long offset, CharSequence value) {
            assert !isCommitted;
            memory.putStr(payloadOffset + offset, value);
        }

        @Override
        public long putStr(CharSequence value) {
            assert !isCommitted;
            return memory.putStr(value);
        }

        @Override
        public void putVarchar(long offset, @Nullable Utf8Sequence value) {
            assert !isCommitted;
            // reuse append API
            final long appendOffset = memory.getAppendOffset();
            memory.jumpTo(payloadOffset + offset);
            putVarchar(value);
            memory.jumpTo(appendOffset);
        }

        @Override
        public long putVarchar(@Nullable Utf8Sequence value) {
            assert !isCommitted;
            VarcharTypeDriver.appendPlainValue(memory, value);
            return memory.getAppendOffset();
        }

        public BlockMemoryHandleImpl reset(int length) {
            assert length >= 0;
            BlockFileWriter.this.isCommitted = false;
            isCommitted = false;
            payloadOffset = blockOffset + BLOCK_HEADER_SIZE;
            memory.jumpTo(payloadOffset + length);
            return this;
        }

        @Override
        public void skip(long bytes) {
            assert !isCommitted;
            memory.skip(bytes);
        }
    }
}
