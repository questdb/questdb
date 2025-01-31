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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.BinarySequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.cairo.mv.DefinitionFileUtils.*;

public class DefinitionFileWriter implements Closeable, Mutable {
    private final BlockMemoryHandleImpl blockMemoryHandle = new BlockMemoryHandleImpl();
    private final FilesFacade ff;
    private int blockCount;
    private long blockOffset;
    private MemoryCMARW file;
    private boolean isCommitted;
    private MemoryCARW memory;

    public DefinitionFileWriter(FilesFacade ff) {
        this.ff = ff;
    }

    public AppendOnlyBlock append() {
        return blockMemoryHandle.reset(0);
    }

    @Override
    public void clear() {
        Misc.free(memory);
        Misc.free(file);
        isCommitted = false;
        blockOffset = 0;
        blockCount = 0;
    }

    @Override
    public void close() throws IOException {
        clear();
        memory = null;
        file = null;
    }

    public boolean commit() {
        if (isCommitted) {
            return false;
        }

        // exclude checksum but include block count
        final long memoryBaseAddress = memory.getPageAddress(0);
        final long checksumAddress = memoryBaseAddress + REGION_HEADER_SIZE + REGION_BLOCK_COUNT_OFFSET;
        final long checksumSize = blockOffset - REGION_HEADER_SIZE - REGION_BLOCK_COUNT_OFFSET;
        final int checksum = getChecksum(checksumAddress, checksumSize);

        memory.putInt(REGION_CHECKSUM_OFFSET, checksum);
        memory.putInt(REGION_BLOCK_COUNT_OFFSET, blockCount);

        long currentVersion = getVersionVolatile();
        final long regionLength = blockOffset;
        final long currentRegionOffset = getRegionOffset(currentVersion);
        final long regionWriteOffset = (regionLength <= currentRegionOffset) ?
                0 : currentRegionOffset + getRegionLength(currentVersion);

        file.jumpTo(regionWriteOffset + HEADER_SIZE + regionLength);
        final long fileBaseAddress = file.getPageAddress(0);
        Vect.memcpy(fileBaseAddress + regionWriteOffset + HEADER_SIZE, memoryBaseAddress, regionLength);

        currentVersion += 1;
        setRegionOffset(currentVersion, regionWriteOffset);
        setRegionLength(currentVersion, regionLength);
        setVersionVolatile(currentVersion);

        isCommitted = true;
        return true;
    }

    public long getRegionLength(final long version) {
        return file.getLong(getRegionLengthOffset(version));
    }

    public long getRegionOffset(final long version) {
        return file.getLong(getRegionOffsetOffset(version));
    }

    public long getVersionVolatile() {
        return Unsafe.getUnsafe().getLongVolatile(
                null,
                file.getPageAddress(0) + HEADER_VERSION_OFFSET
        );
    }

    public void of(@Transient final LPSZ path) {
        of(path, ff.getPageSize());
    }

    public void of(@Transient final LPSZ path, final long pageSize) {
        clear();
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

    public RandomAccessBlock reserve(int bytes) {
        return blockMemoryHandle.reset(bytes);
    }

    private void setRegionLength(final long version, final long length) {
        file.putLong(getRegionLengthOffset(version), length);
    }

    private void setRegionOffset(final long version, final long offset) {
        file.putLong(getRegionLengthOffset(version), offset);
    }

    private void setVersionVolatile(final long version) {
        Unsafe.getUnsafe().putLongVolatile(null, file.getPageAddress(0) + HEADER_VERSION_OFFSET, version);
    }

    // Depends on the state of memory, blockOffset and blockCount from the outer scope
    class BlockMemoryHandleImpl implements RandomAccessBlock, AppendOnlyBlock {
        private boolean isCommitted;
        private long payloadOffset;

        @Override
        public boolean commit(short type, byte version, byte flags) {

            if (isCommitted) {
                return false;
            }

            final int blockLength = length() + BLOCK_HEADER_SIZE;

            memory.putShort(blockOffset + BLOCK_TYPE_OFFSET, type);
            memory.putByte(blockOffset + BLOCK_VERSION_OFFSET, version);
            memory.putByte(blockOffset + BLOCK_FLAGS_OFFSET, flags);
            memory.putInt(blockOffset + BLOCK_LENGTH_OFFSET, blockLength);

            blockOffset += blockLength;
            blockCount += 1;

            isCommitted = true;
            return true;
        }

        @Override
        public int length() {
            long length = memory.getAppendOffset() - payloadOffset;
            assert length <= Integer.MAX_VALUE;
            return (int) length;
        }

        @Override
        public long putBin(BinarySequence value) {
            assert !isCommitted;
            return memory.putBin(value);
        }

        @Override
        public long putBin(long from, long len) {
            assert !isCommitted;
            return memory.putBin(from, len);
        }

        // random access API
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
        public void putLong128(long lo, long hi) {
            assert !isCommitted;
            memory.putLong128(lo, hi);
        }

        // append API

        @Override
        public void putLong256(long offset, Long256 value) {
            assert !isCommitted;
            memory.putLong256(payloadOffset + offset, value);
        }

        @Override
        public void putLong256(long offset, long l0, long l1, long l2, long l3) {
            assert !isCommitted;
            memory.putLong256(payloadOffset + offset, l0, l1, l2, l3);
        }

        @Override
        public void putLong256(long l0, long l1, long l2, long l3) {
            assert !isCommitted;
            memory.putLong256(l0, l1, l2, l3);
        }

        @Override
        public void putLong256(Long256 value) {
            assert !isCommitted;
            memory.putLong256(value);
        }

        @Override
        public void putLong256(@Nullable CharSequence hexString) {
            assert !isCommitted;
            memory.putLong256(hexString);
        }

        @Override
        public void putLong256(@NotNull CharSequence hexString, int start, int end) {
            assert !isCommitted;
            memory.putLong256(hexString, start, end);
        }

        @Override
        public void putLong256Utf8(@Nullable Utf8Sequence hexString) {
            assert !isCommitted;
            memory.putLong256Utf8(hexString);
        }

        @Override
        public long putNullBin() {
            assert !isCommitted;
            return memory.putNullBin();
        }

        @Override
        public void putNullStr(long offset) {
            assert !isCommitted;
            memory.putNullStr(payloadOffset + offset);
        }

        @Override
        public long putNullStr() {
            assert !isCommitted;
            return memory.putNullStr();
        }

        @Override
        public void putRawBytes(long from, long len) {
            assert !isCommitted;
            memory.putBlockOfBytes(from, len);
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
        public void putStr(long offset, CharSequence value, int pos, int len) {
            assert !isCommitted;
            memory.putStr(payloadOffset + offset, value, pos, len);
        }

        @Override
        public long putStr(CharSequence value) {
            assert !isCommitted;
            return memory.putStr(value);
        }

        @Override
        public long putStr(char value) {
            assert !isCommitted;
            return memory.putStr(value);
        }

        @Override
        public long putStr(CharSequence value, int pos, int len) {
            assert !isCommitted;
            return memory.putStr(value, pos, len);
        }

        @Override
        public void putVarchar(long offset, @Nullable Utf8Sequence value) {
            assert !isCommitted;
            memory.putVarchar(payloadOffset + offset, value);
        }

        @Override
        public void putVarchar(long offset, @NotNull Utf8Sequence value, int lo, int hi) {
            assert !isCommitted;
            memory.putVarchar(payloadOffset + offset, value, lo, hi);
        }

        @Override
        public long putVarchar(@Nullable Utf8Sequence value) {
            assert !isCommitted;
            //TODO: check serialization format
            return memory.putVarchar(value);
        }

        @Override
        public long putVarchar(@NotNull Utf8Sequence value, int lo, int hi) {
            assert !isCommitted;
            return memory.putVarchar(value, lo, hi);
        }

        public BlockMemoryHandleImpl reset(int length) {
            assert length >= 0;
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
