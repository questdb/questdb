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

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Zip;
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cairo.lv.LiveViewCheckpointWriter.BLOCK_HEADER_SIZE;
import static io.questdb.cairo.lv.LiveViewCheckpointWriter.FILE_FORMAT_VERSION;
import static io.questdb.cairo.lv.LiveViewCheckpointWriter.FILE_HEADER_BLOCK_COUNT_OFFSET;
import static io.questdb.cairo.lv.LiveViewCheckpointWriter.FILE_HEADER_SIZE;
import static io.questdb.cairo.lv.LiveViewCheckpointWriter.FILE_MAGIC;
import static io.questdb.cairo.lv.LiveViewCheckpointWriter.FILE_TRAILER_SIZE;

/**
 * Reader for live-view checkpoint ({@code .cp}) files. Companion to
 * {@link LiveViewCheckpointWriter}; refer to that class's javadoc for the
 * on-disk layout.
 * <p>
 * Lifecycle:
 * <pre>
 *     reader.of(cpFilePath);
 *     // optional manifest helper:
 *     reader.readManifestInto(manifest);
 *     // generic block iteration:
 *     BlockCursor c = reader.getCursor();
 *     while (c.hasNext()) {
 *         ReadableBlock block = c.next();
 *         switch (block.type()) {
 *             case BLOCK_MANIFEST: ...
 *             case BLOCK_WINDOW_ANCHOR: ...
 *             ...
 *         }
 *     }
 * </pre>
 * <p>
 * {@link #of(LPSZ)} validates the file's magic value and format version, and
 * verifies the CRC32 trailer over header + blocks. A CRC mismatch or magic
 * mismatch makes {@code of} throw a plain {@link CairoException}; the caller
 * in {@code LiveViewRefreshJob} catches it, unlinks the head, and falls back
 * to the {@code viewLowerBoundTimestamp} replay path. The live view is not
 * invalidated by corruption - the {@code .cp} is derived state, recoverable
 * by re-running the refresh from the last applied watermark.
 * <p>
 * A {@code formatVersion} outside the supported range is treated separately.
 * It is not corruption but a real compatibility break, so {@code of} throws
 * with {@link CairoException#LV_CHECKPOINT_FILE_VERSION_MISMATCH} and the
 * caller invalidates the live view rather than unlinking the {@code .cp}.
 * The same rule applies to the per-function snapshot version check at the
 * function-block read path.
 */
public class LiveViewCheckpointReader implements Closeable {

    public static final int SUPPORTED_VERSION_MAX = FILE_FORMAT_VERSION;
    public static final int SUPPORTED_VERSION_MIN = 1;

    private final BlockCursor cursor = new BlockCursor();
    private final FilesFacade ff;
    private final MemoryCMR mem;
    private int blockCount;
    private long bodyEnd;
    private boolean isOpen;

    public LiveViewCheckpointReader(@NotNull CairoConfiguration configuration) {
        this.ff = configuration.getFilesFacade();
        this.mem = Vm.getCMRInstance();
    }

    @Override
    public void close() {
        Misc.free(mem);
        isOpen = false;
        blockCount = 0;
        bodyEnd = 0;
    }

    public int getBlockCount() {
        ensureOpen();
        return blockCount;
    }

    public BlockCursor getCursor() {
        ensureOpen();
        cursor.reset();
        return cursor;
    }

    /**
     * Opens the {@code .cp} file at {@code path} for reading. Validates
     * magic, format version, and the CRC32 trailer. Throws
     * {@link CairoException} on any structural error - the caller is
     * expected to unlink the file and fall into the head-miss replay path.
     */
    public void of(@NotNull LPSZ path) {
        if (isOpen) {
            close();
        }
        final long fileSize = ff.length(path);
        if (fileSize < FILE_HEADER_SIZE + FILE_TRAILER_SIZE) {
            throw CairoException.critical(0)
                    .put("live view checkpoint file too small to be valid, size=")
                    .put(fileSize)
                    .put(", path=")
                    .put(path);
        }

        mem.of(
                ff,
                path,
                ff.getPageSize(),
                fileSize,
                MemoryTag.MMAP_DEFAULT,
                CairoConfiguration.O_NONE,
                -1
        );
        isOpen = true;
        try {
            final int magic = mem.getInt(0);
            if (magic != FILE_MAGIC) {
                throw CairoException.critical(0)
                        .put("live view checkpoint magic mismatch, expected=")
                        .put(FILE_MAGIC)
                        .put(", actual=")
                        .put(magic);
            }
            final int formatVersion = mem.getInt(4);
            if (formatVersion < SUPPORTED_VERSION_MIN) {
                throw CairoException.critical(CairoException.LV_CHECKPOINT_FILE_VERSION_MISMATCH)
                        .put("live view checkpoint format version too old, version=")
                        .put(formatVersion)
                        .put(", supportedMin=")
                        .put(SUPPORTED_VERSION_MIN);
            }
            if (formatVersion > SUPPORTED_VERSION_MAX) {
                throw CairoException.critical(CairoException.LV_CHECKPOINT_FILE_VERSION_MISMATCH)
                        .put("live view checkpoint format version too new, version=")
                        .put(formatVersion)
                        .put(", supportedMax=")
                        .put(SUPPORTED_VERSION_MAX);
            }
            blockCount = mem.getInt(FILE_HEADER_BLOCK_COUNT_OFFSET);
            if (blockCount < 0) {
                throw CairoException.critical(0)
                        .put("live view checkpoint block count negative, blockCount=")
                        .put(blockCount);
            }

            // Verify CRC32 trailer.
            bodyEnd = fileSize - FILE_TRAILER_SIZE;
            if (bodyEnd > Integer.MAX_VALUE) {
                throw CairoException.critical(0)
                        .put("live view checkpoint exceeds maximum supported size, bytes=")
                        .put(bodyEnd);
            }
            final long baseAddress = mem.addressOf(0);
            final int computedCrc = Zip.crc32(0, baseAddress, (int) bodyEnd);
            final int storedCrc = mem.getInt(bodyEnd);
            if (computedCrc != storedCrc) {
                throw CairoException.critical(0)
                        .put("live view checkpoint CRC mismatch, expected=")
                        .put(storedCrc)
                        .put(", computed=")
                        .put(computedCrc);
            }
            cursor.reset();
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    /**
     * Convenience: locates the MANIFEST block (which must be the first block
     * in a valid file), parses it into {@code dst}, and rewinds the cursor.
     * Throws {@link CairoException} if no MANIFEST block is present at the
     * head of the file.
     */
    public void readManifestInto(@NotNull LiveViewCheckpointManifest dst) {
        ensureOpen();
        cursor.reset();
        if (!cursor.hasNext()) {
            throw CairoException.critical(0)
                    .put("live view checkpoint missing MANIFEST block");
        }
        final ReadableBlock manifestBlock = cursor.next();
        if (manifestBlock.type() != LiveViewCheckpointBlockType.BLOCK_MANIFEST) {
            throw CairoException.critical(0)
                    .put("live view checkpoint expected MANIFEST as first block, found=")
                    .put(LiveViewCheckpointBlockType.nameOf(manifestBlock.type()));
        }
        dst.clear();
        long offset = 0;
        dst.setLvSeqTxn(manifestBlock.getLong(offset));
        offset += Long.BYTES;
        dst.setLvRowPosition(manifestBlock.getLong(offset));
        offset += Long.BYTES;
        dst.setBaseSeqTxn(manifestBlock.getLong(offset));
        offset += Long.BYTES;
        dst.setMaxTimestamp(manifestBlock.getLong(offset));
        offset += Long.BYTES;
        dst.setKind(manifestBlock.getByte(offset));
        offset += Byte.BYTES;
        final int windowCount = manifestBlock.getInt(offset);
        offset += Integer.BYTES;
        for (int i = 0; i < windowCount; i++) {
            final CharSequence name = manifestBlock.getStr(offset);
            // The writer never emits a null window name (addWindowName rejects null),
            // so a null slot here means a corrupt or truncated manifest. Throw the same
            // structural-corruption CairoException the magic/CRC checks use, so the caller
            // unlinks the head and falls into the viewLowerBoundTimestamp replay path
            // instead of dereferencing null on the next line.
            if (name == null) {
                throw CairoException.critical(0)
                        .put("live view checkpoint manifest has null window name, index=")
                        .put(i);
            }
            // Length-prefixed: 4 bytes for length + 2 * length bytes for chars.
            offset += Integer.BYTES + (long) name.length() * Character.BYTES;
            dst.addWindowName(name.toString());
        }
        cursor.reset();
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw CairoException.critical(0)
                    .put("live view checkpoint reader is not open");
        }
    }

    public class BlockCursor {
        private final ReadableBlock block = new ReadableBlock();
        private int blockIndex;
        private long nextBlockOffset;

        public boolean hasNext() {
            return blockIndex < blockCount && nextBlockOffset < bodyEnd;
        }

        public ReadableBlock next() {
            if (!hasNext()) {
                throw CairoException.critical(0)
                        .put("no more live view checkpoint blocks at index=")
                        .put(blockIndex);
            }
            final long header = nextBlockOffset;
            final int blockType = mem.getInt(header);
            final int payloadLength = mem.getInt(header + Integer.BYTES);
            if (payloadLength < 0) {
                throw CairoException.critical(0)
                        .put("live view checkpoint block has negative payload length, index=")
                        .put(blockIndex)
                        .put(", length=")
                        .put(payloadLength);
            }
            final long payloadStart = header + BLOCK_HEADER_SIZE;
            if (payloadStart + payloadLength > bodyEnd) {
                throw CairoException.critical(0)
                        .put("live view checkpoint block overruns body, index=")
                        .put(blockIndex)
                        .put(", payloadStart=")
                        .put(payloadStart)
                        .put(", payloadLength=")
                        .put(payloadLength)
                        .put(", bodyEnd=")
                        .put(bodyEnd);
            }
            block.set(blockType, payloadStart, payloadLength);
            nextBlockOffset = payloadStart + payloadLength;
            blockIndex++;
            return block;
        }

        void reset() {
            blockIndex = 0;
            nextBlockOffset = FILE_HEADER_SIZE;
        }
    }

    /**
     * Read-only view onto a single block within the checkpoint file. Offsets
     * passed to {@code getXxx(offset)} are block-relative - {@code offset=0}
     * is the first byte of the block payload (immediately after the
     * 8-byte block header).
     */
    public class ReadableBlock {
        private int length;
        private long payloadStart;
        private int type;

        public long addressOf(long offset) {
            checkBounds(offset);
            return mem.addressOf(payloadStart + offset);
        }

        public byte getByte(long offset) {
            checkBounds(offset);
            return mem.getByte(payloadStart + offset);
        }

        public int getInt(long offset) {
            checkBounds(offset);
            return mem.getInt(payloadStart + offset);
        }

        public long getLong(long offset) {
            checkBounds(offset);
            return mem.getLong(payloadStart + offset);
        }

        public CharSequence getStr(long offset) {
            checkBounds(offset);
            return mem.getStrA(payloadStart + offset);
        }

        /**
         * Underlying memory view; payload starts at {@link #payloadStart()}.
         * Use sparingly - prefer the {@code getXxx(offset)} accessors which
         * already apply the block-relative offset.
         */
        public MemoryR memory() {
            return mem;
        }

        public long payloadStart() {
            return payloadStart;
        }

        public long size() {
            return length;
        }

        public int type() {
            return type;
        }

        private void checkBounds(long offset) {
            if (offset < 0 || offset >= length) {
                throw CairoException.critical(0)
                        .put("live view checkpoint block read out of bounds, offset=")
                        .put(offset)
                        .put(", size=")
                        .put(length);
            }
        }

        private void set(int type, long payloadStart, int length) {
            this.type = type;
            this.payloadStart = payloadStart;
            this.length = length;
        }
    }
}
