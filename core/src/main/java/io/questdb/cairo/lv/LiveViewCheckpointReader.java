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
import io.questdb.cairo.TableUtils;
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
 * {@link #of(LPSZ)} validates the file's magic value, verifies the CRC32
 * trailer over header + blocks, and only then checks the format version. A
 * CRC mismatch or magic mismatch makes {@code of} throw a plain
 * {@link CairoException}; the caller in {@code LiveViewRefreshJob} catches it,
 * unlinks the head, and falls back to the {@code viewLowerBoundTimestamp}
 * replay path. The live view is not invalidated by corruption - the
 * {@code .cp} is derived state, recoverable by re-running the refresh from the
 * last applied watermark.
 * <p>
 * A {@code formatVersion} outside the supported range is treated separately.
 * It is not corruption but a real compatibility break, so {@code of} throws
 * with {@link CairoException#LV_CHECKPOINT_FILE_VERSION_MISMATCH} and the
 * caller invalidates the live view rather than unlinking the {@code .cp}.
 * The version check runs <em>after</em> the CRC so a bit-rotted version field
 * (which the CRC covers) is caught as recoverable corruption first, rather
 * than being mistaken for a compatibility break and forcing invalidation.
 * The same rule applies to the per-function snapshot version check at the
 * function-block read path.
 */
public class LiveViewCheckpointReader implements Closeable {

    public static final int SUPPORTED_VERSION_MAX = FILE_FORMAT_VERSION;
    public static final int SUPPORTED_VERSION_MIN = FILE_FORMAT_VERSION;

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
            // Verify the CRC32 trailer BEFORE the format-version check. The
            // version field (offset 4) and the block count (offset 8) both sit
            // inside the header the CRC covers, so a bit-rotted version field
            // must be classified as recoverable corruption (a plain
            // CairoException, so the caller unlinks the head and replays from
            // viewLowerBoundTimestamp) rather than a compatibility break
            // (LV_CHECKPOINT_FILE_VERSION_MISMATCH, which makes the caller
            // invalidate the view). Only once the CRC confirms the header is
            // intact can a version outside the supported range be trusted as a
            // real format difference rather than corrupted bytes.
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

            // CRC verified: the header is intact, so a version outside the
            // supported range is a genuine compatibility break, not corruption.
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
            // Require room for the whole 8-byte block header before bodyEnd: next() reads two
            // ints off nextBlockOffset, so a bare "< bodyEnd" admits an offset up to bodyEnd-1
            // and the header read runs up to 7 bytes past bodyEnd - past fileSize on a crafted
            // CRC-valid file (FILE_TRAILER_SIZE is only 4), an OOB read that SIGSEGVs with -ea
            // off. A valid last block ends exactly at bodyEnd, so this never rejects real data.
            return blockIndex < blockCount && nextBlockOffset + BLOCK_HEADER_SIZE <= bodyEnd;
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

        public long addressOf(long offset, long size) {
            checkBounds(offset, size);
            return mem.addressOf(payloadStart + offset);
        }

        public byte getByte(long offset) {
            checkBounds(offset, Byte.BYTES);
            return mem.getByte(payloadStart + offset);
        }

        public int getInt(long offset) {
            checkBounds(offset, Integer.BYTES);
            return mem.getInt(payloadStart + offset);
        }

        public long getLong(long offset) {
            checkBounds(offset, Long.BYTES);
            return mem.getLong(payloadStart + offset);
        }

        public CharSequence getStr(long offset) {
            // Bound the 4-byte length prefix, then the payload the prefix declares,
            // so a crafted (CRC-valid) length cannot drive an out-of-bounds native
            // read. A null STR (length -1) carries no payload bytes.
            checkBounds(offset, Integer.BYTES);
            final int strLen = mem.getInt(payloadStart + offset);
            if (strLen != TableUtils.NULL_LEN) {
                if (strLen < 0) {
                    throw CairoException.critical(0)
                            .put("live view checkpoint block has corrupt string length, offset=")
                            .put(offset)
                            .put(", len=")
                            .put(strLen);
                }
                checkBounds(offset + Integer.BYTES, (long) strLen * Character.BYTES);
            }
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

        private void checkBounds(long offset, long size) {
            if (offset < 0 || size < 0 || offset + size > length) {
                throw CairoException.critical(0)
                        .put("live view checkpoint block read out of bounds, offset=")
                        .put(offset)
                        .put(", size=")
                        .put(size)
                        .put(", length=")
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
