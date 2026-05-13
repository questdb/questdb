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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Zip;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Writer for live-view checkpoint ({@code .cp}) files. One instance per
 * refresh worker, reused across cycles to avoid per-cycle allocation.
 * <p>
 * Lifecycle:
 * <pre>
 *     writer.of(liveViewDir, lvSeqTxn);
 *     MemoryA sink = writer.beginBlock(BLOCK_MANIFEST);
 *     // ... append manifest payload via sink.putXxx() ...
 *     writer.endBlock();
 *     // ... more blocks ...
 *     writer.commit(priorLvSeqTxn);   // tmp+rename atomicity per RFC 123
 * </pre>
 * <p>
 * File layout (RFC 123 §"{@code .cp} file framing"):
 * <pre>
 *   File header (16 bytes):
 *     magic: INT          (0x4C56_4350 - "LVCP")
 *     formatVersion: INT  (1 for V1)
 *     blockCount: INT     (patched at commit)
 *     reserved: INT
 *   Per block:
 *     blockType: INT
 *     blockLength: INT    (payload length only, not including this header)
 *     payload: blockLength bytes
 *   File trailer (4 bytes):
 *     checksum: INT       (CRC32 over header + blocks)
 * </pre>
 * <p>
 * Atomicity: the writer opens {@code <lvSeqTxn>.cp.tmp} via
 * {@link MemoryMARW}, fsyncs per {@link CairoConfiguration#getCommitMode()},
 * renames to {@code <lvSeqTxn>.cp}, then unlinks the prior
 * {@code <priorLvSeqTxn>.cp}. Crash anywhere in this sequence is recovered
 * by the startup sweep (RFC 123 §"{@code .cp} file framing - Atomicity").
 */
public class LiveViewCheckpointWriter implements Closeable {

    /**
     * Subdirectory under each live-view directory holding the rolling head
     * checkpoint file. Created at CREATE LIVE VIEW; never deleted while the
     * live view exists.
     */
    public static final String CHECKPOINT_DIR_NAME = "_checkpoints";

    public static final int FILE_FORMAT_VERSION = 1;
    public static final int FILE_HEADER_BLOCK_COUNT_OFFSET = 8;
    public static final int FILE_HEADER_SIZE = 16;
    public static final int FILE_MAGIC = 0x4C56_4350;
    public static final int FILE_TRAILER_SIZE = 4;

    /**
     * 8 bytes: {@code blockType} (INT) + {@code blockLength} (INT). The
     * length field is patched in place at {@link #endBlock()}.
     */
    public static final int BLOCK_HEADER_SIZE = 8;
    public static final String CP_FILE_EXT = ".cp";
    public static final String CP_TMP_FILE_EXT = ".cp.tmp";
    /**
     * 16-digit zero-padded lvSeqTxn, matching the filename convention in
     * RFC 123 §"{@code .cp} file framing - Filename" so lexical enumeration
     * equals numeric ordering during the startup recovery sweep.
     */
    public static final int LV_SEQTXN_PAD_LEN = 16;
    private final int commitMode;
    private final long extendSegmentSize;
    private final FilesFacade ff;
    private final Path finalPath = new Path();
    private final Path liveViewDirCopy = new Path();
    private final MemoryMARW mem;
    private final Path priorPath = new Path();
    private final Path tmpPath = new Path();
    private long activeLvSeqTxn = Numbers.LONG_NULL;
    private int blockCount;
    /**
     * Absolute offset of the in-flight block's header, or {@code -1} when no
     * block is open. The block-length field at {@code currentBlockHeaderOffset
     * + 4} is patched by {@link #endBlock()}.
     */
    private long currentBlockHeaderOffset = -1;
    private boolean isOpen;

    public LiveViewCheckpointWriter(@NotNull CairoConfiguration configuration) {
        this.commitMode = configuration.getCommitMode();
        this.ff = configuration.getFilesFacade();
        // 64KB is plenty for a typical V1 checkpoint - manifest + a few small
        // anchor blocks + per-function state. Mem grows on demand.
        this.extendSegmentSize = 64 * 1024;
        this.mem = Vm.getCMARWInstance();
    }

    /**
     * Appends a {@code <lvSeqTxn>.cp} filename onto {@code path}, with the
     * 16-digit zero-padded prefix RFC 123 specifies for lexical-equals-numeric
     * enumeration during recovery.
     */
    public static void appendCpFileName(@NotNull Path path, long lvSeqTxn) {
        appendPaddedLvSeqTxn(path, lvSeqTxn);
        // put() appends without a path separator - the lvSeqTxn prefix and the
        // .cp extension are part of the same filename token.
        path.put(CP_FILE_EXT);
    }

    /**
     * Appends a {@code <lvSeqTxn>.cp.tmp} filename onto {@code path}.
     */
    public static void appendCpTmpFileName(@NotNull Path path, long lvSeqTxn) {
        appendPaddedLvSeqTxn(path, lvSeqTxn);
        path.put(CP_TMP_FILE_EXT);
    }

    /**
     * Reserves a block header at the current append offset and returns the
     * {@link MemoryA} sink the caller writes the block payload into. Must be
     * paired with {@link #endBlock()}.
     */
    public MemoryA beginBlock(int blockType) {
        ensureOpen();
        if (currentBlockHeaderOffset != -1) {
            throw CairoException.critical(0)
                    .put("previous live view checkpoint block must be ended before starting a new one");
        }
        currentBlockHeaderOffset = mem.getAppendOffset();
        mem.putInt(blockType);
        // blockLength placeholder; patched in endBlock().
        mem.putInt(0);
        return mem;
    }

    @Override
    public void close() {
        Misc.free(mem);
        Misc.free(tmpPath);
        Misc.free(finalPath);
        Misc.free(priorPath);
        Misc.free(liveViewDirCopy);
        isOpen = false;
        currentBlockHeaderOffset = -1;
        blockCount = 0;
        activeLvSeqTxn = Numbers.LONG_NULL;
    }

    /**
     * Finalises the in-flight {@code .cp.tmp}: patches the file header's
     * {@code blockCount} field, appends the CRC32 trailer, fsyncs per
     * {@code cairo.commit.mode}, closes the mmapped file (truncating to the
     * exact written size), renames {@code .cp.tmp} to {@code .cp}, then
     * unlinks the prior head iff {@code priorLvSeqTxn != LONG_NULL}.
     * <p>
     * On rename failure throws {@link CairoException}; the caller is the
     * refresh worker, which logs critical and continues (RFC 123 §"Flush"
     * step 4 - {@code .cp} write failure does not invalidate the view).
     * <p>
     * On unlink failure (prior {@code .cp} could not be removed) the new
     * {@code .cp} stays in place and the prior leaks until the next startup
     * sweep, which retires older files unconditionally. No exception is
     * thrown for this case.
     */
    public void commit(long priorLvSeqTxn) {
        ensureOpen();
        if (currentBlockHeaderOffset != -1) {
            throw CairoException.critical(0)
                    .put("live view checkpoint block in progress at commit");
        }
        // Patch the placeholder blockCount slot written by of().
        mem.putInt(FILE_HEADER_BLOCK_COUNT_OFFSET, blockCount);

        // CRC32 over [0, appendOffset) - header + all blocks. The trailer
        // itself is excluded.
        final long bodyEnd = mem.getAppendOffset();
        if (bodyEnd > Integer.MAX_VALUE) {
            throw CairoException.critical(0)
                    .put("live view checkpoint exceeds maximum supported size, bytes=")
                    .put(bodyEnd);
        }
        final long baseAddress = mem.addressOf(0);
        final int crc = Zip.crc32(0, baseAddress, (int) bodyEnd);
        mem.putInt(crc);

        if (commitMode != CommitMode.NOSYNC) {
            mem.sync(commitMode == CommitMode.ASYNC);
        }

        // Close before rename - on Windows the rename would fail with the
        // file still open, and on POSIX it avoids a stale mmap to the
        // pre-rename inode. TRUNCATE_TO_POINTER trims the trailing mmap pages
        // beyond the actual write size.
        mem.close(true, Vm.TRUNCATE_TO_POINTER);

        // Rename tmp -> .cp.
        finalPath.of(liveViewDirCopy)
                .concat(CHECKPOINT_DIR_NAME)
                .slash();
        appendCpFileName(finalPath, activeLvSeqTxn);
        final int renameResult = ff.rename(tmpPath.$(), finalPath.$());
        if (renameResult != Files.FILES_RENAME_OK) {
            // Try to keep operator visibility into the tmp leftover -
            // recovery sweep will clean it on next start.
            final int errno = ff.errno();
            isOpen = false;
            throw CairoException.critical(errno)
                    .put("could not rename live view checkpoint tmp file, lvSeqTxn=")
                    .put(activeLvSeqTxn)
                    .put(", renameResult=")
                    .put(renameResult);
        }

        // Unlink prior .cp (if any). Best-effort - failure leaves two .cp
        // files; recovery picks the highest lvSeqTxn and retires older ones.
        if (priorLvSeqTxn != Numbers.LONG_NULL && priorLvSeqTxn != activeLvSeqTxn) {
            priorPath.of(liveViewDirCopy)
                    .concat(CHECKPOINT_DIR_NAME)
                    .slash();
            appendCpFileName(priorPath, priorLvSeqTxn);
            ff.removeQuiet(priorPath.$());
        }

        // Reset internal state - writer is reusable after commit().
        isOpen = false;
        currentBlockHeaderOffset = -1;
        blockCount = 0;
        activeLvSeqTxn = Numbers.LONG_NULL;
    }

    /**
     * Closes the in-flight block: reads the bytes appended since
     * {@link #beginBlock(int)} and patches the block-length field at the
     * captured header offset.
     */
    public void endBlock() {
        ensureOpen();
        if (currentBlockHeaderOffset == -1) {
            throw CairoException.critical(0)
                    .put("no live view checkpoint block in progress");
        }
        final long payloadLength = mem.getAppendOffset() - currentBlockHeaderOffset - BLOCK_HEADER_SIZE;
        if (payloadLength < 0 || payloadLength > Integer.MAX_VALUE) {
            throw CairoException.critical(0)
                    .put("live view checkpoint block size out of range, bytes=")
                    .put(payloadLength);
        }
        mem.putInt(currentBlockHeaderOffset + Integer.BYTES, (int) payloadLength);
        currentBlockHeaderOffset = -1;
        blockCount++;
    }

    /**
     * Opens {@code <liveViewDir>/_checkpoints/<lvSeqTxn>.cp.tmp} for writing
     * and writes the file header (with a placeholder
     * {@code blockCount = 0} field, patched at {@link #commit(long)}).
     * <p>
     * Pre-existing {@code .cp.tmp} files at the same path are overwritten -
     * they are orphans from a prior crash and the recovery sweep would
     * remove them anyway.
     */
    public void of(@NotNull LPSZ liveViewDir, long lvSeqTxn) {
        if (isOpen) {
            throw CairoException.critical(0)
                    .put("live view checkpoint writer already open");
        }
        liveViewDirCopy.of(liveViewDir);

        tmpPath.of(liveViewDir).concat(CHECKPOINT_DIR_NAME).slash();
        appendCpTmpFileName(tmpPath, lvSeqTxn);

        mem.of(
                ff,
                tmpPath.$(),
                extendSegmentSize,
                -1,
                MemoryTag.MMAP_DEFAULT,
                CairoConfiguration.O_NONE,
                Files.POSIX_MADV_SEQUENTIAL
        );

        activeLvSeqTxn = lvSeqTxn;
        blockCount = 0;
        currentBlockHeaderOffset = -1;
        isOpen = true;

        // Header: magic, formatVersion, blockCount placeholder, reserved.
        mem.putInt(FILE_MAGIC);
        mem.putInt(FILE_FORMAT_VERSION);
        mem.putInt(0); // blockCount placeholder
        mem.putInt(0); // reserved
        assert mem.getAppendOffset() == FILE_HEADER_SIZE;
    }

    /**
     * Writes a MANIFEST block from the populated {@code manifest} bean.
     * Wraps {@link #beginBlock(int)} / {@link #endBlock()} for the
     * caller; the body matches the RFC 123 §"Checkpoint manifest - MANIFEST
     * block" field order.
     */
    public void writeManifestBlock(@NotNull LiveViewCheckpointManifest manifest) {
        final MemoryA sink = beginBlock(LiveViewCheckpointBlockType.BLOCK_MANIFEST);
        sink.putLong(manifest.getLvSeqTxn());
        sink.putLong(manifest.getLvRowPosition());
        sink.putLong(manifest.getBaseSeqTxn());
        sink.putLong(manifest.getMaxTimestamp());
        sink.putByte(manifest.getKind());
        final int windowCount = manifest.getWindowNames().size();
        sink.putInt(windowCount);
        for (int i = 0; i < windowCount; i++) {
            sink.putStr(manifest.getWindowNames().getQuick(i));
        }
        endBlock();
    }

    private static void appendPaddedLvSeqTxn(@NotNull Path path, long lvSeqTxn) {
        if (lvSeqTxn < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint seqTxn must be non-negative, was ")
                    .put(lvSeqTxn);
        }
        // Manual 16-digit zero-pad. Avoids String.format allocation and keeps
        // the slow-path call out of the GC profile for hot live views.
        final int digits = lvSeqTxn == 0 ? 1 : (int) Math.floor(Math.log10(lvSeqTxn)) + 1;
        for (int i = digits; i < LV_SEQTXN_PAD_LEN; i++) {
            path.put('0');
        }
        path.put(lvSeqTxn);
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw CairoException.critical(0)
                    .put("live view checkpoint writer is not open");
        }
    }
}
