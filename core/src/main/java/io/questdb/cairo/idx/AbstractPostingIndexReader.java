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

package io.questdb.cairo.idx;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Shared base for forward and backward posting index readers.
 * Contains index metadata management, memory mapping, generation lookup,
 * and reload logic common to both iteration directions.
 */
public abstract class AbstractPostingIndexReader implements BitmapIndexReader {

    private static final Log LOG = LogFactory.getLog(AbstractPostingIndexReader.class);
    private static final String INDEX_CORRUPT = "posting index is corrupt";
    protected final PostingGenLookup genLookup = new PostingGenLookup();
    protected final MemoryMR keyMem = Vm.getCMRInstance();
    protected final MemoryMR valueMem = Vm.getCMRInstance();
    protected long columnTop;
    protected int coverCount;
    protected int genCount;
    protected int keyCount;
    protected int[] sidecarColumnIndices;
    protected int[] sidecarColumnTypes;
    protected MemoryMR[] sidecarMems;
    private long activePageOffset;
    private MillisecondClock clock;
    private long columnTxn;
    private int keyCountIncludingNulls;
    private long keyFileSequence = -1;
    private long partitionTxn;
    private long spinLockTimeoutMs;
    private long valueMemSize = -1;

    @Override
    public void close() {
        try {
            genLookup.close();
        } finally {
            try {
                Misc.free(keyMem);
            } finally {
                try {
                    Misc.free(valueMem);
                } finally {
                    closeSidecarMems();
                }
            }
        }
    }

    private void closeSidecarMems() {
        if (sidecarMems != null) {
            for (int i = 0; i < sidecarMems.length; i++) {
                Misc.free(sidecarMems[i]);
                sidecarMems[i] = null;
            }
            sidecarMems = null;
        }
        coverCount = 0;
        sidecarColumnIndices = null;
        sidecarColumnTypes = null;
    }

    @Override
    public long getColumnTop() {
        return columnTop;
    }

    /**
     * Returns the current tier used by the generation lookup.
     * Primarily for testing: 1 = per-key, 2 = SBBF, 0 = none/fallback.
     */
    public int getGenLookupTier() {
        return genLookup.getTier();
    }

    /**
     * Sets the memory budget for gen lookup tier selection.
     * Primarily for testing: a small budget forces Tier 2 (SBBF) or Tier 3 (none).
     */
    public void setGenLookupMemoryBudget(long budget) {
        genLookup.setMemoryBudget(budget);
    }

    @Override
    public long getColumnTxn() {
        return columnTxn;
    }

    @Override
    public long getKeyBaseAddress() {
        return keyMem.addressOf(0);
    }

    @Override
    public int getKeyCount() {
        return keyCountIncludingNulls;
    }

    @Override
    public long getKeyMemorySize() {
        return keyMem.size();
    }

    @Override
    public long getPartitionTxn() {
        return partitionTxn;
    }

    @Override
    public long getValueBaseAddress() {
        return valueMem.addressOf(0);
    }

    /**
     * Returns 0 because PostingIndex does not use the legacy block-linked-list
     * value file layout. The only consumer (GeoHashNative.latestByAndFilterPrefix)
     * expects that layout and cannot operate on PostingIndex data regardless.
     */
    @Override
    public int getValueBlockCapacity() {
        return 0;
    }

    @Override
    public long getValueMemorySize() {
        return valueMem.size();
    }

    @Override
    public boolean isOpen() {
        return keyMem.getFd() != -1;
    }

    @Override
    public void of(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop
    ) {
        this.columnTop = columnTop;
        this.columnTxn = columnNameTxn;
        this.partitionTxn = partitionTxn;
        this.spinLockTimeoutMs = configuration.getSpinLockTimeout();
        this.clock = configuration.getMillisecondClock();
        final int plen = path.size();

        try {
            FilesFacade ff = configuration.getFilesFacade();
            LPSZ name = PostingIndexUtils.keyFileName(path, columnName, columnNameTxn);
            keyMem.of(
                    ff,
                    name,
                    ff.getMapPageSize(),
                    PostingIndexUtils.KEY_FILE_RESERVED,
                    MemoryTag.MMAP_INDEX_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );

            readIndexMetadataFromBestPage();

            int version = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_FORMAT_VERSION);
            if (version != 0 && version != PostingIndexUtils.FORMAT_VERSION) {
                throw CairoException.critical(0).put("Unsupported Posting index version: ").put(version);
            }

            this.valueMem.of(
                    configuration.getFilesFacade(),
                    PostingIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn),
                    valueMemSize,
                    valueMemSize,
                    MemoryTag.MMAP_INDEX_READER
            );

            // Try to open sidecar files for covering index
            openSidecarFilesIfPresent(configuration, path.trimTo(plen), columnName, columnNameTxn);
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    private void openSidecarFilesIfPresent(
            CairoConfiguration configuration,
            Path path,
            CharSequence columnName,
            long columnNameTxn
    ) {
        FilesFacade ff = configuration.getFilesFacade();
        int plen = path.size();
        LPSZ pciFile = PostingIndexUtils.coverInfoFileName(path, columnName, columnNameTxn);
        if (!ff.exists(pciFile)) {
            path.trimTo(plen);
            return;
        }
        MemoryCMR infoMem = Vm.getCMRInstance();
        try {
            infoMem.of(ff, pciFile, ff.getMapPageSize(), 8, MemoryTag.MMAP_INDEX_READER, CairoConfiguration.O_NONE, -1);
            int magic = infoMem.getInt(0);
            if (magic != PostingIndexUtils.COVER_INFO_MAGIC) {
                return;
            }
            int count = infoMem.getInt(4);
            if (count <= 0) {
                return;
            }
            long neededSize = 8 + (long) count * 8;
            infoMem.extend(neededSize);
            sidecarColumnIndices = new int[count];
            sidecarColumnTypes = new int[count];
            for (int i = 0; i < count; i++) {
                sidecarColumnIndices[i] = infoMem.getInt(8 + (long) i * 8);
                sidecarColumnTypes[i] = infoMem.getInt(8 + (long) i * 8 + 4);
            }
            coverCount = count;

            sidecarMems = new MemoryMR[count];
            for (int c = 0; c < count; c++) {
                LPSZ pcFile = PostingIndexUtils.coverDataFileName(path.trimTo(plen), columnName, columnNameTxn, c);
                if (ff.exists(pcFile)) {
                    sidecarMems[c] = Vm.getCMRInstance();
                    long fileLen = ff.length(pcFile);
                    ((MemoryCMR) sidecarMems[c]).of(ff, pcFile, ff.getMapPageSize(), fileLen, MemoryTag.MMAP_INDEX_READER, CairoConfiguration.O_NONE, -1);
                }
                path.trimTo(plen);
            }
        } catch (Throwable e) {
            LOG.error().$("failed to open sidecar files").$((Throwable) e).$();
            closeSidecarMems();
        } finally {
            Misc.free(infoMem);
            path.trimTo(plen);
        }
    }

    @Override
    public void reloadConditionally() {
        // Check both pages for a higher sequence than cached
        Unsafe.getUnsafe().loadFence();
        long seqA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
        long seqB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
        long maxSeq = Math.max(seqA, seqB);
        if (maxSeq != keyFileSequence) {
            readIndexMetadataFromBestPage();
            if (valueMemSize > 0) {
                ((MemoryCMR) this.valueMem).changeSize(valueMemSize);
            }
            // snapshotMetadata (inside readIndexMetadataFromBestPage) already
            // reset builtForGenCount, so ensureGenLookup will rebuild the index.
        }
    }

    public void updateKeyCount() {
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            Unsafe.getUnsafe().loadFence();
            long seqStartA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
            long seqStartB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);

            long bestPage = (seqStartB > seqStartA) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;
            long otherPage = (bestPage == PostingIndexUtils.PAGE_A_OFFSET) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;

            for (int attempt = 0; attempt < 2; attempt++) {
                long tryPage = (attempt == 0) ? bestPage : otherPage;

                long seqStart = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                if (seqStart <= keyFileSequence) {
                    if (attempt == 0) {
                        continue;
                    }
                    return; // no update available on either page
                }
                Unsafe.getUnsafe().loadFence();

                int keyCount = keyMem.getInt(tryPage + PostingIndexUtils.PAGE_OFFSET_KEY_COUNT);
                int genCount = keyMem.getInt(tryPage + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT);
                long valueMemSize = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_VALUE_MEM_SIZE);

                Unsafe.getUnsafe().loadFence();
                long seqEnd = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);

                if (seqStart == seqEnd && keyCount >= this.keyCount
                        && genCount >= 0 && genCount <= PostingIndexUtils.MAX_GEN_COUNT) {
                    genLookup.snapshotMetadata(keyMem, genCount, tryPage);

                    Unsafe.getUnsafe().loadFence();
                    if (keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START) != seqStart) {
                        break; // page overwritten during snapshot, re-pick best page
                    }

                    if (valueMemSize > 0) {
                        ((MemoryCMR) valueMem).changeSize(valueMemSize);
                    }
                    this.activePageOffset = tryPage;
                    this.keyFileSequence = seqStart;
                    this.valueMemSize = valueMemSize;
                    this.keyCount = keyCount;
                    this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
                    this.genCount = genCount;
                    return;
                }
            }

            if (clock.getTicks() > deadline) {
                LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms, updateKeyCount]").$();
                return;
            }
            Os.pause();
        }
    }

    protected void ensureGenLookup() {
        if (genCount == 0 || keyCount == 0) {
            return;
        }
        // Gen dir was already snapshotted into genLookup during readIndexMetadataFromBestPage.
        // Only rebuild the lookup index (tier1/tier2) if genCount changed.
        genLookup.buildLookupIfNeeded(valueMem, keyCount, genCount);
    }

    /**
     * Reads header fields and gen dir entries from the best (highest valid sequence)
     * metadata page. Validates the read with seq_start/seq_end -- if the page was
     * mid-write, falls back to the other page. Snapshots gen dir into genLookup
     * so no further reads from the key file pages are needed during cursor iteration.
     */
    private void readIndexMetadataFromBestPage() {
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long memSize = keyMem.size();
            long seqStartA = memSize >= PostingIndexUtils.PAGE_SIZE ? keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START) : 0;
            long seqStartB = memSize >= PostingIndexUtils.KEY_FILE_RESERVED ? keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START) : 0;

            long bestPage = (seqStartB > seqStartA) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;
            long otherPage = (bestPage == PostingIndexUtils.PAGE_A_OFFSET) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;

            for (int attempt = 0; attempt < 2; attempt++) {
                long tryPage = (attempt == 0) ? bestPage : otherPage;

                long seqStart = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                Unsafe.getUnsafe().loadFence();

                long valueMemSize = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_VALUE_MEM_SIZE);
                int keyCount = keyMem.getInt(tryPage + PostingIndexUtils.PAGE_OFFSET_KEY_COUNT);
                int genCount = keyMem.getInt(tryPage + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT);

                Unsafe.getUnsafe().loadFence();
                long seqEnd = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);

                if (seqStart == seqEnd && seqStart > 0
                        && genCount >= 0 && genCount <= PostingIndexUtils.MAX_GEN_COUNT) {
                    genLookup.snapshotMetadata(keyMem, genCount, tryPage);

                    Unsafe.getUnsafe().loadFence();
                    if (keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START) != seqStart) {
                        break; // page overwritten during snapshot, re-pick best page
                    }

                    this.activePageOffset = tryPage;
                    this.keyFileSequence = seqStart;
                    this.valueMemSize = valueMemSize;
                    this.keyCount = keyCount;
                    this.genCount = genCount;
                    this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
                    return;
                }
            }

            if (clock.getTicks() > deadline) {
                LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms]").$();
                break;
            }
            Os.pause();
        }
        this.keyFileSequence = 0;
        this.valueMemSize = 0;
        this.keyCount = 0;
        this.genCount = 0;
    }
}
