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

package io.questdb.cairo.filter;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cairo.filter.SkipFilterUtils.*;


public class SkipFilterWriterImpl implements Mutable, Closeable, SkipFilterWriter {
    private static final Log LOG = LogFactory.getLog(SkipFilterWriterImpl.class);
    private static final long MAX_VALUE_OFFSET = 37;
    private final MemoryMARW bucketMem = Vm.getCMARWInstance();
    private final CairoConfiguration configuration;
    private final @NotNull FilesFacade ff;
    Rnd rnd = new Rnd();
    private int capacity;
    private int mask;
    private int tagMask;
    private int tagSize;


    public SkipFilterWriterImpl(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.rnd = new Rnd();
    }

    public static long getAllocSize(MemoryMAR bucketMem) {
        return HEADER_RESERVED + (bucketMem.getInt(OFFSET_CAPACITY) * ((long) DEFAULT_BUCKET_SIZE * bucketMem.getInt(OFFSET_TAG_SIZE) / 8));
    }

    public static void initBucketMemory(MemoryMAR bucketMem, int targetNumberOfKeys, int tagSize) {
        assert targetNumberOfKeys < Math.pow(2, 30);
        long numberOfBuckets = targetNumberOfKeys / DEFAULT_BUCKET_SIZE;
        double frac = (double) targetNumberOfKeys / (double) numberOfBuckets / (double) DEFAULT_BUCKET_SIZE;
        if (frac > 0.96) {
            numberOfBuckets <<= 1;
        }
        int capacity = (int) numberOfBuckets;
        bucketMem.jumpTo(0);
        bucketMem.truncate();
        bucketMem.putByte(SIGNATURE); // SIGNATURE
        bucketMem.putLong(1); // SEQUENCE
        Unsafe.getUnsafe().storeFence();
        bucketMem.putInt(capacity); // CAPACITY
        bucketMem.putInt(tagSize); // TAG SIZE
        bucketMem.putInt(DEFAULT_BUCKET_SIZE); // BUCKET SIZE
        bucketMem.putInt(-1); // VICTIM INDEX;
        bucketMem.putInt(-1); // VICTIM TAG;
        Unsafe.getUnsafe().storeFence();
        bucketMem.putLong(1); // SEQUENCE CHECK
        assert bucketMem.getAppendOffset() == MAX_VALUE_OFFSET;
        bucketMem.jumpTo(getAllocSize(bucketMem));
        bucketMem.sync(false);
        bucketMem.jumpTo(0);
    }

    @Override
    public void clear() {
        close();
    }

    @Override
    public void close() {
        if (bucketMem.isOpen()) {
            bucketMem.setSize(getAllocSize());
            closeMemory();
        }
        Misc.free(bucketMem);
    }

    public void closeMemory() {
        bucketMem.close(false);
    }

    public void commit() {
        int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            bucketMem.sync(commitMode == CommitMode.ASYNC);
        }
    }

    public long getAllocSize() {
        return HEADER_RESERVED + (bucketMem.getInt(OFFSET_CAPACITY) * ((long) DEFAULT_BUCKET_SIZE * bucketMem.getInt(OFFSET_TAG_SIZE) / 8));
    }

    public int getCapacity() {
        return Unsafe.getUnsafe().getInt(bucketMem.getInt(OFFSET_CAPACITY));
    }

    public void initBucketMemory(int targetNumberOfKeys, int tagSize) {
        initOptions(targetNumberOfKeys, tagSize);
        bucketMem.jumpTo(0);
        bucketMem.truncate();
        bucketMem.putByte(SIGNATURE); // SIGNATURE
        bucketMem.putLong(1); // SEQUENCE
        Unsafe.getUnsafe().storeFence();
        bucketMem.putInt(capacity); // CAPACITY
        bucketMem.putInt(tagSize); // TAG SIZE
        bucketMem.putInt(DEFAULT_BUCKET_SIZE); // BUCKET SIZE
        bucketMem.putInt(EMPTY); // VICTIM INDEX;
        bucketMem.putInt(EMPTY); // VICTIM TAG;
        Unsafe.getUnsafe().storeFence();
        bucketMem.putLong(1); // SEQUENCE CHECK
        assert bucketMem.getAppendOffset() == MAX_VALUE_OFFSET;
        bucketMem.jumpTo(getAllocSize());
        bucketMem.sync(false);
        bucketMem.jumpTo(0);
    }

    public void initOptions(int targetNumberOfKeys, int tagSize) {
        assert targetNumberOfKeys < Math.pow(2, 30);
        long numberOfBuckets = targetNumberOfKeys / DEFAULT_BUCKET_SIZE;
        double frac = (double) targetNumberOfKeys / (double) numberOfBuckets / (double) DEFAULT_BUCKET_SIZE;
        if (frac > 0.96) {
            numberOfBuckets <<= 1;
        }
        this.capacity = (int) numberOfBuckets;
        this.tagSize = tagSize;
        this.tagMask = (1 << tagSize) - 1;
        this.mask = capacity - 1;
        rnd.reset();
    }

    @Override
    public boolean insert(long key) {
        if (bucketMem.getInt(OFFSET_VICTIM_TAG) != EMPTY) {
            return false;
        }
        long hash = Hash.hashLong64(key);
        int index = bucketFromHash(bucketMem, hash >> 32);
        int tag = tagFromHash(bucketMem, hash);
        return insert(index, tag);
    }

    public boolean insert(int index, int tag) {
        int currentIndex = index;
        int currentTag = tag;

        for (int i = 0; i < DEFAULT_MAX_CUCKOO_KICKS; i++) {
            int result = insertToBucket(currentIndex, currentTag, i > 0);
            switch (result) {
                case 0:
                    // all good
                    return true;
                case -1:
                    // could not even kick one out
                    break;
                default:
                    currentTag = result;
            }
            currentIndex = altBucket(bucketMem, currentIndex, currentTag);
        }

        bucketMem.putInt(OFFSET_VICTIM_INDEX, currentIndex);
        bucketMem.putInt(OFFSET_TAG_SIZE, currentTag);
        return false;
    }

    public int insertToBucket(int index, int tag, boolean kickOut) {
        for (int slot = 0; slot < DEFAULT_BUCKET_SIZE; slot++) {
            int value = readTag(bucketMem, index, slot);
            if (value == 0) {
                writeTag(bucketMem, index, slot, tag);
                Unsafe.getUnsafe().storeFence();
                return 0;
            } else if (value == tag) {
                return 0;
            }
        }

        if (kickOut) {
            int victimSlot = rnd.nextInt(DEFAULT_BUCKET_SIZE);
            int victimTag = readTag(bucketMem, index, victimSlot);
            writeTag(bucketMem, index, victimSlot, tag);
            Unsafe.getUnsafe().storeFence();
            return victimTag;
        }

        // we super failed
        return -1;
    }

    @Override
    public boolean isOpen() {
        return bucketMem.isOpen();
    }

    @Override
    public boolean maybeContains(long key) {
        return false; // todo
    }

    @Override
    public boolean maybeContainsHash(long hash) {
        return false; // todo
    }

    @Override
    public void of(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn, long unIndexedNullCount) {
        throw new UnsupportedOperationException();
    }

    public final void of(CairoConfiguration configuration, long bucketFd, boolean init, int filterCapacity) {
        close();
        final FilesFacade ff = configuration.getFilesFacade();
        boolean fFdUnassigned = true;
        final long bucketAppendPageSize = configuration.getDataIndexKeyAppendPageSize(); // todo
        try {
            if (init) {
                if (ff.truncate(bucketFd, 0)) {
                    fFdUnassigned = false;
                    bucketMem.of(ff, bucketFd, false, null, bucketAppendPageSize, bucketAppendPageSize, MemoryTag.MMAP_FILTER_WRITER);
                    initBucketMemory(filterCapacity, 8); // todo: configurable tag size
                } else {
                    throw CairoException.critical(ff.errno()).put("Could not truncate [fd=").put(bucketFd).put(']');
                }
            } else {
                fFdUnassigned = false;
                bucketMem.of(ff, bucketFd, null, ff.length(bucketFd), MemoryTag.MMAP_FILTER_WRITER);
            }

            long bucketMemSize = bucketMem.size();
            // check if key file header is present
            if (bucketMemSize < HEADER_RESERVED) {
                // Don't truncate the file on close.
                bucketMem.close(false);
                LOG.error().$("file too short [corrupt] [fd=").$(bucketFd).$(']').$();
                throw CairoException.critical(0).put("Filter file too short (w): [fd=").put(bucketFd).put(']');
            }

            // verify header signature
            if (bucketMem.getByte(OFFSET_SIGNATURE) != SIGNATURE) {
                LOG.error().$("unknown format [corrupt] [fd=").$(bucketFd).$(']').$();
                throw CairoException.critical(0).put("Unknown format: [fd=").put(bucketFd).put(']');
            }

            // check if sequence is intact
            if (bucketMem.getLong(OFFSET_SEQUENCE) != bucketMem.getLong(OFFSET_SEQUENCE_CHECK)) {
                LOG.error().$("sequence mismatch [corrupt] at [fd=").$(bucketFd).$(']').$();
                throw CairoException.critical(0).put("Sequence mismatch [fd=").put(bucketFd).put(']');
            }

            initOptions(filterCapacity, 8); // todo: configurable tag size;
        } catch (Throwable e) {
            this.close();
            if (fFdUnassigned) {
                ff.close(bucketFd);
            }
            throw e;
        }
    }

    public final void of(Path path, CharSequence name, long columnNameTxn) {
        of(path, name, columnNameTxn, 0);
    }

    public final void of(Path path, CharSequence name, long columnNameTxn, int filterCapacity) {
        close();
        final int plen = path.size();
        try {
            boolean init = filterCapacity > 0;
            LPSZ bucketFile = SkipFilterUtils.bucketFileName(path, name, columnNameTxn);
            if (init) {
                this.bucketMem.of(ff, bucketFile, configuration.getDataIndexKeyAppendPageSize(), 0L, MemoryTag.MMAP_FILTER_WRITER);
                initBucketMemory(filterCapacity, 8); // todo: review
            } else {
                boolean exists = ff.exists(bucketFile);
                if (!exists) {
                    LOG.error().$(path).$(" not found").$();
                    throw CairoException.fileNotFound().put("index does not exist [path=").put(path).put(']');
                }
                this.bucketMem.of(ff, bucketFile, configuration.getDataIndexKeyAppendPageSize(), ff.length(bucketFile), MemoryTag.MMAP_FILTER_WRITER); // todo: review
            }

            long bucketMemSize = bucketMem.getAppendOffset();
            // check if key file header is present
            if (bucketMemSize < HEADER_RESERVED) {
                // Don't truncate the file on close.
                bucketMem.close(false);
                LOG.error().$("file too short [corrupt] [path=").$(path).$(']').$();
                throw CairoException.critical(0).put("Filter file too short (w): [path=").put(path).put(']');
            }

            // verify header signature
            if (bucketMem.getByte(OFFSET_SIGNATURE) != SIGNATURE) {
                LOG.error().$("unknown format [corrupt] [path=").$(path).$(']').$();
                throw CairoException.critical(0).put("Unknown format: [path=").put(path).put(']');
            }

            // check if sequence is intact
            if (bucketMem.getLong(OFFSET_SEQUENCE) != bucketMem.getLong(OFFSET_SEQUENCE_CHECK)) {
                LOG.error().$("sequence mismatch [corrupt] at [path=").$(path).$(']').$();
                throw CairoException.critical(0).put("Sequence mismatch [path=").put(path).put(']');
            }
        } catch (Throwable e) {
            this.close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    public void sync(boolean async) {
        bucketMem.sync(async);
    }
}
