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

// 2-4 cuckoo filter

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.Hash;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.cairo.filter.SkipFilterUtils.*;

// Flyweight representing a cuckoo filter, defaulting to 2 hash functions, 4 bits fingerprint
// Layout:
// 1 byte capacity - number of buckets in power of 2
// 1 byte number of bits per entry
//
// 4 bytes capacity - number of buckets
// 4 bytes size
//

/**
 * Specialized flyweight probabilistic filter used to help skip-scan partitions.
 * Buffer layout is the following:
 * <pre>
 * | capacity (number of buckets)   | f (tag size) | b (bucket size)  | victim_index | victim_tag  | padding | byte array |
 * +--------------------+------------------------------+------------------+------------+------------+
 * |       4 bytes                  |     4 bytes                  |      4 bytes     | 4 bytes      |     4 bytes | 4 bytes |    -       |
 * +--------------------+------------------------------+------------------+------------+------------+
 * </pre>
 */
// start with 2,4 and 4 bit keys
public class CuckooFilter implements Closeable {
    private static final long HEADER_SIZE = 24;
    private static final long OFFSET_CAPACITY = 0;
    private static final long OFFSET_TAG_SIZE = OFFSET_CAPACITY + 4;
    private static final long OFFSET_BUCKET_SIZE = OFFSET_TAG_SIZE + 4;
    private static final long OFFSET_VICTIM_INDEX = OFFSET_BUCKET_SIZE + 4;
    private static final long OFFSET_VICTIM_TAG = OFFSET_VICTIM_INDEX + 4;
    private final int capacity;
    private final int tagMask;
    private final int tagSize;
    Rnd rnd = new Rnd();
    private long mask;
    private long ptr;


    public CuckooFilter(long targetNumberOfKeys) {
        assert targetNumberOfKeys < Math.pow(2, 30);
        long numberOfBuckets = targetNumberOfKeys / DEFAULT_BUCKET_SIZE;
        double frac = (double) targetNumberOfKeys / (double) numberOfBuckets / (double) DEFAULT_BUCKET_SIZE;
        if (frac > 0.96) {
            numberOfBuckets <<= 1;
        }
        this.capacity = (int) numberOfBuckets;
        this.tagSize = DEFAULT_TAG_SIZE;
        this.tagMask = (1 << DEFAULT_TAG_SIZE) - 1;
        rnd.reset();
    }

    public CuckooFilter(long targetNumberOfKeys, int tagSize) {
        assert targetNumberOfKeys < Math.pow(2, 30);
        long numberOfBuckets = targetNumberOfKeys / DEFAULT_BUCKET_SIZE;
        double frac = (double) targetNumberOfKeys / (double) numberOfBuckets / (double) DEFAULT_BUCKET_SIZE;
        if (frac > 0.96) {
            numberOfBuckets <<= 1;
        }
        this.capacity = (int) numberOfBuckets;
        this.tagSize = tagSize;
        this.tagMask = (1 << tagSize) - 1;
        rnd.reset();
    }

    public int altBucket(int index, int tag) {
        return bucketFromHash(index ^ (tag * 0x5bd1e995L));
    }

    public void clear() {
        zero(ptr);
        setVictimIndex(0);
        setVictimTag(0);
    }

    public void close() throws IOException {

    }

    public long getAllocSize() {
        return HEADER_SIZE + (getCapacity() * ((long) DEFAULT_BUCKET_SIZE * getTagSize() / 8));
    }

    public long getAllocSizeFirst() {
        return HEADER_SIZE + (capacity * ((long) DEFAULT_BUCKET_SIZE * tagSize / 8));
    }

    public double getFillFactor() {
        long count = 0;

        for (int i = 0, n = getCapacity(); i < n; i++) {
            for (int j = 0; j < DEFAULT_BUCKET_SIZE; j++) {
                if (readTag(i, j) == 0) {
                    count++;
                }
            }
        }

        return (double) ((capacity * DEFAULT_BUCKET_SIZE) - count) / (capacity * DEFAULT_BUCKET_SIZE);
    }

    public int getOffset(int index) {
        return (int) (HEADER_SIZE + index * (DEFAULT_BUCKET_SIZE * getTagSize() / 8));
    }

    public boolean insert(long key) {
        if (getVictimTag() != 0) {
            return false;
        }

        long hash = hash(key);
        int index = bucketFromHash(hash >> 32);
        int tag = tagFromHash(hash);
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
            currentIndex = altBucket(currentIndex, currentTag);
        }

        setVictimTag(currentIndex);
        setVictimTag(currentTag);
        return false;
    }

    public int insertToBucket(int index, int tag, boolean kickOut) {
        for (int slot = 0; slot < DEFAULT_BUCKET_SIZE; slot++) {
            int value = readTag(index, slot);
            if (value == 0) {
                writeTag(index, slot, tag);
                return 0;
            } else if (value == tag) {
                return 0;
            }
        }

        if (kickOut) {
            int victimSlot = rnd.nextInt(DEFAULT_BUCKET_SIZE);
            int victimTag = readTag(index, victimSlot);
            writeTag(index, victimSlot, tag);
            return victimTag;
        }

        // we super failed
        return -1;
    }

    public boolean maybeContains(long key) {
        return maybeContainsHash(hash(key));
    }

    public boolean maybeContainsHash(long hash) {
        int i1 = bucketFromHash(hash >> 32);
        int tag = tagFromHash(hash);
        int i2 = altBucket(i1, tag);

        assert i1 == altBucket(i2, tag);
        boolean found = (getVictimTag() != 0 && tag == getVictimTag()
                && (i1 == getVictimIndex() || i2 == getVictimIndex()));
        return found || isTagInBuckets(i1, i2, tag);
    }

    public void of(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn, long unIndexedNullCount) {
        throw new UnsupportedOperationException();
    }

    public CuckooFilter of(long ptr, GroupByAllocator allocator) {
        if (ptr == 0) {
            this.ptr = allocator.malloc(getAllocSizeFirst());
            setCapacity(capacity);
            setTagSize(tagSize);
            setBucketSize(DEFAULT_BUCKET_SIZE);
            setVictimIndex(0);
            setVictimTag(0);
            zero(this.ptr);
            mask = capacity - 1;
        } else {
            this.ptr = ptr;
            mask = getCapacity() - 1;
        }
        return this;
    }

    private void zero(long ptr) {
        // Vectorized fast path for zero default value.
        Vect.memset(ptr + HEADER_SIZE, getAllocSize(), 0);
    }

    // 2b/2f ≤ E
    // E = epsilon
    // Therefore, minimal fingerprint size to achieve epsilon is
    // f >= log2(1/E) + log2(2b) bits
    // 2b/2f <= E
    static boolean validateParameters(int f, int b, double E) {
        return (2.0 * b) / (2.0 * f) >= E;
    }

    int bucketFromHash(long hash) {
        return (int) (hash & mask);
    }

    int findTagInBucket(int index, int tag) {
        for (int slot = 0; slot < DEFAULT_BUCKET_SIZE; slot++) {
            int value = readTag(index, slot);
            if (value == tag) {
                return slot;
            }
        }
        return -1;
    }

    int getBucketSize() {
        return Unsafe.getUnsafe().getInt(this.ptr + OFFSET_BUCKET_SIZE);
    }

    int getCapacity() {
        return Unsafe.getUnsafe().getInt(this.ptr + OFFSET_CAPACITY);
    }

    int getTagSize() {
        return Unsafe.getUnsafe().getInt(this.ptr + OFFSET_TAG_SIZE);
    }

    int getVictimIndex() {
        return Unsafe.getUnsafe().getInt(this.ptr + OFFSET_VICTIM_INDEX);
    }

    int getVictimTag() {
        return Unsafe.getUnsafe().getInt(this.ptr + OFFSET_VICTIM_TAG);
    }

    long hash(long key) {
        return Hash.hashLong64(key);
    }

    boolean isTagInBucket(int index, int tag) {
        return findTagInBucket(index, tag) >= 0;
    }

    boolean isTagInBuckets(int i1, int i2, int tag) {
        return isTagInBucket(i1, tag) | isTagInBucket(i2, tag);
    }

    int readTag(int index, int slot) {
        long offset = getOffset(index);
        int tag;
        switch (getTagSize()) {
            case 2:
                tag = Unsafe.getUnsafe().getByte(ptr + offset) >> (slot * 2);
                break;
            case 4:
                offset += (slot >> 1);
                tag = Unsafe.getUnsafe().getByte(ptr + offset) >> ((slot & 1) << 2);
                break;
            case 8:
                offset += slot;
                tag = Unsafe.getUnsafe().getByte(ptr + offset);
                break;
            case 12:
                offset += (slot + (slot >> 1));
                tag = Unsafe.getUnsafe().getShort(ptr + offset) >> ((slot & 1) << 2);
                break;
            case 16:
                offset += ((long) slot << 1);
                tag = Unsafe.getUnsafe().getShort(ptr + offset);
                break;
            case 32:
                tag = Unsafe.getUnsafe().getInt(ptr + offset);
                break;
            default:
                throw CairoException.nonCritical().put("invalid tag size for cuckoo filter: " + tagSize);
        }

        return tag & tagMask;
    }

    void setBucketSize(int bucketSize) {
        Unsafe.getUnsafe().putInt(this.ptr + OFFSET_BUCKET_SIZE, bucketSize);
    }

    void setCapacity(int capacity) {
        Unsafe.getUnsafe().putInt(this.ptr + OFFSET_CAPACITY, capacity);
    }

    void setTagSize(int tagSize) {
        Unsafe.getUnsafe().putInt(this.ptr + OFFSET_TAG_SIZE, tagSize);
    }

    void setVictimIndex(int victimIndex) {
        Unsafe.getUnsafe().putInt(this.ptr + OFFSET_VICTIM_INDEX, victimIndex);
    }

    void setVictimTag(int victimTag) {
        Unsafe.getUnsafe().putInt(this.ptr + OFFSET_VICTIM_TAG, victimTag);
    }

    int tagFromHash(long hash) {
        final int tag = (int) (hash & ((1L << getTagSize()) - 1));
        return tag == 0 ? tag + 1 : tag;
    }

    void writeTag(int index, int slot, int tag) {
        long offset = getOffset(index);
        int _tag = tag & tagMask;
        int b;
        switch (getTagSize()) {
            case 2:
                b = Unsafe.getUnsafe().getByte(ptr + offset);
                b |= (byte) (_tag << (2 * slot));
                Unsafe.getUnsafe().putByte(ptr + offset, (byte) b);
                break;
            case 4:
                offset += (slot >> 1);
                b = Unsafe.getUnsafe().getByte(ptr + offset);
                if ((slot & 1) == 0) {
                    b &= 0xF0;
                    b |= _tag;
                } else {
                    b &= 0x0F;
                    b |= (_tag << 4);
                }
                Unsafe.getUnsafe().putByte(ptr + offset, (byte) b);
                break;
            case 8:
                offset += slot;
                Unsafe.getUnsafe().putByte(ptr + offset, (byte) _tag);
                break;
            case 12:
                offset += (slot + (slot >> 1));
                b = Unsafe.getUnsafe().getShort(ptr + offset);
                if ((slot & 1) == 0) {
                    b &= 0xF000;
                    b |= _tag;
                } else {
                    b &= 0x000F;
                    b |= (_tag << 4);
                }
                Unsafe.getUnsafe().putShort(ptr + offset, (short) b);
                break;
            case 16:
                offset += slot;
                Unsafe.getUnsafe().putShort(ptr + offset, (short) _tag);
                break;
            case 32:
                Unsafe.getUnsafe().putInt(ptr + offset, _tag);
                break;
            default:
                throw CairoException.nonCritical().put("invalid tag size for cuckoo filter: " + tagSize);
        }
    }


}

