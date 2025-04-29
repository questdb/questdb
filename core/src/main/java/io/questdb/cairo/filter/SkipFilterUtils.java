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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public class SkipFilterUtils {
    public static final int DEFAULT_BUCKET_SIZE = 4;
    public static final int DEFAULT_FILTER_CAPACITY = 4096;
    public static final int DEFAULT_MAX_CUCKOO_KICKS = 500;
    public static final int DEFAULT_TAG_SIZE = 8;
    public static final int EMPTY = -1;
    public static final int HEADER_RESERVED = 64;
    public static final long OFFSET_SIGNATURE = 0;
    public static final long OFFSET_SEQUENCE = OFFSET_SIGNATURE + 1;
    public static final long OFFSET_CAPACITY = OFFSET_SEQUENCE + 8;
    public static final long OFFSET_TAG_SIZE = OFFSET_CAPACITY + 4;
    public static final long OFFSET_BUCKET_SIZE = OFFSET_TAG_SIZE + 4;
    public static final long OFFSET_VICTIM_INDEX = OFFSET_BUCKET_SIZE + 4;
    public static final long OFFSET_VICTIM_TAG = OFFSET_VICTIM_INDEX + 4;
    public static final long OFFSET_SEQUENCE_CHECK = OFFSET_VICTIM_TAG + 4;
    public static byte SIGNATURE = (byte) 0xCF;

    public static int altBucket(MemoryR bucketMem, int index, int tag) {
        return bucketFromHash(bucketMem, index ^ (tag * 0x5bd1e995L));
    }

    public static LPSZ bucketFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".f");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    public static int bucketFromHash(MemoryR bucketMem, long hash) {
        return (int) (hash & bucketMem.getInt(OFFSET_CAPACITY) - 1);
    }

    public static long getOffset(MemoryR bucketMem, int index) {
        return (HEADER_RESERVED + index * ((long) DEFAULT_BUCKET_SIZE * bucketMem.getInt(OFFSET_TAG_SIZE) / 8));
    }

    public static int getTagMask(MemoryR bucketMem) {
        return (1 << bucketMem.getInt(OFFSET_TAG_SIZE)) - 1;
    }

    public static int readTag(MemoryR bucketMem, int index, int slot) {
        long offset = getOffset(bucketMem, index);
        int tag;
        switch (bucketMem.getInt(OFFSET_TAG_SIZE)) {
            case 2:
                tag = bucketMem.getByte(offset) >> (slot * 2);
                break;
            case 4:
                offset += (slot >> 1);
                tag = bucketMem.getByte(offset) >> ((slot & 1) << 2);
                break;
            case 8:
                offset += slot;
                tag = bucketMem.getByte(offset);
                break;
            case 12:
                offset += (slot + (slot >> 1));
                tag = bucketMem.getShort(offset) >> ((slot & 1) << 2);
                break;
            case 16:
                offset += ((long) slot << 1);
                tag = bucketMem.getShort(offset);
                break;
            case 32:
                tag = bucketMem.getInt(offset);
                break;
            default:
                throw CairoException.nonCritical().put("invalid tag size for cuckoo filter: " + bucketMem.getInt(OFFSET_TAG_SIZE));
        }

        return tag & getTagMask(bucketMem);
    }

    public static int tagFromHash(MemoryR bucketMem, long hash) {
        final int tag = (int) (hash & ((1L << bucketMem.getInt(OFFSET_TAG_SIZE)) - 1));
        return tag == 0 ? tag + 1 : tag;
    }

    public static void writeTag(MemoryARW bucketMem, int index, int slot, int tag) {
        long offset = getOffset(bucketMem, index);
        int _tag = tag & getTagMask(bucketMem);
        int b;
        switch (bucketMem.getInt(OFFSET_TAG_SIZE)) {
            case 2:
                b = bucketMem.getByte(offset);
                b |= (byte) (_tag << (2 * slot));
                bucketMem.putByte(offset, (byte) b);
                break;
            case 4:
                offset += (slot >> 1);
                b = bucketMem.getByte(offset);
                if ((slot & 1) == 0) {
                    b &= 0xF0;
                    b |= _tag;
                } else {
                    b &= 0x0F;
                    b |= (_tag << 4);
                }
                bucketMem.putByte(offset, (byte) b);
                break;
            case 8:
                offset += slot;
                bucketMem.putByte(offset, (byte) _tag);
                break;
            case 12:
                offset += (slot + (slot >> 1));
                b = bucketMem.getShort(offset);
                if ((slot & 1) == 0) {
                    b &= 0xF000;
                    b |= _tag;
                } else {
                    b &= 0x000F;
                    b |= (_tag << 4);
                }
                bucketMem.putShort(offset, (short) b);
                break;
            case 16:
                offset += slot;
                bucketMem.putShort(offset, (short) _tag);
                break;
            case 32:
                bucketMem.putInt(offset, _tag);
                break;
            default:
                throw CairoException.nonCritical().put("invalid tag size for cuckoo filter: " + bucketMem.getInt(OFFSET_TAG_SIZE));
        }
    }
}


