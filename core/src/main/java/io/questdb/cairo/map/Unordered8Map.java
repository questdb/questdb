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

package io.questdb.cairo.map;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Hash;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.bytes.Bytes;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

/**
 * Unordered8Map is a general purpose off-heap hash table with 8 bytes keys (LONG, TIMESTAMP, DATE) used
 * to store intermediate data of group by, sample by queries. It provides {@link MapKey} and
 * {@link MapValue}, as well as {@link RecordCursor} interfaces for data access and modification.
 * The preferred way to create an Unordered8Map is {@link MapFactory}.
 * <p>
 * Map iteration provided by {@link RecordCursor} does not preserve the key insertion order, hence
 * the unordered map name.
 * <strong>Important!</strong>
 * Key and value structures must match the ones provided via lists of columns ({@link ColumnTypes})
 * to the map constructor. Later put* calls made on {@link MapKey} and {@link MapValue} must match
 * the declared column types to guarantee memory access safety.
 * <p>
 * Keys must be fixed-size and up to 8 bytes total. Only insertions and updates operations are
 * supported meaning that a key can't be removed from the map once it was inserted.
 * <p>
 * The hash table is organized into the following parts:
 * <ul>
 * <li>1. Off-heap memory for writing new keys</li>
 * <li>2. Off-heap memory for zero key and value</li>
 * <li>3. Off-heap memory for key-value pairs, i.e. the hash table with open addressing</li>
 * </ul>
 * The hash table uses linear probing.
 * <p>
 * Key-value pairs stored in the hash table may have the following layout:
 * <pre>
 * |   Key columns 0..K   | Optional padding | Value columns 0..V |
 * +----------------------+------------------+--------------------+
 * |    up to 8 bytes     |        -         |         -          |
 * +----------------------+------------------+--------------------+
 * </pre>
 */
public class Unordered8Map implements Map, Reopenable {
    static final long KEY_SIZE = Long.BYTES;
    private static final long MAX_SAFE_INT_POW_2 = 1L << 31;
    private static final int MIN_KEY_CAPACITY = 16;

    private final Unordered8MapCursor cursor;
    private final long entrySize;
    private final Key key;
    private final double loadFactor;
    private final int maxResizes;
    private final int memoryTag;
    private final Unordered8MapRecord record;
    private final Unordered8MapValue value;
    private final Unordered8MapValue value2;
    private final Unordered8MapValue value3;
    private int free;
    private boolean hasZero;
    private int initialKeyCapacity;
    private int keyCapacity;
    private long mask;
    private long memLimit; // Hash table memory limit pointer.
    private long memStart; // Hash table memory start pointer.
    private int nResizes;
    private int size = 0;
    private long zeroMemStart; // Zero key-value pair memory start pointer.

    public Unordered8Map(
            int keyType,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes
    ) {
        this(keyType, valueTypes, keyCapacity, loadFactor, maxResizes, MemoryTag.NATIVE_UNORDERED_MAP);
    }

    Unordered8Map(
            int keyType,
            @Nullable @Transient ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int memoryTag
    ) {
        assert loadFactor > 0 && loadFactor < 1d;

        try {
            this.memoryTag = memoryTag;
            this.loadFactor = loadFactor;
            this.keyCapacity = (int) (keyCapacity / loadFactor);
            this.keyCapacity = this.initialKeyCapacity = Math.max(Numbers.ceilPow2(this.keyCapacity), MIN_KEY_CAPACITY);
            this.maxResizes = maxResizes;
            mask = this.keyCapacity - 1;
            free = (int) (this.keyCapacity * loadFactor);
            nResizes = 0;

            if (!isSupportedKeyType(keyType)) {
                throw CairoException.nonCritical().put("unexpected key type: ").put(keyType);
            }

            long valueOffset = 0;
            long[] valueOffsets = null;
            long valueSize = 0;
            if (valueTypes != null) {
                int valueColumnCount = valueTypes.getColumnCount();
                valueOffsets = new long[valueColumnCount];

                for (int i = 0; i < valueColumnCount; i++) {
                    valueOffsets[i] = valueOffset;
                    final int columnType = valueTypes.getColumnType(i);
                    final int size = ColumnType.sizeOf(columnType);
                    if (size <= 0) {
                        throw CairoException.nonCritical().put("value type is not supported: ").put(ColumnType.nameOf(columnType));
                    }
                    valueOffset += size;
                    valueSize += size;
                }
            }

            this.entrySize = Bytes.align8b(KEY_SIZE + valueSize);

            final long sizeBytes = entrySize * this.keyCapacity;
            memStart = Unsafe.malloc(sizeBytes, memoryTag);
            Vect.memset(memStart, sizeBytes, 0);
            memLimit = memStart + sizeBytes;
            zeroMemStart = Unsafe.malloc(entrySize, memoryTag);
            Vect.memset(zeroMemStart, entrySize, 0);

            value = new Unordered8MapValue(valueSize, valueOffsets);
            value2 = new Unordered8MapValue(valueSize, valueOffsets);
            value3 = new Unordered8MapValue(valueSize, valueOffsets);

            record = new Unordered8MapRecord(valueSize, valueOffsets, value, valueTypes);
            cursor = new Unordered8MapCursor(record, this);
            key = new Key();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public static boolean isSupportedKeyType(int columnType) {
        return columnType == ColumnType.LONG || columnType == ColumnType.TIMESTAMP || columnType == ColumnType.DATE;
    }

    @Override
    public void clear() {
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        nResizes = 0;
        hasZero = false;
        Vect.memset(memStart, memLimit - memStart, 0);
        Vect.memset(zeroMemStart, entrySize, 0);
    }

    @Override
    public void close() {
        if (memStart != 0) {
            memLimit = memStart = Unsafe.free(memStart, memLimit - memStart, memoryTag);
            zeroMemStart = Unsafe.free(zeroMemStart, entrySize, memoryTag);
            free = 0;
            size = 0;
            hasZero = false;
        }
    }

    @Override
    public MapRecordCursor getCursor() {
        if (hasZero) {
            return cursor.init(memStart, memLimit, zeroMemStart, size + 1);
        }
        return cursor.init(memStart, memLimit, 0, size);
    }

    @Override
    public int getKeyCapacity() {
        return keyCapacity;
    }

    @Override
    public MapRecord getRecord() {
        return record;
    }

    @Override
    public boolean isOpen() {
        return memStart != 0;
    }

    @Override
    public void merge(Map srcMap, MapValueMergeFunction mergeFunc) {
        assert this != srcMap;
        long srcSize = srcMap.size();
        if (srcSize == 0) {
            return;
        }
        Unordered8Map src8Map = (Unordered8Map) srcMap;

        // First, we handle zero key.
        if (src8Map.hasZero) {
            if (hasZero) {
                mergeFunc.merge(
                        valueAt(zeroMemStart),
                        src8Map.valueAt(src8Map.zeroMemStart)
                );
            } else {
                Vect.memcpy(zeroMemStart, src8Map.zeroMemStart, entrySize);
                hasZero = true;
            }
            // Check if zero was the only element in the source map.
            if (srcSize == 1) {
                return;
            }
        }

        // Pre-grow to avoid rehash during merge.
        long srcNonZeroCount = srcSize - (src8Map.hasZero ? 1 : 0);
        if (size + srcNonZeroCount > keyCapacity * loadFactor) {
            setKeyCapacity((int) (size + srcNonZeroCount));
        }

        final long srcStart = src8Map.memStart;
        final long srcLimit = src8Map.memLimit;
        final long es = entrySize;

        final sun.misc.Unsafe u = Unsafe.getUnsafe();
        final long dstMem = memStart;
        final long dstLim = memLimit;

        // Pipeline state: 8 entries worth of keys, src/dst addresses, and pre-loaded probe values.
        long key0, key1, key2, key3, key4, key5, key6, key7;
        long src0, src1, src2, src3, src4, src5, src6, src7;
        long dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7;
        long p0, p1, p2, p3, p4, p5, p6, p7;  // Pre-loaded first-probe values

        // Prime the pipeline.
        src0 = srcStart;
        src1 = srcStart + es;
        src2 = srcStart + 2 * es;
        src3 = srcStart + 3 * es;
        src4 = srcStart + 4 * es;
        src5 = srcStart + 5 * es;
        src6 = srcStart + 6 * es;
        src7 = srcStart + 7 * es;

        key0 = src0 < srcLimit ? u.getLong(src0) : 0;
        key1 = src1 < srcLimit ? u.getLong(src1) : 0;
        key2 = src2 < srcLimit ? u.getLong(src2) : 0;
        key3 = src3 < srcLimit ? u.getLong(src3) : 0;
        key4 = src4 < srcLimit ? u.getLong(src4) : 0;
        key5 = src5 < srcLimit ? u.getLong(src5) : 0;
        key6 = src6 < srcLimit ? u.getLong(src6) : 0;
        key7 = src7 < srcLimit ? u.getLong(src7) : 0;

        dst0 = key0 != 0 ? dstMem + (Hash.hashLong64(key0) & mask) * es : 0;
        dst1 = key1 != 0 ? dstMem + (Hash.hashLong64(key1) & mask) * es : 0;
        dst2 = key2 != 0 ? dstMem + (Hash.hashLong64(key2) & mask) * es : 0;
        dst3 = key3 != 0 ? dstMem + (Hash.hashLong64(key3) & mask) * es : 0;
        dst4 = key4 != 0 ? dstMem + (Hash.hashLong64(key4) & mask) * es : 0;
        dst5 = key5 != 0 ? dstMem + (Hash.hashLong64(key5) & mask) * es : 0;
        dst6 = key6 != 0 ? dstMem + (Hash.hashLong64(key6) & mask) * es : 0;
        dst7 = key7 != 0 ? dstMem + (Hash.hashLong64(key7) & mask) * es : 0;

        // Pre-load first-probe values (these are used in the first iteration).
        p0 = dst0 != 0 ? u.getLong(dst0) : 0;
        p1 = dst1 != 0 ? u.getLong(dst1) : 0;
        p2 = dst2 != 0 ? u.getLong(dst2) : 0;
        p3 = dst3 != 0 ? u.getLong(dst3) : 0;
        p4 = dst4 != 0 ? u.getLong(dst4) : 0;
        p5 = dst5 != 0 ? u.getLong(dst5) : 0;
        p6 = dst6 != 0 ? u.getLong(dst6) : 0;
        p7 = dst7 != 0 ? u.getLong(dst7) : 0;

        // Main loop - process 8 entries per iteration with software pipelining.
        for (long srcAddr = srcStart; srcAddr < srcLimit; srcAddr += 8 * es) {

            // === PHASE 1: Load NEXT batch source keys (8 parallel loads) ===
            long aheadBase = srcAddr + 8 * es;
            long nk0 = 0, nk1 = 0, nk2 = 0, nk3 = 0, nk4 = 0, nk5 = 0, nk6 = 0, nk7 = 0;
            if (aheadBase < srcLimit) {
                nk0 = u.getLong(aheadBase);
                nk1 = aheadBase + es < srcLimit ? u.getLong(aheadBase + es) : 0;
                nk2 = aheadBase + 2 * es < srcLimit ? u.getLong(aheadBase + 2 * es) : 0;
                nk3 = aheadBase + 3 * es < srcLimit ? u.getLong(aheadBase + 3 * es) : 0;
                nk4 = aheadBase + 4 * es < srcLimit ? u.getLong(aheadBase + 4 * es) : 0;
                nk5 = aheadBase + 5 * es < srcLimit ? u.getLong(aheadBase + 5 * es) : 0;
                nk6 = aheadBase + 6 * es < srcLimit ? u.getLong(aheadBase + 6 * es) : 0;
                nk7 = aheadBase + 7 * es < srcLimit ? u.getLong(aheadBase + 7 * es) : 0;
            }

            // === PHASE 2: Compute NEXT batch hashes (ALU work overlaps with PHASE 1 latency) ===
            long nh0 = Hash.hashLong64(nk0);
            long nh1 = Hash.hashLong64(nk1);
            long nh2 = Hash.hashLong64(nk2);
            long nh3 = Hash.hashLong64(nk3);
            long nh4 = Hash.hashLong64(nk4);
            long nh5 = Hash.hashLong64(nk5);
            long nh6 = Hash.hashLong64(nk6);
            long nh7 = Hash.hashLong64(nk7);

            // === PHASE 3: Compute NEXT batch dest addresses ===
            long nd0 = nk0 != 0 ? dstMem + (nh0 & mask) * es : 0;
            long nd1 = nk1 != 0 ? dstMem + (nh1 & mask) * es : 0;
            long nd2 = nk2 != 0 ? dstMem + (nh2 & mask) * es : 0;
            long nd3 = nk3 != 0 ? dstMem + (nh3 & mask) * es : 0;
            long nd4 = nk4 != 0 ? dstMem + (nh4 & mask) * es : 0;
            long nd5 = nk5 != 0 ? dstMem + (nh5 & mask) * es : 0;
            long nd6 = nk6 != 0 ? dstMem + (nh6 & mask) * es : 0;
            long nd7 = nk7 != 0 ? dstMem + (nh7 & mask) * es : 0;

            // === PHASE 4: Load NEXT batch first-probe values (stored for next iteration) ===
            // These loads are NOT dead code - results are saved and used in the next iteration.
            long np0 = nd0 != 0 ? u.getLong(nd0) : 0;
            long np1 = nd1 != 0 ? u.getLong(nd1) : 0;
            long np2 = nd2 != 0 ? u.getLong(nd2) : 0;
            long np3 = nd3 != 0 ? u.getLong(nd3) : 0;
            long np4 = nd4 != 0 ? u.getLong(nd4) : 0;
            long np5 = nd5 != 0 ? u.getLong(nd5) : 0;
            long np6 = nd6 != 0 ? u.getLong(nd6) : 0;
            long np7 = nd7 != 0 ? u.getLong(nd7) : 0;

            // === PHASE 5: Process CURRENT batch using pre-loaded p0-p7 (zero load latency!) ===

            // Process results. Most resolve on first probe with 0.5 load factor.
            // Use Unsafe.copyMemory (JVM intrinsic, no JNI).
            if (key0 != 0) {
                if (p0 == 0) {
                    u.copyMemory(src0, dst0, es);
                    size++;
                } else if (p0 == key0) {
                    mergeFunc.merge(valueAt(dst0), src8Map.valueAt(src0));
                } else {
                    long d = dst0 + es;
                    if (d >= dstLim) d = dstMem;
                    for (; ; ) {
                        long k = u.getLong(d);
                        if (k == 0) {
                            u.copyMemory(src0, d, es);
                            size++;
                            break;
                        } else if (k == key0) {
                            mergeFunc.merge(valueAt(d), src8Map.valueAt(src0));
                            break;
                        }
                        d += es;
                        if (d >= dstLim) d = dstMem;
                    }
                }
            }

            if (key1 != 0) {
                if (p1 == 0) {
                    u.copyMemory(src1, dst1, es);
                    size++;
                } else if (p1 == key1) {
                    mergeFunc.merge(valueAt(dst1), src8Map.valueAt(src1));
                } else {
                    long d = dst1 + es;
                    if (d >= dstLim) d = dstMem;
                    for (; ; ) {
                        long k = u.getLong(d);
                        if (k == 0) {
                            u.copyMemory(src1, d, es);
                            size++;
                            break;
                        } else if (k == key1) {
                            mergeFunc.merge(valueAt(d), src8Map.valueAt(src1));
                            break;
                        }
                        d += es;
                        if (d >= dstLim) d = dstMem;
                    }
                }
            }

            if (key2 != 0) {
                if (p2 == 0) {
                    u.copyMemory(src2, dst2, es);
                    size++;
                } else if (p2 == key2) {
                    mergeFunc.merge(valueAt(dst2), src8Map.valueAt(src2));
                } else {
                    long d = dst2 + es;
                    if (d >= dstLim) d = dstMem;
                    for (; ; ) {
                        long k = u.getLong(d);
                        if (k == 0) {
                            u.copyMemory(src2, d, es);
                            size++;
                            break;
                        } else if (k == key2) {
                            mergeFunc.merge(valueAt(d), src8Map.valueAt(src2));
                            break;
                        }
                        d += es;
                        if (d >= dstLim) d = dstMem;
                    }
                }
            }

            if (key3 != 0) {
                if (p3 == 0) {
                    u.copyMemory(src3, dst3, es);
                    size++;
                } else if (p3 == key3) {
                    mergeFunc.merge(valueAt(dst3), src8Map.valueAt(src3));
                } else {
                    long d = dst3 + es;
                    if (d >= dstLim) d = dstMem;
                    for (; ; ) {
                        long k = u.getLong(d);
                        if (k == 0) {
                            u.copyMemory(src3, d, es);
                            size++;
                            break;
                        } else if (k == key3) {
                            mergeFunc.merge(valueAt(d), src8Map.valueAt(src3));
                            break;
                        }
                        d += es;
                        if (d >= dstLim) d = dstMem;
                    }
                }
            }

            if (key4 != 0) {
                if (p4 == 0) {
                    u.copyMemory(src4, dst4, es);
                    size++;
                } else if (p4 == key4) {
                    mergeFunc.merge(valueAt(dst4), src8Map.valueAt(src4));
                } else {
                    long d = dst4 + es;
                    if (d >= dstLim) d = dstMem;
                    for (; ; ) {
                        long k = u.getLong(d);
                        if (k == 0) {
                            u.copyMemory(src4, d, es);
                            size++;
                            break;
                        } else if (k == key4) {
                            mergeFunc.merge(valueAt(d), src8Map.valueAt(src4));
                            break;
                        }
                        d += es;
                        if (d >= dstLim) d = dstMem;
                    }
                }
            }

            if (key5 != 0) {
                if (p5 == 0) {
                    u.copyMemory(src5, dst5, es);
                    size++;
                } else if (p5 == key5) {
                    mergeFunc.merge(valueAt(dst5), src8Map.valueAt(src5));
                } else {
                    long d = dst5 + es;
                    if (d >= dstLim) d = dstMem;
                    for (; ; ) {
                        long k = u.getLong(d);
                        if (k == 0) {
                            u.copyMemory(src5, d, es);
                            size++;
                            break;
                        } else if (k == key5) {
                            mergeFunc.merge(valueAt(d), src8Map.valueAt(src5));
                            break;
                        }
                        d += es;
                        if (d >= dstLim) d = dstMem;
                    }
                }
            }

            if (key6 != 0) {
                if (p6 == 0) {
                    u.copyMemory(src6, dst6, es);
                    size++;
                } else if (p6 == key6) {
                    mergeFunc.merge(valueAt(dst6), src8Map.valueAt(src6));
                } else {
                    long d = dst6 + es;
                    if (d >= dstLim) d = dstMem;
                    for (; ; ) {
                        long k = u.getLong(d);
                        if (k == 0) {
                            u.copyMemory(src6, d, es);
                            size++;
                            break;
                        } else if (k == key6) {
                            mergeFunc.merge(valueAt(d), src8Map.valueAt(src6));
                            break;
                        }
                        d += es;
                        if (d >= dstLim) d = dstMem;
                    }
                }
            }

            if (key7 != 0) {
                if (p7 == 0) {
                    u.copyMemory(src7, dst7, es);
                    size++;
                } else if (p7 == key7) {
                    mergeFunc.merge(valueAt(dst7), src8Map.valueAt(src7));
                } else {
                    long d = dst7 + es;
                    if (d >= dstLim) d = dstMem;
                    for (; ; ) {
                        long k = u.getLong(d);
                        if (k == 0) {
                            u.copyMemory(src7, d, es);
                            size++;
                            break;
                        } else if (k == key7) {
                            mergeFunc.merge(valueAt(d), src8Map.valueAt(src7));
                            break;
                        }
                        d += es;
                        if (d >= dstLim) d = dstMem;
                    }
                }
            }

            // === PHASE 6: Rotate prefetched data to current slots ===
            key0 = nk0; key1 = nk1; key2 = nk2; key3 = nk3;
            key4 = nk4; key5 = nk5; key6 = nk6; key7 = nk7;
            dst0 = nd0; dst1 = nd1; dst2 = nd2; dst3 = nd3;
            dst4 = nd4; dst5 = nd5; dst6 = nd6; dst7 = nd7;
            p0 = np0; p1 = np1; p2 = np2; p3 = np3;
            p4 = np4; p5 = np5; p6 = np6; p7 = np7;
            src0 = aheadBase;
            src1 = aheadBase + es;
            src2 = aheadBase + 2 * es;
            src3 = aheadBase + 3 * es;
            src4 = aheadBase + 4 * es;
            src5 = aheadBase + 5 * es;
            src6 = aheadBase + 6 * es;
            src7 = aheadBase + 7 * es;
        }
    }

    @Override
    public void reopen(int keyCapacity, long heapSize) {
        if (memStart == 0) {
            keyCapacity = (int) (keyCapacity / loadFactor);
            initialKeyCapacity = Math.max(Numbers.ceilPow2(keyCapacity), MIN_KEY_CAPACITY);
            restoreInitialCapacity();
        }
    }

    public void reopen() {
        if (memStart == 0) {
            restoreInitialCapacity();
        }
    }

    @Override
    public void restoreInitialCapacity() {
        if (memStart == 0 || keyCapacity != initialKeyCapacity) {
            keyCapacity = initialKeyCapacity;
            mask = keyCapacity - 1;
            final long sizeBytes = entrySize * keyCapacity;
            if (memStart == 0) {
                memStart = Unsafe.malloc(sizeBytes, memoryTag);
            } else {
                memStart = Unsafe.realloc(memStart, memLimit - memStart, sizeBytes, memoryTag);
            }
            memLimit = memStart + sizeBytes;
        }

        if (zeroMemStart == 0) {
            zeroMemStart = Unsafe.malloc(entrySize, memoryTag);
        }

        clear();
    }

    @Override
    public void setKeyCapacity(int newKeyCapacity) {
        long requiredCapacity = (long) (newKeyCapacity / loadFactor);
        if (requiredCapacity > MAX_SAFE_INT_POW_2) {
            throw CairoException.nonCritical().put("map capacity overflow");
        }
        rehash(Numbers.ceilPow2((int) requiredCapacity));
    }

    @Override
    public long size() {
        return hasZero ? size + 1 : size;
    }

    @Override
    public MapValue valueAt(long startAddress) {
        return valueOf(startAddress, false, value);
    }

    @Override
    public MapKey withKey() {
        return key;
    }

    private Unordered8MapValue asNew(long startAddress, long key, long hashCode, Unordered8MapValue value) {
        Unsafe.getUnsafe().putLong(startAddress, key);
        if (--free == 0) {
            rehash();
            // Index may have changed after rehash, so we need to find the key.
            startAddress = getStartAddress(hashCode & mask);
            for (; ; ) {
                long k = Unsafe.getUnsafe().getLong(startAddress);
                if (k == key) {
                    break;
                }
                startAddress = getNextAddress(startAddress);
            }
        }
        size++;
        return valueOf(startAddress, true, value);
    }

    // Advance through the map data structure sequentially,
    // avoiding multiplication and pseudo-random access.
    private long getNextAddress(long entryAddress) {
        entryAddress += entrySize;
        if (entryAddress < memLimit) {
            return entryAddress;
        }
        return memStart;
    }

    private long getStartAddress(long memStart, long index) {
        return memStart + entrySize * index;
    }

    private long getStartAddress(long index) {
        return memStart + entrySize * index;
    }

    private void rehash() {
        rehash((long) keyCapacity << 1);
    }

    private void rehash(long newKeyCapacity) {
        if (nResizes == maxResizes) {
            throw LimitOverflowException.instance().put("limit of ").put(maxResizes).put(" resizes exceeded in unordered map");
        }
        if (newKeyCapacity > MAX_SAFE_INT_POW_2) {
            throw CairoException.nonCritical().put("map capacity overflow");
        }
        if (newKeyCapacity <= keyCapacity) {
            return;
        }

        final long newSizeBytes = entrySize * newKeyCapacity;
        final long newMemStart = Unsafe.malloc(newSizeBytes, memoryTag);
        final long newMemLimit = newMemStart + newSizeBytes;
        Vect.memset(newMemStart, newSizeBytes, 0);
        final int newMask = (int) newKeyCapacity - 1;

        for (long addr = memStart; addr < memLimit; addr += entrySize) {
            long key = Unsafe.getUnsafe().getLong(addr);
            if (key == 0) {
                continue;
            }

            long newAddr = getStartAddress(newMemStart, Hash.hashLong64(key) & newMask);
            while (Unsafe.getUnsafe().getLong(newAddr) != 0) {
                newAddr += entrySize;
                if (newAddr >= newMemLimit) {
                    newAddr = newMemStart;
                }
            }
            Vect.memcpy(newAddr, addr, entrySize);
        }

        Unsafe.free(memStart, memLimit - memStart, memoryTag);

        memStart = newMemStart;
        memLimit = newMemStart + newSizeBytes;
        mask = newMask;
        free += (int) ((newKeyCapacity - keyCapacity) * loadFactor);
        keyCapacity = (int) newKeyCapacity;
        nResizes++;
    }

    private Unordered8MapValue valueOf(long startAddress, boolean newValue, Unordered8MapValue value) {
        return value.of(startAddress, memLimit, newValue);
    }

    long entrySize() {
        return entrySize;
    }

    boolean isZeroKey(long startAddress) {
        return Unsafe.getUnsafe().getLong(startAddress) == 0;
    }

    class Key implements MapKey {
        private long key;

        @Override
        public long commit() {
            return KEY_SIZE; // we don't need to track the actual key size
        }

        @Override
        public void copyFrom(MapKey srcKey) {
            Key src8Key = (Key) srcKey;
            copyFromRawKey(src8Key.key);
        }

        @Override
        public MapValue createValue() {
            if (key != 0) {
                return createNonZeroKeyValue(key, Hash.hashLong64(key));
            }
            return createZeroKeyValue();
        }

        @Override
        public MapValue createValue(long hashCode) {
            if (key != 0) {
                return createNonZeroKeyValue(key, hashCode);
            }
            return createZeroKeyValue();
        }

        @Override
        public MapValue findValue() {
            return findValue(value);
        }

        @Override
        public MapValue findValue2() {
            return findValue(value2);
        }

        @Override
        public MapValue findValue3() {
            return findValue(value3);
        }

        @Override
        public long hash() {
            return Hash.hashLong64(key);
        }

        @Override
        public void put(Record record, RecordSink sink) {
            sink.copy(record, this);
        }

        @Override
        public void putArray(ArrayView view) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putBin(BinarySequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putBool(boolean value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putByte(byte value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putChar(char value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDate(long value) {
            putLong(value);
        }

        @Override
        public void putDecimal128(Decimal128 decimal128) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDecimal256(Decimal256 decimal256) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDouble(double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putFloat(float value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putIPv4(int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putInt(int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putInterval(Interval interval) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong(long value) {
            this.key = value;
        }

        @Override
        public void putLong128(long lo, long hi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong256(Long256 value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong256(long l0, long l1, long l2, long l3) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putRecord(Record value) {
            // no-op
        }

        @Override
        public void putShort(short value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putStr(CharSequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putStr(CharSequence value, int lo, int hi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putTimestamp(long value) {
            putLong(value);
        }

        @Override
        public void putVarchar(Utf8Sequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void skip(int bytes) {
            throw new UnsupportedOperationException();
        }

        private MapValue createNonZeroKeyValue(long key, long hashCode) {
            long startAddress = getStartAddress(hashCode & mask);
            for (; ; ) {
                long k = Unsafe.getUnsafe().getLong(startAddress);
                if (k == 0) {
                    return asNew(startAddress, key, hashCode, value);
                } else if (k == key) {
                    return valueOf(startAddress, false, value);
                }
                startAddress = getNextAddress(startAddress);
            }
        }

        private MapValue createZeroKeyValue() {
            if (hasZero) {
                return valueOf(zeroMemStart, false, value);
            }
            hasZero = true;
            return valueOf(zeroMemStart, true, value);
        }

        private MapValue findValue(Unordered8MapValue value) {
            if (key == 0) {
                return hasZero ? valueOf(zeroMemStart, false, value) : null;
            }

            long startAddress = getStartAddress(Hash.hashLong64(key) & mask);
            for (; ; ) {
                long k = Unsafe.getUnsafe().getLong(startAddress);
                if (k == 0) {
                    return null;
                } else if (k == key) {
                    return valueOf(startAddress, false, value);
                }
                startAddress = getNextAddress(startAddress);
            }
        }

        void copyFromRawKey(long key) {
            this.key = key;
        }
    }
}
