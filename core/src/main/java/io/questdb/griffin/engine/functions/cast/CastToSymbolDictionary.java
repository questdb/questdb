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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionResource;
import io.questdb.std.Chars;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.MutableUtf16Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

/**
 * The native dictionary behind a dynamic cast to SYMBOL. It mints 0-based keys in first-seen
 * order and resolves each key back to its UTF-16 text. All memory is native, tagged
 * {@link MemoryTag#NATIVE_FUNC_RSS} and accounted to the query's {@link MemoryTracker}; it is
 * allocated on the first key and freed by {@link #clear()}, after which the dictionary is empty
 * and reusable. Looking up a value that is already present allocates nothing, native or Java.
 * <p>
 * A dictionary is looked up in exactly one of three ways, fixed by its owner:
 * <ul>
 *     <li>by UTF-16 text, {@link #intern(CharSequence)} - the STRING cast;</li>
 *     <li>by UTF-8 bytes, {@link #intern(Utf8Sequence, MutableUtf16Sink)} - the VARCHAR cast;</li>
 *     <li>by a 64-bit scalar, {@link #keyOf(long)} then {@link #put(long, CharSequence)} - the
 *     numeric casts, which find a repeated value without rendering its text.</li>
 * </ul>
 * The UTF-8 and scalar lookups need a lookup field per key that the text lookup does not, so the
 * owner says which kind it is at construction.
 */
public final class CastToSymbolDictionary extends SqlExecutionResource {
    private static final int CHUNK_ENTRY_SIZE = 2 * Long.BYTES;
    // Every key owns a descriptor: {long textAddress, int hash, int length[, long lookup]}.
    // Holding the hash and the length here rather than in a header on the text itself lets
    // rehash() walk descriptors sequentially instead of chasing one strided read into the text
    // chunks per key, and lets valueOf() answer from a single cache line. The optional lookup
    // field holds the scalar value of a numeric cast, or the address of the UTF-8 bytes of a
    // VARCHAR cast.
    private static final int DESCRIPTOR_HASH_OFFSET = Long.BYTES;
    private static final int DESCRIPTOR_LENGTH_OFFSET = Long.BYTES + Integer.BYTES;
    private static final int DESCRIPTOR_LOOKUP_OFFSET = 2 * Long.BYTES;
    private static final int INITIAL_CHUNK_CAPACITY = 4;
    // Sized so a small dictionary settles without reallocating.
    private static final int INITIAL_DESCRIPTOR_CAPACITY = 16;
    private static final int INITIAL_HASH_CAPACITY = 16;
    private static final long INITIAL_TEXT_CHUNK_SIZE = 256;
    private static final int LOOKUP_DESCRIPTOR_ENTRY_SIZE = 3 * Long.BYTES;
    private static final long MAX_TEXT_CHUNK_SIZE = 1 << 20;
    private static final int MEMORY_TAG = MemoryTag.NATIVE_FUNC_RSS;
    private static final int TEXT_DESCRIPTOR_ENTRY_SIZE = 2 * Long.BYTES;
    private final int descriptorEntrySize;
    private int chunkCapacity;
    private int chunkCount;
    private long chunkSizeHint;
    private long chunksAddress;
    private long currentChunkAddress;
    private long currentChunkSize;
    private long currentChunkUsed;
    private long descriptorsAddress;
    private int descriptorsCapacity;
    private long hashAddress;
    private int hashCapacity;
    private int hashMask;
    private int hashThreshold;
    @Nullable
    private MemoryTracker memoryTracker;
    private int next = 1;

    /**
     * @param hasLookupField false for a dictionary looked up by UTF-16 text only, true for one
     *                       looked up by UTF-8 bytes or by a 64-bit scalar
     */
    public CastToSymbolDictionary(boolean hasLookupField) {
        this.descriptorEntrySize = hasLookupField ? LOOKUP_DESCRIPTOR_ENTRY_SIZE : TEXT_DESCRIPTOR_ENTRY_SIZE;
    }

    /**
     * Frees all native memory and forgets every key. Text handed out by {@link #valueOf} points
     * into the freed memory afterwards, so its holders must be done with it; valueOf() itself
     * answers null for every key from here on, until new keys are minted.
     */
    @Override
    public void clear() {
        hashAddress = Unsafe.free(hashAddress, (long) hashCapacity * Integer.BYTES, MEMORY_TAG, memoryTracker);
        descriptorsAddress = Unsafe.free(
                descriptorsAddress,
                (long) descriptorsCapacity * descriptorEntrySize,
                MEMORY_TAG,
                memoryTracker
        );
        for (int i = 0; i < chunkCount; i++) {
            final long slot = chunksAddress + (long) i * CHUNK_ENTRY_SIZE;
            Unsafe.free(Unsafe.getLong(slot), Unsafe.getLong(slot + Long.BYTES), MEMORY_TAG, memoryTracker);
        }
        chunksAddress = Unsafe.free(chunksAddress, (long) chunkCapacity * CHUNK_ENTRY_SIZE, MEMORY_TAG, memoryTracker);
        chunkCapacity = 0;
        chunkCount = 0;
        chunkSizeHint = 0;
        currentChunkAddress = 0;
        currentChunkSize = 0;
        currentChunkUsed = 0;
        descriptorsCapacity = 0;
        hashCapacity = 0;
        hashMask = 0;
        hashThreshold = 0;
        next = 1;
        memoryTracker = null;
    }

    public void init(SqlExecutionContext executionContext) {
        registerWith(executionContext);
        setMemoryTracker(executionContext.getMemoryTracker());
    }

    /**
     * Returns the key of the given text, minting a new one when the text is not in the dictionary
     * yet. Only for a dictionary constructed without a lookup field.
     */
    public int intern(CharSequence value) {
        assert descriptorEntrySize == TEXT_DESCRIPTOR_ENTRY_SIZE;
        ensureHashTable();
        final int hash = Chars.hashCode(value);
        int slot = findTextSlot(value, hash);
        final int storedKey = Unsafe.getInt(hashAddress + ((long) slot << 2));
        if (storedKey != 0) {
            return storedKey - 1;
        }
        if (growForNewKey()) {
            slot = findTextSlot(value, hash);
        }
        return addKey(slot, hash, value, 0);
    }

    /**
     * Returns the key of the given UTF-8 text, minting a new one when it is not in the dictionary
     * yet. A repeated value is found by its bytes, without decoding it; only a new value is decoded
     * to UTF-16, once, through decodeSink. Malformed UTF-8 reads as NULL, so it gets
     * {@link SymbolTable#VALUE_IS_NULL} and no key. Only for a dictionary constructed with a
     * lookup field.
     */
    public int intern(Utf8Sequence value, MutableUtf16Sink decodeSink) {
        assert descriptorEntrySize == LOOKUP_DESCRIPTOR_ENTRY_SIZE;
        ensureHashTable();
        final int hash = Utf8s.hashCode(value);
        int slot = findUtf8Slot(value, hash);
        final int storedKey = Unsafe.getInt(hashAddress + ((long) slot << 2));
        if (storedKey != 0) {
            return storedKey - 1;
        }
        final CharSequence text = Utf8s.utf8ToUtf16OrView(value, decodeSink);
        if (text == null) {
            return SymbolTable.VALUE_IS_NULL;
        }
        if (growForNewKey()) {
            slot = findUtf8Slot(value, hash);
        }
        // The UTF-8 bytes go into an entry of their own, prefixed with their size, so that a lookup
        // compares sizes before it reads any bytes.
        final int size = value.size();
        final long utf8Address = allocateEntry(alignedEntrySize(Integer.BYTES + (long) size));
        Unsafe.putInt(utf8Address, size);
        value.writeTo(utf8Address + Integer.BYTES, 0, size);
        return addKey(slot, hash, text, utf8Address);
    }

    /**
     * Returns the key of the given scalar, or -1 when it has none yet. Only for a dictionary
     * constructed with a lookup field.
     */
    public int keyOf(long value) {
        assert descriptorEntrySize == LOOKUP_DESCRIPTOR_ENTRY_SIZE;
        if (hashAddress == 0) {
            return -1;
        }
        final int storedKey = Unsafe.getInt(hashAddress + ((long) findScalarSlot(value, Hash.hashLong32(value)) << 2));
        return storedKey - 1;
    }

    /**
     * Mints a key for a scalar that {@link #keyOf(long)} has just reported absent, with the given
     * text as its value. Only for a dictionary constructed with a lookup field.
     */
    public int put(long value, CharSequence text) {
        assert descriptorEntrySize == LOOKUP_DESCRIPTOR_ENTRY_SIZE;
        ensureHashTable();
        growForNewKey();
        final int hash = Hash.hashLong32(value);
        final int slot = findScalarSlot(value, hash);
        assert Unsafe.getInt(hashAddress + ((long) slot << 2)) == 0;
        return addKey(slot, hash, text, value);
    }

    /**
     * Sets the tracker that the native memory allocated from here on is accounted to. Set it while
     * the dictionary is empty: {@link #clear()} frees everything against the current tracker and
     * then forgets it.
     */
    public void setMemoryTracker(@Nullable MemoryTracker memoryTracker) {
        // the hash table is always the first allocation, so its absence means nothing is allocated
        assert hashAddress == 0;
        this.memoryTracker = memoryTracker;
    }

    public int size() {
        return next - 1;
    }

    /**
     * Points the given flyweight at the text of the key and returns it, or returns null for
     * {@link SymbolTable#VALUE_IS_NULL} and for any key the dictionary has not minted. Text chunks
     * are append-only and never reallocated, so the result stays valid across later interning, up
     * to {@link #clear()}.
     */
    public CharSequence valueOf(int key, DirectString view) {
        if (key < 0 || key >= next - 1) {
            return null;
        }
        final long descriptor = descriptorsAddress + (long) key * descriptorEntrySize;
        return view.of(Unsafe.getLong(descriptor), Unsafe.getInt(descriptor + DESCRIPTOR_LENGTH_OFFSET));
    }

    private static long alignedEntrySize(long size) {
        // Keep every entry int-aligned. Besides being friendlier to ARM, this matches the
        // alignment used by the other native UTF-16 maps.
        return (size + 3) & ~3L;
    }

    // Stores the text and the descriptor of a new key, and claims the given empty slot for it.
    // The caller has already made room in the hash table with growForNewKey().
    private int addKey(int slot, int hash, CharSequence text, long lookup) {
        final int len = text.length();
        ensureDescriptorCapacity(next);
        // A descriptor holds the payload's absolute address. Text chunks are never reallocated,
        // so that address stays valid for the life of the dictionary and the flyweights valueOf()
        // hands out survive any later key.
        final long textAddress = allocateEntry(alignedEntrySize((long) len << 1));
        for (int i = 0; i < len; i++) {
            Unsafe.putChar(textAddress + ((long) i << 1), text.charAt(i));
        }
        final int key = next - 1;
        final long descriptor = descriptorsAddress + (long) key * descriptorEntrySize;
        Unsafe.putLong(descriptor, textAddress);
        Unsafe.putInt(descriptor + DESCRIPTOR_HASH_OFFSET, hash);
        Unsafe.putInt(descriptor + DESCRIPTOR_LENGTH_OFFSET, len);
        if (descriptorEntrySize == LOOKUP_DESCRIPTOR_ENTRY_SIZE) {
            Unsafe.putLong(descriptor + DESCRIPTOR_LOOKUP_OFFSET, lookup);
        }
        Unsafe.putInt(hashAddress + ((long) slot << 2), next++);
        return key;
    }

    // Carves entrySize bytes out of the current text chunk, allocating a fresh chunk when it
    // no longer fits. Chunks are append-only and never reallocated, which is what keeps every
    // address handed to a DirectString flyweight valid until clear().
    private long allocateEntry(long entrySize) {
        if (currentChunkAddress == 0 || currentChunkSize - currentChunkUsed < entrySize) {
            newChunk(entrySize);
        }
        final long entryAddress = currentChunkAddress + currentChunkUsed;
        currentChunkUsed += entrySize;
        return entryAddress;
    }

    private void ensureChunkCapacity(int requiredCapacity) {
        if (requiredCapacity <= chunkCapacity) {
            return;
        }
        int newCapacity = Math.max(INITIAL_CHUNK_CAPACITY, chunkCapacity);
        while (newCapacity < requiredCapacity) {
            if (newCapacity > Integer.MAX_VALUE / 2) {
                throw CairoException.nonCritical().put("dynamic symbol dictionary chunk capacity overflow");
            }
            newCapacity *= 2;
        }
        final long oldSize = (long) chunkCapacity * CHUNK_ENTRY_SIZE;
        final long newSize = (long) newCapacity * CHUNK_ENTRY_SIZE;
        // The chunk directory only ever holds {address, size} pairs. Nothing points into it,
        // so unlike the text chunks themselves it is safe to move on growth.
        if (chunksAddress == 0) {
            chunksAddress = Unsafe.malloc(newSize, MEMORY_TAG, memoryTracker);
        } else {
            chunksAddress = Unsafe.realloc(chunksAddress, oldSize, newSize, MEMORY_TAG, memoryTracker);
        }
        chunkCapacity = newCapacity;
    }

    private void ensureDescriptorCapacity(int requiredCapacity) {
        if (requiredCapacity <= descriptorsCapacity) {
            return;
        }
        int newCapacity = Math.max(INITIAL_DESCRIPTOR_CAPACITY, descriptorsCapacity);
        while (newCapacity < requiredCapacity) {
            if (newCapacity > Integer.MAX_VALUE / 2) {
                throw CairoException.nonCritical().put("dynamic symbol dictionary descriptor capacity overflow");
            }
            newCapacity *= 2;
        }
        final long oldSize = (long) descriptorsCapacity * descriptorEntrySize;
        final long newSize = (long) newCapacity * descriptorEntrySize;
        // Descriptors hold addresses but nothing points AT them, so growth may move the block.
        if (descriptorsAddress == 0) {
            descriptorsAddress = Unsafe.malloc(newSize, MEMORY_TAG, memoryTracker);
        } else {
            descriptorsAddress = Unsafe.realloc(descriptorsAddress, oldSize, newSize, MEMORY_TAG, memoryTracker);
        }
        descriptorsCapacity = newCapacity;
    }

    private void ensureHashTable() {
        if (hashAddress == 0) {
            final long size = (long) INITIAL_HASH_CAPACITY * Integer.BYTES;
            hashAddress = Unsafe.malloc(size, MEMORY_TAG, memoryTracker);
            Unsafe.setMemory(hashAddress, size, (byte) 0);
            hashCapacity = INITIAL_HASH_CAPACITY;
            hashMask = hashCapacity - 1;
            hashThreshold = hashCapacity / 2;
        }
    }

    private boolean equalsScalar(long value, int key, int hash) {
        final long descriptor = descriptorsAddress + (long) key * descriptorEntrySize;
        return Unsafe.getInt(descriptor + DESCRIPTOR_HASH_OFFSET) == hash
                && Unsafe.getLong(descriptor + DESCRIPTOR_LOOKUP_OFFSET) == value;
    }

    private boolean equalsText(CharSequence value, int key, int hash) {
        // Hash and length both sit in the descriptor, so a mismatching candidate is rejected
        // without touching the text chunk at all.
        final long descriptor = descriptorsAddress + (long) key * descriptorEntrySize;
        if (Unsafe.getInt(descriptor + DESCRIPTOR_HASH_OFFSET) != hash) {
            return false;
        }
        final int len = Unsafe.getInt(descriptor + DESCRIPTOR_LENGTH_OFFSET);
        if (len != value.length()) {
            return false;
        }
        final long textAddress = Unsafe.getLong(descriptor);
        for (int i = 0; i < len; i++) {
            if (Unsafe.getChar(textAddress + ((long) i << 1)) != value.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private boolean equalsUtf8(Utf8Sequence value, int key, int hash) {
        final long descriptor = descriptorsAddress + (long) key * descriptorEntrySize;
        if (Unsafe.getInt(descriptor + DESCRIPTOR_HASH_OFFSET) != hash) {
            return false;
        }
        final long utf8Address = Unsafe.getLong(descriptor + DESCRIPTOR_LOOKUP_OFFSET);
        final int size = value.size();
        if (Unsafe.getInt(utf8Address) != size) {
            return false;
        }
        final long bytesAddress = utf8Address + Integer.BYTES;
        int i = 0;
        for (; i + 7 < size; i += 8) {
            if (value.longAt(i) != Unsafe.getLong(bytesAddress + i)) {
                return false;
            }
        }
        for (; i < size; i++) {
            if (value.byteAt(i) != Unsafe.getByte(bytesAddress + i)) {
                return false;
            }
        }
        return true;
    }

    private int findScalarSlot(long value, int hash) {
        int slot = Hash.spread(hash) & hashMask;
        while (true) {
            final int storedKey = Unsafe.getInt(hashAddress + ((long) slot << 2));
            if (storedKey == 0 || equalsScalar(value, storedKey - 1, hash)) {
                return slot;
            }
            slot = (slot + 1) & hashMask;
        }
    }

    private int findTextSlot(CharSequence value, int hash) {
        int slot = Hash.spread(hash) & hashMask;
        while (true) {
            final int storedKey = Unsafe.getInt(hashAddress + ((long) slot << 2));
            if (storedKey == 0 || equalsText(value, storedKey - 1, hash)) {
                return slot;
            }
            slot = (slot + 1) & hashMask;
        }
    }

    private int findUtf8Slot(Utf8Sequence value, int hash) {
        int slot = Hash.spread(hash) & hashMask;
        while (true) {
            final int storedKey = Unsafe.getInt(hashAddress + ((long) slot << 2));
            if (storedKey == 0 || equalsUtf8(value, storedKey - 1, hash)) {
                return slot;
            }
            slot = (slot + 1) & hashMask;
        }
    }

    // Makes room in the hash table for one more key. Returns true when it rehashed, which moves
    // every slot, so that the caller must look its slot up again.
    private boolean growForNewKey() {
        if (next == Integer.MAX_VALUE) {
            throw CairoException.nonCritical().put("too many distinct values in dynamic symbol dictionary");
        }
        if (next > hashThreshold) {
            rehash();
            return true;
        }
        return false;
    }

    private void newChunk(long minSize) {
        long size = chunkSizeHint == 0 ? INITIAL_TEXT_CHUNK_SIZE : chunkSizeHint;
        chunkSizeHint = Math.min(size << 1, MAX_TEXT_CHUNK_SIZE);
        if (size < minSize) {
            // An entry bigger than the schedule gets a chunk of its own. It must not drag the
            // schedule up, or one long symbol would make every later chunk that size.
            size = minSize;
        }
        // Grow the directory first: a failure there must not strand a freshly malloc'd chunk.
        ensureChunkCapacity(chunkCount + 1);
        final long base = Unsafe.malloc(size, MEMORY_TAG, memoryTracker);
        final long slot = chunksAddress + (long) chunkCount * CHUNK_ENTRY_SIZE;
        Unsafe.putLong(slot, base);
        Unsafe.putLong(slot + Long.BYTES, size);
        chunkCount++;
        currentChunkAddress = base;
        currentChunkSize = size;
        currentChunkUsed = 0;
    }

    private void rehash() {
        if (hashCapacity > (1 << 29)) {
            throw CairoException.nonCritical().put("dynamic symbol dictionary hash capacity overflow");
        }
        final int newCapacity = hashCapacity << 1;
        final int newMask = newCapacity - 1;
        final long newSize = (long) newCapacity * Integer.BYTES;
        final long newAddress = Unsafe.malloc(newSize, MEMORY_TAG, memoryTracker);
        Unsafe.setMemory(newAddress, newSize, (byte) 0);
        for (int key = 0; key < next - 1; key++) {
            final int hash = Unsafe.getInt(descriptorsAddress + (long) key * descriptorEntrySize + DESCRIPTOR_HASH_OFFSET);
            int slot = Hash.spread(hash) & newMask;
            while (Unsafe.getInt(newAddress + ((long) slot << 2)) != 0) {
                slot = (slot + 1) & newMask;
            }
            Unsafe.putInt(newAddress + ((long) slot << 2), key + 1);
        }
        Unsafe.free(hashAddress, (long) hashCapacity * Integer.BYTES, MEMORY_TAG, memoryTracker);
        hashAddress = newAddress;
        hashCapacity = newCapacity;
        hashMask = newMask;
        hashThreshold = newCapacity / 2;
    }
}
