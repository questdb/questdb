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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.std.Chars;
import io.questdb.std.Hash;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.Nullable;

public class CastStrToSymbolFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Sk)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            return SymbolConstant.newInstance(arg.getStrA(null));
        }
        return new Func(arg);
    }

    public static class Func extends SymbolFunction implements UnaryFunction {
        private static final int CHUNK_ENTRY_SIZE = 2 * Long.BYTES;
        // Every key owns a 16-byte descriptor: {long textAddress, int hash, int length}. Holding
        // the hash and the length here rather than in a header on the text itself lets rehash()
        // walk descriptors sequentially instead of chasing one strided read into the text chunks
        // per key, and lets valueAt() answer from a single cache line. The chunks then carry
        // nothing but UTF-16 payload.
        private static final int DESCRIPTOR_ENTRY_SIZE = 2 * Long.BYTES;
        private static final int DESCRIPTOR_HASH_OFFSET = Long.BYTES;
        private static final int DESCRIPTOR_LENGTH_OFFSET = Long.BYTES + Integer.BYTES;
        private static final int INITIAL_CHUNK_CAPACITY = 4;
        // Sized so a small dictionary settles without reallocating: the old 4-entry / 16-byte
        // floors could not hold a single nine-character symbol and forced a dozen growths before
        // reaching even a modest cardinality.
        private static final int INITIAL_DESCRIPTOR_CAPACITY = 16;
        private static final int INITIAL_HASH_CAPACITY = 16;
        private static final long INITIAL_TEXT_CHUNK_SIZE = 256;
        private static final long MAX_TEXT_CHUNK_SIZE = 1 << 20;
        private static final int MEMORY_TAG = MemoryTag.NATIVE_FUNC_RSS;
        private final Function arg;
        private final DirectString symbolA = new DirectString();
        private final DirectString symbolB = new DirectString();
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

        public Func(Function arg) {
            this.arg = arg;
        }

        @Override
        public void close() {
            try {
                releaseDictionary();
            } finally {
                arg.close();
            }
        }

        @Override
        public void cursorClosed() {
            try {
                arg.cursorClosed();
            } finally {
                // A cached factory can outlive its cursor. Free the query-accounted
                // dictionary before the query's MemoryTracker is returned to its pool.
                releaseDictionary();
            }
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getInt(Record rec) {
            return intern(arg.getStrA(rec));
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            // Pass-through: getSymbol never needs the dictionary (it returns the string
            // directly). Only getInt/valueOf build one. Interning here would grow an
            // unnecessary cardinality-dependent dictionary for values no key consumer uses.
            return arg.getStrA(rec);
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return arg.getStrB(rec);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            // init() may be called again on a cached factory. The normal path releases in
            // cursorClosed(); this defensive release also keeps failed/abandoned cursors safe.
            releaseDictionary();
            arg.init(symbolTableSource, executionContext);
            memoryTracker = executionContext.getMemoryTracker();
        }

        public int intern(@Nullable CharSequence value) {
            if (value == null) {
                return SymbolTable.VALUE_IS_NULL;
            }

            ensureHashTable();
            final int hash = Chars.hashCode(value);
            int slot = findSlot(value, hash);
            final int storedKey = Unsafe.getInt(hashAddress + ((long) slot << 2));
            if (storedKey != 0) {
                return storedKey - 1;
            }

            if (next == Integer.MAX_VALUE) {
                throw CairoException.nonCritical().put("too many distinct values in dynamic symbol dictionary");
            }
            if (next > hashThreshold) {
                rehash();
                slot = findSlot(value, hash);
            }

            final int len = value.length();
            // Keep every payload int-aligned. Besides being friendlier to ARM, this matches the
            // alignment used by the other native UTF-16 maps.
            final long entrySize = (((long) len << 1) + 3) & ~3L;
            ensureDescriptorCapacity(next);
            // A descriptor holds the payload's absolute address. Text chunks are never
            // reallocated, so that address stays valid for the life of the dictionary and the
            // flyweights valueAt() hands out survive any later intern().
            final long textAddress = allocateEntry(entrySize);

            final int key = next - 1;
            final long descriptor = descriptorsAddress + (long) key * DESCRIPTOR_ENTRY_SIZE;
            Unsafe.putLong(descriptor, textAddress);
            Unsafe.putInt(descriptor + DESCRIPTOR_HASH_OFFSET, hash);
            Unsafe.putInt(descriptor + DESCRIPTOR_LENGTH_OFFSET, len);
            for (int i = 0; i < len; i++) {
                Unsafe.putChar(textAddress + ((long) i << 1), value.charAt(i));
            }
            Unsafe.putInt(hashAddress + ((long) slot << 2), next++);
            return key;
        }

        @Override
        public boolean isSymbolTableStatic() {
            return false;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public @Nullable SymbolTable newSymbolTable() {
            // A view owns its A/B flyweights but reads the live dictionary. Unlike copying
            // the dictionary, it needs no native allocation with an otherwise unclear owner.
            // The values it yields point into append-only text chunks, so they stay valid across
            // further intern()/getInt() calls on the source function (see valueAt) and remain
            // readable until the dictionary is released. The view only ever grows: a key minted
            // after the view was taken resolves through it just as well.
            return new SymbolTable() {
                private final DirectString viewA = new DirectString();
                private final DirectString viewB = new DirectString();

                @Override
                public CharSequence valueBOf(int key) {
                    return valueAt(key, viewB);
                }

                @Override
                public CharSequence valueOf(int key) {
                    return valueAt(key, viewA);
                }
            };
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val("::symbol");
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueAt(key, symbolB);
        }

        @Override
        public CharSequence valueOf(int symbolKey) {
            return valueAt(symbolKey, symbolA);
        }

        // Carves entrySize bytes out of the current text chunk, allocating a fresh chunk when it
        // no longer fits. Chunks are append-only and never reallocated, which is what keeps every
        // address handed to a DirectString flyweight valid until releaseDictionary().
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
            final long oldSize = (long) descriptorsCapacity * DESCRIPTOR_ENTRY_SIZE;
            final long newSize = (long) newCapacity * DESCRIPTOR_ENTRY_SIZE;
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
                hashCapacity = INITIAL_HASH_CAPACITY;
                hashMask = hashCapacity - 1;
                hashThreshold = hashCapacity / 2;
                final long size = (long) hashCapacity * Integer.BYTES;
                hashAddress = Unsafe.malloc(size, MEMORY_TAG, memoryTracker);
                Unsafe.setMemory(hashAddress, size, (byte) 0);
            }
        }

        private boolean equalsValue(CharSequence value, int key, int hash) {
            // Hash and length both sit in the descriptor, so a mismatching candidate is rejected
            // without touching the text chunk at all.
            final long descriptor = descriptorsAddress + (long) key * DESCRIPTOR_ENTRY_SIZE;
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

        private int findSlot(CharSequence value, int hash) {
            int slot = Hash.spread(hash) & hashMask;
            while (true) {
                final int storedKey = Unsafe.getInt(hashAddress + ((long) slot << 2));
                if (storedKey == 0 || equalsValue(value, storedKey - 1, hash)) {
                    return slot;
                }
                slot = (slot + 1) & hashMask;
            }
        }

        private int hashValue(int key) {
            return Unsafe.getInt(descriptorsAddress + (long) key * DESCRIPTOR_ENTRY_SIZE + DESCRIPTOR_HASH_OFFSET);
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
                int slot = Hash.spread(hashValue(key)) & newMask;
                while (Unsafe.getInt(newAddress + ((long) slot << 2)) != 0) {
                    slot = (slot + 1) & newMask;
                }
                Unsafe.putInt(newAddress + ((long) slot << 2), key + 1);
            }
            hashAddress = Unsafe.free(
                    hashAddress,
                    (long) hashCapacity * Integer.BYTES,
                    MEMORY_TAG,
                    memoryTracker
            );
            hashAddress = newAddress;
            hashCapacity = newCapacity;
            hashMask = newMask;
            hashThreshold = newCapacity / 2;
        }

        private void releaseDictionary() {
            hashAddress = Unsafe.free(
                    hashAddress,
                    (long) hashCapacity * Integer.BYTES,
                    MEMORY_TAG,
                    memoryTracker
            );
            descriptorsAddress = Unsafe.free(
                    descriptorsAddress,
                    (long) descriptorsCapacity * DESCRIPTOR_ENTRY_SIZE,
                    MEMORY_TAG,
                    memoryTracker
            );
            releaseTextChunks();
            hashCapacity = 0;
            hashMask = 0;
            hashThreshold = 0;
            descriptorsCapacity = 0;
            next = 1;
            memoryTracker = null;
            symbolA.clear();
            symbolB.clear();
        }

        private void releaseTextChunks() {
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
        }

        // Returns a flyweight over the entry's text chunk. Chunks are append-only and never
        // reallocated, so the result stays valid for the life of the dictionary: a caller may hold
        // it across further intern()/getInt() calls on this function. releaseDictionary() frees the
        // chunks and resets next, after which valueAt returns null for every key rather than
        // pointing at freed memory. valueOf/valueBOf and the newSymbolTable() view all resolve
        // through here, so the same lifetime applies to them.
        private CharSequence valueAt(int key, DirectString view) {
            if (key < 0 || key >= next - 1) {
                return null;
            }
            final long descriptor = descriptorsAddress + (long) key * DESCRIPTOR_ENTRY_SIZE;
            return view.of(Unsafe.getLong(descriptor), Unsafe.getInt(descriptor + DESCRIPTOR_LENGTH_OFFSET));
        }
    }
}
