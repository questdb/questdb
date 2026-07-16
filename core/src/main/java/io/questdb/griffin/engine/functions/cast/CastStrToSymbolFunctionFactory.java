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
        private static final int INITIAL_HASH_CAPACITY = 4;
        private static final long INITIAL_TEXT_CAPACITY = 16;
        private static final int LENGTH_OFFSET = Integer.BYTES;
        private static final int MEMORY_TAG = MemoryTag.NATIVE_FUNC_RSS;
        private static final int OFFSET_ENTRY_SIZE = Long.BYTES;
        private static final int TEXT_OFFSET = 2 * Integer.BYTES;
        private final Function arg;
        private final DirectString symbolA = new DirectString();
        private final DirectString symbolB = new DirectString();
        private long hashAddress;
        private int hashCapacity;
        private int hashMask;
        private int hashThreshold;
        @Nullable
        private MemoryTracker memoryTracker;
        private int next = 1;
        private long offsetsAddress;
        private int offsetsCapacity;
        private long textAddress;
        private long textCapacity;
        private long textSize;

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
            // Keep every entry header int-aligned. Besides being friendlier to ARM,
            // this matches the alignment used by the other native UTF-16 maps.
            final long entrySize = (TEXT_OFFSET + ((long) len << 1) + 3) & ~3L;
            final long requiredTextSize = textSize + entrySize;
            if (requiredTextSize < textSize) {
                throw CairoException.nonCritical().put("dynamic symbol dictionary size overflow");
            }
            ensureOffsetCapacity(next);
            ensureTextCapacity(requiredTextSize);

            final int key = next - 1;
            Unsafe.putLong(offsetsAddress + (long) key * OFFSET_ENTRY_SIZE, textSize);
            Unsafe.putInt(textAddress + textSize, hash);
            Unsafe.putInt(textAddress + textSize + LENGTH_OFFSET, len);
            long p = textAddress + textSize + TEXT_OFFSET;
            for (int i = 0; i < len; i++) {
                Unsafe.putChar(p + ((long) i << 1), value.charAt(i));
            }
            textSize = requiredTextSize;
            Unsafe.putInt(hashAddress + ((long) slot << 2), next++);
            return key;
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

        private void ensureOffsetCapacity(int requiredCapacity) {
            if (requiredCapacity <= offsetsCapacity) {
                return;
            }
            int newCapacity = Math.max(2, offsetsCapacity);
            while (newCapacity < requiredCapacity) {
                if (newCapacity > Integer.MAX_VALUE / 2) {
                    throw CairoException.nonCritical().put("dynamic symbol dictionary offset capacity overflow");
                }
                newCapacity *= 2;
            }
            final long oldSize = (long) offsetsCapacity * OFFSET_ENTRY_SIZE;
            final long newSize = (long) newCapacity * OFFSET_ENTRY_SIZE;
            if (offsetsAddress == 0) {
                offsetsAddress = Unsafe.malloc(newSize, MEMORY_TAG, memoryTracker);
            } else {
                offsetsAddress = Unsafe.realloc(offsetsAddress, oldSize, newSize, MEMORY_TAG, memoryTracker);
            }
            offsetsCapacity = newCapacity;
        }

        private void ensureTextCapacity(long requiredCapacity) {
            if (requiredCapacity <= textCapacity) {
                return;
            }
            long newCapacity = Math.max(INITIAL_TEXT_CAPACITY, textCapacity);
            while (newCapacity < requiredCapacity) {
                final long doubled = newCapacity << 1;
                if (doubled <= newCapacity) {
                    newCapacity = requiredCapacity;
                    break;
                }
                newCapacity = doubled;
            }
            if (textAddress == 0) {
                textAddress = Unsafe.malloc(newCapacity, MEMORY_TAG, memoryTracker);
            } else {
                textAddress = Unsafe.realloc(textAddress, textCapacity, newCapacity, MEMORY_TAG, memoryTracker);
            }
            textCapacity = newCapacity;
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

        private boolean equalsValue(CharSequence value, int key, int hash) {
            final long offset = Unsafe.getLong(offsetsAddress + (long) key * OFFSET_ENTRY_SIZE);
            if (Unsafe.getInt(textAddress + offset) != hash) {
                return false;
            }
            final int len = Unsafe.getInt(textAddress + offset + LENGTH_OFFSET);
            if (len != value.length()) {
                return false;
            }
            final long p = textAddress + offset + TEXT_OFFSET;
            for (int i = 0; i < len; i++) {
                if (Unsafe.getChar(p + ((long) i << 1)) != value.charAt(i)) {
                    return false;
                }
            }
            return true;
        }

        private int hashValue(int key) {
            final long offset = Unsafe.getLong(offsetsAddress + (long) key * OFFSET_ENTRY_SIZE);
            return Unsafe.getInt(textAddress + offset);
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
            offsetsAddress = Unsafe.free(
                    offsetsAddress,
                    (long) offsetsCapacity * OFFSET_ENTRY_SIZE,
                    MEMORY_TAG,
                    memoryTracker
            );
            textAddress = Unsafe.free(textAddress, textCapacity, MEMORY_TAG, memoryTracker);
            hashCapacity = 0;
            hashMask = 0;
            hashThreshold = 0;
            offsetsCapacity = 0;
            textCapacity = 0;
            textSize = 0;
            next = 1;
            memoryTracker = null;
            symbolA.clear();
            symbolB.clear();
        }

        private CharSequence valueAt(int key, DirectString view) {
            if (key < 0 || key >= next - 1) {
                return null;
            }
            final long offset = Unsafe.getLong(offsetsAddress + (long) key * OFFSET_ENTRY_SIZE);
            final int len = Unsafe.getInt(textAddress + offset + LENGTH_OFFSET);
            return view.of(textAddress + offset + TEXT_OFFSET, len);
        }
    }
}
