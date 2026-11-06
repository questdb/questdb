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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.IntHashSet;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Keys of a fixed list of values in a static symbol table. {@link #of} resolves the values again only
 * when the dictionary changes, and the per-worker copies of a function share the owner's keys through
 * {@link #offerTo}.
 */
public final class StaticSymbolKeys {
    @TestOnly
    public static final AtomicLong testResolutions = new AtomicLong();
    @TestOnly
    public static boolean isResolutionCounterEnabled = false;
    private final IntHashSet ownKeys = new IntHashSet();
    private long generation;
    private boolean isOffered;
    private IntHashSet keys = ownKeys;
    private int symbolCount;
    private StaticSymbolTable symbolTable;

    /**
     * Adds a key that is not one of the fixed values, such as a bind variable's. The next {@link #of}
     * resolves again.
     */
    public void add(int key) {
        assert keys == ownKeys;
        ownKeys.add(key);
        symbolTable = null;
    }

    public boolean contains(int key) {
        return keys.contains(key);
    }

    public boolean excludes(int key) {
        return keys.excludes(key);
    }

    /**
     * Resolves {@code values} to keys of {@code symbolTable}, unless the keys are still current or the owner
     * offered its own. When the dictionary only gained fewer keys than there are values, it looks up the new
     * keys instead. Returns true when it resolved all values.
     */
    public boolean of(@Nullable StaticSymbolTable symbolTable, CharSequenceHashSet values) {
        final boolean wasOffered = isOffered;
        isOffered = false;
        if (wasOffered || symbolTable == null) {
            return false;
        }
        final int symbolCount = symbolTable.getSymbolCount();
        final long generation = symbolTable.getSymbolTableGeneration();
        if (symbolTable == this.symbolTable
                && generation == this.generation
                && generation != StaticSymbolTable.NO_SYMBOL_TABLE_GENERATION) {
            final int addedCount = symbolCount - this.symbolCount;
            if (addedCount == 0) {
                return false;
            }
            // Within a generation the dictionary only appends, so only the new keys can match a value.
            if (addedCount > 0 && addedCount < values.size()) {
                for (int key = this.symbolCount; key < symbolCount; key++) {
                    if (values.contains(symbolTable.valueOfOnce(key))) {
                        ownKeys.add(key);
                    }
                }
                this.symbolCount = symbolCount;
                return false;
            }
        }
        keys = ownKeys;
        ownKeys.clear();
        for (int i = 0, n = values.size(); i < n; i++) {
            ownKeys.add(symbolTable.keyOf(values.get(i)));
        }
        this.symbolTable = symbolTable;
        this.symbolCount = symbolCount;
        this.generation = generation;
        if (isResolutionCounterEnabled) {
            testResolutions.incrementAndGet();
        }
        return true;
    }

    /**
     * Lends these keys to a per-worker copy for the current execution. They stay unchanged until the owner's
     * next init, which starts after the workers finish.
     */
    public void offerTo(StaticSymbolKeys that) {
        that.keys = keys;
        that.isOffered = true;
        that.symbolTable = null;
    }
}
