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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;
import java.util.Comparator;

/**
 * An aligned per-worker function list that shares thread-safe owner functions and owns only the
 * marked worker-local clones. Callers use the list normally during execution and use the static
 * lifecycle helpers to avoid initializing, clearing, rewinding, or closing shared owner references.
 * <p>
 * Ownership follows positional bits, so the class rejects every inherited structural mutator that
 * would reorder or replace elements without updating the bits. Callers grow the list only through
 * {@link #add(Function, boolean)}; {@link #clear()} stays supported and resets the ownership bits
 * together with the elements.
 */
public final class PerWorkerFunctionList<T extends Function> extends ObjList<T> {
    private final BitSet ownedFunctions = new BitSet();

    public PerWorkerFunctionList(int capacity) {
        super(capacity);
    }

    @Override
    public void add(T value) {
        throw new UnsupportedOperationException("use add(function, isOwned)");
    }

    public void add(T function, boolean isOwned) {
        final int index = size();
        super.add(function);
        if (isOwned) {
            ownedFunctions.set(index);
        }
    }

    @Override
    public void addAll(ReadOnlyObjList<? extends T> that) {
        throw new UnsupportedOperationException("use add(function, isOwned)");
    }

    @Override
    public void addAll(ReadOnlyObjList<? extends T> that, int lo, int hi) {
        throw new UnsupportedOperationException("use add(function, isOwned)");
    }

    @Override
    public void addReverseAll(ReadOnlyObjList<? extends T> that) {
        throw new UnsupportedOperationException("use add(function, isOwned)");
    }

    @Override
    public void clear() {
        super.clear();
        ownedFunctions.clear();
    }

    @Override
    public void extendAndSet(int index, T value) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    @Override
    public void extendPos(int capacity) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    @Override
    public T getAndSetQuick(int index, T value) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    @Override
    public void insert(int index, int length, T defaultValue) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    /**
     * Returns the index of the first owned slot at or after {@code fromIndex}, or -1 when no
     * owned slot remains. Callers iterate the owned slots directly with
     * {@code for (int i = list.nextOwned(0); i > -1; i = list.nextOwned(i + 1))} and skip
     * borrowed slots without scanning them, so a fully borrowed list costs a single probe.
     */
    public int nextOwned(int fromIndex) {
        return ownedFunctions.nextSetBit(fromIndex);
    }

    @Override
    public T popLast() {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    @Override
    public void remove(int index) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    @Override
    public void remove(int from, int to) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    @Override
    public int remove(Object o) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    @Override
    public void set(int from, int to, T value) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    @Override
    public void set(int index, T value) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    @Override
    public void setAll(int count, T value) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    @Override
    public void setPos(int newPos) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    @Override
    public void setQuick(int index, T value) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    @Override
    public void sort(Comparator<T> cmp) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    @Override
    public void sort(int from, int to, Comparator<T> cmp) {
        throw new UnsupportedOperationException("structural mutation invalidates ownership bits");
    }

    public static void clear(ObjList<? extends Function> functions) {
        if (functions instanceof PerWorkerFunctionList<?> perWorkerFunctions) {
            for (int i = perWorkerFunctions.ownedFunctions.nextSetBit(0); i > -1; i = perWorkerFunctions.ownedFunctions.nextSetBit(i + 1)) {
                Misc.clear(perWorkerFunctions.getQuick(i));
            }
        } else {
            Misc.clearObjList(functions);
        }
    }

    public static void close(ObjList<? extends Function> functions) {
        if (functions instanceof PerWorkerFunctionList<?> perWorkerFunctions) {
            perWorkerFunctions.closeOwned();
        } else {
            Misc.freeObjList(functions);
        }
    }

    /**
     * Variant of {@link #close(ObjList)} for cleanup paths that already hold a primary
     * exception: it attaches any failure from the close to the primary as suppressed instead
     * of letting the failure propagate and mask it.
     */
    public static void close(ObjList<? extends Function> functions, @NotNull Throwable primary) {
        try {
            close(functions);
        } catch (Throwable th) {
            if (th != primary) {
                primary.addSuppressed(th);
            }
        }
    }

    public static void init(
            ObjList<? extends Function> functions,
            @Nullable ObjList<? extends Function> ownerFunctions,
            SymbolTableSource symbolTableSource,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (functions instanceof PerWorkerFunctionList<?> perWorkerFunctions) {
            for (int i = perWorkerFunctions.ownedFunctions.nextSetBit(0); i > -1; i = perWorkerFunctions.ownedFunctions.nextSetBit(i + 1)) {
                final Function function = perWorkerFunctions.getQuick(i);
                if (ownerFunctions != null) {
                    ownerFunctions.getQuick(i).offerStateTo(function);
                }
                function.init(symbolTableSource, executionContext);
            }
        } else {
            if (ownerFunctions != null) {
                for (int i = 0, n = functions.size(); i < n; i++) {
                    ownerFunctions.getQuick(i).offerStateTo(functions.getQuick(i));
                }
            }
            Function.init(functions, symbolTableSource, executionContext, null);
        }
    }

    public static boolean isOwned(ObjList<? extends Function> functions, int index) {
        return !(functions instanceof PerWorkerFunctionList<?> perWorkerFunctions) || perWorkerFunctions.ownedFunctions.get(index);
    }

    public static void toTop(ObjList<? extends Function> functions) {
        if (functions instanceof PerWorkerFunctionList<?> perWorkerFunctions) {
            for (int i = perWorkerFunctions.ownedFunctions.nextSetBit(0); i > -1; i = perWorkerFunctions.ownedFunctions.nextSetBit(i + 1)) {
                perWorkerFunctions.getQuick(i).toTop();
            }
        } else {
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).toTop();
            }
        }
    }

    private void closeOwned() {
        // Best-effort cleanup: null every owned slot before its close attempt and keep going
        // when a close() throws, so a failing worker clone can neither leak the clones after
        // it nor leave its own slot closeable twice. The first failure propagates once every
        // owned slot has seen a close attempt, with later failures attached as suppressed.
        Throwable failure = null;
        for (int i = ownedFunctions.nextSetBit(0); i > -1; i = ownedFunctions.nextSetBit(i + 1)) {
            final T function = getQuick(i);
            super.setQuick(i, null);
            failure = Misc.freeBestEffort(failure, function);
        }
        CairoException.rethrowCleanupFailure(failure);
    }
}
