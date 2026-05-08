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

package io.questdb.std;

import io.questdb.mp.CarrierIdentity;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Carrier-keyed thread-local storage. Replaces the previous
 * {@code java.lang.ThreadLocal} extension to make per-thread state safe under
 * raw {@link jdk.internal.vm.Continuation} usage in {@code Worker.loopBody}.
 * <p>
 * Reads are routed through {@link CarrierIdentity#current()}, which is opaque
 * to C2 and therefore not loop-invariant from the JIT's perspective. A cont
 * yielded on carrier A and resumed on carrier B will see B's slot on the next
 * access, instead of the stale value held in the cont's frozen frame state.
 * <p>
 * Worker threads call {@link CarrierIdentity#bind()} once at start-up. Threads
 * that never bind (test runners, ServerMain bootstrap, shutdown hooks) fall
 * back to a per-{@link Thread} store; those threads do not run inside the cont
 * scheduler, so the hoist hazard does not apply to them.
 * <p>
 * The class extends {@code java.lang.ThreadLocal} for source compatibility
 * with callers that hold the field as a {@code java.lang.ThreadLocal<T>}, but
 * the parent's per-{@link Thread} storage map is never used for bound carriers
 * - all reads and writes go through the carrier-keyed array below. The
 * inheritance is purely a type assignment compatibility shim.
 * <p>
 * For the full problem analysis (C2 hoisting of {@code _currentThread} across
 * cont yield/resume) and the rationale for the FFI-backed carrier identity,
 * see {@code io/questdb/mp/continuation/CARRIER_LOCAL.md}.
 */
public class CarrierLocal<T> extends java.lang.ThreadLocal<T> implements Closeable {
    private static final AtomicInteger SLOT_SEQ = new AtomicInteger();
    // Per-carrier rows of per-instance values. The outer array is indexed by
    // carrier id; each inner map is keyed by slot. Reads of `rows` and of an
    // individual row entry are unsynchronized; outer growth and inner-row
    // installation happen under the class monitor and publish via the volatile
    // write to `rows`. Mutations of an installed inner map are confined to the
    // owning carrier's thread.
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static volatile IntObjHashMap<Object>[] rows = new IntObjHashMap[0];
    private final java.lang.ThreadLocal<T> fallback;
    private final IntFunction<? extends T> initial;
    private final int slot;

    public CarrierLocal() {
        this(id -> null);
    }

    public CarrierLocal(ObjectFactory<T> factory) {
        this(id -> factory.newInstance());
    }

    private CarrierLocal(IntFunction<? extends T> initial) {
        this.slot = SLOT_SEQ.getAndIncrement();
        this.initial = initial;
        this.fallback = java.lang.ThreadLocal.withInitial(() -> initial.apply(CarrierIdentity.UNBOUND));
    }

    /**
     * Frees and clears every slot of the row for the given carrier id, then
     * detaches the row so it can be GC'd. Called by {@link CarrierIdentity#unbind()}
     * when a carrier-bound thread exits, so {@link #rows} does not grow unbounded
     * across thread churn.
     */
    public static void releaseRow(int id) {
        synchronized (CarrierLocal.class) {
            IntObjHashMap<Object>[] r = rows;
            if (id < 0 || id >= r.length) {
                return;
            }
            IntObjHashMap<Object> row = r[id];
            if (row == null) {
                return;
            }
            Object[] values = row.getValues();
            for (int i = 0; i < values.length; i++) {
                Misc.freeIfCloseable(values[i]);
            }
            r[id] = null;
            rows = r;
        }
    }

    public static <T> CarrierLocal<T> withInitial(Supplier<? extends T> initial) {
        return new CarrierLocal<>(id -> initial.get());
    }

    @Override
    public void close() {
        int id = CarrierIdentity.current();
        if (id < 0) {
            Misc.freeIfCloseable(fallback.get());
            fallback.remove();
            return;
        }
        IntObjHashMap<Object> row = rowFor(id);
        Misc.freeIfCloseable(row.get(slot));
        row.put(slot, null);
    }

    @SuppressWarnings("unchecked")
    public T get() {
        int id = CarrierIdentity.current();
        if (id < 0) {
            return fallback.get();
        }
        IntObjHashMap<Object> row = rowFor(id);
        Object v = row.get(slot);
        if (v == null) {
            v = initial.apply(id);
            row.put(slot, v);
        }
        return (T) v;
    }

    public void remove() {
        int id = CarrierIdentity.current();
        if (id < 0) {
            fallback.remove();
            return;
        }
        rowFor(id).put(slot, null);
    }

    public void set(T value) {
        int id = CarrierIdentity.current();
        if (id < 0) {
            fallback.set(value);
            return;
        }
        rowFor(id).put(slot, value);
    }

    private static IntObjHashMap<Object> ensureRow(int id) {
        synchronized (CarrierLocal.class) {
            IntObjHashMap<Object>[] r = rows;
            if (id >= r.length) {
                @SuppressWarnings({"rawtypes", "unchecked"})
                IntObjHashMap<Object>[] grown = new IntObjHashMap[Math.max(id + 1, r.length * 2)];
                System.arraycopy(r, 0, grown, 0, r.length);
                rows = grown;
                r = grown;
            }
            IntObjHashMap<Object> row = r[id];
            if (row == null) {
                row = new IntObjHashMap<>();
                r[id] = row;
            }
            // Volatile self-assign publishes the plain r[id] store. Pairs with
            // the reader's volatile read of rows; without it, a reader could
            // observe the new map reference but uninitialized contents.
            rows = r;
            return row;
        }
    }

    private IntObjHashMap<Object> rowFor(int id) {
        IntObjHashMap<Object>[] r = rows;
        if (id < r.length) {
            IntObjHashMap<Object> row = r[id];
            if (row != null) {
                return row;
            }
        }
        return ensureRow(id);
    }
}
