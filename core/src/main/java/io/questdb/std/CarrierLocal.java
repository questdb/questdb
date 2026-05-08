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
    private static final int INITIAL_ROW_CAPACITY = 16;
    private static final AtomicInteger SLOT_SEQ = new AtomicInteger();
    // Per-carrier rows of per-instance values, indexed [carrierId][slot].
    // Reads are unsynchronized; growth happens under the class monitor and
    // publishes via the volatile write to `rows`.
    private static volatile Object[][] rows = new Object[0][];
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
        Object[] row = rowFor(id);
        Misc.freeIfCloseable(row[slot]);
        row[slot] = null;
    }

    @SuppressWarnings("unchecked")
    public T get() {
        int id = CarrierIdentity.current();
        if (id < 0) {
            return fallback.get();
        }
        Object[] row = rowFor(id);
        Object v = row[slot];
        if (v == null) {
            v = initial.apply(id);
            row[slot] = v;
        }
        return (T) v;
    }

    public void remove() {
        int id = CarrierIdentity.current();
        if (id < 0) {
            fallback.remove();
            return;
        }
        Object[] row = rowFor(id);
        row[slot] = null;
    }

    public void set(T value) {
        int id = CarrierIdentity.current();
        if (id < 0) {
            fallback.set(value);
            return;
        }
        Object[] row = rowFor(id);
        row[slot] = value;
    }

    private static Object[] ensureRow(int id, int requiredSlot) {
        synchronized (CarrierLocal.class) {
            Object[][] r = rows;
            if (id >= r.length) {
                Object[][] grown = new Object[Math.max(id + 1, r.length * 2)][];
                System.arraycopy(r, 0, grown, 0, r.length);
                rows = grown;
                r = grown;
            }
            Object[] row = r[id];
            if (row == null) {
                row = new Object[Math.max(requiredSlot + 1, INITIAL_ROW_CAPACITY)];
                r[id] = row;
            } else if (row.length <= requiredSlot) {
                Object[] grown = new Object[Math.max(requiredSlot + 1, row.length * 2)];
                System.arraycopy(row, 0, grown, 0, row.length);
                r[id] = grown;
                row = grown;
            }
            // Volatile self-assign publishes the plain r[id] store and the
            // preceding arraycopy. Pairs with the reader's volatile read of
            // rows; without it, a reader could observe the new row reference
            // but null contents, and get() would clobber the migrated value.
            rows = r;
            return row;
        }
    }

    private Object[] rowFor(int id) {
        Object[][] r = rows;
        if (id < r.length) {
            Object[] row = r[id];
            if (row != null && row.length > slot) {
                return row;
            }
        }
        return ensureRow(id, slot);
    }
}
