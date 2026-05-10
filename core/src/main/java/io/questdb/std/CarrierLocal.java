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

import java.lang.ref.WeakReference;
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
 * The per-carrier storage is a port of {@code java.lang.ThreadLocal.ThreadLocalMap}:
 * an open-addressed hash table whose entries weakly reference the {@code CarrierLocal}
 * key. When a {@code CarrierLocal} instance becomes unreachable, GC clears the
 * weak reference and the next operation that touches the bucket expunges the
 * stale entry. This bounds the per-carrier table size by live keys, not by the
 * cumulative count of {@code CarrierLocal} constructions.
 * <p>
 * Worker threads call {@link CarrierIdentity#bind()} once at start-up. Threads
 * that never bind (test runners, ServerMain bootstrap, shutdown hooks) fall
 * back to a per-{@link Thread} store; those threads do not run inside the cont
 * scheduler, so the hoist hazard does not apply to them.
 * <p>
 * The class extends {@code java.lang.ThreadLocal} for source compatibility
 * with callers that hold the field as a {@code java.lang.ThreadLocal<T>}, but
 * the parent's per-{@link Thread} storage map is never used for bound carriers
 * - all reads and writes go through the carrier-keyed table below. The
 * inheritance is purely a type assignment compatibility shim.
 * <p>
 * For the full problem analysis (C2 hoisting of {@code _currentThread} across
 * cont yield/resume) and the rationale for the FFI-backed carrier identity,
 * see {@code io/questdb/mp/continuation/CARRIER_LOCAL.md}.
 */
public class CarrierLocal<T> extends java.lang.ThreadLocal<T> {
    private static final int HASH_INCREMENT = 0x61c88647;
    private static final AtomicInteger nextHashCode = new AtomicInteger();
    // Per-carrier maps. Outer is indexed by carrier id; each inner map is
    // private to one carrier and is only mutated by that carrier's worker
    // thread. Outer growth happens under the class monitor and publishes via
    // the volatile write to `rows`.
    private static volatile CarrierLocalMap[] rows = new CarrierLocalMap[0];
    private final int carrierLocalHashCode = nextHashCode.getAndAdd(HASH_INCREMENT);
    private final java.lang.ThreadLocal<T> fallback;
    private final IntFunction<? extends T> initial;

    public CarrierLocal() {
        this(id -> null);
    }

    public CarrierLocal(ObjectFactory<T> factory) {
        this(id -> factory.newInstance());
    }

    private CarrierLocal(IntFunction<? extends T> initial) {
        this.initial = initial;
        this.fallback = java.lang.ThreadLocal.withInitial(() -> initial.apply(CarrierIdentity.UNBOUND));
    }

    /**
     * Frees every closeable value stored in the row for the given carrier id,
     * then detaches the row so it can be GC'd. Called by
     * {@link CarrierIdentity#unbind()} when a carrier-bound thread exits, so
     * {@link #rows} does not grow unbounded across thread churn.
     */
    public static void releaseRow(int id) {
        synchronized (CarrierLocal.class) {
            CarrierLocalMap[] r = rows;
            if (id < 0 || id >= r.length) {
                return;
            }
            CarrierLocalMap map = r[id];
            if (map == null) {
                return;
            }
            CarrierLocalMap.Entry[] tab = map.table;
            if (tab != null) {
                for (CarrierLocalMap.Entry e : tab) {
                    if (e != null) {
                        Misc.freeIfCloseable(e.value);
                        e.value = null;
                    }
                }
            }
            r[id] = null;
            rows = r;
        }
    }

    public static <T> CarrierLocal<T> withInitial(Supplier<? extends T> initial) {
        return new CarrierLocal<>(id -> initial.get());
    }

    @Override
    @SuppressWarnings("unchecked")
    public T get() {
        int id = CarrierIdentity.current();
        if (id < 0) {
            return fallback.get();
        }
        CarrierLocalMap map = mapForOrNull(id);
        if (map != null) {
            CarrierLocalMap.Entry e = map.getEntry(this);
            if (e != null) {
                return (T) e.value;
            }
        }
        return setInitialValue(id);
    }

    /**
     * Removes the current carrier's entry. Matches JDK ThreadLocal's contract:
     * the previously stored value is left to the GC. Use
     * {@link #removeAndFree()} if the value is {@link java.io.Closeable} and
     * eager release is needed.
     */
    @Override
    public void remove() {
        int id = CarrierIdentity.current();
        if (id < 0) {
            fallback.remove();
            return;
        }
        CarrierLocalMap map = mapForOrNull(id);
        if (map != null) {
            CarrierLocalMap.Entry e = map.getEntry(this);
            if (e != null) {
                // Null the value before triggering map removal so the
                // stale-entry sweep inside map.remove does not see a value
                // and try to free it. Keeps user-visible remove() free of
                // close side effects, matching JDK semantics.
                e.value = null;
                map.remove(this);
            }
        }
    }

    /**
     * Frees the value (if {@link java.io.Closeable}) for the current carrier
     * and removes the entry. No-op if no value is set; does NOT trigger the
     * initial supplier. Use this in place of the removed {@code close()}
     * method when callers rely on eager release of native resources held in
     * the slot.
     */
    public void removeAndFree() {
        int id = CarrierIdentity.current();
        if (id < 0) {
            Misc.freeIfCloseable(fallback.get());
            fallback.remove();
            return;
        }
        CarrierLocalMap map = mapForOrNull(id);
        if (map != null) {
            CarrierLocalMap.Entry e = map.getEntry(this);
            if (e != null) {
                Misc.freeIfCloseable(e.value);
                e.value = null;
                map.remove(this);
            }
        }
    }

    @Override
    public void set(T value) {
        int id = CarrierIdentity.current();
        if (id < 0) {
            fallback.set(value);
            return;
        }
        setOrCreate(id, value);
    }

    private static CarrierLocalMap createMap(int id, CarrierLocal<?> firstKey, Object firstValue) {
        synchronized (CarrierLocal.class) {
            CarrierLocalMap[] r = rows;
            if (id >= r.length) {
                CarrierLocalMap[] grown = new CarrierLocalMap[Math.max(id + 1, r.length * 2)];
                System.arraycopy(r, 0, grown, 0, r.length);
                rows = grown;
                r = grown;
            }
            CarrierLocalMap m = r[id];
            if (m == null) {
                m = new CarrierLocalMap(firstKey, firstValue);
                r[id] = m;
            } else {
                m.set(firstKey, firstValue);
            }
            // Volatile self-assign publishes the plain r[id] store. Pairs
            // with the reader's volatile read of rows; without it, a reader
            // could observe the new map reference but uninitialized contents.
            rows = r;
            return m;
        }
    }

    private CarrierLocalMap mapForOrNull(int id) {
        CarrierLocalMap[] r = rows;
        if (id < r.length) {
            return r[id];
        }
        return null;
    }

    private T setInitialValue(int id) {
        T value = initial.apply(id);
        setOrCreate(id, value);
        return value;
    }

    private void setOrCreate(int id, T value) {
        CarrierLocalMap map = mapForOrNull(id);
        if (map != null) {
            map.set(this, value);
        } else {
            createMap(id, this, value);
        }
    }

    /**
     * Open-addressed hash table holding one entry per live {@link CarrierLocal}
     * key for a single carrier. Direct port of
     * {@code java.lang.ThreadLocal.ThreadLocalMap}: entries weakly reference the
     * key, stale entries (cleared weak refs) are expunged opportunistically by
     * {@link #set}, {@link #getEntryAfterMiss}, and {@link #rehash}.
     * <p>
     * Single-thread access only - mutated by the owning carrier's worker.
     */
    private static final class CarrierLocalMap {
        private static final int INITIAL_CAPACITY = 16;
        private Entry[] table;
        private int size = 0;
        private int threshold;

        CarrierLocalMap(CarrierLocal<?> firstKey, Object firstValue) {
            table = new Entry[INITIAL_CAPACITY];
            int i = firstKey.carrierLocalHashCode & (INITIAL_CAPACITY - 1);
            table[i] = new Entry(firstKey, firstValue);
            size = 1;
            setThreshold(INITIAL_CAPACITY);
        }

        private static int nextIndex(int i, int len) {
            return ((i + 1 < len) ? i + 1 : 0);
        }

        private static int prevIndex(int i, int len) {
            return ((i - 1 >= 0) ? i - 1 : len - 1);
        }

        private boolean cleanSomeSlots(int i, int n) {
            boolean removed = false;
            Entry[] tab = table;
            int len = tab.length;
            do {
                i = nextIndex(i, len);
                Entry e = tab[i];
                if (e != null && e.refersTo(null)) {
                    n = len;
                    removed = true;
                    i = expungeStaleEntry(i);
                }
            } while ((n >>>= 1) != 0);
            return removed;
        }

        private int expungeStaleEntry(int staleSlot) {
            Entry[] tab = table;
            int len = tab.length;

            Misc.freeIfCloseable(tab[staleSlot].value);
            tab[staleSlot].value = null;
            tab[staleSlot] = null;
            size--;

            Entry e;
            int i;
            for (i = nextIndex(staleSlot, len);
                 (e = tab[i]) != null;
                 i = nextIndex(i, len)) {
                CarrierLocal<?> k = e.get();
                if (k == null) {
                    Misc.freeIfCloseable(e.value);
                    e.value = null;
                    tab[i] = null;
                    size--;
                } else {
                    int h = k.carrierLocalHashCode & (len - 1);
                    if (h != i) {
                        tab[i] = null;
                        while (tab[h] != null) {
                            h = nextIndex(h, len);
                        }
                        tab[h] = e;
                    }
                }
            }
            return i;
        }

        private void expungeStaleEntries() {
            Entry[] tab = table;
            int len = tab.length;
            for (int j = 0; j < len; j++) {
                Entry e = tab[j];
                if (e != null && e.refersTo(null)) {
                    expungeStaleEntry(j);
                }
            }
        }

        private Entry getEntry(CarrierLocal<?> key) {
            int i = key.carrierLocalHashCode & (table.length - 1);
            Entry e = table[i];
            if (e != null && e.refersTo(key)) {
                return e;
            }
            return getEntryAfterMiss(key, i, e);
        }

        private Entry getEntryAfterMiss(CarrierLocal<?> key, int i, Entry e) {
            Entry[] tab = table;
            int len = tab.length;

            while (e != null) {
                if (e.refersTo(key)) {
                    return e;
                }
                if (e.refersTo(null)) {
                    expungeStaleEntry(i);
                } else {
                    i = nextIndex(i, len);
                }
                e = tab[i];
            }
            return null;
        }

        private void rehash() {
            expungeStaleEntries();
            if (size >= threshold - threshold / 4) {
                resize();
            }
        }

        private void remove(CarrierLocal<?> key) {
            Entry[] tab = table;
            int len = tab.length;
            int i = key.carrierLocalHashCode & (len - 1);
            for (Entry e = tab[i];
                 e != null;
                 e = tab[i = nextIndex(i, len)]) {
                if (e.refersTo(key)) {
                    e.clear();
                    expungeStaleEntry(i);
                    return;
                }
            }
        }

        private void replaceStaleEntry(CarrierLocal<?> key, Object value, int staleSlot) {
            Entry[] tab = table;
            int len = tab.length;
            Entry e;

            int slotToExpunge = staleSlot;
            for (int i = prevIndex(staleSlot, len);
                 (e = tab[i]) != null;
                 i = prevIndex(i, len)) {
                if (e.refersTo(null)) {
                    slotToExpunge = i;
                }
            }

            for (int i = nextIndex(staleSlot, len);
                 (e = tab[i]) != null;
                 i = nextIndex(i, len)) {
                if (e.refersTo(key)) {
                    e.value = value;

                    tab[i] = tab[staleSlot];
                    tab[staleSlot] = e;

                    if (slotToExpunge == staleSlot) {
                        slotToExpunge = i;
                    }
                    cleanSomeSlots(expungeStaleEntry(slotToExpunge), len);
                    return;
                }

                if (e.refersTo(null) && slotToExpunge == staleSlot) {
                    slotToExpunge = i;
                }
            }

            Misc.freeIfCloseable(tab[staleSlot].value);
            tab[staleSlot].value = null;
            tab[staleSlot] = new Entry(key, value);

            if (slotToExpunge != staleSlot) {
                cleanSomeSlots(expungeStaleEntry(slotToExpunge), len);
            }
        }

        private void resize() {
            Entry[] oldTab = table;
            int oldLen = oldTab.length;
            int newLen = oldLen * 2;
            Entry[] newTab = new Entry[newLen];
            int count = 0;

            for (Entry e : oldTab) {
                if (e != null) {
                    CarrierLocal<?> k = e.get();
                    if (k == null) {
                        Misc.freeIfCloseable(e.value);
                        e.value = null;
                    } else {
                        int h = k.carrierLocalHashCode & (newLen - 1);
                        while (newTab[h] != null) {
                            h = nextIndex(h, newLen);
                        }
                        newTab[h] = e;
                        count++;
                    }
                }
            }

            setThreshold(newLen);
            size = count;
            table = newTab;
        }

        private void set(CarrierLocal<?> key, Object value) {
            Entry[] tab = table;
            int len = tab.length;
            int i = key.carrierLocalHashCode & (len - 1);

            for (Entry e = tab[i];
                 e != null;
                 e = tab[i = nextIndex(i, len)]) {
                if (e.refersTo(key)) {
                    e.value = value;
                    return;
                }
                if (e.refersTo(null)) {
                    replaceStaleEntry(key, value, i);
                    return;
                }
            }

            tab[i] = new Entry(key, value);
            int sz = ++size;
            if (!cleanSomeSlots(i, sz) && sz >= threshold) {
                rehash();
            }
        }

        private void setThreshold(int len) {
            threshold = len * 2 / 3;
        }

        static final class Entry extends WeakReference<CarrierLocal<?>> {
            Object value;

            Entry(CarrierLocal<?> k, Object v) {
                super(k);
                value = v;
            }
        }
    }
}
