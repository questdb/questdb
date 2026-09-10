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
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public final class FiberLocal<T> {
    private static final AtomicInteger NEXT_INDEX = new AtomicInteger();
    private static final ThreadLocal<Holder> UNBOUND = ThreadLocal.withInitial(Holder::new);
    private static volatile Holder[] holders = new Holder[0];
    private final ObjectFactory<T> factory;
    private final int index = NEXT_INDEX.getAndIncrement();

    public FiberLocal(ObjectFactory<T> factory) {
        this.factory = factory;
    }

    public static void bindCarrier(int id) {
        synchronized (FiberLocal.class) {
            Holder[] rows = holders;
            if (id >= rows.length) {
                rows = Arrays.copyOf(rows, Numbers.ceilPow2(id + 1));
            }
            rows[id] = new Holder();
            holders = rows;
        }
    }

    public static ObjList<Object> enter(ObjList<Object> slots) {
        final Holder holder = holder(CarrierIdentity.current());
        final ObjList<Object> previous = holder.current;
        holder.current = slots;
        return previous;
    }

    public static void exit(ObjList<Object> previous) {
        holder(CarrierIdentity.current()).current = previous;
    }

    @TestOnly
    public static boolean isCarrierRegisteredForTesting(int id) {
        final Holder[] rows = holders;
        return id >= 0 && id < rows.length && rows[id] != null;
    }

    public static void releaseCarrier(int id) {
        synchronized (FiberLocal.class) {
            final Holder[] rows = holders;
            if (id >= 0 && id < rows.length) {
                rows[id] = null;
                holders = rows;
            }
        }
    }

    public T get() {
        final ObjList<Object> slots = holder(CarrierIdentity.current()).current;
        if (index < slots.size()) {
            @SuppressWarnings("unchecked")
            final T value = (T) slots.getQuick(index);
            if (value != null) {
                return value;
            }
        }
        return newValue(slots);
    }

    public void removeAndFree() {
        final ObjList<Object> slots = holder(CarrierIdentity.current()).current;
        if (index < slots.size()) {
            slots.setQuick(index, Misc.freeIfCloseable(slots.getQuick(index)));
        }
    }

    private static Holder holder(int id) {
        if (id < 0) {
            return UNBOUND.get();
        }
        final Holder[] rows = holders;
        if (id < rows.length) {
            final Holder holder = rows[id];
            if (holder != null) {
                return holder;
            }
        }
        throw new IllegalStateException("carrier has no Fiber-local slots [id=" + id + ']');
    }

    private T newValue(ObjList<Object> slots) {
        final T value = factory.newInstance();
        slots.extendAndSet(index, value);
        return value;
    }

    @SuppressWarnings("unused")
    private static final class Holder extends HolderSlots {
        long p9, p10, p11, p12, p13, p14, p15;
    }

    @SuppressWarnings("unused")
    private static class HolderPadding {
        long p1, p2, p3, p4, p5, p6, p7;
    }

    private static class HolderSlots extends HolderPadding {
        ObjList<Object> current = new ObjList<>();
    }
}
