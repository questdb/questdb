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

import java.util.concurrent.atomic.AtomicInteger;

public final class FiberLocal<T> {
    private static final CarrierLocal<ObjList<Object>> CARRIER_SLOTS = new CarrierLocal<>(ObjList::new);
    private static final AtomicInteger NEXT_INDEX = new AtomicInteger();
    private static SlotProvider slotProvider = CARRIER_SLOTS::get;
    private final ObjectFactory<T> factory;
    private final int index = NEXT_INDEX.getAndIncrement();

    public FiberLocal(ObjectFactory<T> factory) {
        this.factory = factory;
    }

    public static ObjList<Object> carrierSlots() {
        return CARRIER_SLOTS.get();
    }

    public static void installSlotProvider(SlotProvider provider) {
        slotProvider = provider;
    }

    public T get() {
        final ObjList<Object> slots = slotProvider.current();
        if (index < slots.size()) {
            @SuppressWarnings("unchecked")
            final T value = (T) slots.getQuick(index);
            if (value != null) {
                return value;
            }
        }
        final T value = factory.newInstance();
        slots.extendAndSet(index, value);
        return value;
    }

    public void removeAndFree() {
        final ObjList<Object> slots = slotProvider.current();
        if (index < slots.size()) {
            slots.setQuick(index, Misc.freeIfCloseable(slots.getQuick(index)));
        }
    }

    @FunctionalInterface
    public interface SlotProvider {
        ObjList<Object> current();
    }
}
