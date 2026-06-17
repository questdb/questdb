/*******************************************************************************
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

package io.questdb.mp;

import io.questdb.std.CarrierLocal;
import io.questdb.std.Os;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Carrier identity for cont-aware carrier-local storage. {@link #current()}
 * returns the integer assigned by the most recent {@link #bind()} on the
 * <em>actual OS-level carrier executing now</em>, even when called from inside
 * a continuation that has been yielded and resumed on a different carrier.
 * <p>
 * The lookup goes through an FFI critical downcall into a Rust
 * {@code thread_local!} slot. C2 cannot fold this call with a hoisted
 * {@code Thread.currentThread()} value, which is what protects callers from
 * the cross-carrier corruption described in the {@code wait_wal_table}
 * investigation. {@link Thread#currentThread()} is hoistable across
 * {@link jdk.internal.vm.Continuation#yield} on raw continuation users
 * because we cannot apply {@code @ChangesCurrentThread} from outside the boot
 * loader; this primitive sidesteps that hazard entirely.
 * <p>
 * Threads that never call {@link #bind()} report {@link #UNBOUND}.
 */
public final class CarrierIdentity {
    public static final int UNBOUND = -1;
    private static final MethodHandle BIND;
    private static final MethodHandle CURRENT;
    private static final AtomicInteger NEXT_ID = new AtomicInteger();
    private static final ConcurrentQueue<IdHolder> RECYCLED = ConcurrentQueue.createConcurrentQueue(IdHolder::new);

    private CarrierIdentity() {
    }

    /**
     * Allocates a carrier id and pins it to the current OS thread. Returns the
     * assigned id so callers can log or store it. Each call allocates a fresh
     * id, except that ids freed by {@link #unbind()} are recycled here first
     * to bound the {@link CarrierLocal} row index over the JVM lifetime.
     * Reused ids are safe: the slot is nulled under the {@link CarrierLocal}
     * monitor before the id is pushed back, and a new {@link CarrierLocal} map
     * is created on first access under the same lock.
     * <p>
     * Pool-local worker ids are NOT safe to use here because every QuestDB
     * worker pool numbers its workers from 0; two workers in different pools
     * would alias to the same row in {@link CarrierLocal}.
     */
    public static int bind() {
        IdHolder holder = new IdHolder();
        int id = RECYCLED.tryDequeue(holder) ? holder.id : NEXT_ID.getAndIncrement();
        try {
            BIND.invokeExact(id);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
        return id;
    }

    public static int current() {
        try {
            return (int) CURRENT.invokeExact();
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    /**
     * Releases the {@link CarrierLocal} row pinned to the current carrier id and
     * resets the per-thread slot to {@link #UNBOUND}. Idempotent; a no-op on threads
     * that never called {@link #bind()}. Designed to be called from a thread's exit
     * path (e.g. a Worker's finally block) so per-carrier state does not accumulate
     * across thread churn.
     */
    public static void unbind() {
        int id = current();
        if (id < 0) {
            return;
        }
        CarrierLocal.releaseRow(id);
        try {
            BIND.invokeExact(UNBOUND);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
        // Order matters: push to RECYCLED only AFTER releaseRow has nulled the
        // slot and the per-thread Rust TLS has been reset. A concurrent bind()
        // that pops this id would otherwise observe a stale CarrierLocal map.
        IdHolder holder = new IdHolder();
        holder.id = id;
        RECYCLED.enqueue(holder);
    }

    private static final class IdHolder implements ValueHolder<IdHolder> {
        int id;

        @Override
        public void clear() {
            id = 0;
        }

        @Override
        public void copyTo(IdHolder dest) {
            dest.id = id;
        }
    }

    static {
        // Force the Rust cdylib (libquestdbr) into the process. Os's static
        // initializer loads it; Os.type is a runtime-assigned final, so the
        // reference below triggers that init.
        if (Os.type == Os._32Bit) {
            throw new ExceptionInInitializerError("CarrierIdentity requires a 64-bit JVM");
        }
        SymbolLookup lookup = SymbolLookup.loaderLookup();
        Linker linker = Linker.nativeLinker();
        BIND = linker.downcallHandle(
                lookup.find("qdb_carrier_bind").orElseThrow(
                        () -> new ExceptionInInitializerError("symbol qdb_carrier_bind not found in libquestdbr")),
                FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT),
                Linker.Option.critical(false));
        CURRENT = linker.downcallHandle(
                lookup.find("qdb_carrier_current").orElseThrow(
                        () -> new ExceptionInInitializerError("symbol qdb_carrier_current not found in libquestdbr")),
                FunctionDescriptor.of(ValueLayout.JAVA_INT),
                Linker.Option.critical(false));
    }
}
