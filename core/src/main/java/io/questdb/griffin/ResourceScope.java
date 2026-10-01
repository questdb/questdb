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

package io.questdb.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.Objects;

/**
 * Owns executable roots until cleanup or transfer. Slots stay stable until reset;
 * callers must discard preparation entries and other slot borrows before reset.
 */
public final class ResourceScope implements Closeable, Mutable {
    final ObjList<Closeable> resources = new ObjList<>();

    @Override
    public void clear() {
        final Throwable failure = closeOwned(-1, null);
        resources.clear();
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        clear();
    }

    /**
     * Closes owned roots in reverse reservation order, clearing each slot before its
     * close attempt. Callers arrange scopes/reservations so dependents close before
     * providers. The retained slot remains owned; use -1 to retain nothing.
     */
    public @Nullable Throwable closeOwned(int retainedSlot, @Nullable Throwable primary) {
        if (retainedSlot != -1) {
            checkSlot(retainedSlot);
        }
        for (int i = resources.size() - 1; i >= 0; i--) {
            if (i != retainedSlot) {
                final Closeable resource = resources.getQuick(i);
                resources.setQuick(i, null);
                primary = Misc.freeBestEffort(primary, resource);
            }
        }
        return primary;
    }

    /**
     * Closes every owned root on a failure path, in reverse reservation order; close failures
     * become suppressed exceptions of the primary.
     */
    public void closeOwned(@NotNull Throwable primary) {
        for (int i = resources.size() - 1; i >= 0; i--) {
            final Closeable resource = resources.getQuick(i);
            resources.setQuick(i, null);
            Misc.free(resource, primary);
        }
    }

    /**
     * Transfers one owned root to the caller without closing it. Empty slots cannot
     * be claimed again, including slots whose close attempt failed.
     */
    public @NotNull Closeable detach(int slot) {
        checkSlot(slot);
        final Closeable resource = resources.getQuick(slot);
        if (resource == null) {
            throw new IllegalStateException("resource slot is not owned: " + slot);
        }
        resources.setQuick(slot, null);
        return resource;
    }

    /**
     * Initializes a fresh reservation without allocation. The caller must initialize
     * it exactly once and never refill a detached/closed slot. A composite's adopted
     * children must already have been detached from their previous owners.
     */
    public void own(int slot, @NotNull Closeable resource) {
        checkSlot(slot);
        if (resources.getQuick(slot) != null) {
            throw new IllegalStateException("resource slot is already owned: " + slot);
        }
        resources.setQuick(slot, Objects.requireNonNull(resource, "resource"));
    }

    /**
     * Reserves before acquisition so a capacity-growth failure cannot orphan a newly
     * created resource. Detached/closed slots are not reused until reset.
     */
    public int reserve() {
        final int slot = resources.size();
        resources.add(null);
        return slot;
    }

    private void checkSlot(int slot) {
        if (slot < 0 || slot >= resources.size()) {
            throw new IndexOutOfBoundsException("resource slot: " + slot);
        }
    }
}
