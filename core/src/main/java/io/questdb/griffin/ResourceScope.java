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
import io.questdb.cairo.sql.Function;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.Objects;

/**
 * Owns resources by slot until they are detached or closed. Slots stay stable until reset;
 * callers must discard slot borrows before reset.
 */
public final class ResourceScope implements Closeable, Mutable {
    private final ObjList<Closeable> resources = new ObjList<>();

    @Override
    public void clear() {
        final Throwable failure = closeOwned(null);
        resources.clear();
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        clear();
    }

    /**
     * Closes every owned resource in reverse reservation order, clearing each slot before its close
     * attempt. Callers arrange reservations so dependents close before providers. Close failures
     * become suppressed exceptions of {@code primary}; with no primary, the first close failure is
     * returned with the later ones suppressed.
     */
    public @Nullable Throwable closeOwned(@Nullable Throwable primary) {
        for (int i = resources.size() - 1; i >= 0; i--) {
            final Closeable resource = resources.getQuick(i);
            resources.setQuick(i, null);
            primary = Misc.freeBestEffort(primary, resource);
        }
        return primary;
    }

    /**
     * Transfers one owned resource to the caller without closing it. Empty slots cannot
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
     * Borrows the function a slot owns; ownership stays with the scope.
     */
    public Function function(int slot) {
        return (Function) resources.getQuick(slot);
    }

    public boolean isOwned(int slot) {
        return resources.getQuick(slot) != null;
    }

    /**
     * Initializes a fresh reservation without allocation. The caller must initialize
     * it exactly once and never refill a detached/closed slot.
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
