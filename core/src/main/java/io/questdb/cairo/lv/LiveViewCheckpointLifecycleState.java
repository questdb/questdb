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

package io.questdb.cairo.lv;

import io.questdb.std.IntList;
import io.questdb.std.LongIntHashMap;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Engine-shared, allocation-free-after-warmup lifecycle state for checkpoint timelines.
 *
 * <p>The refresh notification queue may route consecutive turns of one live view to different
 * workers. Consequently this state cannot live on a worker's timeline writer: doing so gives one
 * orphan-scan counter and one reconciliation flag per worker. The engine owns one instance and all
 * worker-local writers use it. The live-view refresh latch serializes a single table generation;
 * synchronized methods protect the primitive slot registry when different views refresh in
 * parallel.</p>
 *
 * <p>Every operation binds one engine-issued lifecycle identity. The engine never reuses this
 * process-local long, so rename preserves state through the shared instance while drop/recreate and
 * explicit table-id reuse receive a different identity.</p>
 */
public final class LiveViewCheckpointLifecycleState implements Mutable {
    private static final int FLAG_ORPHAN_SCAN_COMPLETED = 2;
    private static final int FLAG_ORPHAN_SCAN_NEEDED = 4;
    private static final int FLAG_RECONCILED = 1;
    private final IntList flags = new IntList();
    private final IntList freeSlots = new IntList();
    private final ObjList<LongList> pendingRetirements = new ObjList<>();
    private final ObjList<LongList> retirementListPool = new ObjList<>();
    private final LongIntHashMap slotsByLifecycleId = new LongIntHashMap();
    private final IntList sweepsSinceOrphanScan = new IntList();

    public synchronized boolean beginPublication(long lifecycleIdentity) {
        final int slot = bindGeneration(lifecycleIdentity);
        final int currentFlags = flags.getQuick(slot);
        flags.setQuick(slot, currentFlags | FLAG_ORPHAN_SCAN_NEEDED);
        return (currentFlags & FLAG_ORPHAN_SCAN_NEEDED) != 0;
    }

    @Override
    public synchronized void clear() {
        for (int i = 0, n = pendingRetirements.size(); i < n; i++) {
            final LongList pending = pendingRetirements.getQuick(i);
            if (pending != null) {
                pending.clear();
            }
        }
        slotsByLifecycleId.clear();
        flags.clear();
        freeSlots.clear();
        pendingRetirements.clear();
        retirementListPool.clear();
        sweepsSinceOrphanScan.clear();
    }

    public synchronized void clearPendingRetirements(long lifecycleIdentity) {
        final int slot = bindGeneration(lifecycleIdentity);
        recyclePendingRetirements(slot);
    }

    public synchronized void clearReconciled(long lifecycleIdentity) {
        final int slot = bindGeneration(lifecycleIdentity);
        flags.setQuick(slot, flags.getQuick(slot) & ~FLAG_RECONCILED);
    }

    public synchronized void finishPublication(
            long lifecycleIdentity,
            boolean hasPriorOrphanRisk
    ) {
        final int slot = bindGeneration(lifecycleIdentity);
        if (!hasPriorOrphanRisk) {
            flags.setQuick(slot, flags.getQuick(slot) & ~FLAG_ORPHAN_SCAN_NEEDED);
        }
    }

    public synchronized @Nullable LongList getPendingRetirements(long lifecycleIdentity) {
        return pendingRetirements.getQuick(bindGeneration(lifecycleIdentity));
    }

    public synchronized int incrementSweepsSinceOrphanScan(long lifecycleIdentity) {
        final int slot = bindGeneration(lifecycleIdentity);
        final int count = sweepsSinceOrphanScan.getQuick(slot) + 1;
        sweepsSinceOrphanScan.setQuick(slot, count);
        return count;
    }

    public synchronized boolean isOrphanScanCompleted(long lifecycleIdentity) {
        final int slot = bindGeneration(lifecycleIdentity);
        return (flags.getQuick(slot) & FLAG_ORPHAN_SCAN_COMPLETED) != 0;
    }

    public synchronized boolean isOrphanScanNeeded(long lifecycleIdentity) {
        final int slot = bindGeneration(lifecycleIdentity);
        return (flags.getQuick(slot) & FLAG_ORPHAN_SCAN_NEEDED) != 0;
    }

    public synchronized boolean isReconciled(long lifecycleIdentity) {
        final int slot = bindGeneration(lifecycleIdentity);
        return (flags.getQuick(slot) & FLAG_RECONCILED) != 0;
    }

    public synchronized void markOrphanRisk(long lifecycleIdentity) {
        final int slot = bindGeneration(lifecycleIdentity);
        flags.setQuick(slot, flags.getQuick(slot) | FLAG_ORPHAN_SCAN_NEEDED);
    }

    public synchronized void markOrphanScanCompleted(
            long lifecycleIdentity,
            boolean hasFailures
    ) {
        final int slot = bindGeneration(lifecycleIdentity);
        int currentFlags = flags.getQuick(slot) | FLAG_ORPHAN_SCAN_COMPLETED;
        if (!hasFailures) {
            currentFlags &= ~FLAG_ORPHAN_SCAN_NEEDED;
        }
        flags.setQuick(slot, currentFlags);
        sweepsSinceOrphanScan.setQuick(slot, 0);
    }

    public synchronized void markReconciled(long lifecycleIdentity) {
        final int slot = bindGeneration(lifecycleIdentity);
        flags.setQuick(slot, flags.getQuick(slot) | FLAG_RECONCILED);
    }

    public synchronized void replacePendingRetirements(
            long lifecycleIdentity,
            LongList segmentIds
    ) {
        final int slot = bindGeneration(lifecycleIdentity);
        if (segmentIds.size() == 0) {
            recyclePendingRetirements(slot);
            return;
        }
        LongList pending = pendingRetirements.getQuick(slot);
        if (pending == null) {
            if (retirementListPool.size() > 0) {
                final int poolIndex = retirementListPool.size() - 1;
                pending = retirementListPool.getQuick(poolIndex);
                retirementListPool.remove(poolIndex);
            } else {
                pending = new LongList();
            }
            pendingRetirements.setQuick(slot, pending);
        } else {
            pending.clear();
        }
        pending.add(segmentIds);
    }

    /**
     * Removes every live entry for a dropped lifecycle identity. A later CREATE starts
     * from an empty slot; its first pending list reuses the displaced shell.
     */
    public synchronized void reset(long lifecycleIdentity) {
        final int mapIndex = slotsByLifecycleId.keyIndex(lifecycleIdentity);
        if (mapIndex < 0) {
            final int slot = slotsByLifecycleId.valueAt(mapIndex);
            recyclePendingRetirements(slot);
            flags.setQuick(slot, 0);
            sweepsSinceOrphanScan.setQuick(slot, 0);
            slotsByLifecycleId.removeAt(mapIndex);
            freeSlots.add(slot);
        }
    }

    @TestOnly
    public synchronized int getActiveGenerationCountForTest() {
        return slotsByLifecycleId.size();
    }

    @TestOnly
    public synchronized int getPendingRetirementIdentityForTest(long lifecycleIdentity) {
        final LongList pending = getPendingRetirements(lifecycleIdentity);
        return pending == null ? 0 : System.identityHashCode(pending);
    }

    @TestOnly
    public synchronized int getRetirementPoolSizeForTest() {
        return retirementListPool.size();
    }

    @TestOnly
    public synchronized int getSweepCountForTest(long lifecycleIdentity) {
        return sweepsSinceOrphanScan.getQuick(bindGeneration(lifecycleIdentity));
    }

    private int bindGeneration(long lifecycleIdentity) {
        if (lifecycleIdentity < 0) {
            throw new IllegalArgumentException("negative live view checkpoint lifecycle identity");
        }
        final int mapIndex = slotsByLifecycleId.keyIndex(lifecycleIdentity);
        if (mapIndex < 0) {
            return slotsByLifecycleId.valueAt(mapIndex);
        }

        final int slot;
        if (freeSlots.size() > 0) {
            slot = freeSlots.getLast();
            freeSlots.removeIndex(freeSlots.size() - 1);
            flags.setQuick(slot, 0);
            pendingRetirements.setQuick(slot, null);
            sweepsSinceOrphanScan.setQuick(slot, 0);
        } else {
            slot = flags.size();
            flags.add(0);
            pendingRetirements.add(null);
            sweepsSinceOrphanScan.add(0);
        }
        slotsByLifecycleId.putAt(mapIndex, lifecycleIdentity, slot);
        return slot;
    }

    private void recyclePendingRetirements(int slot) {
        final LongList pending = pendingRetirements.getQuick(slot);
        if (pending != null) {
            pending.clear();
            pendingRetirements.setQuick(slot, null);
            retirementListPool.add(pending);
        }
    }
}
