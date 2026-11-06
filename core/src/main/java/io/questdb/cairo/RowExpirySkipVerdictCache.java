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

package io.questdb.cairo;

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.LongLongHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

/**
 * SKIP verdicts that {@link RowExpiryCleanupJob} keeps across sweeps for policies it checks by counting
 * rows. A verdict maps a view's logical partition floor to the content generation the job counted, so a
 * later sweep skips the survivor count for a partition whose content has not changed since.
 * <p>
 * The cache holds at most {@code cairo.mat.view.row.expiry.cleanup.max.cached.partitions} verdicts across
 * all views. Past that many partitions the cache stops accepting new ones and keeps the ones it already
 * holds.
 * <p>
 * Not thread-safe: the job owns one instance and calls it only from its {@code runSerially()} tick.
 */
public class RowExpirySkipVerdictCache {
    // The generation a lookup returns for a floor without a verdict.
    private static final long NO_GENERATION = Long.MIN_VALUE;
    private final int maxCachedPartitions;
    private int cachedPartitionCount;
    // Per directory name: the predicate those SKIP verdicts were computed for, and floor -> generation.
    // A dropped view, a changed predicate, a partition the job wipes, or a floor absent from a sweep's
    // non-active snapshot removes its own entries. The cache is never cleared as a whole while any
    // view keeps a policy, so a full cache still skips the partitions it already holds.
    private CharSequenceObjHashMap<ViewVerdicts> viewVerdicts;

    public RowExpirySkipVerdictCache(int maxCachedPartitions) {
        this.maxCachedPartitions = maxCachedPartitions;
        this.viewVerdicts = new CharSequenceObjHashMap<>(4);
    }

    /**
     * Creates a cache over {@code viewVerdicts}, so a test can count or fail lookups on the per-directory
     * map. The map must be empty.
     */
    @TestOnly
    @SuppressWarnings("unchecked")
    public RowExpirySkipVerdictCache(int maxCachedPartitions, CharSequenceObjHashMap<?> viewVerdicts) {
        assert viewVerdicts.size() == 0;
        this.maxCachedPartitions = maxCachedPartitions;
        this.viewVerdicts = (CharSequenceObjHashMap<ViewVerdicts>) viewVerdicts;
    }

    /**
     * Drops every view's verdicts. The job calls it when no view carries a policy.
     */
    public void clear() {
        if (viewVerdicts.size() > 0) {
            viewVerdicts.clear();
            cachedPartitionCount = 0;
        }
    }

    /**
     * Removes the verdict for one partition, which the job has just wiped.
     */
    public void forget(TableToken tableToken, long floorTs) {
        final ViewVerdicts verdicts = viewVerdicts.get(tableToken.getDirName());
        if (verdicts == null) {
            return;
        }
        final int index = verdicts.generations.keyIndex(floorTs);
        if (index < 0) {
            verdicts.generations.removeAt(index);
            cachedPartitionCount--;
        }
    }

    /**
     * Returns the number of SKIP verdicts the cache holds across all views, which
     * {@code cairo.mat.view.row.expiry.cleanup.max.cached.partitions} bounds.
     */
    public int getCachedPartitionCount() {
        return cachedPartitionCount;
    }

    /**
     * Returns the floor -> generation map of the view's SKIP verdicts, or null when the view has no entry.
     */
    @TestOnly
    public LongLongHashMap getGenerations(TableToken tableToken) {
        final ViewVerdicts verdicts = viewVerdicts.get(tableToken.getDirName());
        return verdicts != null ? verdicts.generations : null;
    }

    /**
     * Returns the predicate the view's SKIP verdicts belong to, or null when the view has no entry.
     */
    @TestOnly
    public String getPredicate(TableToken tableToken) {
        final ViewVerdicts verdicts = viewVerdicts.get(tableToken.getDirName());
        return verdicts != null ? verdicts.predicate : null;
    }

    /**
     * Returns the per-directory map. A test compares the map and its values by identity.
     */
    @TestOnly
    public CharSequenceObjHashMap<?> getViewVerdicts() {
        return viewVerdicts;
    }

    /**
     * Reports whether the view holds a SKIP verdict for {@code floorTs} at {@code generation}. The lookup
     * creates the view's entry for {@code predicate}, and drops the view's verdicts when they belong to
     * another predicate.
     */
    public boolean isSkipCurrent(TableToken tableToken, String predicate, long floorTs, long generation) {
        final ViewVerdicts verdicts = viewVerdicts(tableToken, predicate);
        return verdicts.generations.get(floorTs) == generation;
    }

    /**
     * Keeps the entries of the views in the discovery snapshot and drops the rest. {@code tokens} and
     * {@code predicates} pair by index, and the snapshot holds each policied directory once. A view whose
     * predicate changed keeps its entry with no verdicts.
     */
    public void prune(ObjList<TableToken> tokens, ObjList<String> predicates) {
        if (viewVerdicts.size() == 0) {
            return;
        }
        // Discovery contains each policied directory once. Probe the existing hash map rather
        // than scanning the discovery snapshot separately for every cached view.
        int retainedCacheCount = 0;
        int retainedPartitionCount = 0;
        for (int i = 0, n = tokens.size(); i < n; i++) {
            final ViewVerdicts verdicts = viewVerdicts.get(tokens.getQuick(i).getDirName());
            if (verdicts != null) {
                final String predicate = predicates.getQuick(i);
                if (!Chars.equals(verdicts.predicate, predicate)) {
                    cachedPartitionCount -= verdicts.generations.size();
                    verdicts.generations.clear();
                    verdicts.predicate = predicate;
                }
                retainedCacheCount++;
                retainedPartitionCount += verdicts.generations.size();
            }
        }
        if (retainedCacheCount == viewVerdicts.size()) {
            return;
        }
        // removeAt also scans the map's key list. Rebuild only when a directory disappears,
        // retaining the same entry objects, including empty ones, for every surviving policy.
        // Leave a spare slot so the last insertion cannot trigger a rehash.
        final CharSequenceObjHashMap<ViewVerdicts> retainedVerdicts = new CharSequenceObjHashMap<>(retainedCacheCount + 1);
        for (int i = 0, n = tokens.size(); i < n; i++) {
            final String dirName = tokens.getQuick(i).getDirName();
            final ViewVerdicts verdicts = viewVerdicts.get(dirName);
            if (verdicts != null) {
                retainedVerdicts.put(dirName, verdicts);
            }
        }
        // Publish evictions and their accounting only after the replacement is complete.
        viewVerdicts = retainedVerdicts;
        cachedPartitionCount = retainedPartitionCount;
    }

    /**
     * Installs {@code generations} as the view's SKIP verdicts for {@code predicate}, as if earlier sweeps
     * had stored them. It replaces the view's verdicts and adjusts the cached-partition count by the
     * difference between the new and the replaced verdict counts.
     */
    @TestOnly
    public void putGenerations(TableToken tableToken, String predicate, LongLongHashMap generations) {
        final ViewVerdicts verdicts = viewVerdicts(tableToken, predicate);
        cachedPartitionCount += generations.size() - verdicts.generations.size();
        verdicts.generations = generations;
    }

    /**
     * Removes SKIP verdicts whose logical floor is absent from {@code floors}.
     * <p>
     * {@code floors} is the view's complete non-active snapshot. A floor missing from it was
     * removed outside the job, TTL included, and its slot can be reused. Floors still in the
     * snapshot stay cached. The snapshot holds each logical floor once, so if the number of
     * snapshot floors the cache holds equals the cache's size, every cached floor is still in the
     * snapshot and the existing map stays as it is. Otherwise the cache builds a new map from the
     * snapshot floors it holds. Either way it makes at most two passes over the snapshot
     * and never removes keys one at a time.
     */
    public void releaseAbsent(TableToken tableToken, LongList floors) {
        final ViewVerdicts verdicts = viewVerdicts.get(tableToken.getDirName());
        if (verdicts == null || verdicts.generations.size() == 0) {
            return;
        }
        final LongLongHashMap generations = verdicts.generations;
        final int floorCount = floors.size();
        if (floorCount == 0) {
            cachedPartitionCount -= generations.size();
            generations.clear();
            return;
        }
        int retainedCount = 0;
        for (int i = 0; i < floorCount; i++) {
            if (generations.keyIndex(floors.getQuick(i)) < 0) {
                retainedCount++;
            }
        }
        if (retainedCount == generations.size()) {
            return;
        }
        final LongLongHashMap retainedGenerations = newGenerations(retainedCount);
        for (int i = 0; i < floorCount; i++) {
            final long floorTs = floors.getQuick(i);
            final int index = generations.keyIndex(floorTs);
            if (index < 0) {
                retainedGenerations.put(floorTs, generations.valueAt(index));
            }
        }
        cachedPartitionCount -= generations.size() - retainedGenerations.size();
        verdicts.generations = retainedGenerations;
    }

    /**
     * Stores a SKIP verdict for {@code floorTs} at {@code generation}.
     */
    public void remember(TableToken tableToken, String predicate, long floorTs, long generation) {
        final ViewVerdicts verdicts = viewVerdicts(tableToken, predicate);
        final int index = verdicts.generations.keyIndex(floorTs);
        // An existing floor is updated in place, including when the cache is full. A new floor past
        // the cap stays uncached and is counted again on the next sweep.
        if (index > -1) {
            if (cachedPartitionCount >= maxCachedPartitions) {
                return;
            }
            cachedPartitionCount++;
        }
        verdicts.generations.putAt(index, floorTs, generation);
    }

    // Maps a logical partition floor to the content generation its SKIP verdict was computed for.
    // LONG_NULL marks an empty slot, and a logical partition floor never takes that value.
    private static LongLongHashMap newGenerations(int capacity) {
        return new LongLongHashMap(capacity, 0.5, Numbers.LONG_NULL, NO_GENERATION);
    }

    private ViewVerdicts viewVerdicts(TableToken tableToken, String predicate) {
        final CharSequence dirName = tableToken.getDirName();
        final int index = viewVerdicts.keyIndex(dirName);
        if (index < 0) {
            final ViewVerdicts verdicts = viewVerdicts.valueAt(index);
            if (!Chars.equals(verdicts.predicate, predicate)) {
                cachedPartitionCount -= verdicts.generations.size();
                verdicts.generations.clear();
                verdicts.predicate = predicate;
            }
            return verdicts;
        }
        final ViewVerdicts verdicts = new ViewVerdicts(predicate);
        viewVerdicts.putAt(index, Chars.toString(dirName), verdicts);
        return verdicts;
    }

    /**
     * SKIP generations for one view directory, valid only for {@link #predicate}.
     */
    private static final class ViewVerdicts {
        LongLongHashMap generations = newGenerations(4);
        String predicate;

        private ViewVerdicts(String predicate) {
            this.predicate = predicate;
        }
    }
}
