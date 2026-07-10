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

package io.questdb.cairo;

import io.questdb.std.LongList;
import io.questdb.std.Mutable;

/**
 * In-memory reverse index of the donor-link families of a single table:
 * a donor partition version {@code (floorTs, nameTxn)} maps to the set of {@code DONOR_LINKED}
 * suffix children that read their column bytes from it through a {@code _dlink} pointer file.
 * <p>
 * The link file removes the kernel's inode refcount (hardlinks keep donor inodes alive
 * automatically), so donor lifetime must be enforced in-app: a donor version dir must NOT be
 * purged while any link child references it, regardless of scoreboard txn. This registry is the
 * writer-side authority for that interlock and for the {@code clearPartitionDonor} unstick
 * decision; the async {@code O3PartitionPurgeJob} derives the same set independently from disk
 * (scanning {@code _txn} for {@code DONOR_LINKED} children and reading their link files).
 * <p>
 * Only LINK-FILE edges are recorded. Hardlink-mode splits are left out entirely, so hardlink-mode
 * purge behavior is unchanged (the kernel link count remains the sole retention authority there).
 * <p>
 * Link resolution is single-level: a grandchild's link file points at the real byte-holding donor
 * version, not at an intermediate link child, so a link child never donates to another link child.
 * A partition is therefore either a real donor (has byte files, zero or more outgoing edges) or a
 * link child (holds only a {@code _dlink}, exactly one incoming edge) -- never both.
 * <p>
 * The registry is maintained on the single writer thread. Splits and squashes are rare and already
 * dominated by mkdir / link / fsync I/O, so the O(n) flat-list scans here (n = number of live link
 * children, typically tens) are negligible and keep the structure zero-allocation.
 */
public class PartitionFamilyRegistry implements Mutable {
    static final long NO_DONOR = Long.MIN_VALUE;
    // Flat triples: [donorFloorTs, donorNameTxn, childFloorTs].
    private static final int STRIDE = 3;
    private final LongList edges = new LongList();

    /**
     * Records a link edge from a donor version {@code (donorTs, donorNameTxn)} to a suffix child
     * at floor timestamp {@code childTs}. Idempotent: re-adding the same edge (e.g. on rebuild) is
     * a no-op. A child that already had a (different) donor is repointed to the new one, so the
     * forward mapping stays single-valued.
     */
    public void addLinkChild(long donorTs, long donorNameTxn, long childTs) {
        final int existing = indexOfChild(childTs);
        if (existing > -1) {
            edges.setQuick(existing, donorTs);
            edges.setQuick(existing + 1, donorNameTxn);
            return;
        }
        edges.add(donorTs);
        edges.add(donorNameTxn);
        edges.add(childTs);
    }

    @Override
    public void clear() {
        edges.clear();
    }

    /**
     * @return the donor floor timestamp for the given link child, or {@link #NO_DONOR} if the
     * child is unknown.
     */
    public long donorTsOfChild(long childTs) {
        final int i = indexOfChild(childTs);
        return i > -1 ? edges.getQuick(i) : NO_DONOR;
    }

    /**
     * @return the child floor timestamp of the edge at flat position {@code i * STRIDE}; used with
     * {@link #donorTsAt}/{@link #donorNameTxnAt} to iterate for leak detection.
     */
    public long childTsAt(int i) {
        return edges.getQuick(i * STRIDE + 2);
    }

    public long donorNameTxnAt(int i) {
        return edges.getQuick(i * STRIDE + 1);
    }

    public long donorTsAt(int i) {
        return edges.getQuick(i * STRIDE);
    }

    /**
     * @return true if any live link child references the donor version {@code (donorTs, donorNameTxn)}.
     * This is the purge interlock predicate: such a version dir must never be removed.
     */
    public boolean hasLinkChildFor(long donorTs, long donorNameTxn) {
        for (int i = 0, n = edges.size(); i < n; i += STRIDE) {
            if (edges.getQuick(i) == donorTs && edges.getQuick(i + 1) == donorNameTxn) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return the number of live link children of the donor version {@code (donorTs, donorNameTxn)}.
     */
    public int linkChildCountFor(long donorTs, long donorNameTxn) {
        int count = 0;
        for (int i = 0, n = edges.size(); i < n; i += STRIDE) {
            if (edges.getQuick(i) == donorTs && edges.getQuick(i + 1) == donorNameTxn) {
                count++;
            }
        }
        return count;
    }

    /**
     * Repoints every child of the donor version {@code (oldTs, oldNameTxn)} onto a fresh compact
     * donor {@code (newTs, newNameTxn)} (rebase). Children keep their identity; only the byte source
     * changes.
     */
    public void repointDonor(long oldTs, long oldNameTxn, long newTs, long newNameTxn) {
        for (int i = 0, n = edges.size(); i < n; i += STRIDE) {
            if (edges.getQuick(i) == oldTs && edges.getQuick(i + 1) == oldNameTxn) {
                edges.setQuick(i, newTs);
                edges.setQuick(i + 1, newNameTxn);
            }
        }
    }

    /**
     * Drops the edge for a link child (the child was folded, materialized, dropped or superseded).
     *
     * @return true if the child existed and was removed.
     */
    public boolean removeChild(long childTs) {
        final int i = indexOfChild(childTs);
        if (i > -1) {
            edges.removeIndexBlock(i, STRIDE);
            return true;
        }
        return false;
    }

    /**
     * Drops every edge whose donor is {@code (donorTs, donorNameTxn)}. Used when a donor version is
     * torn down wholesale (e.g. TRUNCATE, or a rebase that materializes all its children).
     */
    public void removeDonorVersion(long donorTs, long donorNameTxn) {
        for (int i = edges.size() - STRIDE; i >= 0; i -= STRIDE) {
            if (edges.getQuick(i) == donorTs && edges.getQuick(i + 1) == donorNameTxn) {
                edges.removeIndexBlock(i, STRIDE);
            }
        }
    }

    /**
     * @return the number of link edges (one per live link child).
     */
    public int size() {
        return edges.size() / STRIDE;
    }

    private int indexOfChild(long childTs) {
        for (int i = 0, n = edges.size(); i < n; i += STRIDE) {
            if (edges.getQuick(i + 2) == childTs) {
                return i;
            }
        }
        return -1;
    }
}
