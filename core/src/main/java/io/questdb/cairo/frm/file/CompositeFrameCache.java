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

package io.questdb.cairo.frm.file;

import io.questdb.cairo.TxReader;
import io.questdb.cairo.frm.Frame;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

/**
 * The writable and read-only frames a {@link io.questdb.cairo.TableWriter} last ran merge-append plans through, kept
 * open across commits for the few partitions a stream of inserts keeps landing on - the last one and the one before
 * it, typically. A plan that finds its partition here starts with every column file open and every read-only mapping
 * in place, so a commit opens, maps and allocates nothing a previous commit already did.
 * <p>
 * Nothing is kept by default. A plan that went through MARKS its entry; once the insert commit that ran it has landed
 * in {@code _txn}, the writer CERTIFIES the cache: every marked entry is stamped with that table txn, and every other
 * entry is closed. A lookup reuses an entry only when its stamp is the table's current txn - so any commit at all
 * between the two inserts, whatever it was (an ALTER, an UPDATE, a writer command, compaction, a squash, a
 * truncate), leaves the entry unusable without anything having to name it. Within the inserting commit itself the
 * writer certifies nothing when something other than the plans may have written the files the frames hold; see
 * {@link #certify}. On top of the stamp an entry is keyed by the partition's name txn, the table's metadata version and
 * the partition's physical extent, and a lookup under any other key closes it. All of it coarse on purpose: the cache
 * is a fast path for the steady state, and anything that is not the steady state simply reopens.
 * <p>
 * Partition tasks run on worker threads, one task per partition at a time, so an entry has at most one user and the
 * lookup is the only thing several threads contend on.
 */
public class CompositeFrameCache implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(CompositeFrameCache.class);
    private final Entry[] entries;
    private long clock;

    public CompositeFrameCache(int capacity) {
        assert capacity > 0;
        this.entries = new Entry[capacity];
        for (int i = 0; i < capacity; i++) {
            entries[i] = new Entry();
        }
    }

    /**
     * Hands out the entry for {@code partitionTimestamp}: a populated one when the cache holds this version of the
     * partition under this metadata version, at exactly the extent the last plan through it left, certified at the
     * table's current txn - otherwise an empty one for the caller to {@link #fill}. Null when every slot is in use by
     * another partition's task, in which case the caller runs with frames of its own. Every entry returned is in use
     * by the caller until it {@link #release}s it.
     *
     * @param tableTxn the table's committed txn, the one the commit about to write builds on
     */
    public synchronized Entry acquire(long partitionTimestamp, long nameTxn, long metadataVersion, long extent, long tableTxn) {
        Entry victim = null;
        for (int i = 0, n = entries.length; i < n; i++) {
            final Entry e = entries[i];
            if (e.inUse) {
                continue;
            }
            if (e.partitionTimestamp == partitionTimestamp) {
                if (e.certifiedTxn != tableTxn || e.nameTxn != nameTxn || e.metadataVersion != metadataVersion || e.extent != extent) {
                    // Not certified at the txn this commit builds on - something committed since, or the commit that
                    // ran the last plan never certified it - or another version of the partition, another table
                    // shape, or an extent something other than a plan through this entry moved.
                    e.evict();
                }
                return e.use(partitionTimestamp, nameTxn, metadataVersion, ++clock);
            }
            // The empty slot wins over any populated one; among populated ones the longest idle goes.
            if (victim == null || (victim.target != null && (e.target == null || e.lastUsed < victim.lastUsed))) {
                victim = e;
            }
        }
        if (victim == null) {
            return null;
        }
        victim.evict();
        return victim.use(partitionTimestamp, nameTxn, metadataVersion, ++clock);
    }

    /**
     * Run once the insert commit that ran this commit's plans has landed in {@code _txn}: stamps every entry a plan
     * {@link #release}d this commit with {@code tableTxn}, which makes it reusable by the next commit, and closes every
     * other entry. An entry is not stamped either when its partition is no longer attached under the name txn it was
     * opened at - the commit dropped it, or rebuilt it as a fresh version.
     *
     * @param canKeep false when something other than the plans may have written the frames' files in this same txn;
     *                every entry is closed then
     */
    public synchronized void certify(TxReader txReader, long tableTxn, boolean canKeep) {
        for (int i = 0, n = entries.length; i < n; i++) {
            final Entry e = entries[i];
            if (e.inUse) {
                // Cannot happen at a commit boundary; if it does, the entry closes when it is released.
                e.evictOnRelease = true;
            } else if (canKeep && e.marked && e.target != null
                    && txReader.getPartitionNameTxnByPartitionTimestamp(e.partitionTimestamp, Long.MIN_VALUE) == e.nameTxn) {
                e.certifiedTxn = tableTxn;
                e.marked = false;
            } else {
                e.evict();
            }
        }
    }

    @Override
    public void close() {
        evictAll();
    }

    /**
     * Closes every entry nobody is using and marks the ones in use to close on release.
     */
    public synchronized void evictAll() {
        for (int i = 0, n = entries.length; i < n; i++) {
            final Entry e = entries[i];
            if (e.inUse) {
                e.evictOnRelease = true;
            } else {
                e.evict();
            }
        }
    }

    /**
     * Populates an empty entry {@link #acquire} handed out with the frames the caller opened for it. The frames
     * belong to the cache from here on: it closes them, the caller does not.
     */
    public synchronized void fill(Entry entry, Frame target, Frame source) {
        assert entry.inUse && entry.target == null;
        entry.target = (FrameImpl) target;
        entry.source = (FrameImpl) source;
    }

    /**
     * The number of entries a commit building on {@code tableTxn} would reuse; for tests.
     */
    public synchronized int getReusableCount(long tableTxn) {
        int count = 0;
        for (int i = 0, n = entries.length; i < n; i++) {
            if (entries[i].target != null && entries[i].certifiedTxn == tableTxn) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns an entry {@link #acquire} handed out, with the extent the plan left the partition at. A plan that went
     * through MARKS the entry, which keeps it only until {@link #certify}; until then nothing reuses it. With
     * {@code success} false, or after an {@link #evictAll} that ran while the entry was in use, its frames are closed.
     */
    public synchronized void release(Entry entry, boolean success, long extent) {
        assert entry.inUse;
        entry.inUse = false;
        entry.certifiedTxn = -1;
        if (!success || entry.evictOnRelease) {
            entry.evict();
        } else {
            entry.extent = extent;
            entry.marked = true;
        }
    }

    public static final class Entry {
        // The table txn certify() stamped this entry with; -1 when not certified, which no lookup matches.
        private long certifiedTxn = -1;
        private boolean evictOnRelease;
        // The partition's physical extent E the last plan through this entry left it at.
        private long extent = -1;
        private boolean inUse;
        private long lastUsed;
        // Set when a plan went through this entry, cleared when certify() stamps it.
        private boolean marked;
        private long metadataVersion;
        private long nameTxn;
        private long partitionTimestamp = Long.MIN_VALUE;
        private FrameImpl source;
        private FrameImpl target;

        /**
         * The read-only frame over the partition, or null when the entry is empty.
         */
        public FrameImpl getSource() {
            return source;
        }

        /**
         * The writable frame over the partition, or null when the entry is empty.
         */
        public FrameImpl getTarget() {
            return target;
        }

        public boolean isPopulated() {
            return target != null;
        }

        private void evict() {
            if (target != null) {
                LOG.debug().$("closing cached partition frames [partitionTimestamp=").$ts(partitionTimestamp)
                        .$(", nameTxn=").$(nameTxn)
                        .I$();
            }
            target = Misc.free(target);
            source = Misc.free(source);
            partitionTimestamp = Long.MIN_VALUE;
            nameTxn = -1;
            metadataVersion = -1;
            extent = -1;
            certifiedTxn = -1;
            marked = false;
            evictOnRelease = false;
        }

        private Entry use(long partitionTimestamp, long nameTxn, long metadataVersion, long clock) {
            this.partitionTimestamp = partitionTimestamp;
            this.nameTxn = nameTxn;
            this.metadataVersion = metadataVersion;
            this.lastUsed = clock;
            this.inUse = true;
            return this;
        }
    }
}
