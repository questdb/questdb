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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that the per-object row-expiry policy (predicate + cleanup interval) is persisted to and read
 * back from the trailing variable-length section of the {@code _meta} file. EXPIRE ROWS is now
 * MATERIALIZED-VIEW-ONLY (rejected on plain tables / CTAS / LIKE), so every policy round-trip is exercised
 * through a PASSTHROUGH materialized view over a plain base table. The predecessor of this class
 * (operating on plain {@code create table ... EXPIRE ROWS}) was deleted in 165f6becc2 when EXPIRE became
 * mat-view-only; this restores the _meta round-trip + backward-compat coverage adapted to mat views.
 * <p>
 * Mat views are WAL tables, so the _meta serializers/readers are shared with plain tables — the policy
 * section lives in the same trailing region. The two _meta serializers (CREATE path, ALTER-rewrite path)
 * and both readers (reader metadata, writer metadata) are covered.
 */
public class RowExpiryMetadataTest extends AbstractCairoTest {

    private static final long MICROS_PER_MINUTE = 60_000_000L;
    // _meta column-flag layout (mirrors TableUtils, whose members are package-private): the long flags word
    // sits at META_OFFSET_COLUMN_TYPES + i*META_COLUMN_DATA_SIZE + 4, and the covering bit is 1<<6.
    private static final int META_FLAG_BIT_COVERING = 1 << 6;

    @Before
    public void setUp() {
        super.setUp();
        // Mat views are gated behind dev mode (as in MatViewExpireRowsTest).
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testReadsOldFormatMetaWithoutZeroingTtlOrMisreadingExpiry() throws Exception {
        // Backward-compat: the LATEST minor version (== EXPIRE_ROWS == 3) must NOT zero TTL on an object
        // written before this feature, and the trailing expiry section must read as ABSENT (null/0) — not
        // as garbage — when the stored minor version predates it. Simulated by downgrading the VIEW's _meta
        // minor-version field (preserving its checksum) on a real passthrough mat view that carries BOTH a
        // TTL (alter materialized view ... set ttl, confirmed supported) and an EXPIRE ROWS policy.
        assertMemoryLeak(() -> {
            execute("create table base (v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view v as (select * from base) EXPIRE ROWS WHEN v < 2.0 CLEANUP EVERY 30m");
            drainWalAndMatViewQueues();
            execute("alter materialized view v set ttl 7 days");
            drainWalAndMatViewQueues();
            final TableToken token = engine.verifyTableName("v");

            // Sanity: at the LATEST minor version both TTL and expiry are present on the view.
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals(7 * 24, metadata.getTtlHoursOrMonths());
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(30 * MICROS_PER_MINUTE, metadata.getExpiryCleanupIntervalMicros());
            }

            // Minor version 1 (TTL exists, but predates EXPIRE ROWS == 3): TTL preserved, expiry absent.
            setMetaFormatMinorVersion(token, (short) 1);
            reloadMetadata();
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("TTL must survive the LATEST bump", 7 * 24, metadata.getTtlHoursOrMonths());
                assertNull("expiry must read as absent on a pre-EXPIRE-ROWS _meta", metadata.getExpiryPredicate());
                assertEquals(0, metadata.getExpiryCleanupIntervalMicros());
            }

            // Minor version 0 (predates TTL too): TTL gated off to 0, expiry still absent — no garbage.
            setMetaFormatMinorVersion(token, (short) 0);
            reloadMetadata();
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals(0, metadata.getTtlHoursOrMonths());
                assertNull(metadata.getExpiryPredicate());
                assertEquals(0, metadata.getExpiryCleanupIntervalMicros());
            }
        });
    }

    @Test
    public void testExpiryRoundTripsWithCoveringIndexColumn() throws Exception {
        // THE KEY OFFSET-WALK GUARD. The trailing expiry section sits AFTER the per-column covering-index
        // section in _meta, so TableUtils.getMetaExpiryPolicyOffset must skip a NON-EMPTY covering section to
        // locate the policy. Every other expiry test uses an empty covering section, so this is the only guard
        // over a non-empty one.
        //
        // PATH TAKEN: a materialized view CANNOT carry a posting/covering INCLUDE index — the mat-view CREATE
        // grammar rejects "index type posting include (...)" ("unexpected token [index]"), and the only index
        // form a mat view accepts (", index (col)") is a plain symbol index that produces NO covering section.
        // So this is a LOW-LEVEL test: build a real _meta with a NON-EMPTY covering section on a PLAIN table
        // (posting INCLUDE), then PATCH a trailing EXPIRE policy onto that _meta in place (the create path
        // already wrote an empty policy there) and re-read it through the public reader. The reader runs the
        // real getMetaExpiryPolicyOffset over the non-empty covering section; if the offset walk regresses and
        // fails to skip it, the reader lands on the wrong bytes and reads back null / a different interval, so
        // the assertions below FAIL. (The patch helper mirrors the walk only to find WHERE to write; the
        // verification is done entirely by the SUT reader.)
        assertMemoryLeak(() -> {
            execute("create table t (" +
                    "s symbol index type posting include (v), v double, ts timestamp" +
                    ") timestamp(ts) partition by day wal");

            final TableToken token = engine.verifyTableName("t");

            // Sanity: the POSTING INCLUDE produced a non-empty covering section on 's'.
            final int columnCount;
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                final IntList covering = metadata.getColumnMetadata(metadata.getColumnIndex("s")).getCoveringColumnIndices();
                assertNotNull("expected a covering-index section", covering);
                assertTrue("expected a NON-EMPTY covering-index section", covering.size() > 0);
                columnCount = metadata.getColumnCount();
                // The freshly-created table has the LATEST minor version (>= EXPIRE_ROWS), so the trailing
                // policy section is already present (written empty); patching it in place is enough.
                assertNull(metadata.getExpiryPredicate());
                assertEquals(0, metadata.getExpiryCleanupIntervalMicros());
            }

            // Patch a real policy onto the trailing section (AFTER the covering section), then re-read.
            patchExpiryPolicy(token, columnCount, "v < 2.0", 30 * MICROS_PER_MINUTE);
            reloadMetadata();

            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                // The covering section must still be intact (offset walk must not have clobbered it).
                final IntList covering = metadata.getColumnMetadata(metadata.getColumnIndex("s")).getCoveringColumnIndices();
                assertNotNull(covering);
                assertTrue("covering section must survive the patch", covering.size() > 0);
                // And the policy must read back from PAST the non-empty covering section.
                assertEquals("offset walk must skip the non-empty covering section", "v < 2.0", metadata.getExpiryPredicate());
                assertEquals(30 * MICROS_PER_MINUTE, metadata.getExpiryCleanupIntervalMicros());
            }
            // Writer metadata reader (second reader) must agree.
            try (TableWriter writer = engine.getWriter(token, "test")) {
                assertEquals("v < 2.0", writer.getExpiryPredicate());
                assertEquals(30 * MICROS_PER_MINUTE, writer.getExpiryCleanupIntervalMicros());
            }
        });
    }

    @Test
    public void testCreateMatViewWithExpiryPersists() throws Exception {
        // Round-trip persistence: create a passthrough view with a policy, drop pooled readers, and assert the
        // predicate + interval read back from disk via BOTH the reader metadata and the writer metadata.
        assertMemoryLeak(() -> {
            execute("create table base (s symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view v as (select * from base) EXPIRE ROWS WHEN v < 2.0 CLEANUP EVERY 30m");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("v");

            // Read back via the reader metadata (reader #1).
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(30 * MICROS_PER_MINUTE, metadata.getExpiryCleanupIntervalMicros());
            }

            // Re-open from disk (drop pooled readers) to confirm it round-trips from the file.
            engine.releaseInactive();
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(30 * MICROS_PER_MINUTE, metadata.getExpiryCleanupIntervalMicros());
            }

            // Read back via the writer metadata (reader #2).
            try (TableWriter writer = engine.getWriter(token, "test")) {
                assertEquals("v < 2.0", writer.getExpiryPredicate());
                assertEquals(30 * MICROS_PER_MINUTE, writer.getExpiryCleanupIntervalMicros());
            }
        });
    }

    @Test
    public void testExpirySurvivesUnrelatedAlter() throws Exception {
        // The policy must survive an unrelated ALTER that rewrites _meta via the second serializer. A mat
        // view rejects ADD COLUMN / structural ALTERs, so we use a supported metadata ALTER (set ttl), which
        // still forces a full _meta rewrite. The expiry section must be re-emitted byte-consistently.
        assertMemoryLeak(() -> {
            execute("create table base (s symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view v as (select * from base) EXPIRE ROWS WHEN v < 2.0");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("v");
            // Default cleanup interval is 1h when CLEANUP EVERY is omitted.
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(3_600_000_000L, metadata.getExpiryCleanupIntervalMicros());
            }

            // Force a _meta rewrite via the ALTER-rewrite serializer.
            execute("alter materialized view v set ttl 3 days");
            drainWalAndMatViewQueues();

            engine.releaseInactive();
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("predicate must survive an unrelated ALTER", "v < 2.0", metadata.getExpiryPredicate());
                assertEquals(3_600_000_000L, metadata.getExpiryCleanupIntervalMicros());
                assertEquals("the ALTER must have taken effect", 3 * 24, metadata.getTtlHoursOrMonths());
            }
            try (TableWriter writer = engine.getWriter(token, "test")) {
                assertEquals("v < 2.0", writer.getExpiryPredicate());
                assertEquals(3_600_000_000L, writer.getExpiryCleanupIntervalMicros());
            }
        });
    }

    @Test
    public void testNoExpiryByDefault() throws Exception {
        // A view created WITHOUT a policy reads null/0 from every reader, on disk too.
        assertMemoryLeak(() -> {
            execute("create table base (s symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view v as (select * from base)");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("v");

            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertNull(metadata.getExpiryPredicate());
                assertEquals(0, metadata.getExpiryCleanupIntervalMicros());
            }

            engine.releaseInactive();
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertNull(metadata.getExpiryPredicate());
                assertEquals(0, metadata.getExpiryCleanupIntervalMicros());
            }

            try (TableWriter writer = engine.getWriter(token, "test")) {
                assertNull(writer.getExpiryPredicate());
                assertEquals(0, writer.getExpiryCleanupIntervalMicros());
            }
        });
    }

    // ---- helpers ----

    // Patches a non-empty expiry policy (predicate string + cleanup interval) onto the trailing section of an
    // object's _meta, in place. The CREATE path already wrote an empty policy there (META_FORMAT_MINOR_VERSION
    // == EXPIRE_ROWS), so this overwrites it. The write OFFSET is found by mirroring getMetaExpiryPolicyOffset
    // (column names section, then the per-column covering section); the VERIFICATION is done by the SUT reader,
    // which independently re-walks that offset — so if the SUT's walk regresses, the reader mis-reads.
    private void patchExpiryPolicy(TableToken token, int columnCount, String predicate, long cleanupIntervalMicros) {
        try (
                MemoryMARW mem = Vm.getCMARWInstance();
                Path path = new Path()
        ) {
            path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);

            // Skip the column-names section.
            long offset = TableUtils.getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                final int strLen = mem.getInt(offset);
                offset += Vm.getStorageLength(strLen);
            }
            // Skip the per-column covering section (an int count, then count ints, for each covering column).
            for (int i = 0; i < columnCount; i++) {
                if (isColumnCovering(mem, i)) {
                    final int includeCount = mem.getInt(offset);
                    offset += Integer.BYTES;
                    if (includeCount > 0) {
                        offset += (long) includeCount * Integer.BYTES;
                    }
                }
            }
            // Overwrite the trailing policy: putStr(predicate) then putLong(interval). putStr auto-extends.
            mem.putStr(offset, predicate);
            offset += Vm.getStorageLength(predicate);
            mem.putLong(offset, cleanupIntervalMicros);
        }
    }

    private boolean isColumnCovering(MemoryMARW mem, int columnIndex) {
        final long flags = mem.getLong(TableUtils.META_OFFSET_COLUMN_TYPES + columnIndex * TableUtils.META_COLUMN_DATA_SIZE + 4);
        return (flags & META_FLAG_BIT_COVERING) != 0;
    }

    // Drops pooled readers/writers and refreshes the metadata cache so the next read re-reads _meta from disk.
    private void reloadMetadata() {
        engine.releaseInactive();
        try (MetadataCacheWriter w = engine.getMetadataCache().writeLock()) {
            w.clearCache();
        }
        engine.getMetadataCache().onStartupAsyncHydrator();
    }

    // Rewrites the _meta minor-version high short (preserving the low-short checksum so isMetaFormatAtLeast
    // still trusts the field), simulating an object written by an older QuestDB that did not know about the
    // given version's trailing/scalar fields. Verbatim from the deleted predecessor.
    private void setMetaFormatMinorVersion(TableToken token, short version) {
        try (
                MemoryMARW mem = Vm.getCMARWInstance();
                Path path = new Path()
        ) {
            path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            final int field = mem.getInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION);
            mem.putInt(
                    TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION,
                    Numbers.encodeLowHighShorts(Numbers.decodeLowShort(field), version)
            );
        }
    }
}
