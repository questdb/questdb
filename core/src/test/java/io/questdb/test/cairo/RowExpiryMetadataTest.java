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
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that the per-table row-expiry policy (predicate + cleanup interval) is persisted to and
 * read back from the trailing variable-length section of the {@code _meta} file. Covers both _meta
 * serializers (CREATE path and ALTER-rewrite path) and both readers.
 */
public class RowExpiryMetadataTest extends AbstractCairoTest {

    private static final long MICROS_PER_MINUTE = 60_000_000L;

    @Test
    public void testReadsOldFormatMetaWithoutZeroingTtlOrMisreadingExpiry() throws Exception {
        // Backward-compat: bumping META_FORMAT_MINOR_VERSION_LATEST to 2 (EXPIRE ROWS) must NOT zero TTL on
        // a table written before this feature, and the trailing expiry section must read as absent (null/0)
        // — not as garbage — when the stored minor version predates it. Simulated by downgrading the _meta
        // minor-version field (preserving its checksum) on a real table; this is the on-disk-format safety
        // that cannot be patched after a customer's old table is mis-read.
        assertMemoryLeak(() -> {
            execute("create table t (v double, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("alter table t set ttl 7 days");
            execute("alter table t set expire rows when v < 2.0 cleanup every 30m");
            final TableToken token = engine.verifyTableName("t");

            // Sanity: at the LATEST minor version both TTL and expiry are present.
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals(7 * 24, metadata.getTtlHoursOrMonths());
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
            }

            // Minor version 1 (TTL exists, but predates EXPIRE ROWS = 2): TTL preserved, expiry absent.
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
        // The trailing expiry section sits AFTER the per-column covering-index section in _meta. A POSTING
        // index that INCLUDEs a column produces a NON-EMPTY covering section, so getMetaExpiryPolicyOffset
        // must skip past it to locate the policy. Every other expiry test uses an empty covering section,
        // so this guards the offset walk over a non-empty one (both _meta readers + the metadata cache).
        assertMemoryLeak(() -> {
            execute("create table t (" +
                    "s symbol index type posting include (v), v double, ts timestamp" +
                    ") timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN v < 2.0 CLEANUP EVERY 30m");

            final TableToken token = engine.verifyTableName("t");

            // Re-read from disk (drop pooled readers) so the offset walk runs against the file.
            engine.releaseInactive();
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                // Sanity: the POSTING INCLUDE produced a non-empty covering section, so the offset walk is
                // genuinely tested over it (otherwise this would degenerate to the empty-covering case).
                final IntList covering = metadata.getColumnMetadata(metadata.getColumnIndex("s")).getCoveringColumnIndices();
                assertTrue("expected a non-empty covering-index section", covering != null && covering.size() > 0);
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(30 * MICROS_PER_MINUTE, metadata.getExpiryCleanupIntervalMicros());
            }
            try (TableWriter writer = engine.getWriter(token, "test")) {
                assertEquals("v < 2.0", writer.getExpiryPredicate());
                assertEquals(30 * MICROS_PER_MINUTE, writer.getExpiryCleanupIntervalMicros());
            }
        });
    }

    @Test
    public void testCreateTableLikeInheritsExpiryPolicy() throws Exception {
        // CREATE TABLE ... (LIKE src) copies src's EXPIRE ROWS policy, and the LIKE create path
        // re-validates it against the (copied) columns before creating the table (a distinct validation
        // branch from plain/CTAS creates). The policy must round-trip onto the new table.
        assertMemoryLeak(() -> {
            execute("create table src (s symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN v < 2.0 CLEANUP EVERY 30m");
            execute("create table cpy (like src)");

            final TableToken token = engine.verifyTableName("cpy");
            engine.releaseInactive();
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(30 * MICROS_PER_MINUTE, metadata.getExpiryCleanupIntervalMicros());
            }
        });
    }

    @Test
    public void testCreateTableWithExpiryPersists() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table t (s symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                            "EXPIRE ROWS WHEN v < 2.0 CLEANUP EVERY 30m"
            );

            final TableToken token = engine.verifyTableName("t");

            // Read back via the reader metadata (Reader #1).
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

            // Read back via the writer metadata (Reader #2).
            try (TableWriter writer = engine.getWriter(token, "test")) {
                assertEquals("v < 2.0", writer.getExpiryPredicate());
                assertEquals(30 * MICROS_PER_MINUTE, writer.getExpiryCleanupIntervalMicros());
            }
        });
    }

    @Test
    public void testTtlAndExpiryCoexist() throws Exception {
        // TTL (an older minor-version _meta scalar) and the new trailing expiry section must both
        // round-trip. Guards the version-bump interaction: a broken offset walk or a mis-gated TTL read
        // (e.g. the LATEST bump zeroing TTL) — or the ALTER rewrite dropping TTL when adding expiry —
        // would surface here. bypass wal so the ALTERs rewrite _meta synchronously via serializer #2.
        assertMemoryLeak(() -> {
            execute("create table t (s symbol, v double, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("alter table t set ttl 7 days");
            execute("alter table t set expire rows when v < 2.0 cleanup every 30m");

            final TableToken token = engine.verifyTableName("t");

            engine.releaseInactive();
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(30 * MICROS_PER_MINUTE, metadata.getExpiryCleanupIntervalMicros());
                assertEquals(7 * 24, metadata.getTtlHoursOrMonths()); // 7 days = 168 hours
            }
            try (TableWriter writer = engine.getWriter(token, "test")) {
                assertEquals("v < 2.0", writer.getExpiryPredicate());
                assertEquals(7 * 24, writer.getTtlHoursOrMonths());
            }
        });
    }

    @Test
    public void testExpirySurvivesAlter() throws Exception {
        assertMemoryLeak(() -> {
            // Non-WAL table so that ALTER goes straight through TableWriter and rewrites _meta
            // via the ALTER-rewrite serializer (serializer #2) synchronously.
            execute(
                    "create table t (s symbol, v double, ts timestamp) timestamp(ts) partition by day bypass wal " +
                            "EXPIRE ROWS WHEN v < 2.0"
            );

            final TableToken token = engine.verifyTableName("t");

            // Sanity: present right after CREATE (serializer #1). Default cleanup interval is 1h.
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(3_600_000_000L, metadata.getExpiryCleanupIntervalMicros());
            }

            // Force a _meta rewrite via serializer #2.
            execute("alter table t add column x int");

            // Read back via reader metadata (proves serializer #2 + Reader #2/#1 stay byte-consistent).
            engine.releaseInactive();
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("predicate must survive ALTER", "v < 2.0", metadata.getExpiryPredicate());
                assertEquals(3_600_000_000L, metadata.getExpiryCleanupIntervalMicros());
                // The new column must also be present, proving the ALTER actually happened.
                assertEquals(3, metadata.getColumnIndex("x"));
            }

            // And via the writer metadata (Reader #2).
            try (TableWriter writer = engine.getWriter(token, "test")) {
                assertEquals("v < 2.0", writer.getExpiryPredicate());
                assertEquals(3_600_000_000L, writer.getExpiryCleanupIntervalMicros());
            }
        });
    }

    @Test
    public void testNoExpiryByDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (s symbol, v double, ts timestamp) timestamp(ts) partition by day wal");

            final TableToken token = engine.verifyTableName("t");

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

    // Drops pooled readers/writers and refreshes the metadata cache so the next read re-reads _meta from disk.
    private void reloadMetadata() {
        engine.releaseInactive();
        try (MetadataCacheWriter w = engine.getMetadataCache().writeLock()) {
            w.clearCache();
        }
        engine.getMetadataCache().onStartupAsyncHydrator();
    }

    // Rewrites the _meta minor-version high short (preserving the low-short checksum so isMetaFormatAtLeast
    // still trusts the field), simulating a table written by an older QuestDB that did not know about the
    // given version's trailing/scalar fields.
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
