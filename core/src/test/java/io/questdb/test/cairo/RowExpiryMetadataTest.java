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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Verifies that the per-table row-expiry policy (predicate + cleanup interval) is persisted to and
 * read back from the trailing variable-length section of the {@code _meta} file. Covers both _meta
 * serializers (CREATE path and ALTER-rewrite path) and both readers.
 */
public class RowExpiryMetadataTest extends AbstractCairoTest {

    private static final long MICROS_PER_MINUTE = 60_000_000L;

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
}
