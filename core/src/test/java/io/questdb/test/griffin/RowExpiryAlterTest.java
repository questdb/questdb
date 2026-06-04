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

package io.questdb.test.griffin;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.wal.seq.MetadataServiceStub;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * End-to-end tests for {@code ALTER TABLE t SET EXPIRE ROWS WHEN <pred> [CLEANUP EVERY <dur>]} and
 * {@code ALTER TABLE t DROP EXPIRE}, mirroring the SET TTL ALTER. Verifies that the policy is set on
 * the table metadata, that the read-time row-expiry filter picks the change up via the metadata cache,
 * that DROP clears it, that it survives a reader re-open, that it round-trips through the replication
 * (metadata-change-log) serializer, and that an invalid predicate is rejected at ALTER time.
 * <p>
 * {@code now()} is pinned via {@link #setCurrentMicros(long)} for the time-based predicate cases.
 */
public class RowExpiryAlterTest extends AbstractCairoTest {

    // 2024-01-10T00:00:00.000000Z
    private static final long NOW_MICROS = 1704844800000000L;

    @Test
    public void testAlterDropExpire() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')");
            execute("insert into t values ('BBB', 5.0, '2024-01-09T12:00:00.000000Z')");
            drainWalQueue();

            execute("alter table t set expire rows when v < 2.0");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("t");
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
            }
            // While the policy is active, the v=1.0 row is hidden.
            assertSql(
                    "sym\tv\n" +
                            "BBB\t5.0\n",
                    "select sym, v from t order by sym"
            );

            execute("alter table t drop expire");
            drainWalQueue();

            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertNull(metadata.getExpiryPredicate());
                assertEquals(0, metadata.getExpiryCleanupIntervalMicros());
            }
            // Policy gone: all rows are visible again.
            assertSql(
                    "sym\tv\n" +
                            "AAA\t1.0\n" +
                            "BBB\t5.0\n",
                    "select sym, v from t order by sym"
            );
        });
    }

    @Test
    public void testAlterExpireReplicationRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            final TableToken token = engine.verifyTableName("t");

            final String predicate = "v < 2.0 and sym = 'AAA'";
            final long cleanupMicros = 30 * 60_000_000L;

            // Build a SET_EXPIRE AlterOperation exactly as the compiler would.
            final AlterOperationBuilder builder = new AlterOperationBuilder();
            final AlterOperation op = builder.ofSetExpire(0, token, token.getTableId(), predicate, cleanupMicros).build();
            assertEquals(AlterOperation.SET_EXPIRE, op.getCommand());

            // Serialize then deserialize via the public body serializer used by the metadata-change log,
            // then apply to a capturing service and assert the predicate + interval survived the round-trip.
            try (MemoryCARW mem = new MemoryCARWImpl(1024, 1, MemoryTag.NATIVE_DEFAULT)) {
                op.serializeBody(mem);
                final long len = mem.getAppendOffset();

                final AlterOperation deserialized = new AlterOperation();
                deserialized.deserializeBody(mem, 0, len);
                assertEquals(AlterOperation.SET_EXPIRE, deserialized.getCommand());

                final CapturingMetadataService svc = new CapturingMetadataService();
                deserialized.apply(svc, true);
                assertEquals(predicate, svc.predicate);
                assertEquals(cleanupMicros, svc.cleanupIntervalMicros);
            }

            // And the DROP-EXPIRE encoding (null predicate -> empty string -> null on apply).
            final AlterOperationBuilder dropBuilder = new AlterOperationBuilder();
            final AlterOperation dropOp = dropBuilder.ofSetExpire(0, token, token.getTableId(), null, 0).build();
            try (MemoryCARW mem = new MemoryCARWImpl(1024, 1, MemoryTag.NATIVE_DEFAULT)) {
                dropOp.serializeBody(mem);
                final AlterOperation deserialized = new AlterOperation();
                deserialized.deserializeBody(mem, 0, mem.getAppendOffset());
                final CapturingMetadataService svc = new CapturingMetadataService();
                deserialized.apply(svc, true);
                assertNull(svc.predicate);
                assertEquals(0, svc.cleanupIntervalMicros);
            }
        });
    }

    @Test
    public void testAlterExpireSurvivesReopen() throws Exception {
        assertMemoryLeak(() -> {
            // bypass wal so the ALTER rewrites _meta synchronously through TableWriter.
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("alter table t set expire rows when v < 2.0 cleanup every 15m");

            final TableToken token = engine.verifyTableName("t");

            // Drop pooled readers/writers and re-read from disk.
            engine.releaseInactive();
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(15 * 60_000_000L, metadata.getExpiryCleanupIntervalMicros());
            }
            try (TableWriter writer = engine.getWriter(token, "test")) {
                assertEquals("v < 2.0", writer.getExpiryPredicate());
                assertEquals(15 * 60_000_000L, writer.getExpiryCleanupIntervalMicros());
            }
        });
    }

    @Test
    public void testAlterSetExpire() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // v < 2 -> hidden
            execute("insert into t values ('BBB', 2.5, '2024-01-09T12:00:00.000000Z')"); // v >= 2 -> visible
            execute("insert into t values ('CCC', 0.5, '2024-01-09T18:00:00.000000Z')"); // v < 2 -> hidden
            drainWalQueue();

            execute("alter table t set expire rows when v < 2.0");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("t");
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                // No CLEANUP EVERY given -> defaults to 1 hour.
                assertEquals(3_600_000_000L, metadata.getExpiryCleanupIntervalMicros());
            }

            // The read-time filter (reading the policy from the metadata cache) hides v<2 rows.
            assertSql(
                    "sym\tv\n" +
                            "BBB\t2.5\n",
                    "select sym, v from t order by sym"
            );
        });
    }

    @Test
    public void testAlterSetExpireReplaces() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')");
            execute("insert into t values ('BBB', 5.0, '2024-01-09T12:00:00.000000Z')");
            drainWalQueue();

            // Predicate A: hide v < 2  -> only BBB visible.
            execute("alter table t set expire rows when v < 2.0");
            drainWalQueue();
            assertSql(
                    "sym\tv\n" +
                            "BBB\t5.0\n",
                    "select sym, v from t order by sym"
            );

            // Predicate B replaces A: hide v > 4 -> only AAA visible.
            execute("alter table t set expire rows when v > 4.0");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("t");
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v > 4.0", metadata.getExpiryPredicate());
            }
            assertSql(
                    "sym\tv\n" +
                            "AAA\t1.0\n",
                    "select sym, v from t order by sym"
            );
        });
    }

    @Test
    public void testAlterSetExpireWithCleanupEvery() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // expired
            execute("insert into t values ('BBB', 2.0, '2024-01-09T12:00:00.000000Z')"); // live
            drainWalQueue();

            execute("alter table t set expire rows when ts < dateadd('d', -1, now()) cleanup every 30m");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("t");
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("ts < dateadd('d', -1, now())", metadata.getExpiryPredicate());
                assertEquals(30 * 60_000_000L, metadata.getExpiryCleanupIntervalMicros());
            }

            // Only the live (2024-01-09) row survives the time-based filter.
            assertSql(
                    "sym\tv\n" +
                            "BBB\t2.0\n",
                    "select sym, v from t order by sym"
            );
        });
    }

    @Test
    public void testInvalidPredicateRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            // Error is positioned at the start of the predicate ("no_such_col", index 35).
            assertExceptionNoLeakCheck(
                    "alter table t set expire rows when no_such_col > 5",
                    35,
                    "invalid EXPIRE ROWS predicate"
            );
        });
    }

    /**
     * Minimal {@link MetadataServiceStub} that records the arguments of {@link #setMetaExpiry}. Only the
     * methods left abstract by {@link MetadataServiceStub} are stubbed here; they are never invoked by the
     * SET_EXPIRE apply path.
     */
    private static class CapturingMetadataService implements MetadataServiceStub {
        long cleanupIntervalMicros = -1;
        String predicate = "<unset>";

        @Override
        public void addColumn(CharSequence columnName, int columnType, int symbolCapacity, boolean symbolCacheFlag, byte indexType, int indexValueBlockCapacity, boolean isSequential, boolean isDedupKey, io.questdb.cairo.SecurityContext securityContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void changeColumnType(CharSequence columnName, int newType, int symbolCapacity, boolean symbolCacheFlag, byte indexType, int indexValueBlockCapacity, boolean isSequential, io.questdb.cairo.SecurityContext securityContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public io.questdb.cairo.sql.TableRecordMetadata getMetadata() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableToken getTableToken() {
            return null;
        }

        @Override
        public int getTimestampType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeColumn(CharSequence columnName, io.questdb.cairo.SecurityContext securityContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameColumn(CharSequence columnName, CharSequence newName, io.questdb.cairo.SecurityContext securityContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameTable(CharSequence fromNameTable, CharSequence toTableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMetaExpiry(String predicate, long cleanupIntervalMicros) {
            this.predicate = predicate;
            this.cleanupIntervalMicros = cleanupIntervalMicros;
        }
    }
}
