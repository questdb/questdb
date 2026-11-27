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

package io.questdb.test.cutlass.http.processors;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewBackfillValidator;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.processors.LineHttpTudCache;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.cutlass.line.tcp.LineWalAppender;
import io.questdb.cutlass.line.tcp.SymbolCache;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.WeakClosableObjectPool;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicInteger;

public class LineHttpTudCacheTest extends AbstractCairoTest {
    // base max 12:00 and REFRESH LIMIT 1 HOUR put the view's boundary bucket floor at 11:00
    private static final String MANAGED_ZONE_MESSAGE = "backfill row falls in or past the managed zone of materialized view [view=v, "
            + "maxRowTs=2024-09-10T12:45:00.000000Z, rowBucket=[2024-09-10T12:00:00.000000Z, 2024-09-10T13:00:00.000000Z), "
            + "boundaryBucketFloor=2024-09-10T11:00:00.000000Z]; backfill bucket end must be at-or-before the boundary bucket floor";

    @Test
    public void testCommitAllAuthorizesViewBeforeValidating() throws Exception {
        // The commit authorizes an insert before its validator reads the view's frozen zone. The check commitAll()
        // runs ahead of the first commit keeps that order, so an identity without INSERT on the view gets its
        // authorization refusal, not the managed-zone message, which carries the base table's latest bucket.
        assertMemoryLeak(() -> {
            createBackfillableView();
            final InsertRefusingContext refusingContext = new InsertRefusingContext("v");
            refusingContext.isRefusing = true;
            try (IlpRequest request = new IlpRequest()) {
                request.append(AllowAllSecurityContext.INSTANCE, "t x=1i " + micros("2024-09-10T00:00"));
                request.append(refusingContext, "v,sym=a price=95.0 " + micros("2024-09-10T12:45"));
                final long tSeqTxn = seqTxn("t");
                final long viewSeqTxn = seqTxn("v");
                final Throwable failure = request.commitAllFailure();
                Assert.assertTrue(failure instanceof CairoException);
                final CairoException e = (CairoException) failure;
                Assert.assertTrue(e.isAuthorizationError());
                TestUtils.assertEquals("insert refused [table=v]", e.getFlyweightMessage());
                Assert.assertEquals(tSeqTxn, seqTxn("t"));
                Assert.assertEquals(viewSeqTxn, seqTxn("v"));
                request.discard();
            }
            assertRowCount("t", 0);
        });
    }

    @Test
    public void testCommitAllRefusesManagedZoneViewRowBeforeFirstCommit() throws Exception {
        // A view row in the managed zone refuses its request before any table of the request commits: the tables
        // the request names before the view commit nothing either, so a client that sends the request again cannot
        // commit them twice. The refusal carries the marker of a refusal that repeats at every replay.
        assertMemoryLeak(() -> {
            createBackfillableView();
            try (IlpRequest request = new IlpRequest()) {
                request.append(AllowAllSecurityContext.INSTANCE, "t x=1i " + micros("2024-09-10T00:00"));
                request.append(AllowAllSecurityContext.INSTANCE, "t2 x=1i " + micros("2024-09-10T00:00"));
                request.append(AllowAllSecurityContext.INSTANCE, "v,sym=a price=95.0 " + micros("2024-09-10T12:45"));
                final long tSeqTxn = seqTxn("t");
                final long t2SeqTxn = seqTxn("t2");
                final long viewSeqTxn = seqTxn("v");
                final Throwable failure = request.commitAllFailure();
                Assert.assertTrue(failure instanceof CairoException);
                final CairoException e = (CairoException) failure;
                TestUtils.assertEquals(MANAGED_ZONE_MESSAGE, e.getFlyweightMessage());
                Assert.assertTrue(e.isSchemaMismatch());
                Assert.assertEquals(tSeqTxn, seqTxn("t"));
                Assert.assertEquals(t2SeqTxn, seqTxn("t2"));
                Assert.assertEquals(viewSeqTxn, seqTxn("v"));
                request.discard();

                // the connection's next request commits
                request.append(AllowAllSecurityContext.INSTANCE, "t x=2i " + micros("2024-09-10T00:00"));
                request.append(AllowAllSecurityContext.INSTANCE, "v,sym=a price=7.0 " + micros("2024-09-10T08:00"));
                request.commitAll();
                Assert.assertEquals(tSeqTxn + 1, seqTxn("t"));
                Assert.assertEquals(t2SeqTxn, seqTxn("t2"));
                Assert.assertEquals(viewSeqTxn + 1, seqTxn("v"));
            }
            drainWalAndMatViewQueues();
            assertQuery("SELECT x FROM t").noLeakCheck().expectSize().returns("x\n2\n");
            assertRowCount("t2", 0);
            assertQuery("v").noLeakCheck().timestamp("ts").expectSize().returns("""
                    sym\tprice\tts
                    a\t7.0\t2024-09-10T08:00:00.000000Z
                    a\t1.0\t2024-09-10T09:00:00.000000Z
                    a\t2.0\t2024-09-10T12:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testCommitAllSkipsDroppedViewEntry() throws Exception {
        // A dropped table's rows are discarded, not committed, so the check ahead of the commits skips its entry as
        // the commit loop does.
        assertMemoryLeak(() -> {
            createBackfillableView();
            try (IlpRequest request = new IlpRequest()) {
                request.append(AllowAllSecurityContext.INSTANCE, "t x=1i " + micros("2024-09-10T00:00"));
                final WalTableUpdateDetails viewTud = request.append(AllowAllSecurityContext.INSTANCE, "v,sym=a price=95.0 " + micros("2024-09-10T12:45"));
                viewTud.setIsDropped();
                final long tSeqTxn = seqTxn("t");
                final long viewSeqTxn = seqTxn("v");
                request.commitAll();
                Assert.assertEquals(tSeqTxn + 1, seqTxn("t"));
                Assert.assertEquals(viewSeqTxn, seqTxn("v"));
            }
            drainWalAndMatViewQueues();
            assertRowCount("t", 1);
        });
    }

    @Test
    public void testCommitAllSkipsRowlessViewEntry() throws Exception {
        // The connection keeps the view's entry from an earlier request, bound to that request's identity. A request
        // that writes no view row does not commit the view, so it does not authorize the view's entry either.
        assertMemoryLeak(() -> {
            createBackfillableView();
            final InsertRefusingContext context = new InsertRefusingContext("v");
            try (IlpRequest request = new IlpRequest()) {
                request.append(context, "v,sym=a price=7.0 " + micros("2024-09-10T08:00"));
                request.commitAll();
                request.clear();

                context.isRefusing = true;
                final long tSeqTxn = seqTxn("t");
                request.append(context, "t x=1i " + micros("2024-09-10T00:00"));
                request.commitAll();
                Assert.assertEquals(tSeqTxn + 1, seqTxn("t"));
            }
            drainWalAndMatViewQueues();
            assertRowCount("t", 1);
        });
    }

    @Test
    public void testCommitAllValidatesEachViewOnceBeforeItsCommit() throws Exception {
        // The check ahead of the commits runs the view's validator once per request, whatever the view's row count,
        // and once more at the view's own commit. A request without view rows does not run it, and a plain table's
        // rows meet only its commit's validator.
        assertMemoryLeak(() -> {
            createBackfillableView();
            final TableToken viewToken = engine.verifyTableName("v");
            final AtomicInteger validationCount = new AtomicInteger();
            try (IlpRequest request = new IlpRequest()) {
                final WalTableUpdateDetails viewTud = request.append(AllowAllSecurityContext.INSTANCE, "v,sym=a price=1.0 " + micros("2024-09-10T00:00"));
                final MatViewBackfillValidator validator = new MatViewBackfillValidator(engine, viewToken);
                final WalWriter viewWriter = (WalWriter) viewTud.getWriter();
                viewWriter.setPreCommitValidator((txnType, dedupMode, txnMinTs, txnMaxTs) -> {
                    validationCount.incrementAndGet();
                    validator.validate(txnType, dedupMode, txnMinTs, txnMaxTs);
                });
                try {
                    final long firstTs = micros("2024-09-10T00:00");
                    for (int i = 1; i < 1_000; i++) {
                        request.append(AllowAllSecurityContext.INSTANCE, "v,sym=a price=1.0 " + (firstTs + i * 1_000_000L));
                    }
                    request.commitAll();
                    Assert.assertEquals(2, validationCount.get());

                    final WalTableUpdateDetails plainTud = request.append(AllowAllSecurityContext.INSTANCE, "t x=1i " + micros("2024-09-10T00:00"));
                    // the pool gives a plain table's writer no validator; this one counts the checks of its rows
                    final WalWriter plainWriter = (WalWriter) plainTud.getWriter();
                    final AtomicInteger plainValidationCount = new AtomicInteger();
                    plainWriter.setPreCommitValidator((txnType, dedupMode, txnMinTs, txnMaxTs) -> plainValidationCount.incrementAndGet());
                    try {
                        request.commitAll();
                    } finally {
                        plainWriter.setPreCommitValidator(null);
                    }
                    Assert.assertEquals(2, validationCount.get());
                    Assert.assertEquals(1, plainValidationCount.get());
                } finally {
                    // the pool's writer goes back with the validator the pool gives every view writer
                    viewWriter.setPreCommitValidator(new MatViewBackfillValidator(engine, viewToken));
                }
            }
            drainWalAndMatViewQueues();
            assertRowCount("t", 1);
            assertQuery("SELECT count() FROM v WHERE ts < '2024-09-10T01:00'").noLeakCheck().noRandomAccess().expectSize().returns("count\n1000\n");
        });
    }

    @Test
    public void testCommitAllViewRefusedAtCommitAfterEarlyCheck() throws Exception {
        // The frozen zone can shrink between the check ahead of the commits and the view's own commit, as when ALTER
        // ... SET REFRESH LIMIT lands in between. The commit's own check then refuses the view after the tables
        // ahead of it committed, once: the refusal carries the marker of a refusal that repeats at every replay, so
        // no client sends the request again. A validator that accepts its first call and refuses the second forces
        // that order.
        assertMemoryLeak(() -> {
            createBackfillableView();
            final TableToken viewToken = engine.verifyTableName("v");
            final AtomicInteger validationCount = new AtomicInteger();
            try (IlpRequest request = new IlpRequest()) {
                request.append(AllowAllSecurityContext.INSTANCE, "t x=1i " + micros("2024-09-10T00:00"));
                final WalTableUpdateDetails viewTud = request.append(AllowAllSecurityContext.INSTANCE, "v,sym=a price=7.0 " + micros("2024-09-10T08:00"));
                final WalWriter viewWriter = (WalWriter) viewTud.getWriter();
                viewWriter.setPreCommitValidator((txnType, dedupMode, txnMinTs, txnMaxTs) -> {
                    if (validationCount.incrementAndGet() > 1) {
                        throw CairoException.schemaMismatch().put("frozen zone moved [view=v]");
                    }
                });
                try {
                    final long tSeqTxn = seqTxn("t");
                    final long viewSeqTxn = seqTxn("v");
                    final Throwable failure = request.commitAllFailure();
                    Assert.assertTrue(failure instanceof CairoException);
                    final CairoException e = (CairoException) failure;
                    TestUtils.assertEquals("frozen zone moved [view=v]", e.getFlyweightMessage());
                    Assert.assertTrue(e.isSchemaMismatch());
                    Assert.assertEquals(2, validationCount.get());
                    Assert.assertEquals(tSeqTxn + 1, seqTxn("t"));
                    Assert.assertEquals(viewSeqTxn, seqTxn("v"));
                } finally {
                    viewWriter.setPreCommitValidator(new MatViewBackfillValidator(engine, viewToken));
                }
                request.discard();
            }
            drainWalAndMatViewQueues();
            assertRowCount("t", 1);
            assertQuery("SELECT count() FROM v WHERE ts = '2024-09-10T08:00'").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testResetAndDistressedClearReleaseFreedEntries() throws Exception {
        // Only writer closes that succeed: when a free throws, reset() and clear() stop at
        // that entry and do not clear the table map.
        assertMemoryLeak(() -> {
            final LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            final DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            // The 16th lookup grows the table map, and the 17th lands in the grown map.
            final int tableCount = 17;
            for (int t = 0; t < tableCount; t++) {
                execute("CREATE TABLE l_t" + t + " (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            for (boolean isDistressedClear : new boolean[]{false, true}) {
                final String freeAll = isDistressedClear ? "distressed clear" : "reset";
                // The parser and the symbol cache pool as LineHttpProcessorState builds them, and
                // closed in its order: the cache, the pool, the parser.
                try (
                        LineTcpParser parser = new LineTcpParser();
                        WeakClosableObjectPool<SymbolCache> symbolCachePool = new WeakClosableObjectPool<>(
                                () -> new SymbolCache(lineConfig.getMicrosecondClock(), lineConfig.getSymbolCacheWaitUsBeforeReload()),
                                5
                        );
                        LineHttpTudCache cache = new LineHttpTudCache(engine, true, false, defaultColumnTypes, PartitionBy.DAY)
                ) {
                    final ObjList<WeakReference<Object>> freed = openAndFreeAll(
                            freeAll, cache, parser, symbolCachePool, tableCount, isDistressedClear);
                    assertUnreachable(freeAll + ": freed entries still reachable", freed);

                    // The cache goes on serving tables. Using it here also keeps it reachable
                    // through the collection above.
                    Assert.assertNotNull(freeAll, lookUp(cache, parser, symbolCachePool, "l_t0"));
                }
            }
        });
    }

    // Collects garbage until no referent is reachable, then asserts that none is. A full
    // collection, which System.gc() runs under the suite's ParallelGC, clears the weak
    // reference of every unreachable object; the retries cover a request the JVM skipped.
    private static void assertUnreachable(String what, ObjList<WeakReference<Object>> refs) {
        int reachableCount = refs.size();
        for (int attempt = 0; attempt < 20 && reachableCount > 0; attempt++) {
            System.gc();
            reachableCount = 0;
            for (int i = 0, n = refs.size(); i < n; i++) {
                if (refs.getQuick(i).get() != null) {
                    reachableCount++;
                }
            }
        }
        Assert.assertEquals(what, 0, reachableCount);
    }

    // A WAL base table with rows at 09:10 and 12:00 on 2024-09-10 and a view v of its 1h buckets with REFRESH LIMIT
    // 1 HOUR: base max 12:00 puts the boundary bucket floor at 11:00. Plain WAL tables t and t2 take other rows.
    private static void createBackfillableView() throws Exception {
        execute("CREATE TABLE base_price (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO base_price VALUES ('a', 1.0, '2024-09-10T09:10'), ('a', 2.0, '2024-09-10T12:00')");
        execute("CREATE MATERIALIZED VIEW v AS (SELECT sym, last(price) price, ts FROM base_price SAMPLE BY 1h)");
        drainWalAndMatViewQueues();
        execute("ALTER MATERIALIZED VIEW v SET REFRESH LIMIT 1 HOUR");
        execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE t2 (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalAndMatViewQueues();
        Assert.assertTrue(engine.isBackfillableMatView(engine.verifyTableName("v")));
    }

    // Parses one ILP line for the table and looks the table up with the parsed line, as
    // LineHttpProcessorState does for every line.
    private static WalTableUpdateDetails lookUp(
            LineHttpTudCache cache,
            LineTcpParser parser,
            WeakClosableObjectPool<SymbolCache> symbolCachePool,
            String tableName
    ) throws LineHttpTudCache.TableCreateException {
        final String line = tableName + " val=1i 1000000\n";
        final long lo = TestUtils.toMemory(line);
        try {
            Assert.assertEquals(LineTcpParser.ParseResult.MEASUREMENT_COMPLETE, parser.of(lo).parseMeasurement(lo + line.length()));
            return cache.getTableUpdateDetails(AllowAllSecurityContext.INSTANCE, parser, symbolCachePool);
        } finally {
            Unsafe.free(lo, line.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long micros(String timestamp) {
        return parseFloorPartialTimestamp(timestamp);
    }

    // Opens an entry for each table, then frees them all with reset() or the distressed
    // clear(). Returns weak references to the freed entries and nothing else: no frame of
    // the caller keeps them.
    private static ObjList<WeakReference<Object>> openAndFreeAll(
            String freeAll,
            LineHttpTudCache cache,
            LineTcpParser parser,
            WeakClosableObjectPool<SymbolCache> symbolCachePool,
            int tableCount,
            boolean isDistressedClear
    ) throws LineHttpTudCache.TableCreateException {
        final ObjList<WeakReference<Object>> refs = new ObjList<>();
        for (int t = 0; t < tableCount; t++) {
            final WalTableUpdateDetails tud = lookUp(cache, parser, symbolCachePool, "l_t" + t);
            Assert.assertNotNull(freeAll, tud);
            refs.add(new WeakReference<>(tud));
        }
        if (isDistressedClear) {
            cache.setDistressed();
            cache.clear();
        } else {
            cache.reset();
        }
        return refs;
    }

    private static long seqTxn(String tableName) {
        return engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(tableName));
    }

    private void assertRowCount(String tableName, long expectedCount) throws Exception {
        assertQuery("SELECT count() FROM " + tableName).noLeakCheck().noRandomAccess().expectSize().returns("count\n" + expectedCount + "\n");
    }

    // Refuses an insert into one table while isRefusing is set, as an identity without INSERT on it.
    private static class InsertRefusingContext extends AllowAllSecurityContext {
        private final String refusedTableName;
        private volatile boolean isRefusing;

        private InsertRefusingContext(String refusedTableName) {
            this.refusedTableName = refusedTableName;
        }

        @Override
        public void authorizeInsert(TableToken tableToken) {
            if (isRefusing && tableToken.getTableName().equals(refusedTableName)) {
                throw CairoException.authorization().put("insert refused [table=").put(tableToken.getTableName()).put(']');
            }
        }
    }

    // The table cache, the parser and the appender of one ILP/HTTP connection, as LineHttpProcessorState holds them.
    // Lines append in micros, as the request's default precision.
    private class IlpRequest implements AutoCloseable {
        private final LineWalAppender appender;
        private final LineHttpTudCache cache;
        private final LineTcpParser parser = new LineTcpParser();
        private final WeakClosableObjectPool<SymbolCache> symbolCachePool;

        private IlpRequest() {
            final LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            symbolCachePool = new WeakClosableObjectPool<>(
                    () -> new SymbolCache(lineConfig.getMicrosecondClock(), lineConfig.getSymbolCacheWaitUsBeforeReload()),
                    5
            );
            cache = new LineHttpTudCache(engine, true, true, new DefaultColumnTypes(lineConfig), PartitionBy.DAY);
            appender = new LineWalAppender(
                    true,
                    false,
                    CommonUtils.TIMESTAMP_UNIT_MICROS,
                    configuration.getMaxFileNameLength(),
                    configuration.getMaxSqlRecompileAttempts()
            );
        }

        @Override
        public void close() {
            // closed in LineHttpProcessorState's order: the appender, the cache, the pool, the parser
            appender.close();
            cache.close();
            symbolCachePool.close();
            parser.close();
        }

        WalTableUpdateDetails append(SecurityContext securityContext, String line) throws Exception {
            final String terminatedLine = line + '\n';
            final long lo = TestUtils.toMemory(terminatedLine);
            try {
                Assert.assertEquals(LineTcpParser.ParseResult.MEASUREMENT_COMPLETE, parser.of(lo).parseMeasurement(lo + terminatedLine.length()));
                final WalTableUpdateDetails tud = cache.getTableUpdateDetails(securityContext, parser, symbolCachePool);
                appender.appendToWal(securityContext, parser, tud);
                return tud;
            } finally {
                Unsafe.free(lo, terminatedLine.length(), MemoryTag.NATIVE_DEFAULT);
            }
        }

        // what LineHttpProcessorState.clear() does before the next request
        void clear() {
            cache.clear();
        }

        void commitAll() {
            try {
                cache.commitAll();
            } catch (Throwable th) {
                throw new AssertionError("commitAll() failed", th);
            }
        }

        Throwable commitAllFailure() {
            try {
                cache.commitAll();
            } catch (Throwable th) {
                return th;
            }
            throw new AssertionError("expected commitAll() to fail");
        }

        // what LineHttpProcessorState.commit() does after a failed commitAll(), then the next clear(): frees every
        // entry, and the pool rolls each writer back
        void discard() {
            cache.setDistressed();
            cache.clear();
        }
    }
}
