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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.mv.MatViewBackfillValidator;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertContains;

/**
 * Direct unit coverage for {@link MatViewBackfillValidator} passthrough
 * branches and bucket-whole enforcement. The MatViewTest suite exercises the
 * validator end-to-end through SQL; this class targets the passthrough paths
 * (non-DATA txn type, REPLACE_RANGE dedup mode, empty txn, sampler setup)
 * which are coverage-dead through SQL, the refusal of user commits into a view
 * without a frozen zone (no REFRESH LIMIT, or the wall-clock escape hatch on),
 * and the write handles that outlive ALTER ... SET REFRESH LIMIT 0.
 */
public class MatViewBackfillValidatorTest extends AbstractCairoTest {
    // the backfill gate's refusal, which the commit-time check repeats for a view without a frozen zone
    private static final String GATE_MESSAGE = "cannot modify materialized view [view=price_1h]";

    @Test
    public void testValidateAcceptsBucketBelowBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("INSERT INTO base_price VALUES('a', 9.0, '2024-09-10T12:00')");
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 1 HOUR");
            drainWalAndMatViewQueues();

            currentMicros = parseFloorPartialTimestamp("2024-09-10T12:30:00.000000Z");

            final MatViewBackfillValidator validator = newValidator();
            // Boundary: anchor=min(12:00,12:30)=12:00; LIMIT=1h => raw=11:00,
            // snapped to 1h bucket floor = 11:00. Row at 09:00 sits in bucket
            // [09:00, 10:00); bucket end = 10:00 <= 11:00 => accepted.
            validator.validate(
                    WalTxnType.DATA,
                    WalUtils.WAL_DEDUP_MODE_DEFAULT,
                    parseFloorPartialTimestamp("2024-09-10T09:00:00.000000Z"),
                    parseFloorPartialTimestamp("2024-09-10T09:00:00.000000Z")
            );
        });
    }

    @Test
    public void testValidatePassthroughOnEmptyTxn() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 1 HOUR");
            drainWalAndMatViewQueues();

            currentMicros = parseFloorPartialTimestamp("2024-09-10T12:30:00.000000Z");
            final MatViewBackfillValidator validator = newValidator();

            // Empty txn sentinels: txnMaxTs < 0 short-circuits before any
            // state lookup. No exception expected.
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, Long.MAX_VALUE, -1);
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, Long.MAX_VALUE, Long.MIN_VALUE);
        });
    }

    @Test
    public void testRefusalsCarrySchemaMismatchMarker() throws Exception {
        // Both refusals repeat under byte-identical replay: the row's bucket does not leave the managed zone, and a
        // view without REFRESH LIMIT gains no frozen zone, in the seconds a client retries. The protocols tell such a
        // refusal from a transient one by the schema-mismatch marker, so both carry it, and neither is critical.
        assertMemoryLeak(() -> {
            createRefreshedView();
            setRefreshLimit("1 HOUR");
            final MatViewBackfillValidator validator = newValidator();
            final long managedTs = parseFloorPartialTimestamp("2024-09-10T11:30");
            try {
                validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, managedTs, managedTs);
                Assert.fail("expected the bucket-whole rejection");
            } catch (CairoException e) {
                TestUtils.assertEquals(
                        managedZoneMessage("2024-09-10T11:30:00", "2024-09-10T11:00:00", "2024-09-10T12:00:00", "2024-09-10T11:00:00"),
                        e.getFlyweightMessage()
                );
                Assert.assertTrue(e.isSchemaMismatch());
                Assert.assertFalse(e.isCritical());
            }

            setRefreshLimit("0 HOUR");
            final long frozenTs = parseFloorPartialTimestamp("2024-09-10T09:00");
            try {
                validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, frozenTs, frozenTs);
                Assert.fail("expected the commit to be refused without a frozen zone");
            } catch (CairoException e) {
                TestUtils.assertEquals(GATE_MESSAGE, e.getFlyweightMessage());
                Assert.assertTrue(e.isSchemaMismatch());
                Assert.assertFalse(e.isCritical());
            }
        });
    }

    @Test
    public void testRejectionFollowsAppliedRefreshLimit() throws Exception {
        // A commit is checked under the view definition that the view's WAL has applied. While SET REFRESH LIMIT 0
        // waits for its apply, the old limit holds for a compiled INSERT as it does for the backfill gate; once it
        // applies, the compiled INSERT is refused.
        assertMemoryLeak(() -> {
            createRefreshedView();
            setRefreshLimit("1 HOUR");
            final TableToken viewToken = engine.verifyTableName("price_1h");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (InsertOperation insert = compileInsert(compiler)) {
                    execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 0 HOUR");
                    // sequenced, not applied: the gate and the commit-time check still read the limit of 1 hour
                    Assert.assertTrue(engine.isBackfillableMatView(viewToken));
                    executeInsert(insert, 2.0, "2024-09-10T08:00");
                    final CompiledQuery freshInsert = compiler.compile("INSERT INTO price_1h VALUES ('a', 3.0, '2024-09-10T07:30')", sqlExecutionContext);
                    Assert.assertEquals(CompiledQuery.INSERT, freshInsert.getType());
                    freshInsert.popInsertOperation().close();

                    // the apply runs the ALTER, then the backfill sequenced after it
                    drainWalQueue();
                    Assert.assertFalse(engine.isBackfillableMatView(viewToken));
                    assertInsertRejectedByGate(insert, 4.0, "2024-09-10T07:00");
                }
            }
            drainWalAndMatViewQueues();
            assertViewRows("""
                    sym\tprice\tts
                    a\t2.0\t2024-09-10T08:00:00.000000Z
                    a\t8.0\t2024-09-10T10:00:00.000000Z
                    a\t9.0\t2024-09-10T12:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testStaleInsertAsSelectRejectedAfterRefreshLimitRemoved() throws Exception {
        // An INSERT AS SELECT keeps its compiled plan the way an INSERT does, and its commit meets the same check:
        // once SET REFRESH LIMIT 0 has applied, it lands no row.
        assertMemoryLeak(() -> {
            createRefreshedView();
            setRefreshLimit("1 HOUR");
            execute("CREATE TABLE src (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO src VALUES ('a', 1.0, '2024-09-10T09:00')");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("INSERT INTO price_1h SELECT sym, price, ts FROM src", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT_AS_SELECT, cq.getType());
                try (InsertOperation insert = cq.popInsertOperation()) {
                    insert.execute(sqlExecutionContext);
                    drainWalAndMatViewQueues();

                    setRefreshLimit("0 HOUR");
                    execute("INSERT INTO src VALUES ('a', 2.0, '2024-09-10T08:00')");
                    final long viewSeqTxn = viewSeqTxn();
                    try {
                        insert.execute(sqlExecutionContext);
                        Assert.fail("expected the commit to be refused without REFRESH LIMIT");
                    } catch (CairoException e) {
                        TestUtils.assertEquals(GATE_MESSAGE, e.getFlyweightMessage());
                    }
                    Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                }
            }
            drainWalAndMatViewQueues();
            assertViewRows("""
                    sym\tprice\tts
                    a\t1.0\t2024-09-10T09:00:00.000000Z
                    a\t8.0\t2024-09-10T10:00:00.000000Z
                    a\t9.0\t2024-09-10T12:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testStaleInsertRejectedAfterRefreshLimitRemoved() throws Exception {
        // A compiled INSERT passes the backfill gate once, when it compiles; pg-wire keeps it prepared and in its
        // insert cache. Executed again after SET REFRESH LIMIT 0 has applied, its commit is refused with the gate's
        // message, and the view keeps its rows through the next refresh. Once the limit is back, the same INSERT
        // backfills the frozen zone again.
        assertMemoryLeak(() -> {
            createRefreshedView();
            setRefreshLimit("1 HOUR");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (InsertOperation insert = compileInsert(compiler)) {
                    executeInsert(insert, 1.0, "2024-09-10T09:00");
                    drainWalAndMatViewQueues();

                    setRefreshLimit("0 HOUR");
                    // a fresh compile meets the backfill gate
                    assertExceptionNoLeakCheck(
                            "INSERT INTO price_1h VALUES ('a', 2.0, '2024-09-10T12:00')",
                            12,
                            GATE_MESSAGE,
                            sqlExecutionContext
                    );
                    final long viewSeqTxn = viewSeqTxn();
                    assertInsertRejectedByGate(insert, 77.0, "2024-09-10T12:00");
                    assertInsertRejectedByGate(insert, 2.0, "2024-09-10T08:00");
                    Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                    drainWalAndMatViewQueues();
                    assertViewRows("""
                            sym\tprice\tts
                            a\t1.0\t2024-09-10T09:00:00.000000Z
                            a\t8.0\t2024-09-10T10:00:00.000000Z
                            a\t9.0\t2024-09-10T12:00:00.000000Z
                            """);

                    execute("INSERT INTO base_price VALUES ('a', 5.0, '2024-09-10T13:05')");
                    drainWalQueue();
                    execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
                    drainWalAndMatViewQueues();
                    assertViewRows("""
                            sym\tprice\tts
                            a\t1.0\t2024-09-10T09:00:00.000000Z
                            a\t8.0\t2024-09-10T10:00:00.000000Z
                            a\t9.0\t2024-09-10T12:00:00.000000Z
                            a\t5.0\t2024-09-10T13:00:00.000000Z
                            """);
                    // the view holds what its SQL gives on the base, and the backfill accepted under the limit
                    assertSqlCursors(
                            "SELECT sym::STRING sym, price, ts FROM (" +
                                    "SELECT sym, last(price) price, ts FROM base_price SAMPLE BY 1h " +
                                    "UNION ALL SELECT 'a', 1.0, '2024-09-10T09:00'::TIMESTAMP" +
                                    ") ORDER BY ts",
                            "SELECT sym::STRING sym, price, ts FROM price_1h ORDER BY ts"
                    );
                    assertQuery("SELECT view_status FROM materialized_views()")
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns("view_status\nvalid\n");

                    setRefreshLimit("1 HOUR");
                    executeInsert(insert, 2.0, "2024-09-10T08:00");
                    // the anchor min(13:05, 12:30) = 12:30 less 1 hour is 11:30, which floors to the 11:00 bucket
                    try {
                        executeInsert(insert, 77.0, "2024-09-10T12:00");
                        Assert.fail("expected the bucket-whole rejection");
                    } catch (CairoException e) {
                        TestUtils.assertEquals(
                                managedZoneMessage("2024-09-10T12:00:00", "2024-09-10T12:00:00", "2024-09-10T13:00:00", "2024-09-10T11:00:00"),
                                e.getFlyweightMessage()
                        );
                    }
                }
            }
            drainWalAndMatViewQueues();
            assertViewRows("""
                    sym\tprice\tts
                    a\t2.0\t2024-09-10T08:00:00.000000Z
                    a\t1.0\t2024-09-10T09:00:00.000000Z
                    a\t8.0\t2024-09-10T10:00:00.000000Z
                    a\t9.0\t2024-09-10T12:00:00.000000Z
                    a\t5.0\t2024-09-10T13:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testValidateAcceptsFrozenRowWithRefreshLimitInMonths() throws Exception {
        // A REFRESH LIMIT in months is stored as a negative count. The view still has a frozen zone, so a user commit
        // takes the bucket-whole check rather than the refusal of a view without REFRESH LIMIT.
        assertMemoryLeak(() -> {
            createRefreshedView();
            setRefreshLimit("1 MONTH");
            final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName("price_1h"));
            Assert.assertEquals(-1, state.getViewDefinition().getRefreshLimitHoursOrMonths());
            final MatViewBackfillValidator validator = newValidator();

            // the anchor min(12:00, 12:30) = 2024-09-10T12:00 less one month puts the boundary floor at 2024-08-10T12:00
            final long frozenTs = parseFloorPartialTimestamp("2024-08-01T00:00");
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, frozenTs, frozenTs);
            assertRejectedInManagedZone(validator, "2024-09-01T00:00:00", "2024-09-01T00:00:00", "2024-09-01T01:00:00", "2024-08-10T12:00:00");
        });
    }

    @Test
    public void testValidatePassesNonUserCommitsWithoutRefreshLimit() throws Exception {
        // Only a DATA commit with the default dedup mode and rows is a user write. Refresh and row-expiry commits, DDL
        // and state events, the other dedup modes and empty commits reach a view without REFRESH LIMIT unchecked.
        assertMemoryLeak(() -> {
            createRefreshedView();
            final MatViewBackfillValidator validator = newValidator();
            final long ts = parseFloorPartialTimestamp("2024-09-10T12:00");
            validator.validate(WalTxnType.MAT_VIEW_DATA, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE, ts, ts);
            validator.validate(WalTxnType.MAT_VIEW_DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, ts, ts);
            validator.validate(WalTxnType.SQL, WalUtils.WAL_DEDUP_MODE_DEFAULT, ts, ts);
            validator.validate(WalTxnType.TRUNCATE, WalUtils.WAL_DEDUP_MODE_DEFAULT, ts, ts);
            validator.validate(WalTxnType.MAT_VIEW_INVALIDATE, WalUtils.WAL_DEDUP_MODE_DEFAULT, ts, ts);
            validator.validate(WalTxnType.MAT_VIEW_DEFINITION, WalUtils.WAL_DEDUP_MODE_DEFAULT, ts, ts);
            validator.validate(WalTxnType.VIEW_DEFINITION, WalUtils.WAL_DEDUP_MODE_DEFAULT, ts, ts);
            validator.validate(WalTxnType.LIVE_VIEW_DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, ts, ts);
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE, ts, ts);
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_NO_DEDUP, ts, ts);
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_UPSERT_NEW, ts, ts);
            // empty commits: no row moved the max timestamp off its sentinel
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, Long.MAX_VALUE, -1);
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, Long.MAX_VALUE, Long.MIN_VALUE);
        });
    }

    @Test
    public void testValidatePassthroughOnNonDataTxn() throws Exception {
        // Refresh-job writes carry MAT_VIEW_DATA; the validator must not
        // gate them with the bucket-whole rule because the refresh job
        // owns the managed zone by definition. Same for SQL/TRUNCATE/etc.
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("INSERT INTO base_price VALUES('a', 9.0, '2024-09-10T12:00')");
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 1 HOUR");
            drainWalAndMatViewQueues();

            currentMicros = parseFloorPartialTimestamp("2024-09-10T12:30:00.000000Z");
            final MatViewBackfillValidator validator = newValidator();

            // A row at 11:30 would be rejected on DATA+DEFAULT (bucket
            // [11:00,12:00) end 12:00 > boundary floor 11:00). With a
            // non-DATA txn type, the validator short-circuits.
            final long managedZoneTs = parseFloorPartialTimestamp("2024-09-10T11:30:00.000000Z");
            validator.validate(WalTxnType.MAT_VIEW_DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, managedZoneTs, managedZoneTs);
            validator.validate(WalTxnType.SQL, WalUtils.WAL_DEDUP_MODE_DEFAULT, managedZoneTs, managedZoneTs);
            validator.validate(WalTxnType.TRUNCATE, WalUtils.WAL_DEDUP_MODE_DEFAULT, managedZoneTs, managedZoneTs);
        });
    }

    @Test
    public void testValidatePassthroughOnReplaceRangeDedup() throws Exception {
        // The refresh job's range commits use DATA + REPLACE_RANGE; that
        // combination must pass through the validator because refresh owns
        // the managed zone.
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("INSERT INTO base_price VALUES('a', 9.0, '2024-09-10T12:00')");
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 1 HOUR");
            drainWalAndMatViewQueues();

            currentMicros = parseFloorPartialTimestamp("2024-09-10T12:30:00.000000Z");
            final MatViewBackfillValidator validator = newValidator();

            // Managed-zone bucket that would be rejected on DEFAULT dedup
            // mode passes through on REPLACE_RANGE.
            final long managedZoneTs = parseFloorPartialTimestamp("2024-09-10T11:30:00.000000Z");
            validator.validate(
                    WalTxnType.DATA,
                    WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE,
                    managedZoneTs,
                    managedZoneTs
            );
        });
    }

    @Test
    public void testValidateRejectsUserCommitUnderWallClockEscapeHatch() throws Exception {
        // With the wall-clock escape hatch on, the frozen-zone feature is off and the view accepts no user write, as
        // the backfill gate says: the commit-time check refuses it with the gate's message instead of checking its
        // bucket against a boundary the feature no longer publishes. Commits other than a user write pass, and the
        // view takes backfill again once the hatch is off.
        assertMemoryLeak(() -> {
            createRefreshedView();
            setRefreshLimit("1 HOUR");
            final MatViewBackfillValidator validator = newValidator();
            final long frozenTs = parseFloorPartialTimestamp("2024-09-10T09:00");
            final long managedTs = parseFloorPartialTimestamp("2024-09-10T11:30");

            // hatch off: the anchor min(12:00, 12:30) less 1 hour puts the boundary floor at 11:00
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, frozenTs, frozenTs);
            assertRejectedInManagedZone(validator, "2024-09-10T11:30:00", "2024-09-10T11:00:00", "2024-09-10T12:00:00", "2024-09-10T11:00:00");

            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_LIMIT_WALL_CLOCK_ENABLED, "true");
            assertRejectedByGate(validator, frozenTs, frozenTs);
            assertRejectedByGate(validator, managedTs, managedTs);
            validator.validate(WalTxnType.MAT_VIEW_DATA, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE, managedTs, managedTs);
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE, managedTs, managedTs);
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, Long.MAX_VALUE, -1);

            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_LIMIT_WALL_CLOCK_ENABLED, "false");
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, frozenTs, frozenTs);
        });
    }

    @Test
    public void testValidateRejectsUserCommitWithoutRefreshLimit() throws Exception {
        // Without REFRESH LIMIT the view has no frozen zone and accepts no user write: a commit from a handle that
        // passed the backfill gate before ALTER ... SET REFRESH LIMIT 0 is refused with the gate's message, wherever
        // its rows fall.
        assertMemoryLeak(() -> {
            createRefreshedView();
            final MatViewBackfillValidator validator = newValidator();
            // the epoch, the earliest designated timestamp a row can have
            assertRejectedByGate(validator, 0, 0);
            final long frozenTs = parseFloorPartialTimestamp("2024-09-10T09:00");
            assertRejectedByGate(validator, frozenTs, frozenTs);
            final long managedTs = parseFloorPartialTimestamp("2024-09-10T12:00");
            assertRejectedByGate(validator, managedTs, managedTs);
            // the latest designated timestamp a row can have
            final long latestTs = parseFloorPartialTimestamp("9999-12-31T23:59:59.999999");
            assertRejectedByGate(validator, latestTs, latestTs);
            assertRejectedByGate(validator, 0, managedTs);
        });
    }

    @Test
    public void testWalWriterMarksDistressedOnNonCairoExceptionFromValidator() throws Exception {
        // WalWriter.commit0 contract: a validator that throws anything other
        // than CairoException is treated as an internal validator fault. The
        // writer marks itself distressed (so the pool expels this tenant on
        // next acquisition), attempts rollback, and rethrows. This pins that
        // contract so a future refactor of commit0 cannot silently downgrade
        // a corrupted validator into "writer keeps serving traffic".
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE base_price (" +
                            "  sym SYMBOL, price DOUBLE, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalAndMatViewQueues();

            final TableToken baseToken = engine.verifyTableName("base_price");
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                walWriter.setPreCommitValidator((txnType, dedupMode, txnMinTs, txnMaxTs) -> {
                    throw new RuntimeException("simulated validator fault");
                });

                final TableWriter.Row row = walWriter.newRow(MicrosTimestampDriver.floor("2024-09-10T12:00"));
                row.putSym(0, "a");
                row.putDouble(1, 1.0);
                row.append();

                try {
                    walWriter.commit();
                    Assert.fail("expected the validator's runtime exception to propagate");
                } catch (RuntimeException expected) {
                    Assert.assertEquals("simulated validator fault", expected.getMessage());
                }
                Assert.assertTrue(
                        "writer must be marked distressed after non-CairoException validator fault",
                        walWriter.isDistressed()
                );
            }
        });
    }

    @Test
    public void testWalWriterValidateUserRowsDistressesOnValidatorFault() throws Exception {
        // validateUserRows() runs the pre-commit validator without committing, under the validator's contract: a
        // throw other than CairoException is a validator fault, which distresses the writer so the pool expels it.
        // The check leaves the rows where they are; the caller owns them.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base_price (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("base_price"))) {
                walWriter.setPreCommitValidator((txnType, dedupMode, txnMinTs, txnMaxTs) -> {
                    throw new RuntimeException("simulated validator fault");
                });
                appendViewRow(walWriter, 1.0, "2024-09-10T12:00");
                final long ts = parseFloorPartialTimestamp("2024-09-10T12:00");
                try {
                    walWriter.validateUserRows(ts, ts);
                    Assert.fail("expected the validator's runtime exception to propagate");
                } catch (RuntimeException expected) {
                    Assert.assertEquals("simulated validator fault", expected.getMessage());
                }
                Assert.assertTrue(walWriter.isDistressed());
                Assert.assertEquals(1, walWriter.getUncommittedRowCount());
            }
        });
    }

    @Test
    public void testWalWriterValidateUserRowsIsNoOpWithoutValidator() throws Exception {
        // A writer of anything but a materialized view has no pre-commit validator: the check passes every row and
        // changes nothing.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base_price (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            final TableToken baseToken = engine.verifyTableName("base_price");
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                appendViewRow(walWriter, 1.0, "2024-09-10T12:00");
                walWriter.validateUserRows(0, parseFloorPartialTimestamp("9999-12-31T23:59:59.999999"));
                walWriter.validateUncommittedUserRows();
                Assert.assertFalse(walWriter.isDistressed());
                Assert.assertEquals(1, walWriter.getUncommittedRowCount());
                walWriter.commit();
            }
            Assert.assertEquals(1, engine.getTableSequencerAPI().lastTxn(baseToken));
        });
    }

    @Test
    public void testWalWriterValidateUserRowsRefusesWithoutRollingBack() throws Exception {
        // The view's writer checks rows against the frozen zone before any table of a request commits. The check
        // runs the validator the commit runs, on the given bounds or on the transaction's own, and refuses as the
        // commit would; unlike the commit, it rolls nothing back and leaves the writer healthy.
        assertMemoryLeak(() -> {
            createRefreshedView();
            setRefreshLimit("1 HOUR");
            final long viewSeqTxn = viewSeqTxn();
            try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("price_1h"))) {
                // no rows: the transaction's bounds are the empty sentinels
                walWriter.validateUncommittedUserRows();
                appendViewRow(walWriter, 2.0, "2024-09-10T09:00");
                walWriter.validateUncommittedUserRows();
                appendViewRow(walWriter, 3.0, "2024-09-10T11:30");
                final String managedZoneMessage = managedZoneMessage("2024-09-10T11:30:00", "2024-09-10T11:00:00", "2024-09-10T12:00:00", "2024-09-10T11:00:00");
                try {
                    walWriter.validateUncommittedUserRows();
                    Assert.fail("expected the managed-zone row to be refused");
                } catch (CairoException e) {
                    TestUtils.assertEquals(managedZoneMessage, e.getFlyweightMessage());
                    Assert.assertTrue(e.isSchemaMismatch());
                }
                // the bounds of one block, not of the transaction
                final long frozenTs = parseFloorPartialTimestamp("2024-09-10T09:00");
                walWriter.validateUserRows(frozenTs, frozenTs);
                final long managedTs = parseFloorPartialTimestamp("2024-09-10T11:30");
                try {
                    walWriter.validateUserRows(frozenTs, managedTs);
                    Assert.fail("expected the managed-zone bounds to be refused");
                } catch (CairoException e) {
                    TestUtils.assertEquals(managedZoneMessage, e.getFlyweightMessage());
                }
                Assert.assertFalse(walWriter.isDistressed());
                Assert.assertEquals(2, walWriter.getUncommittedRowCount());
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());

                walWriter.rollback();
                appendViewRow(walWriter, 4.0, "2024-09-10T09:00");
                walWriter.validateUncommittedUserRows();
                walWriter.commit();
                Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            }
            drainWalAndMatViewQueues();
            assertViewRows("""
                    sym\tprice\tts
                    a\t4.0\t2024-09-10T09:00:00.000000Z
                    a\t8.0\t2024-09-10T10:00:00.000000Z
                    a\t9.0\t2024-09-10T12:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testWalWriterPreparedCommitMarksDistressedOnNonCairoExceptionFromValidator() throws Exception {
        // prepareCommit() runs the validator as commit0 does: a fault other than CairoException distresses the
        // writer, which prepares nothing.
        assertMemoryLeak(() -> {
            createRefreshedView();
            setRefreshLimit("1 HOUR");
            final long viewSeqTxn = viewSeqTxn();
            try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("price_1h"))) {
                walWriter.setPreCommitValidator((txnType, dedupMode, txnMinTs, txnMaxTs) -> {
                    throw new RuntimeException("simulated validator fault");
                });
                appendViewRow(walWriter, 2.0, "2024-09-10T09:00");
                try {
                    walWriter.prepareCommit();
                    Assert.fail("expected the validator's runtime exception to propagate");
                } catch (RuntimeException expected) {
                    Assert.assertEquals("simulated validator fault", expected.getMessage());
                }
                Assert.assertTrue(walWriter.isDistressed());
                Assert.assertFalse(walWriter.isCommitPrepared());
            }
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            drainWalAndMatViewQueues();
            assertViewRows("""
                    sym\tprice\tts
                    a\t8.0\t2024-09-10T10:00:00.000000Z
                    a\t9.0\t2024-09-10T12:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testWalWriterPreparedCommitRejectsManagedZoneRow() throws Exception {
        // A QWP commit batch under SYNC or ADAPTIVE prepares each table's commit before it sequences it. The prepare
        // runs the check a plain commit runs: it refuses a managed-zone row and rolls the transaction back, nothing
        // reaches the view's WAL, and the writer stays healthy and prepares a frozen-zone row next.
        assertMemoryLeak(() -> {
            createRefreshedView();
            setRefreshLimit("1 HOUR");
            try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("price_1h"))) {
                appendViewRow(walWriter, 2.0, "2024-09-10T09:00");
                appendViewRow(walWriter, 3.0, "2024-09-10T11:30");
                final long viewSeqTxn = viewSeqTxn();
                try {
                    walWriter.prepareCommit();
                    Assert.fail("expected the prepare to refuse a managed-zone row");
                } catch (CairoException e) {
                    TestUtils.assertEquals(
                            managedZoneMessage("2024-09-10T11:30:00", "2024-09-10T11:00:00", "2024-09-10T12:00:00", "2024-09-10T11:00:00"),
                            e.getFlyweightMessage()
                    );
                    Assert.assertFalse(e.isCritical());
                }
                Assert.assertFalse(walWriter.isDistressed());
                Assert.assertFalse(walWriter.isCommitPrepared());
                Assert.assertEquals(0, walWriter.getUncommittedRowCount());
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());

                appendViewRow(walWriter, 4.0, "2024-09-10T09:00");
                walWriter.prepareCommit();
                Assert.assertTrue(walWriter.isCommitPrepared());
                walWriter.commit();
                Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            }
            drainWalAndMatViewQueues();
            assertViewRows("""
                    sym\tprice\tts
                    a\t4.0\t2024-09-10T09:00:00.000000Z
                    a\t8.0\t2024-09-10T10:00:00.000000Z
                    a\t9.0\t2024-09-10T12:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testWalWriterRejectsCommitAfterRefreshLimitRemoved() throws Exception {
        // A transaction that a writer opened while the view had REFRESH LIMIT commits under the definition applied at
        // commit: once SET REFRESH LIMIT 0 has applied, the commit is refused with the gate's message and rolled
        // back, and nothing reaches the view's WAL. The writer stays healthy and commits again once the limit is back.
        assertMemoryLeak(() -> {
            createRefreshedView();
            setRefreshLimit("1 HOUR");
            try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("price_1h"))) {
                appendViewRow(walWriter, 2.0, "2024-09-10T09:00");
                setRefreshLimit("0 HOUR");
                final long viewSeqTxn = viewSeqTxn();
                try {
                    walWriter.commit();
                    Assert.fail("expected the commit to be refused without REFRESH LIMIT");
                } catch (CairoException e) {
                    TestUtils.assertEquals(GATE_MESSAGE, e.getFlyweightMessage());
                }
                Assert.assertFalse(walWriter.isDistressed());
                Assert.assertEquals(0, walWriter.getUncommittedRowCount());
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());

                setRefreshLimit("1 HOUR");
                appendViewRow(walWriter, 3.0, "2024-09-10T09:00");
                walWriter.commit();
            }
            drainWalAndMatViewQueues();
            assertViewRows("""
                    sym\tprice\tts
                    a\t3.0\t2024-09-10T09:00:00.000000Z
                    a\t8.0\t2024-09-10T10:00:00.000000Z
                    a\t9.0\t2024-09-10T12:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testWalWriterRollsBackAndStaysHealthyOnCairoExceptionFromValidator() throws Exception {
        // The companion to the distressed-writer test: a CairoException is
        // the sanctioned rejection channel. The writer rolls back the
        // pending txn but stays healthy and can be reused.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE base_price (" +
                            "  sym SYMBOL, price DOUBLE, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalAndMatViewQueues();

            final TableToken baseToken = engine.verifyTableName("base_price");
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                walWriter.setPreCommitValidator((txnType, dedupMode, txnMinTs, txnMaxTs) -> {
                    throw CairoException.nonCritical().put("sanctioned validator reject");
                });

                final TableWriter.Row row = walWriter.newRow(MicrosTimestampDriver.floor("2024-09-10T12:00"));
                row.putSym(0, "a");
                row.putDouble(1, 1.0);
                row.append();

                try {
                    walWriter.commit();
                    Assert.fail("expected the validator's CairoException to propagate");
                } catch (CairoException expected) {
                    assertContains(expected.getFlyweightMessage(), "sanctioned validator reject");
                }
                Assert.assertFalse(
                        "writer must stay healthy after a sanctioned CairoException reject",
                        walWriter.isDistressed()
                );

                // Clear the validator and verify the writer remains usable.
                walWriter.setPreCommitValidator(null);
                final TableWriter.Row goodRow = walWriter.newRow(MicrosTimestampDriver.floor("2024-09-10T12:00"));
                goodRow.putSym(0, "a");
                goodRow.putDouble(1, 2.0);
                goodRow.append();
                walWriter.commit();
            }
        });
    }

    @Test
    public void testValidateRejectsBucketStraddleAtBoundary() throws Exception {
        // Bucket-whole rule: a row whose containing sample-by bucket ends
        // past the boundary's bucket floor must be rejected, even if the
        // row's own timestamp is below the boundary.
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("INSERT INTO base_price VALUES('a', 9.0, '2024-09-10T12:00')");
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 1 HOUR");
            drainWalAndMatViewQueues();

            currentMicros = parseFloorPartialTimestamp("2024-09-10T12:30:00.000000Z");
            final MatViewBackfillValidator validator = newValidator();

            // Boundary floor=11:00; row at 11:30 sits in bucket [11:00,12:00)
            // -> bucket end 12:00 > 11:00 -> rejected.
            try {
                final long managedZoneTs = parseFloorPartialTimestamp("2024-09-10T11:30:00.000000Z");
                validator.validate(
                        WalTxnType.DATA,
                        WalUtils.WAL_DEDUP_MODE_DEFAULT,
                        managedZoneTs,
                        managedZoneTs
                );
                Assert.fail("expected bucket-whole rejection");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "backfill row falls in or past the managed zone");
            }
        });
    }

    @Test
    public void testBoundaryFromAnchorNeverExceedsAnchorAndNeverThrows() {
        final TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
        final long anchor = driver.parseFloorLiteral("2024-09-10T12:00:00.000000Z");

        // Normal cases: boundary = anchor - LIMIT, strictly below the anchor.
        Assert.assertEquals(anchor - driver.fromHours(1), MatViewBackfillValidator.boundaryFromAnchor(driver, anchor, 1));
        Assert.assertTrue("months boundary must be before the anchor",
                MatViewBackfillValidator.boundaryFromAnchor(driver, anchor, -3) < anchor);

        // Safety contract that replaces the old assert: a REFRESH LIMIT only ever moves the
        // boundary back from the anchor, so boundaryFromAnchor must NEVER return a value
        // after the anchor and must NEVER throw -- even for absurd/corrupted limits whose
        // duration arithmetic overflows. (An AssertionError here would be a non-CairoException
        // that permanently distresses the mat-view WAL writer on a user commit.) When the
        // arithmetic overflows it saturates to Long.MIN_VALUE -- the whole view frozen, the
        // conservative direction.
        for (int limit : new int[]{Integer.MAX_VALUE, Integer.MIN_VALUE, 1_000_000_000, -1_000_000_000, 100_000, -100_000}) {
            final long boundary = MatViewBackfillValidator.boundaryFromAnchor(driver, anchor, limit);
            Assert.assertTrue("boundary must be at-or-before the anchor for limit=" + limit, boundary <= anchor);
        }
    }

    @Test
    public void testComputeFrozenBoundaryBucketFloorStaticOverloads() throws Exception {
        // Direct coverage for the public static computeFrozenBoundaryBucketFloor overloads.
        // Production only calls the 5-arg overload from the materialized_views() cursor, so
        // the 3-arg/4-arg entry points and their LIMIT==0 / escape-hatch guard branches are
        // reachable only from a direct caller. The 3-arg overload builds the sampler and opens
        // the base reader; the 4-arg applies the guards before delegating.
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("INSERT INTO base_price VALUES('a', 9.0, '2024-09-10T12:00')");
            drainWalAndMatViewQueues();
            currentMicros = parseFloorPartialTimestamp("2024-09-10T12:30:00.000000Z");

            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);

            // No REFRESH LIMIT yet: the 3-arg overload builds a sampler, then the 4-arg
            // overload short-circuits on LIMIT == 0. The 5-arg overload's own LIMIT==0 guard
            // does the same when a caller reuses a sampler.
            MatViewDefinition def = state.getViewDefinition();
            final TimestampSampler noLimitSampler = MatViewBackfillValidator.createBucketSampler(def);
            Assert.assertNotNull(noLimitSampler);
            Assert.assertEquals(
                    Numbers.LONG_NULL,
                    MatViewBackfillValidator.computeFrozenBoundaryBucketFloor(engine, def, state)
            );
            Assert.assertEquals(
                    Numbers.LONG_NULL,
                    MatViewBackfillValidator.computeFrozenBoundaryBucketFloor(engine, def, state, noLimitSampler, Long.MIN_VALUE)
            );

            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 1 HOUR");
            drainWalAndMatViewQueues();
            def = state.getViewDefinition();

            // LIMIT 1h, anchor = min(max(base_ts)=12:00, now=12:30) = 12:00, boundary = 11:00,
            // snapped to the 1h bucket floor = 11:00 (base driver units == micros here). The
            // 3-arg overload opens the base reader itself to read max(base_ts).
            final long expectedFloor = parseFloorPartialTimestamp("2024-09-10T11:00:00.000000Z");
            Assert.assertEquals(
                    expectedFloor,
                    MatViewBackfillValidator.computeFrozenBoundaryBucketFloor(engine, def, state)
            );

            // 5-arg overload with a caller-managed sampler + max(base_ts) snapshot: same floor.
            final TimestampSampler sampler = MatViewBackfillValidator.createBucketSampler(def);
            Assert.assertNotNull(sampler);
            final long maxBaseTs = parseFloorPartialTimestamp("2024-09-10T12:00:00.000000Z");
            Assert.assertEquals(
                    expectedFloor,
                    MatViewBackfillValidator.computeFrozenBoundaryBucketFloor(engine, def, state, sampler, maxBaseTs)
            );

            // Escape hatch on: every overload surfaces LONG_NULL so the gate and the metadata
            // stay consistent, no matter the LIMIT.
            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_LIMIT_WALL_CLOCK_ENABLED, "true");
            Assert.assertEquals(
                    Numbers.LONG_NULL,
                    MatViewBackfillValidator.computeFrozenBoundaryBucketFloor(engine, def, state)
            );
            Assert.assertEquals(
                    Numbers.LONG_NULL,
                    MatViewBackfillValidator.computeFrozenBoundaryBucketFloor(engine, def, state, sampler, maxBaseTs)
            );
        });
    }

    @Test
    public void testIsBackfillableMatViewGate() throws Exception {
        // Entry-point gate contract (CairoEngine.isBackfillableMatView): a plain table is never
        // backfillable-as-a-mat-view; a mat view is backfillable only with a non-zero REFRESH
        // LIMIT and the wall-clock escape hatch off.
        assertMemoryLeak(() -> {
            createBaseAndView();
            drainWalAndMatViewQueues();

            final TableToken baseToken = engine.verifyTableName("base_price");
            final TableToken viewToken = engine.verifyTableName("price_1h");

            // Non-mat-view token -> false.
            Assert.assertFalse(engine.isBackfillableMatView(baseToken));
            // Mat view without REFRESH LIMIT -> false.
            Assert.assertFalse(engine.isBackfillableMatView(viewToken));

            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 1 HOUR");
            drainWalAndMatViewQueues();
            // Mat view with a non-zero REFRESH LIMIT -> true.
            Assert.assertTrue(engine.isBackfillableMatView(viewToken));

            // Escape hatch on -> false regardless of LIMIT.
            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_LIMIT_WALL_CLOCK_ENABLED, "true");
            Assert.assertFalse(engine.isBackfillableMatView(viewToken));
        });
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("price_1h ORDER BY ts, price").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    // The base holds rows at 10:10 and 12:00 on 2024-09-10, the FULL refresh builds the view's 10:00 and 12:00
    // buckets, and the clock reads 12:30: with REFRESH LIMIT 1 HOUR the frozen zone ends at the 11:00 bucket floor.
    private void createRefreshedView() throws Exception {
        createBaseAndView();
        execute("INSERT INTO base_price VALUES ('a', 8.0, '2024-09-10T10:10'), ('a', 9.0, '2024-09-10T12:00')");
        drainWalQueue();
        execute("REFRESH MATERIALIZED VIEW price_1h FULL");
        drainWalAndMatViewQueues();
        assertViewRows("""
                sym\tprice\tts
                a\t8.0\t2024-09-10T10:00:00.000000Z
                a\t9.0\t2024-09-10T12:00:00.000000Z
                """);
        currentMicros = parseFloorPartialTimestamp("2024-09-10T12:30");
    }

    private static void appendViewRow(WalWriter walWriter, double price, String ts) {
        final TableWriter.Row row = walWriter.newRow(parseFloorPartialTimestamp(ts));
        row.putSym(0, "a");
        row.putDouble(1, price);
        row.append();
    }

    private static void assertInsertRejectedByGate(InsertOperation insert, double price, String ts) throws SqlException {
        try {
            executeInsert(insert, price, ts);
            Assert.fail("expected the commit to be refused without REFRESH LIMIT [ts=" + ts + ']');
        } catch (CairoException e) {
            TestUtils.assertEquals(GATE_MESSAGE, e.getFlyweightMessage());
        }
    }

    private static void assertRejectedByGate(MatViewBackfillValidator validator, long txnMinTs, long txnMaxTs) {
        try {
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, txnMinTs, txnMaxTs);
            Assert.fail("expected the commit to be refused without a frozen zone [txnMaxTs=" + txnMaxTs + ']');
        } catch (CairoException e) {
            TestUtils.assertEquals(GATE_MESSAGE, e.getFlyweightMessage());
            Assert.assertFalse(e.isCritical());
        }
    }

    // Timestamps without the fraction and zone suffix, which the message prints as .000000Z.
    private static void assertRejectedInManagedZone(
            MatViewBackfillValidator validator,
            String maxRowTs,
            String rowBucketLo,
            String rowBucketHi,
            String boundaryBucketFloor
    ) {
        final long ts = parseFloorPartialTimestamp(maxRowTs);
        try {
            validator.validate(WalTxnType.DATA, WalUtils.WAL_DEDUP_MODE_DEFAULT, ts, ts);
            Assert.fail("expected the bucket-whole rejection [maxRowTs=" + maxRowTs + ']');
        } catch (CairoException e) {
            TestUtils.assertEquals(managedZoneMessage(maxRowTs, rowBucketLo, rowBucketHi, boundaryBucketFloor), e.getFlyweightMessage());
            Assert.assertFalse(e.isCritical());
        }
    }

    // INSERT with the price and the timestamp as bind variables, which each execution reads anew.
    private static InsertOperation compileInsert(SqlCompiler compiler) throws SqlException {
        bindVariableService.clear();
        bindVariableService.setDouble("price", 0.0);
        bindVariableService.setTimestamp("ts", 0);
        final CompiledQuery cq = compiler.compile("INSERT INTO price_1h VALUES ('a', :price, :ts)", sqlExecutionContext);
        Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
        return cq.popInsertOperation();
    }

    private static void createBaseAndView() throws Exception {
        execute(
                "CREATE TABLE base_price (" +
                        "  sym SYMBOL, price DOUBLE, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL"
        );
        execute(
                "CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL DEFERRED AS " +
                        "(SELECT sym, last(price) AS price, ts FROM base_price SAMPLE BY 1h) " +
                        "PARTITION BY DAY"
        );
    }

    private static void executeInsert(InsertOperation insert, double price, String ts) throws SqlException {
        bindVariableService.setDouble("price", price);
        bindVariableService.setTimestamp("ts", parseFloorPartialTimestamp(ts));
        insert.execute(sqlExecutionContext);
    }

    private static String managedZoneMessage(String maxRowTs, String rowBucketLo, String rowBucketHi, String boundaryBucketFloor) {
        return "backfill row falls in or past the managed zone of materialized view [view=price_1h, maxRowTs=" + maxRowTs
                + ".000000Z, rowBucket=[" + rowBucketLo + ".000000Z, " + rowBucketHi + ".000000Z), boundaryBucketFloor="
                + boundaryBucketFloor + ".000000Z]; backfill bucket end must be at-or-before the boundary bucket floor";
    }

    private static MatViewBackfillValidator newValidator() {
        final TableToken viewToken = engine.verifyTableName("price_1h");
        return new MatViewBackfillValidator(engine, viewToken);
    }

    private static void setRefreshLimit(String limit) throws Exception {
        execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT " + limit);
        drainWalAndMatViewQueues();
    }

    private static long viewSeqTxn() {
        return engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("price_1h"));
    }
}
