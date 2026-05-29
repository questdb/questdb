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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.mv.MatViewBackfillValidator;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertContains;

/**
 * Direct unit coverage for {@link MatViewBackfillValidator} passthrough
 * branches and bucket-whole enforcement. The MatViewTest suite exercises the
 * validator end-to-end through SQL; this class targets the passthrough paths
 * (non-DATA txn type, REPLACE_RANGE dedup mode, empty txn, LIMIT=0, sampler
 * setup) which are coverage-dead through SQL and pins the documented
 * stale-TUD-cache behaviour where LIMIT=0 bypasses the bucket-whole check.
 */
public class MatViewBackfillValidatorTest extends AbstractCairoTest {

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
    public void testValidatePassthroughOnLimitZero() throws Exception {
        // Pin the documented stale-TUD-cache behaviour: when REFRESH LIMIT is 0
        // the validator returns early without enforcing the bucket-whole rule.
        // The entry-point gate (engine.isBackfillableMatView) is meant to be
        // the authoritative contract; this branch exists so internal write
        // paths and stale ILP/QWP TUD caches do not crash but are documented
        // as a known limitation -- direct INSERTs are caught by the
        // compile-time gate, but ILP rows can slip through until cache TTL.
        assertMemoryLeak(() -> {
            createBaseAndView();
            // No SET REFRESH LIMIT => refreshLimitHoursOrMonths == 0.
            drainWalAndMatViewQueues();

            currentMicros = parseFloorPartialTimestamp("2024-09-10T12:30:00.000000Z");
            final MatViewBackfillValidator validator = newValidator();

            // A row that would be rejected if LIMIT were set is silently
            // passed through here. The contract is documented in
            // MatViewBackfillValidator's javadoc.
            validator.validate(
                    WalTxnType.DATA,
                    WalUtils.WAL_DEDUP_MODE_DEFAULT,
                    parseFloorPartialTimestamp("2024-09-10T12:00:00.000000Z"),
                    parseFloorPartialTimestamp("2024-09-10T12:00:00.000000Z")
            );
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

    private static MatViewBackfillValidator newValidator() {
        final TableToken viewToken = engine.verifyTableName("price_1h");
        return new MatViewBackfillValidator(engine, viewToken);
    }
}
