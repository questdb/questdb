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

package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Crash consistency for the live {@code _meta} body checksum.
 * <p>
 * The checksum is carried as version-gated fields inside the record
 * ({@code META_OFFSET_BODY_LEN_64} / {@code META_OFFSET_BODY_CHECKSUM_64}), and several paths mutate
 * {@code _meta} IN PLACE rather than rewriting it -- the adaptive enrolment record writes straight to
 * an fd, and {@code resetMetadataVersion} edits the version field. An in-place mutation and its
 * checksum refresh are two separate writes, so a crash can land between them.
 * <p>
 * The invariant:
 * <pre>
 *   After a crash at ANY durability op, reopening the table must yield EITHER a _meta that loads
 *   cleanly, OR no table at all -- but NEVER a healthy _meta rejected as corrupt.
 * </pre>
 * A false "_meta checksum mismatch" is the worst outcome this design can produce: the bytes are
 * intact, the table is fine, and the database refuses to open it. That is strictly worse than having
 * no checksum, which is why it is swept here at every crash point rather than at one chosen one.
 */
public class MetaChecksumCrashTest extends AbstractCrashConsistencyTest {

    @Test
    public void testCrashSweepAcrossEnrolmentNeverRejectsHealthyMeta() throws Exception {
        // Enrolment writes the adaptive commit mode into _meta through a raw fd and then refreshes the
        // checksum before its fsync. Sweep every durability op so the window between those two writes
        // is actually landed on, rather than hoping one chosen point hits it.
        final int ops = countDurabilityOps();
        Assert.assertTrue("expected a real durability-op sequence to sweep, got " + ops, ops >= 8);

        for (int crashAt = 1; crashAt <= ops; crashAt++) {
            final int point = crashAt;
            runWithCrashFacade(() -> {
                final String t = "m" + point; // the db root persists across iterations
                execute("create table " + t + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into " + t + " values ('2024-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                markDurableBaseline();

                crashFf.armCrashAt(point);
                try {
                    execute("insert into " + t + " values ('2024-01-02T00:00:00.000000Z', 2)");
                    drainWalQueue();
                } catch (CrashSimulationError | CairoException e) {
                    // expected at most crash points
                }
                // The injection must actually have fired, or this iteration swept nothing.
                Assert.assertFalse("crash armed at op " + point + " never fired", crashFf.isCrashArmed());
                crashAndReopen();

                assertMetaLoadsOrTableIsGone(t, point);
            });
        }
    }

    @Test
    public void testCrashSweepAcrossStructuralChangeNeverRejectsHealthyMeta() throws Exception {
        // ALTER goes through rewriteMetadata: a full rewrite into _meta.swp, then a rename. The rename
        // is atomic, so the outcome must be the old _meta or the new one -- each with a checksum that
        // matches its own bytes, never a mix.
        final int ops = countDurabilityOpsForAlter();
        Assert.assertTrue("the ALTER workload must issue durability ops", ops > 0);

        for (int crashAt = 1; crashAt <= ops; crashAt++) {
            final int point = crashAt;
            runWithCrashFacade(() -> {
                final String t = "ma" + point;
                execute("create table " + t + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                drainWalQueue();
                markDurableBaseline();

                crashFf.armCrashAt(point);
                try {
                    execute("alter table " + t + " add column w double");
                    drainWalQueue();
                } catch (CrashSimulationError | CairoException e) {
                    // expected at most crash points
                }
                Assert.assertFalse("crash armed at op " + point + " never fired", crashFf.isCrashArmed());
                crashAndReopen();

                assertMetaLoadsOrTableIsGone(t, point);
            });
        }
    }

    @Test
    public void testTornChecksumFieldIsNeverReadAsAValidChecksum() throws Exception {
        // The bytes carrying the checksum can themselves be torn. Zeroing them must degrade to
        // "absent" -- the back-compatible direction -- not to a mismatch against real content.
        runWithCrashFacade(() -> {
            execute("create table mt (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into mt values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("mt");
            markDurableBaseline();

            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
                // Tear exactly the 16 bytes holding bodyLen + checksum.
                crashFf.tornTail(path.$(), TableUtils.META_OFFSET_BODY_LEN_64, 16);
            }
            crashAndReopen();

            // bodyLen == 0 classifies ABSENT, so the table loads unverified rather than being condemned.
            engine.getTableMetadata(engine.verifyTableName("mt")).close();
        });
    }

    private void assertMetaLoadsOrTableIsGone(String tableName, int crashPoint) {
        final TableToken token;
        try {
            token = engine.verifyTableName(tableName);
        } catch (CairoException e) {
            return; // the table did not survive the crash at all: acceptable
        }
        try {
            engine.getTableMetadata(token).close();
        } catch (CairoException e) {
            final CharSequence msg = e.getFlyweightMessage();
            if (containsAscii(msg, "checksum mismatch") || containsAscii(msg, "body length is impossible")) {
                Assert.fail("crash at op " + crashPoint
                        + " left a healthy _meta rejected as corrupt: " + msg);
            }
            // any other failure is a pre-existing recovery outcome, not this feature's concern
        }
    }

    private boolean containsAscii(CharSequence haystack, String needle) {
        return haystack != null && haystack.toString().contains(needle);
    }

    private int countDurabilityOps() throws Exception {
        final int[] ops = new int[1];
        runWithCrashFacade(() -> {
            execute("create table probe (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into probe values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            markDurableBaseline();
            final int before = crashFf.durabilityOpCount();
            execute("insert into probe values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            ops[0] = crashFf.durabilityOpCount() - before;
        });
        return ops[0];
    }

    private int countDurabilityOpsForAlter() throws Exception {
        final int[] ops = new int[1];
        runWithCrashFacade(() -> {
            execute("create table probea (ts timestamp, v long) timestamp(ts) partition by day wal");
            drainWalQueue();
            markDurableBaseline();
            final int before = crashFf.durabilityOpCount();
            execute("alter table probea add column w double");
            drainWalQueue();
            ops[0] = crashFf.durabilityOpCount() - before;
        });
        return ops[0];
    }
}
