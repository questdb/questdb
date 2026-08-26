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

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Cross-table ATTACH, driven through SQL.
 * <p>
 * {@code CompositeDetachedArtifact.checkSameTable} refuses a foreign artifact, and
 * {@code CompositeAttachArtifactTest#testRefusesAnArtifactFromAnotherTable} covers it -- but by calling
 * {@code checkSameTable} DIRECTLY. That proves the check works without establishing where the refusal
 * reaches a user, which is precisely the gap that let the UPDATE gate sit on the WAL-apply path
 * (statement returns OK, table suspends afterwards) until 2026-08-26.
 * <p>
 * WHY THIS MATTERS MORE THAN AN ORDINARY GATE: a cellKey is table-LOCAL. The artifact carries
 * {@code _meta}, {@code _cv} and {@code _txn} but not the dimension dictionaries or the {@code _cell}
 * registry, so a foreign artifact's cellKeys cannot be decoded here. Accepting one attaches its cells
 * onto whatever local cells happen to share those ordinals -- silently wrong data under a different
 * dimension value, which invariant 2 forbids. A row-count assertion is therefore not optional: the
 * table staying live proves nothing if foreign rows arrived.
 * <p>
 * SCAFFOLDING NOTE, learned the hard way: ATTACH consumes {@code <partition>.attachable}, NOT
 * {@code <partition>.detached}. A first version of this test moved the artifact but left it named
 * {@code .detached}; ATTACH then found nothing, returned OK, and changed nothing -- which reads
 * exactly like "the gate did not fire" and would have been reported as a defect. The row count is what
 * distinguished "silently attached foreign data" from "did nothing at all".
 */
public class CompositeAttachStatementGateTest extends AbstractCairoTest {

    /**
     * POSITIVE CONTROL. The identical detach/rename/attach dance against the SAME table must SUCCEED.
     * Without it, a passing cross-table assertion could be explained by ATTACH being broken generally,
     * or by the scaffolding never producing a usable artifact -- the exact failure this test already
     * hit once.
     */
    @Test(timeout = 120_000)
    public void testSameTableAttachRoundTripsThroughSql() throws Exception {
        assertMemoryLeak(() -> {
            createComposite("same");
            execute("ALTER TABLE same DETACH PARTITION LIST '2023-03-05'");
            drainWalQueue();

            final Path dir = tableDirOf("same");
            Files.move(dir.resolve("2023-03-05.detached"), dir.resolve("2023-03-05.attachable"));

            execute("ALTER TABLE same ATTACH PARTITION LIST '2023-03-05'");
            drainWalQueue();

            Assert.assertFalse("same-table attach must not suspend",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("same")));
            final StringSink sink = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM same", sink);
            TestUtils.assertContains(sink, "3");
        });
    }

    @Test(timeout = 120_000)
    public void testCrossTableAttachIsRefusedAndAttachesNothing() throws Exception {
        assertMemoryLeak(() -> {
            // recv must NOT already hold the donor's day, or ATTACH rejects with
            // ATTACH_ERR_PARTITION_EXISTS before the cross-table check is ever consulted -- measured,
            // and the second way this test managed to look conclusive while proving nothing.
            createCompositeOn("recv", "2023-02-01", "2023-02-02");
            createCompositeOn("donor", "2023-03-05", "2023-03-06");

            execute("ALTER TABLE donor DETACH PARTITION LIST '2023-03-05'");
            drainWalQueue();

            // Foreign artifact, correctly named so ATTACH actually consumes it.
            Files.move(tableDirOf("donor").resolve("2023-03-05.detached"),
                    tableDirOf("recv").resolve("2023-03-05.attachable"));

            boolean refusedAtStatement;
            String message = "";
            try {
                execute("ALTER TABLE recv ATTACH PARTITION LIST '2023-03-05'");
                refusedAtStatement = false;
            } catch (Throwable t) {
                refusedAtStatement = true;
                message = String.valueOf(t.getMessage());
            }
            drainWalQueue();

            final boolean suspended =
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("recv"));
            final StringSink sink = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM recv", sink);
            System.out.println("=== XATTACH refusedAtStatement=" + refusedAtStatement
                    + " suspended=" + suspended + " msg=" + message + " count=" + sink);

            // THE LOAD-BEARING GUARANTEE, and the one this test locks: no foreign row may arrive,
            // however the refusal is delivered. recv holds exactly its own 3 seeded rows. Invariant 2
            // holds -- the gate does its actual job.
            TestUtils.assertContains(sink, "3");

            // KNOWN GAP, deliberately NOT asserted either way (2026-08-26).
            //
            // MEASURED here: refusedAtStatement=false, suspended=TRUE. The statement returns OK and the
            // table suspends -- the same invariant-6 shape as the UPDATE gate fixed in 96f7279084.
            //
            // The control that makes this a defect rather than "how WAL ALTER works": every SIBLING
            // attach validation in TableWriter#attachPartition RETURNS an AttachDetachStatus and is
            // TOLERATED without suspending (measured: ATTACH_ERR_PARTITION_EXISTS logs
            // "tolerated WAL command failure" and leaves the table live). Only the composite
            // cross-table check throws a critical CairoException, and only it suspends.
            //
            // Not asserted because the remedy is a genuine choice, not a mechanical fix:
            //   (a) return a new AttachDetachStatus (e.g. ATTACH_ERR_FOREIGN_TABLE) so it is tolerated
            //       like its siblings -- consistent and small, but the user still gets no statement
            //       error, so invariant 6 is still not met; or
            //   (b) inspect the artifact at COMPILE time so the statement itself fails -- true
            //       invariant 6, but it puts filesystem artifact inspection in the SQL compiler.
            // Asserting today's behaviour would lock the bug in; asserting the fixed behaviour would
            // leave a red test. So the guarantee above is locked and the gap is documented.
            System.out.println("=== XATTACH known gap: refusedAtStatement=" + refusedAtStatement
                    + " suspended=" + suspended);
        });
    }

    private void createComposite(String name) throws Exception {
        createCompositeOn(name, "2023-03-05", "2023-03-06");
    }

    /**
     * Two cells on {@code day}, so it is a genuine multi-cell container, PLUS a row on {@code laterDay}
     * so {@code day} is not the ACTIVE partition -- DETACH refuses the active one with
     * DETACH_ERR_ACTIVE, which is the third distinct way this test's scaffolding managed to fail while
     * looking like a product verdict.
     */
    private void createCompositeOn(String name, String day, String laterDay) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                + "PARTITION BY DAY, exch LAYOUT PLAIN WAL");
        execute("INSERT INTO " + name + " VALUES ('" + day + "T01:00:00.000000Z','A',1.0),"
                + "('" + day + "T02:00:00.000000Z','B',2.0),"
                + "('" + laterDay + "T01:00:00.000000Z','A',3.0)");
        drainWalQueue();
        engine.releaseInactive();
    }

    private Path tableDirOf(String name) throws Exception {
        final Path root = Paths.get(configuration.getDbRoot());
        try (Stream<Path> walk = Files.walk(root, 1)) {
            for (Path p : walk.filter(Files::isDirectory).toList()) {
                if (p.getFileName().toString().startsWith(name + "~")) {
                    return p;
                }
            }
        }
        throw new AssertionError("table dir not found for " + name);
    }
}
