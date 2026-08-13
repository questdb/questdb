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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * WHAT ORDERS WHAT inside an adaptive WAL commit: is the barrier between a segment's COLUMN data and its
 * {@code _event} record load-bearing on its own, or is the only load-bearing barrier the one between
 * {@code {columns, _event}} as a set and the SHARED SEQUENCER txn-log record that publishes them?
 *
 * <p><b>Why the question matters.</b> {@code WalWriter.syncIfRequired0} pays one {@code fdatasync} per column
 * plus one for each {@code _event} file ({@code _event}, {@code _event.i}, {@code _event.c}) plus one for the
 * sequencer log, on the ingest thread, every commit — each a separate filesystem journal force.
 * {@code TableWriter.syncColumnsBatchedSync} already replaces exactly that shape on the materialized-state
 * path with ONE {@code syncfs}, which journals every inode in a single force. A {@code syncfs} cannot order
 * columns against {@code _event}, though: both become durable in the same barrier, so a crash inside it can
 * leave {@code _event} durable while a column is not. Porting the batched flush to the WAL path is therefore
 * sound only if that state is harmless. This decides it by construction rather than by argument — a
 * guarantee concluded by reasoning is not a guarantee.
 *
 * <p><b>Three cases over one construction.</b> Global {@code adaptive}, {@code W=0} (every barrier inline
 * and deterministic), per-inode journaling so no flush incidentally journals a neighbouring inode. Each case
 * runs an unarmed COUNT commit to learn the commit's real durability-op sequence, then replays an identical
 * commit with a crash armed at a named op and hand-promotes a chosen file set to durable
 * ({@code markFileDurable} models the kernel writing a mapped file back on its own).
 * <p>The facade applies an op's durability effect and only THEN fires the armed crash, so "armed at op k"
 * means ops {@code 1..k} took effect and nothing after them did.
 * <ol>
 *   <li>{@link #testEventRecordDurableAheadOfColumnsIsInertWithoutTheSequencerRecord()} — crash after op 1,
 *       so the first column barrier ran and no later one did; promote {@code _event*} only. That is the
 *       interleaving a {@code syncfs} risks: {@code _event} durable while a column of the same txn is not,
 *       and no sequencer record naming either. The txn must be invisible, and the case asserts the last
 *       column really is still at its pre-commit durable content.</li>
 *   <li>{@link #testTxnBeforeTheSequencerBarrierIsInvisibleDespiteDurableColumnsAndEvents()} — crash on the
 *       op immediately BEFORE the sequencer barrier: every column and {@code _event} file durable, sequencer
 *       barrier not run, nothing promoted. The txn must be invisible.</li>
 *   <li>{@link #testTxnBecomesVisibleOnceTheSequencerBarrierRuns()} — the same commit crashed ONE op later,
 *       so the sequencer barrier has run. The txn must be visible.</li>
 * </ol>
 * Cases 2 and 3 are the single-gate A/B: adjacent crash points straddling exactly one barrier, everything
 * else identical, and they disagree. That identifies the sequencer barrier as the publication point, which
 * is what makes case 1's result mean "no sequencer record names this" rather than "the test wrote nothing".
 *
 * <p>If case 1 ever starts failing, the WAL batched-flush port is unsound as specified and the {@code _event}
 * barrier must stay separate from the column barrier.
 */
public class AdaptiveWalEventOrderCrashTest extends AbstractAdaptiveCrashTest {

    private static final int BASELINE_ROWS = 4;
    /**
     * Rows durable and applied before the ARMED commit: the baseline plus the unarmed count-pass row.
     */
    private static final int ROWS_BEFORE_ARMED_COMMIT = BASELINE_ROWS + 1;

    /**
     * The state a WAL-side {@code syncfs} could leave behind: a segment's {@code _event} record is durable,
     * the columns of that txn are not, and no sequencer txn-log record names it. The record must be inert.
     */
    @Test
    public void testEventRecordDurableAheadOfColumnsIsInertWithoutTheSequencerRecord() throws Exception {
        withAdaptiveW0(() -> {
            final TableToken tt = seedBaseline();
            final Path segmentDir = latestWalSegmentDir(tt);
            final Path eventFile = segmentDir.resolve(WalUtils.EVENT_FILE_NAME);
            final List<String> phaseOps = countPassOps(tt);
            // The LAST column barrier of the commit — the column this case must leave NON-durable.
            final Path lastColumn = segmentDir.resolve(lastColumnFileName(phaseOps));

            markDurableBaseline();
            final byte[] eventBefore = crashFf.durableContentOf(eventFile.toString());
            final byte[] lastColumnBefore = crashFf.durableContentOf(lastColumn.toString());

            // The facade applies an op's durability effect and THEN fires the armed crash, so arming at op 1
            // means the FIRST column barrier completed and nothing after it did. The event record is already
            // appended to the mapped _event file (events.appendData precedes syncIfRequired), so promoting
            // _event below yields exactly the interleaving a syncfs risks: _event durable while a column of
            // the same txn is not, and no sequencer record naming either.
            armAndInsert(tt, 1, ROWS_BEFORE_ARMED_COMMIT);

            // Hand the kernel's writeback to the _event files ONLY. This is the syncfs interleaving.
            promoteDurable(segmentDir, name -> name.startsWith(WalUtils.EVENT_FILE_NAME));
            assertPromotionChanged("_event", eventBefore, crashFf.durableContentOf(eventFile.toString()));
            Assert.assertArrayEquals(
                    "this case is only about _event running AHEAD of the columns: " + lastColumn.getFileName()
                            + " must still be at its pre-commit durable content",
                    lastColumnBefore, crashFf.durableContentOf(lastColumn.toString())
            );

            recoverAfterCrash(new TableToken[]{tt});

            Assert.assertFalse("table must not be suspended", anyTableSuspended(tt));
            assertRows(
                    "a durable _event record that no durable sequencer record names must be INERT",
                    ROWS_BEFORE_ARMED_COMMIT
            );

            // The orphan record is still physically on disk — ignored, not erased. If any reader ever began
            // scanning _event independently of the sequencer, the row-count assertion above would break.
            Assert.assertFalse(
                    "the promoted orphan event record must survive the crash on disk",
                    Arrays.equals(eventBefore, crashFf.durableContentOf(eventFile.toString()))
            );

            // ...and the table is still usable.
            execute("insert into x values ('2024-10-01T09:00:00.000000Z', 99)");
            drainWalQueue();
            final List<Long> after = readVs(engine, "x");
            Assert.assertEquals("table must accept writes after recovery", ROWS_BEFORE_ARMED_COMMIT + 1, after.size());
            Assert.assertEquals(Long.valueOf(99), after.get(after.size() - 1));
        });
    }

    /**
     * Control arm A of the single-gate A/B: crash on the op immediately BEFORE the sequencer barrier, so
     * every column and every {@code _event} file of the txn is durable and the sequencer barrier has not
     * run. Nothing is promoted. The txn must be invisible.
     */
    @Test
    public void testTxnBeforeTheSequencerBarrierIsInvisibleDespiteDurableColumnsAndEvents() throws Exception {
        withAdaptiveW0(() -> {
            final TableToken tt = seedBaseline();
            final int firstSeqOp = firstSequencerOp(countPassOps(tt));

            markDurableBaseline();
            armAndInsert(tt, firstSeqOp - 1, ROWS_BEFORE_ARMED_COMMIT);

            recoverAfterCrash(new TableToken[]{tt});

            Assert.assertFalse("table must not be suspended", anyTableSuspended(tt));
            assertRows(
                    "a segment txn the sequencer barrier never covered must be invisible even though all of "
                            + "its columns and _event files ARE durable",
                    ROWS_BEFORE_ARMED_COMMIT
            );
        });
    }

    /**
     * Control arm B: the SAME commit crashed ONE op later — the sequencer barrier has now run. Nothing else
     * differs and nothing is promoted, so the two arms straddle exactly one barrier; they must disagree.
     * That is what identifies the sequencer barrier as the publication point, and so what makes the inert
     * result in {@link #testEventRecordDurableAheadOfColumnsIsInertWithoutTheSequencerRecord()} evidence
     * about {@code _event} rather than an artifact of a workload that wrote nothing.
     */
    @Test
    public void testTxnBecomesVisibleOnceTheSequencerBarrierRuns() throws Exception {
        withAdaptiveW0(() -> {
            final TableToken tt = seedBaseline();
            final int firstSeqOp = firstSequencerOp(countPassOps(tt));

            markDurableBaseline();
            armAndInsert(tt, firstSeqOp, ROWS_BEFORE_ARMED_COMMIT);

            recoverAfterCrash(new TableToken[]{tt});

            Assert.assertFalse("table must not be suspended", anyTableSuspended(tt));
            assertRows(
                    "once the sequencer barrier covers it, the segment txn IS published",
                    ROWS_BEFORE_ARMED_COMMIT + 1
            );
        });
    }

    /**
     * Arm a crash at op {@code k} of the next commit, insert row {@code v}, and assert the crash fired.
     */
    private void armAndInsert(TableToken tt, int k, int v) throws SqlException {
        crashFf.armCrashAt(crashFf.durabilityOpCount() + k);
        boolean fired = false;
        try {
            execute("insert into x values ('2024-10-01T0" + v + ":00:00.000000Z', " + v + ")");
        } catch (CrashSimulationError expected) {
            fired = true;
        }
        if (!fired) {
            fired = anyTableSuspended(tt); // the WAL-apply path swallows the Error into a suspend
        }
        Assert.assertTrue(
                "the crash armed at op " + k + " never fired — the commit's durability-op sequence is not "
                        + "what this test assumes",
                fired && !crashFf.isCrashArmed()
        );
    }

    /**
     * Guard against a vacuous promotion: {@code durableContentOf} returns null for an untracked file, and
     * {@code Arrays.equals(null, bytes)} is false, so a bare inequality check would pass even when the
     * promotion did nothing. Require both a tracked before-image and a real change.
     */
    private void assertPromotionChanged(String what, byte[] before, byte[] after) {
        Assert.assertNotNull(what + " must be tracked by the durability model before promotion", before);
        Assert.assertNotNull(what + " must be tracked by the durability model after promotion", after);
        Assert.assertFalse(
                "promoting " + what + " must change its durable content — otherwise the bytes under test "
                        + "were never written and this case proves nothing",
                Arrays.equals(before, after)
        );
    }

    private void assertRows(String why, int expected) {
        final List<Long> rows = readVs(engine, "x");
        Assert.assertEquals(why, expected, rows.size());
        for (int i = 0; i < expected; i++) {
            Assert.assertEquals("row " + i, Long.valueOf(i), rows.get(i));
        }
    }

    /**
     * Run ONE unarmed adaptive commit and return the durability ops it performed, one line each
     * ({@code <n> <kind> <db-root-relative path>}). The row it writes is fully durable and becomes part of
     * the pre-armed baseline, so the armed commit that follows is structurally identical to this one — same
     * table, same segment, same schema — and its op indices therefore mean what this log says they mean.
     */
    private List<String> countPassOps(TableToken tt) throws SqlException {
        final int base = crashFf.durabilityOpCount();
        execute("insert into x values ('2024-10-01T0" + BASELINE_ROWS + ":00:00.000000Z', " + BASELINE_ROWS + ")");
        final List<String> full = crashFf.durabilityOpLog();
        final List<String> phase = new ArrayList<>(
                full.subList(base, Math.min(full.size(), crashFf.durabilityOpCount())));
        Assert.assertFalse("an adaptive W=0 commit must perform durability ops", phase.isEmpty());
        drainWalQueue();
        Assert.assertFalse("count pass must not suspend the table", anyTableSuspended(tt));
        return phase;
    }

    /**
     * The file name of the LAST column barrier in the commit phase — the ops before the first {@code _event}
     * op are the column barriers, in traversal order. Read from the op log rather than assumed from the
     * schema, so it stays correct if the column set or the barrier order changes.
     */
    private String lastColumnFileName(List<String> phaseOps) {
        String last = null;
        for (String op : phaseOps) {
            final String name = op.substring(op.lastIndexOf('/') + 1);
            if (name.startsWith(WalUtils.EVENT_FILE_NAME)) {
                break;
            }
            last = name;
        }
        Assert.assertNotNull(
                "the commit phase must contain at least one column barrier before its _event barriers:\n"
                        + String.join("\n", phaseOps),
                last
        );
        return last;
    }

    private Path dbRootPath() {
        return Paths.get(engine.getConfiguration().getDbRoot().toString()).toAbsolutePath();
    }

    /**
     * The 1-based index, within the commit phase, of its FIRST sequencer-log barrier. Everything before it
     * is a column or {@code _event} barrier, so arming there yields "columns and events durable, sequencer
     * record written but volatile". Located by scanning the op log rather than pinned to a number, so the
     * case does not silently target the wrong op when the schema or the barrier set changes.
     */
    /**
     * True when the op's PATH ends with {@code marker}. The op log lines are "&lt;kind&gt; &lt;path&gt;", so the
     * path is the last whitespace-separated field.
     */
    private static boolean endsWithOp(String op, String marker) {
        final int sp = op.lastIndexOf(' ');
        final String path = sp < 0 ? op : op.substring(sp + 1);
        return path.endsWith(marker);
    }

    private int firstSequencerOp(List<String> phaseOps) {
        // endsWith, NOT contains: the V1 sequencer's CRC sidecar is "_txnlog.c", whose path CONTAINS
        // "txn_seq/_txnlog". A contains-match therefore locks onto the sidecar op -- which is ordered
        // deliberately BEFORE the header -- and identifies the wrong barrier, crashing an op early.
        // The barrier this test is about is the header write that publishes the txn.
        final String marker = WalUtils.SEQ_DIR + "/" + WalUtils.TXNLOG_FILE_NAME;
        for (int i = 0; i < phaseOps.size(); i++) {
            if (endsWithOp(phaseOps.get(i), marker)) {
                Assert.assertTrue(
                        "the sequencer barrier must not be the commit's first durability op — this case needs "
                                + "the column/event barriers to run first:\n" + String.join("\n", phaseOps),
                        i > 0
                );
                return i + 1; // ops are 1-based within the phase
            }
        }
        throw new AssertionError("no sequencer-log durability op in the commit phase:\n"
                + String.join("\n", phaseOps));
    }

    /**
     * The WAL segment directory the table is currently writing into: the highest {@code walN/segment} under
     * the table dir that holds an {@code _event} file. Discovered rather than assumed.
     */
    private Path latestWalSegmentDir(TableToken tt) {
        final Path tableDir = dbRootPath().resolve(tt.getDirName());
        final List<Path> segments = new ArrayList<>();
        try (Stream<Path> walDirs = Files.list(tableDir)) {
            walDirs.filter(Files::isDirectory)
                    .filter(d -> d.getFileName().toString().startsWith(WalUtils.WAL_NAME_BASE)
                            && !d.getFileName().toString().equals(WalUtils.SEQ_DIR))
                    .forEach(walDir -> {
                        try (Stream<Path> segDirs = Files.list(walDir)) {
                            segDirs.filter(Files::isDirectory)
                                    .filter(s -> Files.exists(s.resolve(WalUtils.EVENT_FILE_NAME)))
                                    .forEach(segments::add);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Assert.assertFalse("no WAL segment with an _event file under " + tableDir, segments.isEmpty());
        segments.sort(Comparator.comparing(Path::toString));
        return segments.get(segments.size() - 1);
    }

    /**
     * Promote every matching regular file in {@code dir} to durable — the kernel writing those mapped files
     * back of its own accord, with no barrier from QuestDB.
     */
    private void promoteDurable(Path dir, Predicate<String> nameFilter) {
        try (Stream<Path> files = Files.walk(dir)) {
            files.filter(Files::isRegularFile)
                    .filter(f -> nameFilter.test(f.getFileName().toString()))
                    .forEach(f -> crashFf.markFileDurable(f.toString()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Strict read: all rows of {@code table} in ts order, throwing on any error.
     */
    private List<Long> readVs(CairoEngine eng, String table) {
        final List<Long> out = new ArrayList<>();
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(eng);
                RecordCursorFactory f = eng.select("select v from " + table + " order by ts", ctx)
        ) {
            try (RecordCursor c = f.getCursor(ctx)) {
                Record r = c.getRecord();
                while (c.hasNext()) {
                    out.add(r.getLong(0));
                }
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    /**
     * Create table {@code x} and commit + apply {@link #BASELINE_ROWS} rows under a durable epoch, so the
     * baseline is a durable cut recovery can rewind to.
     */
    private TableToken seedBaseline() throws SqlException {
        execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
        for (int i = 0; i < BASELINE_ROWS; i++) {
            execute("insert into x values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
        }
        drainWalQueue();
        return engine.verifyTableName("x");
    }

    /**
     * Global {@code adaptive}, {@code W=0} (every barrier inline, so op indices are deterministic), an epoch
     * on the first applied batch, and per-inode journaling so flushing one file never incidentally journals
     * another.
     */
    private void withAdaptiveW0(TestUtils.LeakProneCode body) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false;
                body.run();
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }
}
