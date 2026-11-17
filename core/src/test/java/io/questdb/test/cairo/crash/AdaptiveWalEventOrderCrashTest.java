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
 * What orders what inside an adaptive WAL commit: is the barrier between a segment's column data and its {@code _event}
 * record needed on its own, or is the only needed barrier the one between {@code {columns, _event}} as a set and the
 * shared sequencer txn-log record that publishes them?
 *
 * <p>{@code WalWriter.syncIfRequired0} pays one {@code fdatasync} per column, one per {@code _event} file
 * ({@code _event}, {@code _event.i}) and one for the sequencer log, on the ingest thread, every commit.
 * {@code TableWriter.syncColumnsBatchedSync} replaces that shape on the materialized-state path with one
 * {@code syncfs}. A {@code syncfs} cannot order columns against {@code _event}: a crash inside it can leave
 * {@code _event} durable while a column is not. Batching the WAL path the same way is sound only if that state
 * is harmless, and this class constructs it.
 *
 * <p><b>Three cases over one construction.</b> Global {@code adaptive}, {@code W=0} (every barrier inline and
 * deterministic), per-inode journaling so no flush journals a neighbouring inode. Each case runs an unarmed
 * count commit to learn the commit's durability-op sequence, then replays an identical commit with a crash
 * armed at a named op and promotes a chosen file set to durable ({@code markFileDurable} models the kernel
 * writing a mapped file back on its own).
 * <p>The facade applies an op's durability effect and then fires the armed crash, so "armed at op k" means ops
 * {@code 1..k} took effect and nothing after them did.
 * <ol>
 *   <li>{@link #testEventRecordDurableAheadOfColumnsIsInertWithoutTheSequencerRecord()}: crash on the first
 *       column barrier and promote {@code _event*} only, the interleaving a {@code syncfs} risks: {@code _event}
 *       durable, a column of the same txn not, and no sequencer record naming either. The txn must be
 *       invisible; the case also asserts the last column is still at its pre-commit durable content.</li>
 *   <li>{@link #testTxnBeforeTheSequencerBarrierIsInvisibleDespiteDurableColumnsAndEvents()}: crash on the op
 *       just before the sequencer barrier: every column and {@code _event} file durable, sequencer barrier not
 *       run, nothing promoted. The txn must be invisible.</li>
 *   <li>{@link #testTxnBecomesVisibleOnceTheSequencerBarrierRuns()}: the same commit crashed one op later, so
 *       the sequencer barrier has run. The txn must be visible.</li>
 * </ol>
 * Cases 2 and 3 straddle exactly one barrier and disagree, which identifies the sequencer barrier as the
 * publication point. That makes case 1's result mean "no sequencer record names this" rather than "the test
 * wrote nothing".
 *
 * <p>Case 1 is the precondition for batching the WAL column and {@code _event} barriers into one barrier; if it
 * fails, the {@code _event} barrier must stay separate from the column barrier.
 */
public class AdaptiveWalEventOrderCrashTest extends AbstractAdaptiveCrashTest {

    private static final int BASELINE_ROWS = 4;
    /**
     * Rows durable and applied before the armed commit: the baseline plus the unarmed count-pass row.
     */
    private static final int ROWS_BEFORE_ARMED_COMMIT = BASELINE_ROWS + 1;

    /**
     * The state a WAL-side {@code syncfs} could leave behind: a segment's {@code _event} record is durable,
     * the columns of that txn are not, and no sequencer txn-log record names it. The record must be inert.
     */
    @Test
    public void testEventRecordDurableAheadOfColumnsIsInertWithoutTheSequencerRecord() throws Exception {
        // The writeback drain puts every column's bytes in the device cache before the first barrier, whose
        // device flush then makes them all durable. This case needs a column left behind, so the drain is off.
        node1.setProperty(PropertyKey.CAIRO_WAL_COMMIT_WRITEBACK_DRAIN, false);
        withAdaptiveW0(() -> {
            final TableToken tt = seedBaseline();
            final Path segmentDir = latestWalSegmentDir(tt);
            final Path eventFile = segmentDir.resolve(WalUtils.EVENT_FILE_NAME);
            final List<String> phaseOps = countPassOps(tt);
            // The first column barrier of the commit, where the crash is armed, and the last one, the column this case
            // leaves non-durable.
            final int firstColumnOp = firstColumnBarrierOp(phaseOps);
            final Path firstColumn = segmentDir.resolve(fileName(phaseOps.get(firstColumnOp - 1)));
            final Path lastColumn = segmentDir.resolve(lastColumnFileName(phaseOps));

            markDurableBaseline();
            final byte[] eventBefore = crashFf.durableContentOf(eventFile.toString());
            final byte[] firstColumnBefore = crashFf.durableContentOf(firstColumn.toString());
            final byte[] lastColumnBefore = crashFf.durableContentOf(lastColumn.toString());

            // The facade applies an op's durability effect and then fires the armed crash, so arming at the first
            // column barrier means it completed and nothing after it did. The event record is already appended to the
            // mapped _event file (events.appendData precedes syncIfRequired), so promoting _event below yields the
            // interleaving a syncfs risks: _event durable while a column of the same txn is not, and no sequencer
            // record naming either.
            armAndInsert(tt, firstColumnOp, ROWS_BEFORE_ARMED_COMMIT);
            Assert.assertFalse(
                    "the crash must follow the first column barrier: " + firstColumn.getFileName() + " must be durable",
                    Arrays.equals(firstColumnBefore, crashFf.durableContentOf(firstColumn.toString()))
            );

            // Hand the kernel's writeback to the _event files only: the syncfs interleaving.
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

            // The orphan record is still on disk, ignored rather than erased. A reader that scanned _event
            // independently of the sequencer would break the row-count assertion above.
            Assert.assertFalse(
                    "the promoted orphan event record must survive the crash on disk",
                    Arrays.equals(eventBefore, crashFf.durableContentOf(eventFile.toString()))
            );

            // The table is still usable.
            execute("insert into x values ('2024-10-01T09:00:00.000000Z', 99)");
            drainWalQueue();
            final List<Long> after = readVs(engine, "x");
            Assert.assertEquals("table must accept writes after recovery", ROWS_BEFORE_ARMED_COMMIT + 1, after.size());
            Assert.assertEquals(Long.valueOf(99), after.get(after.size() - 1));
        });
    }

    /**
     * Control arm A: crash on the op just before the sequencer barrier, so every column and every {@code _event}
     * file of the txn is durable and the sequencer barrier has not run. Nothing is promoted. The txn must be
     * invisible.
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
     * Control arm B: the same commit crashed one op later, after the sequencer barrier. Nothing else differs and
     * nothing is promoted, so the two arms straddle exactly one barrier and must disagree. That identifies the
     * sequencer barrier as the publication point, so the inert result in
     * {@link #testEventRecordDurableAheadOfColumnsIsInertWithoutTheSequencerRecord()} is about {@code _event}
     * rather than a workload that wrote nothing.
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
     * Arms a crash at op {@code k} of the next commit, inserts row {@code v}, and asserts the crash fired.
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
     * Guards against a vacuous promotion: {@code durableContentOf} returns null for an untracked file, and
     * {@code Arrays.equals(null, bytes)} is false, so a bare inequality check would pass when the promotion did
     * nothing. Requires a tracked before-image and a real change.
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
     * Runs one unarmed adaptive commit and returns the durability ops it performed, one line each
     * ({@code <n> <kind> <db-root-relative path>}). Its row is fully durable and joins the pre-armed baseline, so
     * the armed commit that follows is structurally identical (same table, segment and schema) and its op indices
     * mean what this log says.
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
     * The file name of the last column barrier in the commit phase: the {@code fdatasync} ops before the first
     * {@code _event} one are the column barriers, in traversal order. Read from the op log rather than assumed from
     * the schema, so it stays correct if the column set or the barrier order changes.
     */
    private String lastColumnFileName(List<String> phaseOps) {
        String last = null;
        for (String op : phaseOps) {
            if (!isFdatasync(op)) {
                continue;
            }
            final String name = fileName(op);
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
     * True when the op's path ends with {@code marker}. The op log lines are "&lt;kind&gt; &lt;path&gt;", so the
     * path is the last whitespace-separated field.
     */
    private static boolean endsWithOp(String op, String marker) {
        final int sp = op.lastIndexOf(' ');
        final String path = sp < 0 ? op : op.substring(sp + 1);
        return path.endsWith(marker);
    }

    private static String fileName(String op) {
        return op.substring(op.lastIndexOf('/') + 1);
    }

    private static boolean isFdatasync(String op) {
        return op.split(" ")[1].equals("fdatasync");
    }

    /**
     * The 1-based index, within the commit phase, of its first column barrier: the first {@code fdatasync}, which
     * runs before the {@code _event} and sequencer barriers. Located in the op log rather than pinned to a number,
     * so the case still crashes right after a column barrier when the sync calls ahead of the barriers change.
     */
    private int firstColumnBarrierOp(List<String> phaseOps) {
        for (int i = 0; i < phaseOps.size(); i++) {
            final String op = phaseOps.get(i);
            if (isFdatasync(op)) {
                Assert.assertFalse(
                        "the commit's first barrier must be a column barrier:\n" + String.join("\n", phaseOps),
                        fileName(op).startsWith(WalUtils.EVENT_FILE_NAME) || op.contains(WalUtils.SEQ_DIR)
                );
                return i + 1; // ops are 1-based within the phase
            }
        }
        throw new AssertionError("no fdatasync in the commit phase:\n" + String.join("\n", phaseOps));
    }

    /**
     * The 1-based index, within the commit phase, of its first sequencer-log barrier. Everything before it is a
     * column or {@code _event} barrier, so arming there yields "columns and events durable, sequencer record written
     * but volatile". Located in the op log rather than pinned to a number, so the case does not target the wrong op
     * when the schema or the barrier set changes.
     */
    private int firstSequencerOp(List<String> phaseOps) {
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
     * Promotes every matching regular file in {@code dir} to durable: the kernel writing those mapped files back
     * on its own, with no barrier from QuestDB.
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
     * Creates table {@code x} and commits and applies {@link #BASELINE_ROWS} rows under a durable epoch, so the
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
