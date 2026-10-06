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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A process crash right after a partition operation must not change what the table holds. After a crash,
 * startup recovery rewinds an adaptive WAL table to its last save point and re-applies the WAL after it, so
 * an operation that consumes or destroys files the WAL cannot recreate is lost unless a save point follows
 * it. Each case runs a partition operation on a table with a given save-point history, crashes before any
 * later save point, restarts, and requires the rows, the partition list and the WAL apply position to equal
 * what the table showed before the crash, which is the result of the same statements without a crash.
 * Each operation runs both through the WAL and directly in the writer of a hard-suspended table, the way
 * {@code FORCE DROP PARTITION} always runs; a direct change is not in the WAL at all.
 * <p>
 * The crash is a process kill: everything written reached the page cache, nothing is synced on the way
 * out. A clean close would take a save point, so the close-time save point is switched off, and the restart
 * is a fresh {@link CairoEngine}, which runs startup recovery before the WAL apply resumes. The NOSYNC
 * history is the control: nothing is rewound there.
 */
@RunWith(Parameterized.class)
public class AdaptivePartitionDdlCrashTest extends AbstractCairoTest {
    private static final long NO_PERIODIC_SAVE_POINT_MS = 3_600_000L;
    private static final String PARTITION = "2024-01-02";
    private final History history;
    private final boolean isDirect;
    private final Operation operation;

    public AdaptivePartitionDdlCrashTest(History history, Operation operation, boolean isDirect) {
        this.history = history;
        this.operation = operation;
        this.isDirect = isDirect;
    }

    @Parameterized.Parameters(name = "{0}, {1}, direct={2}")
    public static Collection<Object[]> data() {
        final List<Object[]> params = new ArrayList<>();
        for (History history : History.values()) {
            for (Operation operation : Operation.values()) {
                params.add(new Object[]{history, operation, false});
                // TRUNCATE is not forced through on a suspended table.
                if (operation != Operation.TRUNCATE) {
                    params.add(new Object[]{history, operation, true});
                }
            }
        }
        return params;
    }

    @Test
    public void testCrashAfterOperation() throws Exception {
        final boolean startsNosync = history == History.NOSYNC || history == History.ENROLLED_ON_WRITER_OPEN;
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, startsNosync ? "nosync" : "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, history == History.INLINE_SAVE_POINTS_ONLY ? -1 : NO_PERIODIC_SAVE_POINT_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            CairoEngine e = engine;
            try {
                e.execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                insertDay(e, "2024-01-01", 0);
                insertDay(e, PARTITION, 10);
                insertDay(e, "2024-01-03", 20);
                prepare(e);
                switch (history) {
                    case ENROLLED_ON_WRITER_OPEN -> {
                        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
                        e = restart(e);
                    }
                    case PERIODIC_SAVE_POINT -> takePeriodicSavePoint(e);
                    default -> {
                    }
                }
                insertDay(e, "2024-01-04", 30);
                apply(e);
                insertDay(e, "2024-01-05", 40);
                final String expected = snapshot(e);

                e = restart(e);
                TestUtils.drainWalQueue(e);
                Assert.assertEquals(expected, snapshot(e));
            } finally {
                if (e != engine) {
                    e.close();
                }
            }
        });
    }

    private static void alter(CairoEngine e, String sql) throws SqlException {
        e.execute(sql);
        TestUtils.drainWalQueue(e);
        Assert.assertFalse(sql, e.getTableSequencerAPI().isSuspended(e.verifyTableName("t")));
    }

    private static void insertDay(CairoEngine e, String day, int base) throws SqlException {
        final StringBuilder sql = new StringBuilder("INSERT INTO t VALUES ");
        for (int i = 1; i <= 3; i++) {
            sql.append(i > 1 ? ", " : "")
                    .append("('").append(day).append("T0").append(i - 1).append(":00:00.000000Z', 's")
                    .append(base + i).append("', ").append(base + i).append(')');
        }
        alter(e, sql.toString());
    }

    private static String partitions(CairoEngine e) throws SqlException {
        final StringSink sink = new StringSink();
        e.print(
                "SELECT name, minTimestamp, maxTimestamp, numRows, attached, detached, attachable, isParquet FROM table_partitions('t')",
                sink
        );
        return sink.toString();
    }

    // Startup in a fresh engine, as after a process kill: pooled writers are closed without a save point.
    private static CairoEngine restart(CairoEngine e) {
        e.releaseAllReaders();
        e.releaseAllWriters();
        e.releaseInactive();
        if (e != engine) {
            e.close();
        }
        return new CairoEngine(configuration);
    }

    private static String snapshot(CairoEngine e) throws SqlException {
        final StringSink sink = new StringSink();
        final StringBuilder snapshot = new StringBuilder();
        e.print("SELECT * FROM t", sink);
        snapshot.append(sink);
        snapshot.append(partitions(e));
        e.print("SELECT suspended, writerTxn, sequencerTxn FROM wal_tables() WHERE name = 't'", sink);
        snapshot.append(sink);
        return snapshot.toString();
    }

    private static void takePeriodicSavePoint(CairoEngine e) throws SqlException {
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        alter(e, "INSERT INTO t VALUES ('2024-01-03T12:00:00.000000Z', 's99', 99)");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, NO_PERIODIC_SAVE_POINT_MS);
        final SeqTxnTracker tracker = e.getTableSequencerAPI().getTxnTracker(e.verifyTableName("t"));
        Assert.assertEquals("the apply job must take a save point", tracker.getSeqTxn(), tracker.getDurableEpochSeqTxn());
    }

    private void apply(CairoEngine e) throws SqlException {
        final String sql = switch (operation) {
            case ATTACH_COPIED_DIR, ATTACH_MOVED_DIR, ATTACH_WITH_COPY_SETTING ->
                    "ALTER TABLE t ATTACH PARTITION LIST '" + PARTITION + "'";
            case CONVERT_TO_NATIVE -> "ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '" + PARTITION + "'";
            case CONVERT_TO_PARQUET -> "ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '" + PARTITION + "'";
            case DETACH -> "ALTER TABLE t DETACH PARTITION LIST '" + PARTITION + "'";
            case DROP -> "ALTER TABLE t DROP PARTITION LIST '" + PARTITION + "'";
            case FORCE_DROP -> "ALTER TABLE t FORCE DROP PARTITION LIST '" + PARTITION + "'";
            case SQUASH -> {
                // An out-of-order row splits the last partition, so there is something to squash.
                setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
                setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 4);
                final String unsplit = partitions(e);
                alter(e, "INSERT INTO t VALUES ('2024-01-04T01:30:00.000000Z', 's98', 98)");
                Assert.assertNotEquals("the last partition must be split", unsplit.split("\n").length, partitions(e).split("\n").length);
                yield "ALTER TABLE t SQUASH PARTITIONS";
            }
            case TRUNCATE -> "TRUNCATE TABLE t";
            case TTL -> "ALTER TABLE t SET TTL 2 DAYS";
        };
        final String before = partitions(e);
        if (isDirect) {
            // A hard-suspended table that denies WAL writes takes the change directly in its writer,
            // outside the WAL.
            e.execute("ALTER TABLE t SUSPEND WAL");
            e.execute(sql);
            alter(e, "ALTER TABLE t RESUME WAL");
        } else {
            alter(e, sql);
        }
        Assert.assertNotEquals("the operation must change the partitions", before, partitions(e));
    }

    private void prepare(CairoEngine e) throws SqlException {
        switch (operation) {
            case ATTACH_COPIED_DIR, ATTACH_MOVED_DIR, ATTACH_WITH_COPY_SETTING -> {
                alter(e, "ALTER TABLE t DETACH PARTITION LIST '" + PARTITION + "'");
                final FilesFacade ff = e.getConfiguration().getFilesFacade();
                final TableToken token = e.verifyTableName("t");
                try (Path detached = new Path(); Path attachable = new Path()) {
                    detached.of(e.getConfiguration().getDbRoot()).concat(token).concat(PARTITION + TableUtils.DETACHED_DIR_MARKER);
                    attachable.of(e.getConfiguration().getDbRoot()).concat(token)
                            .concat(PARTITION + e.getConfiguration().getAttachPartitionSuffix());
                    if (operation == Operation.ATTACH_COPIED_DIR) {
                        Assert.assertEquals(0, ff.copyRecursive(detached, attachable, e.getConfiguration().getMkDirMode()));
                    } else {
                        Assert.assertEquals(Files.FILES_RENAME_OK, ff.rename(detached.$(), attachable.$()));
                    }
                }
                if (operation == Operation.ATTACH_WITH_COPY_SETTING) {
                    setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_COPY, "true");
                }
            }
            case CONVERT_TO_NATIVE -> alter(e, "ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '" + PARTITION + "'");
            default -> {
            }
        }
    }

    public enum History {
        // Created on an adaptive instance: the creation save point is the last one.
        ENROLLED_AT_CREATE,
        // Written under NOSYNC, then restarted in ADAPTIVE: the writer enrols the table when it opens.
        ENROLLED_ON_WRITER_OPEN,
        // A periodic save point, then more transactions.
        PERIODIC_SAVE_POINT,
        // The periodic save point is off, so only the inline save points of DDL happen.
        INLINE_SAVE_POINTS_ONLY,
        // The control: nothing is rewound after a crash.
        NOSYNC
    }

    public enum Operation {
        // The .attachable directory is a copy of .detached, renamed into place by the attach.
        ATTACH_COPIED_DIR,
        // The .attachable directory is the moved .detached, so the attach consumes the only copy.
        ATTACH_MOVED_DIR,
        // cairo.attach.partition.copy=true: the attach copies and leaves .attachable in place.
        ATTACH_WITH_COPY_SETTING,
        CONVERT_TO_NATIVE,
        CONVERT_TO_PARQUET,
        DETACH,
        DROP,
        FORCE_DROP,
        SQUASH,
        TRUNCATE,
        TTL
    }
}
