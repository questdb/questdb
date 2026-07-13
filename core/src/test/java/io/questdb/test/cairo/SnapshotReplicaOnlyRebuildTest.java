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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.TableSnapshotRestore;
import io.questdb.cairo.TableToken;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies that {@link TableSnapshotRestore} (the checkpoint/snapshot rebuild path) respects the
 * replica-only index gate for native and parquet partitions:
 * <ul>
 *   <li>On a <em>skipping primary</em> ({@code skipReplicaOnlyIndexes()==true}), a REPLICA ONLY
 *       index column must NOT be rebuilt during restore — no {@code .k}/{@code .v} files should
 *       appear in the partition directory.  A regular (non-replica-only) index on the same table
 *       must still be rebuilt normally.</li>
 *   <li>On a <em>non-skipping node</em> ({@code skipReplicaOnlyIndexes()==false}), the rebuild
 *       should materialize ALL indexed columns, including replica-only ones.</li>
 * </ul>
 *
 * <p>The skip=true scenario is driven through the full in-process
 * {@code engine.checkpointRecover()} path (same as {@link io.questdb.test.griffin.CheckpointTest})
 * with {@code CAIRO_CHECKPOINT_RECOVERY_REBUILD_COLUMN_INDEXES=true}.  The skip=false scenario
 * calls {@link TableSnapshotRestore#rebuildTableFiles} directly with a
 * {@link CairoConfigurationWrapper} that overrides {@code skipReplicaOnlyIndexes()} to
 * {@code false}, exercising the guard at the method level without a second engine lifecycle.</p>
 */
public class SnapshotReplicaOnlyRebuildTest extends AbstractCairoTest {

    // Two distinct snapshot-instance UUIDs so checkpointRecover() treats the
    // engine as "restarted" and triggers full restoration.
    private static final String SNAPSHOT_ID = "00000000-0000-0000-0000-000000000001";
    private static final String RESTARTED_ID = "00000000-0000-0000-0000-000000000002";

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Simulate a skipping primary: skipReplicaOnlyIndexes() returns true.
        configurationFactory = (root, telemetry, overrides) ->
                new CairoTestConfiguration(root, telemetry, overrides) {
                    @Override
                    public boolean skipReplicaOnlyIndexes() {
                        return true;
                    }
                };
        AbstractCairoTest.setUpStatic();
    }

    /**
     * skip=true path - full checkpoint + engine.checkpointRecover().
     * The replica-only column must NOT get its parquet index rebuilt; the normal column MUST.
     */
    @Test
    public void testSkipNodeDoesNotRebuildReplicaOnlyIndex() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, SNAPSHOT_ID);
            setProperty(PropertyKey.CAIRO_CHECKPOINT_RECOVERY_REBUILD_COLUMN_INDEXES, "true");

            // Table with two indexed symbol columns:
            //   s  - REPLICA ONLY index  (must be skipped on a primary)
            //   t  - normal index        (must always be rebuilt)
            // BYPASS WAL avoids WAL-apply complexity; the rebuild path is identical.
            execute("CREATE TABLE x (" +
                    "  s SYMBOL INDEX CAPACITY 256 REPLICA ONLY," +
                    "  t SYMBOL INDEX CAPACITY 256," +
                    "  ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            execute("""
                    INSERT INTO x VALUES
                    ('a', 'A', '2024-01-01T00:00:00.000000Z'),
                    ('b', 'B', '2024-01-01T01:00:00.000000Z'),
                    ('c', 'C', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");

            // Take a checkpoint, then simulate an engine restart so recovery fires.
            execute("checkpoint create");
            engine.clear();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, RESTARTED_ID);
            engine.checkpointRecover();

            // Replica-only column: NO index files in any partition directory.
            Assert.assertFalse(
                    "replica-only column 's' must NOT have index files after restore on a skipping node",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s")
            );

            // Normal column: index files MUST exist (rebuild ran normally).
            Assert.assertTrue(
                    "normal-indexed column 't' MUST have index files after restore on a skipping node",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "t")
            );
            assertQuery("SELECT s, t FROM x WHERE s = 'a'")
                    .returns("s\tt\na\tA\n");

            engine.checkpointRelease();
        });
    }

    /**
     * skip=false path - directly invokes {@link TableSnapshotRestore#rebuildTableFiles} with a
     * configuration wrapper that returns {@code skipReplicaOnlyIndexes()==false}.
     * Both the replica-only column AND the normal column must get their indexes rebuilt.
     */
    @Test
    public void testNonSkipNodeRebuildsAllIndexes() throws Exception {
        assertMemoryLeak(() -> {
            // Create a table with the same schema (BYPASS WAL, same two columns).
            execute("CREATE TABLE y (" +
                    "  s SYMBOL INDEX CAPACITY 256 REPLICA ONLY," +
                    "  t SYMBOL INDEX CAPACITY 256," +
                    "  ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            execute("""
                    INSERT INTO y VALUES
                    ('a', 'A', '2024-01-02T00:00:00.000000Z'),
                    ('b', 'B', '2024-01-02T01:00:00.000000Z'),
                    ('c', 'C', '2024-01-03T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE y CONVERT PARTITION TO PARQUET LIST '2024-01-02'");

            // Release writer so the table files are fully flushed.
            engine.releaseAllWriters();

            // Locate the table directory.
            final TableToken token = engine.verifyTableName("y");
            final String dbRoot = engine.getConfiguration().getDbRoot();

            // Build a configuration that wraps the current (skip=true) config but
            // overrides skipReplicaOnlyIndexes() to false — simulating a replica node.
            final CairoConfiguration noSkipConfig = new CairoConfigurationWrapper(configuration) {
                @Override
                public boolean skipReplicaOnlyIndexes() {
                    return false;
                }
            };

            // Directly drive the rebuild on the live table directory.  The rebuild
            // works on the table's own partition files (not a checkpoint copy), which
            // is sufficient for verifying that the guard is correctly bypassed.
            try (
                    TableSnapshotRestore restore = new TableSnapshotRestore(noSkipConfig);
                    Path tablePath = new Path().of(dbRoot).concat(token).slash()
            ) {
                restore.rebuildTableFiles(tablePath, new AtomicInteger(), true /* rebuildIndexes */);
            }

            // Both columns must have index files when skip=false.
            Assert.assertTrue(
                    "replica-only column 's' MUST have index files after rebuild on a non-skipping node",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "y", "s")
            );
            Assert.assertTrue(
                    "normal-indexed column 't' MUST have index files after rebuild on a non-skipping node",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "y", "t")
            );
            assertQuery("SELECT s, t FROM y WHERE s = 'a'")
                    .returns("s\tt\na\tA\n");
        });
    }
}
