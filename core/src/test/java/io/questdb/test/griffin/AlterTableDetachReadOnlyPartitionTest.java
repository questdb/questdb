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

package io.questdb.test.griffin;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.griffin.SqlException;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * A read-only partition is refused by DETACH whatever it is on disk. A backup restore turns a soft link into a plain
 * directory and keeps the read-only flag, so the answer must not depend on the link, or a restored replica would
 * diverge from its primary.
 */
public class AlterTableDetachReadOnlyPartitionTest extends AbstractCairoTest {

    @Test
    public void testSoftLinkAndRestoredDirectoryAreRefusedAlike() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            for (String variant : new String[]{"link", "restored"}) {
                final String name = "t_" + variant;
                execute("CREATE TABLE " + name + " (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO " + name + " VALUES (1, '2020-01-01T00:00:00'), (2, '2020-01-02T00:00:00'), (3, '2020-01-03T00:00:00')");
                drainWalQueue();

                // move the first partition out of the table and attach it back through a soft link: read-only
                execute("ALTER TABLE " + name + " DETACH PARTITION LIST '2020-01-01'");
                drainWalQueue();
                final TableToken tt = engine.verifyTableName(name);
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                final var tableDir = Paths.get(configuration.getDbRoot().toString(), tt.getDirName());
                final var other = Paths.get(root, "other_" + variant);
                Files.createDirectories(other);
                final var target = other.resolve("2020-01-01" + TableUtils.DETACHED_DIR_MARKER);
                Files.move(tableDir.resolve("2020-01-01" + TableUtils.DETACHED_DIR_MARKER), target);
                Files.createSymbolicLink(tableDir.resolve("2020-01-01" + TableUtils.ATTACHABLE_DIR_MARKER), target);
                execute("ALTER TABLE " + name + " ATTACH PARTITION LIST '2020-01-01'");
                drainWalQueue();
                engine.releaseAllReaders();
                engine.releaseAllWriters();

                final var partitionDir = Paths.get(readOnlyPartitionDir(tt));
                Assert.assertTrue(Files.isSymbolicLink(partitionDir));
                if (variant.equals("restored")) {
                    // a backup restore writes plain directories: replace the link with a copy of its target
                    Files.delete(partitionDir);
                    Files.createDirectories(partitionDir);
                    try (var files = Files.list(target)) {
                        for (var f : files.toList()) {
                            Files.copy(f, partitionDir.resolve(f.getFileName()), StandardCopyOption.COPY_ATTRIBUTES);
                        }
                    }
                    Assert.assertFalse(Files.isSymbolicLink(partitionDir));
                }

                try (TableReader reader = engine.getReader(tt)) {
                    Assert.assertTrue(reader.getTxFile().isPartitionReadOnly(0));
                }
                try {
                    execute("ALTER TABLE " + name + " DETACH PARTITION LIST '2020-01-01'");
                    Assert.fail("DETACH of a read-only partition must be refused [variant=" + variant + "]");
                } catch (SqlException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "cannot detach partition, partition is read-only [table=" + name + ", partition=2020-01-01]"
                    );
                }
                drainWalQueue();
                Assert.assertFalse(Files.exists(tableDir.resolve("2020-01-01" + TableUtils.DETACHED_DIR_MARKER)));
                assertQuery("SELECT count() FROM " + name).noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            }
        });
    }

    private String readOnlyPartitionDir(TableToken tt) {
        try (TableReader reader = engine.getReader(tt); Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(tt.getDirName());
            TableUtils.setPathForNativePartition(
                    p,
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    reader.getTxFile().getPartitionTimestampByIndex(0),
                    reader.getTxFile().getPartitionNameTxn(0)
            );
            return p.toString();
        }
    }
}
