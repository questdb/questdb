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
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.TableToken;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A helper handed someone else's {@link Path} must give it back at the length it received it.
 * <p>
 * This is not style. {@code DurableEpochManifest.write} left the caller's path at
 * {@code <table>/_epoch.manifest.<generation>}, and on Linux nobody noticed because the very next call --
 * {@code TableWriter.fsyncEpochDirectory} -- did a {@code trimTo} on the way past. That {@code trimTo}
 * lived INSIDE an {@code if (!Os.isWindows())} guard, so on Windows the whole cleanup vanished along with
 * the directory fsync it was bracketed with. The stale suffix then survived into the next consumer of the
 * path, which concatenated onto it and tried to open
 * {@code <table>/_epoch.manifest.1/_meta.swp}. Every structural DDL on an adaptive table failed at that
 * open -- 60 times in one CI leg -- and, because the swap died before reaching its own fsync, it also hid
 * the separate read-only-handle barrier bug underneath it (see {@code WriteAccessBarrierSweepTest}).
 * <p>
 * The lesson generalises past this one method: <b>a platform guard must not wrap a side effect that is not
 * platform-specific</b>. Restoring a path is not a Windows concern. Neither test can be written on the
 * platform that breaks, so the contract is asserted directly instead -- and it is observable on Linux,
 * because the path is left dirty on EVERY platform. Only the accidental cleanup was Linux-only.
 */
public class DurableEpochPathRestoreTest extends AbstractCairoTest {

    @Test
    public void testFsyncDirectoryRestoresTheCallersPath() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = enrolledAdaptiveTable("dir_restore");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                final int rootLen = path.size();
                path.concat("some_child_component");

                DurableEpochManifest.fsyncDirectory(configuration, path, rootLen);

                // Must hold on Windows too, where the fsync itself is impossible and skipped. A guard that
                // returns early may not also skip putting the caller's path back.
                Assert.assertEquals(
                        "fsyncDirectory must leave the path at rootLen on every platform, got: " + path,
                        rootLen,
                        path.size()
                );
            }
        });
    }

    @Test
    public void testWriteRestoresTheCallersPath() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = enrolledAdaptiveTable("write_restore");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                final int rootLen = path.size();

                DurableEpochManifest.write(configuration, token, path, rootLen, 0, 1, 1, 0, 0);

                Assert.assertEquals(
                        "write must hand the caller's path back at rootLen, got: " + path,
                        rootLen,
                        path.size()
                );
            }
        });
    }

    /**
     * A WAL table enrolled in adaptive, so its generation-zero epoch payloads exist on disk for the
     * manifest write to checksum.
     */
    private TableToken enrolledAdaptiveTable(String name) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "0");
        execute("CREATE TABLE " + name + " (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + name + " VALUES (1, '2024-06-10T00:00:00.000000Z')");
        drainWalQueue();
        return engine.verifyTableName(name);
    }
}
