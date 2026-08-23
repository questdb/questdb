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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.WindowsBarrierContractFilesFacade;
import org.junit.Assert;
import org.junit.Test;

/**
 * The {@code data.parquet} barrier must be issued through a handle the platform will actually accept.
 * <p>
 * Both parquet producers -- the CONVERT path ({@code TableUtils.produceParquetFromNative}) and the O3
 * rewrite path ({@code O3PartitionJob}) -- fsync {@code data.parquet} before {@code _pm} is published, so
 * that a committed {@code _pm} can never name data-file bytes still sitting in the page cache. That barrier
 * is only worth having if it runs on every supported platform.
 * <p>
 * It did not. Both sites opened the file with {@code openRONoCache} and fsynced THAT fd, which
 * {@code ERROR_ACCESS_DENIED}s on Windows -- so under any commit mode other than {@code nosync} a Windows
 * instance could not produce parquet at all, taking Enterprise cold storage (whose promotion path is
 * exactly this conversion) with it. See {@link WindowsBarrierContractFilesFacade} for the mechanism.
 * <p>
 * These two tests pin the two sites individually. {@code WriteAccessBarrierSweepTest} covers the wider
 * storage surface, so a NEW barrier written the same wrong way is caught without anyone remembering to
 * come back here.
 */
public class ParquetBarrierHandleAccessTest extends AbstractCairoTest {

    @Test
    public void testConvertToParquetBarrierUsesAWriteCapableHandle() throws Exception {
        final WindowsBarrierContractFilesFacade ff = new WindowsBarrierContractFilesFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO x VALUES (2, '2024-06-11T00:00:00.000000Z')");

            ff.clearCounters();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            ff.assertNoReadOnlyFileBarrier();
            // Proves the code under test was reached: a conversion that skipped the barrier entirely would
            // also report zero violations.
            Assert.assertTrue(
                    "data.parquet was never fsynced, so this test would pass with the barrier deleted"
                            + ff.debugDump(),
                    ff.barrierCount("data.parquet") > 0
            );
            assertQuery("x WHERE ts IN '2024-06-10'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("id\tts\n1\t2024-06-10T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testO3RewriteOfAParquetPartitionUsesAWriteCapableHandle() throws Exception {
        final WindowsBarrierContractFilesFacade ff = new WindowsBarrierContractFilesFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (1, '2024-06-10T00:00:10.000000Z')");
            execute("INSERT INTO x VALUES (3, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            ff.clearCounters();
            // Lands INSIDE the parquet partition and before its existing row, so the partition is rewritten
            // through O3PartitionJob rather than appended to.
            execute("INSERT INTO x VALUES (2, '2024-06-10T00:00:05.000000Z')");

            ff.assertNoReadOnlyFileBarrier();
            Assert.assertTrue(
                    "the O3 rewrite never fsynced data.parquet, so this test would pass with the barrier"
                            + " deleted" + ff.debugDump(),
                    ff.barrierCount("data.parquet") > 0
            );
            assertQuery("x WHERE ts IN '2024-06-10'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("id\tts\n2\t2024-06-10T00:00:05.000000Z\n1\t2024-06-10T00:00:10.000000Z\n");
        });
    }
}
