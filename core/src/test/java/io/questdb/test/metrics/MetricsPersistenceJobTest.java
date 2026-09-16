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

package io.questdb.test.metrics;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.metrics.MetricSnapshotVisitor;
import io.questdb.metrics.MetricType;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.metrics.MetricsPersistenceJob;
import io.questdb.metrics.Target;
import io.questdb.std.str.BorrowableUtf8Sink;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class MetricsPersistenceJobTest extends AbstractCairoTest {

    @Test
    public void testCreatesAndSamplesMetricsTable() throws Exception {
        assertMemoryLeak(() -> {
            engine.getMetrics().healthMetrics().incrementUnhandledErrors();
            final MetricsConfiguration configuration = new MetricsConfiguration() {
                @Override
                public boolean isEnabled() {
                    return true;
                }

                @Override
                public boolean isPersistEnabled() {
                    return true;
                }

                @Override
                public boolean isPersistParquetEnabled() {
                    return false;
                }
            };

            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration)) {
                Assert.assertTrue(job.isEnabled());
                job.runSerially();
            }

            assertQuery("SELECT unhandled_errors FROM \"sys.metrics\"")
                    .expectSize()
                    .returns("""
                            unhandled_errors
                            1
                            """);

            final TableToken tableToken = engine.verifyTableName(MetricsPersistenceJob.TABLE_NAME);
            Assert.assertFalse(tableToken.isWal());
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                Assert.assertEquals(PartitionBy.DAY, metadata.getPartitionBy());
                Assert.assertEquals(7 * 24, metadata.getTtlHoursOrMonths());
            }
        });
    }

    @Test
    public void testBackwardsClockDoesNotAppendOutOfOrderRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE "sys.metrics" (ts TIMESTAMP)
                    TIMESTAMP(ts) PARTITION BY DAY TTL 7 DAYS BYPASS WAL
                    """);
            execute("INSERT INTO \"sys.metrics\" VALUES (dateadd('d', 1, now()))");

            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration(null))) {
                Assert.assertTrue(job.isEnabled());
                job.runSerially();
            }

            assertQuery("SELECT count() FROM \"sys.metrics\"")
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testCarriesVirtualMetricsBetweenRefreshes() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger reads = new AtomicInteger();
            final Target target = new Target() {
                @Override
                public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
                }

                @Override
                public void snapshot(MetricSnapshotVisitor visitor) {
                    if (visitor.isVirtualMetricsEnabled()) {
                        visitor.visitLong(
                                "test_virtual",
                                MetricType.VIRTUAL_LONG_GAUGE,
                                reads.incrementAndGet()
                        );
                    }
                }
            };
            final MetricsConfiguration configuration = new MetricsConfiguration() {
                @Override
                public long getPersistIntervalMicros() {
                    return 1;
                }

                @Override
                public long getPersistVirtualIntervalMicros() {
                    return 60_000_000;
                }

                @Override
                public boolean isEnabled() {
                    return true;
                }

                @Override
                public boolean isPersistEnabled() {
                    return true;
                }

                @Override
                public boolean isPersistParquetEnabled() {
                    return false;
                }
            };

            engine.getMetrics().getRegistry().addTarget(target);
            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration)) {
                try {
                    job.runSerially();
                    job.runSerially();
                } finally {
                    engine.getMetrics().getRegistry().removeTarget(target);
                }
            }

            Assert.assertEquals(2, reads.get());
            assertQuery("SELECT test_virtual FROM \"sys.metrics\"")
                    .expectSize()
                    .returns("""
                            test_virtual
                            2
                            2
                            """);
        });
    }

    @Test
    public void testConvertsPreviousPartitionsToParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE "sys.metrics" (ts TIMESTAMP)
                    TIMESTAMP(ts) PARTITION BY DAY TTL 7 DAYS BYPASS WAL
                    """);
            execute("INSERT INTO \"sys.metrics\" VALUES (dateadd('d', -2, now()))");
            execute("INSERT INTO \"sys.metrics\" VALUES (dateadd('d', -1, now()))");

            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration(null, true))) {
                Assert.assertTrue(job.isEnabled());
                job.runSerially();
            }

            final TableToken tableToken = engine.verifyTableName(MetricsPersistenceJob.TABLE_NAME);
            try (TableReader reader = engine.getReader(tableToken)) {
                Assert.assertEquals(3, reader.getPartitionCount());
                Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormat(0));
                Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormat(1));
                Assert.assertEquals(PartitionFormat.NATIVE, reader.getPartitionFormat(2));
            }
        });
    }

    @Test
    public void testDisablesItselfForIncompatibleSchema() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE "sys.metrics" (
                        ts TIMESTAMP,
                        unhandled_errors DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY TTL 7 DAYS BYPASS WAL
                    """);

            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration(null))) {
                Assert.assertFalse(job.isEnabled());
                Assert.assertFalse(job.runSerially());
            }
        });
    }

    @Test
    public void testDiscoversTargetsAddedAfterStartup() throws Exception {
        assertMemoryLeak(() -> {
            final Target target = new Target() {
                @Override
                public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
                }

                @Override
                public void snapshot(MetricSnapshotVisitor visitor) {
                    visitor.visitLong("late_metric", MetricType.LONG_GAUGE, 42);
                }
            };

            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration(null))) {
                engine.getMetrics().getRegistry().addTarget(target);
                try {
                    job.runSerially();
                } finally {
                    engine.getMetrics().getRegistry().removeTarget(target);
                }
            }

            assertQuery("SELECT late_metric FROM \"sys.metrics\"")
                    .expectSize()
                    .returns("""
                            late_metric
                            42
                            """);
        });
    }

    @Test
    public void testEvolvesCompatibleSchema() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE "sys.metrics" (
                        ts TIMESTAMP,
                        legacy LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY TTL 7 DAYS BYPASS WAL
                    """);

            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration(null))) {
                Assert.assertTrue(job.isEnabled());
                job.runSerially();
            }

            final TableToken tableToken = engine.verifyTableName(MetricsPersistenceJob.TABLE_NAME);
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                Assert.assertTrue(metadata.getColumnIndexQuiet("legacy") > -1);
                Assert.assertTrue(metadata.getColumnIndexQuiet("unhandled_errors") > -1);
            }
            assertQuery("SELECT count() FROM \"sys.metrics\"")
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testExcludesMatchingMetrics() throws Exception {
        assertMemoryLeak(() -> {
            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration("unhandled_errors"))) {
                Assert.assertTrue(job.isEnabled());
                job.runSerially();
            }

            final TableToken tableToken = engine.verifyTableName(MetricsPersistenceJob.TABLE_NAME);
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                Assert.assertEquals(-1, metadata.getColumnIndexQuiet("unhandled_errors"));
            }
        });
    }

    @Test
    public void testInvalidExclusionDisablesPersistence() throws Exception {
        assertMemoryLeak(() -> {
            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration("["))) {
                Assert.assertFalse(job.isEnabled());
                Assert.assertFalse(job.runSerially());
            }
        });
    }

    private static MetricsConfiguration configuration(String exclude) {
        return configuration(exclude, false);
    }

    private static MetricsConfiguration configuration(String exclude, boolean parquetEnabled) {
        return new MetricsConfiguration() {
            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public CharSequence getPersistExclude() {
                return exclude;
            }

            @Override
            public boolean isPersistEnabled() {
                return true;
            }

            @Override
            public boolean isPersistParquetEnabled() {
                return parquetEnabled;
            }
        };
    }
}
