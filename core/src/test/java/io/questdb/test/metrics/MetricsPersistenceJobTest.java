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

import io.questdb.DefaultServerConfiguration;
import io.questdb.Metrics;
import io.questdb.WorkerPoolManager;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.metrics.MetricSnapshotVisitor;
import io.questdb.metrics.MetricType;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.metrics.MetricsPersistenceJob;
import io.questdb.metrics.MetricsRegistryImpl;
import io.questdb.metrics.Target;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.ObjList;
import io.questdb.std.str.BorrowableUtf8Sink;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class MetricsPersistenceJobTest extends AbstractCairoTest {

    @Test
    public void testConstructorDoesNotInitializeTable() throws Exception {
        assertMemoryLeak(() -> {
            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration(null))) {
                Assert.assertTrue(job.isEnabled());
                Assert.assertNull(engine.getTableTokenIfExists(MetricsPersistenceJob.TABLE_NAME));
            }
        });
    }

    @Test
    public void testCreatesAndSamplesMetricsTable() throws Exception {
        assertMemoryLeak(() -> {
            engine.getMetrics().healthMetrics().incrementUnhandledErrors();
            try (MetricsPersistenceJob job = new MetricsPersistenceJob(
                    engine,
                    configuration(MetricsConfiguration.DEFAULT_PERSIST_EXCLUDE)
            )) {
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
            engine.getMetrics().getRegistry().addTarget(target);
            try (MetricsPersistenceJob job = new MetricsPersistenceJob(
                    engine,
                    configuration(MetricsConfiguration.DEFAULT_PERSIST_EXCLUDE, false, 1)
            )) {
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
    public void testClearsRemovedVirtualMetricOnRefresh() throws Exception {
        assertMemoryLeak(() -> {
            final Target target = engine.getMetrics().getRegistry().newVirtualGauge("test_removed_virtual", () -> 42);
            final MetricsConfiguration configuration = new MetricsConfiguration() {
                @Override
                public long getPersistIntervalMicros() {
                    return 0;
                }

                @Override
                public long getPersistVirtualIntervalMicros() {
                    return 0;
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

            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration)) {
                try {
                    job.runSerially();
                    engine.getMetrics().getRegistry().removeTarget(target);
                    job.runSerially();
                } finally {
                    engine.getMetrics().getRegistry().removeTarget(target);
                }
            }

            assertQuery("SELECT test_removed_virtual IS NULL missing FROM \"sys.metrics\"")
                    .expectSize()
                    .returns("""
                            missing
                            false
                            true
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

            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration(null, false, 0))) {
                job.runSerially();
                engine.getMetrics().getRegistry().addTarget(target);
                try {
                    job.runSerially();
                } finally {
                    engine.getMetrics().getRegistry().removeTarget(target);
                }
            }

            assertQuery("SELECT late_metric FROM \"sys.metrics\" WHERE late_metric IS NOT NULL")
                    .returns("""
                            late_metric
                            42
                            """);
        });
    }

    @Test
    public void testDoesNotReapDroppedTableMetricsWhenScrapingEnabled() throws Exception {
        assertDroppedTableMetricsReaping(true, false);
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
    public void testPersistsFreshWorkerMetricsInEveryRow() throws Exception {
        assertMemoryLeak(() -> {
            final Metrics metrics = engine.getMetrics();
            final AtomicInteger metricUpdates = new AtomicInteger();
            final WorkerPoolConfiguration poolConfiguration = () -> 1;
            final WorkerPoolManager workerPoolManager = new WorkerPoolManager(new DefaultServerConfiguration(root) {
                @Override
                public WorkerPool createWorkerPool(WorkerPoolConfiguration configuration) {
                    return new WorkerPool(configuration) {
                        @Override
                        public void updateWorkerMetrics() {
                            final int update = metricUpdates.incrementAndGet();
                            metrics.workerMetrics().update(1000 - update, 1000 + update);
                        }
                    };
                }

                @Override
                public Metrics getMetrics() {
                    return metrics;
                }

                @Override
                public WorkerPoolConfiguration getSharedWorkerPoolNetworkConfiguration() {
                    return poolConfiguration;
                }

                @Override
                public WorkerPoolConfiguration getSharedWorkerPoolQueryConfiguration() {
                    return poolConfiguration;
                }

                @Override
                public WorkerPoolConfiguration getSharedWorkerPoolWriteConfiguration() {
                    return poolConfiguration;
                }
            }) {
                @Override
                protected void configureWorkerPools(WorkerPool sharedPoolQuery, WorkerPool sharedPoolWrite) {
                }
            };

            metrics.workerMetrics().clear();
            try {
                setCurrentMicros(1);
                try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration(null, false, 0))) {
                    job.runSerially();
                    setCurrentMicros(2);
                    job.runSerially();
                }
            } finally {
                setCurrentMicros(-1);
                workerPoolManager.halt();
                // the manager registered itself as a scrape target on the shared Metrics.ENABLED
                metrics.getRegistry().removeTarget(workerPoolManager);
                metrics.clear();
            }

            assertQuery("SELECT workers_job_start_micros_min, workers_job_start_micros_max FROM \"sys.metrics\"")
                    .expectSize()
                    .returns("""
                            workers_job_start_micros_min\tworkers_job_start_micros_max
                            994\t1006
                            991\t1009
                            """);
        });
    }

    @Test
    public void testPrecreatesConfiguredMetricColumns() throws Exception {
        assertMemoryLeak(() -> {
            final MetricsConfiguration configuration = new MetricsConfiguration() {
                @Override
                public void appendPersistedMetricDefinitions(MetricSnapshotVisitor visitor) {
                    visitor.visitLong("precreated_only", MetricType.LONG_GAUGE, 0);
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

            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration)) {
                job.runSerially();
            }

            final TableToken tableToken = engine.verifyTableName(MetricsPersistenceJob.TABLE_NAME);
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                Assert.assertTrue(metadata.getColumnIndexQuiet("precreated_only") > -1);
            }
        });
    }

    @Test
    public void testReapsDroppedTableMetricsWithPersistenceOnly() throws Exception {
        assertDroppedTableMetricsReaping(false, true);
    }

    @Test
    public void testRecreatesIncompatibleSchema() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE "sys.metrics" (
                        ts TIMESTAMP,
                        unhandled_errors DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY TTL 7 DAYS BYPASS WAL
                    """);

            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration(null))) {
                Assert.assertTrue(job.isEnabled());
                job.runSerially();
                Assert.assertTrue(job.isEnabled());
            }

            final TableToken tableToken = engine.verifyTableName(MetricsPersistenceJob.TABLE_NAME);
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                final int columnIndex = metadata.getColumnIndexQuiet("unhandled_errors");
                Assert.assertTrue(columnIndex > -1);
                Assert.assertEquals(ColumnType.LONG, metadata.getColumnType(columnIndex));
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
    public void testRecreatesSchemaWithInvalidTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE "sys.metrics" (
                        ts LONG,
                        event_ts TIMESTAMP
                    ) TIMESTAMP(event_ts) PARTITION BY DAY TTL 7 DAYS BYPASS WAL
                    """);

            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration(null))) {
                job.runSerially();
                Assert.assertTrue(job.isEnabled());
            }

            final TableToken tableToken = engine.verifyTableName(MetricsPersistenceJob.TABLE_NAME);
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                final int timestampIndex = metadata.getTimestampIndex();
                Assert.assertEquals("ts", metadata.getColumnName(timestampIndex));
                Assert.assertEquals(ColumnType.TIMESTAMP, metadata.getColumnType(timestampIndex));
            }
        });
    }

    @Test
    public void testRecreatesSchemaWithUnsupportedColumnType() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE "sys.metrics" (
                        ts TIMESTAMP,
                        legacy VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY TTL 7 DAYS BYPASS WAL
                    """);

            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration(null))) {
                job.runSerially();
                Assert.assertTrue(job.isEnabled());
            }

            final TableToken tableToken = engine.verifyTableName(MetricsPersistenceJob.TABLE_NAME);
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                Assert.assertEquals(-1, metadata.getColumnIndexQuiet("legacy"));
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

    @Test
    public void testBacksOffAfterRepeatedCairoExceptions() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger snapshots = new AtomicInteger();
            final Target target = new Target() {
                @Override
                public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
                }

                @Override
                public void snapshot(MetricSnapshotVisitor visitor) {
                    snapshots.incrementAndGet();
                    throw CairoException.critical(28).put("persistent metrics persistence failure");
                }
            };

            engine.getMetrics().getRegistry().addTarget(target);
            setCurrentMicros(0);
            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration(null, false, 7))) {
                job.runSerially();
                Assert.assertEquals(1, snapshots.get());

                job.runSerially();
                Assert.assertEquals("first retry must be immediate", 2, snapshots.get());

                long now = 0;
                final long[] retryDelays = {1, 2, 4, 8, 16, 32, 60, 60};
                for (int i = 0; i < retryDelays.length; i++) {
                    final long delayMicros = retryDelays[i] * 1_000_000;
                    setCurrentMicros(now + delayMicros - 1);
                    job.runSerially();
                    Assert.assertEquals("retry ran before backoff elapsed", i + 2, snapshots.get());

                    now += delayMicros;
                    setCurrentMicros(now);
                    job.runSerially();
                    Assert.assertEquals("retry did not run when backoff elapsed", i + 3, snapshots.get());
                }
            } finally {
                setCurrentMicros(-1);
                engine.getMetrics().getRegistry().removeTarget(target);
            }
        });
    }

    @Test
    public void testRetriesAfterTransientCairoException() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger snapshots = new AtomicInteger();
            final Target target = new Target() {
                @Override
                public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
                }

                @Override
                public void snapshot(MetricSnapshotVisitor visitor) {
                    if (snapshots.incrementAndGet() == 1) {
                        throw CairoException.critical(28).put("transient metrics persistence failure");
                    }
                    visitor.visitLong("transient_metric", MetricType.LONG_GAUGE, 42);
                }
            };

            try (MetricsPersistenceJob job = new MetricsPersistenceJob(engine, configuration(null, false, 0))) {
                job.runSerially();
                engine.getMetrics().getRegistry().addTarget(target);
                try {
                    job.runSerially();
                    Assert.assertTrue(job.isEnabled());
                    job.runSerially();
                } finally {
                    engine.getMetrics().getRegistry().removeTarget(target);
                }
            }

            assertQuery("SELECT transient_metric FROM \"sys.metrics\" WHERE transient_metric IS NOT NULL")
                    .returns("""
                            transient_metric
                            42
                            """);
        });
    }

    private static void assertDroppedTableMetricsReaping(boolean isScrapeEnabled, boolean isExpectedReapEnabled) throws Exception {
        assertMemoryLeak(() -> {
            final Metrics metrics = new Metrics(true, isScrapeEnabled, new MetricsRegistryImpl());
            final ObjList<Boolean> reapFlags = new ObjList<>();
            metrics.getRegistry().addTarget(new Target() {
                @Override
                public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
                }

                @Override
                public void snapshot(MetricSnapshotVisitor visitor) {
                    reapFlags.add(visitor.isReapDroppedTableMetricsEnabled());
                }
            });

            try (
                    CairoEngine localEngine = new CairoEngine(new DefaultCairoConfiguration(temp.newFolder().getAbsolutePath()) {
                        @Override
                        public Metrics getMetrics() {
                            return metrics;
                        }
                    });
                    MetricsPersistenceJob job = new MetricsPersistenceJob(localEngine, new MetricsConfiguration() {
                        @Override
                        public boolean isEnabled() {
                            return isScrapeEnabled;
                        }

                        @Override
                        public boolean isPersistEnabled() {
                            return true;
                        }

                        @Override
                        public boolean isPersistParquetEnabled() {
                            return false;
                        }
                    })
            ) {
                job.runSerially();
                // Assert outside snapshot(), since runSerially() catches Throwable.
                Assert.assertTrue("persistence must remain enabled", job.isEnabled());
                Assert.assertEquals("discovery and sample snapshots", 2, reapFlags.size());
                Assert.assertFalse("schema discovery must not reap", reapFlags.getQuick(0));
                Assert.assertEquals("sample reap policy", isExpectedReapEnabled, reapFlags.getQuick(1).booleanValue());
            }
        });
    }

    private static MetricsConfiguration configuration(String exclude) {
        return configuration(exclude, false);
    }

    private static MetricsConfiguration configuration(String exclude, boolean parquetEnabled) {
        return configuration(exclude, parquetEnabled, 1_000_000);
    }

    private static MetricsConfiguration configuration(String exclude, boolean parquetEnabled, long intervalMicros) {
        return new MetricsConfiguration() {
            @Override
            public CharSequence getPersistExclude() {
                return exclude;
            }

            @Override
            public long getPersistIntervalMicros() {
                return intervalMicros;
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
                return parquetEnabled;
            }
        };
    }
}
