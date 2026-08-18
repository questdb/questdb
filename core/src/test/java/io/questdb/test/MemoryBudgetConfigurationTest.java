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

package io.questdb.test;

import io.questdb.BuildInformationHolder;
import io.questdb.PropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Properties;

public class MemoryBudgetConfigurationTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    protected static final Log LOG = LogFactory.getLog(MemoryBudgetConfigurationTest.class);
    protected static String root;

    @AfterClass
    public static void afterClass() {
        TestUtils.removeTestPath(root);
    }

    @BeforeClass
    public static void setupMimeTypes() throws Exception {
        File root = new File(temp.getRoot(), "root");
        TestUtils.copyMimeTypes(root.getAbsolutePath());
        MemoryBudgetConfigurationTest.root = root.getAbsolutePath();
    }

    @Test
    public void testBudgetUnsetLeavesCopyBufferAtStockDefault() throws Exception {
        PropServerConfiguration conf = newPropServerConfiguration(new Properties());
        Assert.assertEquals(2 * 1024 * 1024,
                conf.getCairoConfiguration().getSqlCopyBufferSize());
    }

    @Test
    public void testBudgetCollapsesTheCopyBuffer() throws Exception {
        // 2MB per worker of CSV-import buffer is the largest fixed native
        // allocation at boot and must not survive a 256MB budget.
        Properties p = new Properties();
        p.setProperty(PropertyKey.CAIRO_MEMORY_BUDGET.getPropertyPath(), "256M");
        PropServerConfiguration conf = newPropServerConfiguration(p);
        Assert.assertTrue(conf.getCairoConfiguration().getSqlCopyBufferSize() < 2 * 1024 * 1024);
    }

    @Test
    public void testExplicitCopyBufferBeatsTheBudget() throws Exception {
        Properties p = new Properties();
        p.setProperty(PropertyKey.CAIRO_MEMORY_BUDGET.getPropertyPath(), "256M");
        p.setProperty(PropertyKey.CAIRO_SQL_COPY_BUFFER_SIZE.getPropertyPath(), "1M");
        PropServerConfiguration conf = newPropServerConfiguration(p);
        Assert.assertEquals("explicit configuration must win",
                1024 * 1024, conf.getCairoConfiguration().getSqlCopyBufferSize());
    }

    @Test
    public void testBudgetReducesSharedWorkerCount() throws Exception {
        Properties p = new Properties();
        p.setProperty(PropertyKey.CAIRO_MEMORY_BUDGET.getPropertyPath(), "256M");
        PropServerConfiguration conf = newPropServerConfiguration(p);
        int workers = conf.getSharedWorkerPoolNetworkConfiguration().getWorkerCount();
        Assert.assertTrue("a 256M budget must not run a worker per CPU, was " + workers,
                workers <= 4);
        Assert.assertTrue(workers >= 1);
    }

    @Test
    public void testExplicitWorkerCountBeatsTheBudget() throws Exception {
        Properties p = new Properties();
        p.setProperty(PropertyKey.CAIRO_MEMORY_BUDGET.getPropertyPath(), "256M");
        p.setProperty(PropertyKey.SHARED_WORKER_COUNT.getPropertyPath(), "9");
        PropServerConfiguration conf = newPropServerConfiguration(p);
        Assert.assertEquals(9, conf.getSharedWorkerPoolNetworkConfiguration().getWorkerCount());
    }

    @Test
    public void testBudgetDerivesQueryAndWalApplyLimits() throws Exception {
        Properties p = new Properties();
        p.setProperty(PropertyKey.CAIRO_MEMORY_BUDGET.getPropertyPath(), "256M");
        PropServerConfiguration conf = newPropServerConfiguration(p);
        long q = conf.getCairoConfiguration().getQueryMemoryLimitBytes();
        long w = conf.getCairoConfiguration().getWalApplyMemoryLimitBytes();
        Assert.assertTrue("query limit must be set by the budget", q > 0);
        Assert.assertTrue("wal apply limit must be set by the budget", w > 0);
        Assert.assertTrue("limits must fit inside the budget", q + w <= 256L * 1024 * 1024);
    }

    @Test
    public void testLimitsRemainUnlimitedWithoutABudget() throws Exception {
        PropServerConfiguration conf = newPropServerConfiguration(new Properties());
        // 0 means "no limit" and is the shipped default.
        Assert.assertEquals(0, conf.getCairoConfiguration().getQueryMemoryLimitBytes());
        Assert.assertEquals(0, conf.getCairoConfiguration().getWalApplyMemoryLimitBytes());
    }

    @Test
    public void testBudgetShrinksSqlPoolCapacities() throws Exception {
        Properties p = new Properties();
        p.setProperty(PropertyKey.CAIRO_MEMORY_BUDGET.getPropertyPath(), "256M");
        PropServerConfiguration conf = newPropServerConfiguration(p);
        PropServerConfiguration stock = newPropServerConfiguration(new Properties());
        Assert.assertTrue("expression pool must shrink under a budget",
                conf.getCairoConfiguration().getSqlExpressionPoolCapacity()
                        < stock.getCairoConfiguration().getSqlExpressionPoolCapacity());
        Assert.assertTrue("pool must stay usable",
                conf.getCairoConfiguration().getSqlExpressionPoolCapacity() >= 16);
    }

    @Test
    public void testSqlPoolCapacitiesUnchangedWithoutABudget() throws Exception {
        PropServerConfiguration conf = newPropServerConfiguration(new Properties());
        // Pin the shipped defaults so a budget change can never silently move them.
        Assert.assertEquals(64, conf.getCairoConfiguration().getSqlCharacterStoreSequencePoolCapacity());
        Assert.assertEquals(4096, conf.getCairoConfiguration().getSqlColumnPoolCapacity());
        Assert.assertEquals(8192, conf.getCairoConfiguration().getSqlExpressionPoolCapacity());
        Assert.assertEquals(64, conf.getCairoConfiguration().getSqlJoinContextPoolCapacity());
        Assert.assertEquals(2048, conf.getCairoConfiguration().getSqlLexerPoolCapacity());
        Assert.assertEquals(1024, conf.getCairoConfiguration().getSqlModelPoolCapacity());
    }

    @Test
    public void testBudgetShrinksO3ColumnMemoryAndAppendPage() throws Exception {
        Properties p = new Properties();
        p.setProperty(PropertyKey.CAIRO_MEMORY_BUDGET.getPropertyPath(), "256M");
        PropServerConfiguration conf = newPropServerConfiguration(p);
        Assert.assertTrue(conf.getCairoConfiguration().getO3ColumnMemorySize() < 8 * 1024 * 1024);
        Assert.assertTrue(conf.getCairoConfiguration().getDataAppendPageSize() < 16 * 1024 * 1024);
    }

    @Test
    public void testAppendSizingUnchangedWithoutABudget() throws Exception {
        PropServerConfiguration conf = newPropServerConfiguration(new Properties());
        Assert.assertEquals(8 * 1024 * 1024, conf.getCairoConfiguration().getO3ColumnMemorySize());
        Assert.assertEquals(16 * 1024 * 1024, conf.getCairoConfiguration().getDataAppendPageSize());
    }

    @Test
    public void testBudgetShrinksHttpConnectionBuffers() throws Exception {
        Properties p = new Properties();
        p.setProperty(PropertyKey.CAIRO_MEMORY_BUDGET.getPropertyPath(), "256M");
        PropServerConfiguration conf = newPropServerConfiguration(p);
        Assert.assertTrue(conf.getHttpServerConfiguration().getSendBufferSize() < 2 * 1024 * 1024);
        Assert.assertTrue(conf.getHttpServerConfiguration().getRecvBufferSize() < 2 * 1024 * 1024);
    }

    @Test
    public void testHttpConnectionBuffersUnchangedWithoutABudget() throws Exception {
        PropServerConfiguration conf = newPropServerConfiguration(new Properties());
        Assert.assertEquals(2 * 1024 * 1024, conf.getHttpServerConfiguration().getSendBufferSize());
        Assert.assertEquals(2 * 1024 * 1024, conf.getHttpServerConfiguration().getRecvBufferSize());
    }

    /**
     * A budget must <em>bound</em> parallel GROUP BY, not switch it off.
     * <p>
     * Measured at a 512 MB budget over 15 TSBS queries x 7 repetitions:
     * 12 query workers rejected {@code double-groupby-1/5/all} in 3 of 7 reps
     * against the query limit, while 4 workers were 105/105 clean and ran the
     * whole set 2.7x faster than disabling parallel GROUP BY outright. So
     * capping the worker count dominates disabling the feature on both
     * reliability and speed.
     */
    @Test
    public void testBudgetCapsQueryWorkersRatherThanDisablingParallelGroupBy() throws Exception {
        Properties p = new Properties();
        p.setProperty(PropertyKey.CAIRO_MEMORY_BUDGET.getPropertyPath(), "512M");
        PropServerConfiguration conf = newPropServerConfiguration(p);

        Assert.assertTrue(
                "parallel GROUP BY must stay enabled — bounding beats disabling",
                conf.getCairoConfiguration().isSqlParallelGroupByEnabled()
        );
        int queryWorkers = conf.getSharedWorkerPoolQueryConfiguration().getWorkerCount();
        Assert.assertTrue("query workers must be capped, was " + queryWorkers, queryWorkers <= 8);
        Assert.assertTrue("query workers must never be zero", queryWorkers >= 1);
    }

    @Test
    public void testExplicitQueryWorkerCountBeatsTheBudget() throws Exception {
        Properties p = new Properties();
        p.setProperty(PropertyKey.CAIRO_MEMORY_BUDGET.getPropertyPath(), "512M");
        p.setProperty(PropertyKey.SHARED_QUERY_WORKER_COUNT.getPropertyPath(), "7");
        PropServerConfiguration conf = newPropServerConfiguration(p);
        Assert.assertEquals(7, conf.getSharedWorkerPoolQueryConfiguration().getWorkerCount());
    }

    @Test
    public void testParallelGroupByUnchangedWithoutABudget() throws Exception {
        // Zero-regression: with no budget this must stay exactly what it ships
        // as, which is enabled whenever there is at least one query worker.
        PropServerConfiguration conf = newPropServerConfiguration(new Properties());
        Assert.assertTrue(conf.getCairoConfiguration().isSqlParallelGroupByEnabled());
    }

    /**
     * The three defaults that killed a 128 MiB server. Stock values are sane on
     * a machine with memory to spare and fatal under a budget: a 4 GiB group-by
     * chunk ceiling is no ceiling, pre-sizing allocates for expected cardinality
     * before reading a row, and 1M-row page frames size a scan's working set for
     * a much larger box. With these derived, the full 15-query TSBS set runs
     * stably at 128 MiB.
     */
    @Test
    public void testBudgetBoundsTheGroupByAllocatorAndPageFrames() throws Exception {
        Properties p = new Properties();
        p.setProperty(PropertyKey.CAIRO_MEMORY_BUDGET.getPropertyPath(), "96M");
        PropServerConfiguration conf = newPropServerConfiguration(p);
        CairoConfiguration c = conf.getCairoConfiguration();

        Assert.assertTrue("4 GiB group-by chunk ceiling must not survive a budget",
                c.getGroupByAllocatorMaxChunkSize() < 64L * 1024 * 1024);
        Assert.assertTrue(c.getGroupByAllocatorMaxChunkSize() > 0);
        Assert.assertTrue("group-by presize must be off under a budget",
                !c.isGroupByPresizeEnabled());
        Assert.assertTrue("1M-row page frames must not survive a budget",
                c.getSqlPageFrameMaxRows() < 1_000_000);
        Assert.assertTrue(c.getSqlPageFrameMaxRows() >= 1000);
        Assert.assertTrue(c.getSqlPageFrameMinRows() >= 100);
        Assert.assertTrue(c.getSqlPageFrameMinRows() <= c.getSqlPageFrameMaxRows());
    }

    @Test
    public void testGroupByAndPageFrameDefaultsUnchangedWithoutABudget() throws Exception {
        // Zero-regression: pin the shipped values.
        CairoConfiguration c = newPropServerConfiguration(new Properties()).getCairoConfiguration();
        Assert.assertEquals(4L * 1024 * 1024 * 1024, c.getGroupByAllocatorMaxChunkSize());
        Assert.assertEquals(128 * 1024, c.getGroupByAllocatorDefaultChunkSize());
        Assert.assertTrue(c.isGroupByPresizeEnabled());
        Assert.assertEquals(1_000_000, c.getSqlPageFrameMaxRows());
        Assert.assertEquals(100_000, c.getSqlPageFrameMinRows());
    }

    @Test
    public void testExplicitGroupByAndPageFrameSettingsBeatTheBudget() throws Exception {
        // Overriding must always be possible -- an operator who knows their
        // workload can hand back any of these.
        Properties p = new Properties();
        p.setProperty(PropertyKey.CAIRO_MEMORY_BUDGET.getPropertyPath(), "96M");
        p.setProperty(PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_MAX_CHUNK_SIZE.getPropertyPath(), "256M");
        p.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_PRESIZE_ENABLED.getPropertyPath(), "true");
        p.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getPropertyPath(), "500000");
        CairoConfiguration c = newPropServerConfiguration(p).getCairoConfiguration();
        Assert.assertEquals(256L * 1024 * 1024, c.getGroupByAllocatorMaxChunkSize());
        Assert.assertTrue(c.isGroupByPresizeEnabled());
        Assert.assertEquals(500_000, c.getSqlPageFrameMaxRows());
    }

    protected PropServerConfiguration newPropServerConfiguration(Properties properties) throws Exception {
        return new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder());
    }
}
