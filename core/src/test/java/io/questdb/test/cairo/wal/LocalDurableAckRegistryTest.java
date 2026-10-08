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
import io.questdb.cairo.wal.DefaultDurableAckRegistry;
import io.questdb.cairo.wal.DurabilityTier;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cairo.wal.LocalDurableAckRegistry;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The OSS durable-ack registry: which tier it offers in which commit mode, and what it reports for a table
 * directory in each state. The frontier values themselves are covered by {@link DurableFrontierTest}.
 */
public class LocalDurableAckRegistryTest extends AbstractCairoTest {

    @Test
    public void testDefaultDurableAckRegistryIsDisabled() {
        final DurableAckRegistry noOp = DefaultDurableAckRegistry.INSTANCE;
        Assert.assertEquals(-1L, noOp.getLocalDurableSeqTxn("some~dir"));
        Assert.assertFalse(noOp.isEnabled());
    }

    @Test
    public void testEnterpriseRegistryOverridesDefault() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertTrue(engine.getDurableAckRegistry() instanceof LocalDurableAckRegistry);
            final DurableAckRegistry fakeEnterprise = new DurableAckRegistry() {
                @Override
                public long getLocalDurableSeqTxn(CharSequence tableDirName) {
                    return 42L;
                }

                @Override
                public long getReplicatedDurableSeqTxn(CharSequence tableDirName) {
                    return 99L;
                }

                @Override
                public boolean isEnabled() {
                    return true;
                }
            };
            engine.setDurableAckRegistry(fakeEnterprise);
            Assert.assertSame(fakeEnterprise, engine.getDurableAckRegistry());
            Assert.assertEquals(99L, engine.getDurableAckRegistry().getReplicatedDurableSeqTxn("any"));
            Assert.assertEquals(42L, engine.getDurableAckRegistry().getLocalDurableSeqTxn("any"));
            engine.setDurableAckRegistry(new LocalDurableAckRegistry(engine));
        });
    }

    /**
     * A directory that stops resolving to a live table (drop, drop then purge, the old directory of a REBASE WAL)
     * reports TABLE_GONE, so a QWP connection's pending entry for it counts as covered. A rename keeps the
     * directory and its frontier. OSS has no upload tier, so the replicated frontier is always -1.
     */
    @Test
    public void testTableStatesReportFrontierOrTableGone() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            final DurableAckRegistry registry = engine.getDurableAckRegistry();
            execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into t values ('2024-01-01T00:00:00.000000Z', 1)");
            final String dirName = engine.verifyTableName("t").getDirName();
            Assert.assertNotEquals(DurableAckRegistry.TABLE_GONE, registry.getLocalDurableSeqTxn(dirName));
            Assert.assertNotEquals(DurableAckRegistry.TABLE_GONE, LocalDurableAckRegistry.resolveLocalDurableSeqTxn(engine, dirName));
            Assert.assertEquals(-1L, registry.getReplicatedDurableSeqTxn(dirName));

            execute("rename table t to t2");
            Assert.assertEquals(dirName, engine.verifyTableName("t2").getDirName());
            Assert.assertNotEquals("a renamed table is not gone", DurableAckRegistry.TABLE_GONE, registry.getLocalDurableSeqTxn(dirName));

            execute("drop table t2");
            Assert.assertEquals(DurableAckRegistry.TABLE_GONE, registry.getLocalDurableSeqTxn(dirName));
            execute("create table t2 (ts timestamp, v long) timestamp(ts) partition by day wal");
            final String newDirName = engine.verifyTableName("t2").getDirName();
            Assert.assertNotEquals(dirName, newDirName);
            Assert.assertEquals(DurableAckRegistry.TABLE_GONE, registry.getLocalDurableSeqTxn(dirName));
            Assert.assertNotEquals(DurableAckRegistry.TABLE_GONE, registry.getLocalDurableSeqTxn(newDirName));

            drainWalQueue();
            drainPurgeJob();
            Assert.assertNull("the purge must have removed the dropped token", engine.getTableTokenByDirName(dirName));
            Assert.assertFalse(engine.isWalTableDropped(dirName));
            Assert.assertEquals(DurableAckRegistry.TABLE_GONE, registry.getLocalDurableSeqTxn(dirName));

            // REBASE WAL retires the old directory without calling onTableDropped.
            execute("insert into t2 values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            execute("alter table t2 suspend wal");
            execute("alter table t2 rebase wal");
            drainWalQueue();
            final String rebasedDirName = engine.verifyTableName("t2").getDirName();
            Assert.assertNotEquals(newDirName, rebasedDirName);
            Assert.assertEquals(DurableAckRegistry.TABLE_GONE, registry.getLocalDurableSeqTxn(newDirName));
            Assert.assertNotEquals(DurableAckRegistry.TABLE_GONE, registry.getLocalDurableSeqTxn(rebasedDirName));
        });
    }

    @Test
    public void testTierAvailabilityByCommitMode() throws Exception {
        // The local frontier advances only under ADAPTIVE, so outside it the LOCAL tier is denied at the
        // handshake rather than granted as an ack stream that never flows. OSS never offers REPLICATED.
        for (String mode : new String[]{"adaptive", "nosync", "sync", "async"}) {
            node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, mode);
            final boolean adaptive = "adaptive".equals(mode);
            assertMemoryLeak(() -> {
                for (DurableAckRegistry registry : new DurableAckRegistry[]{engine.getDurableAckRegistry(), new LocalDurableAckRegistry(engine)}) {
                    Assert.assertEquals(mode, adaptive, registry.isEnabled());
                    Assert.assertEquals(mode, adaptive, registry.isTierAvailable(DurabilityTier.LOCAL));
                    Assert.assertEquals(mode, adaptive, registry.isTierSetAvailable(DurabilityTier.LOCAL));
                    Assert.assertFalse(mode, registry.isTierAvailable(DurabilityTier.REPLICATED));
                    Assert.assertFalse(mode, registry.isTierSetAvailable(DurabilityTier.REPLICATED));
                    Assert.assertFalse(mode, registry.isTierSetAvailable(DurabilityTier.LOCAL | DurabilityTier.REPLICATED));
                }
            });
        }
    }

    @Test
    public void testTierHeaderParseAndFormat() {
        // The handshake tests cover the common header values; these are the remaining parse and format cases.
        Assert.assertEquals(DurabilityTier.REPLICATED | DurabilityTier.LEGACY_TRUE, DurabilityTier.fromHeaderValue(new Utf8String("TRUE")));
        Assert.assertEquals(DurabilityTier.LOCAL | DurabilityTier.REPLICATED, DurabilityTier.fromHeaderValue(new Utf8String("replicated,local")));
        Assert.assertEquals(DurabilityTier.NONE, DurabilityTier.fromHeaderValue(new Utf8String("bogus")));
        Assert.assertEquals(DurabilityTier.NONE, DurabilityTier.fromHeaderValue(null));
        Assert.assertTrue(DurabilityTier.hasLocal(DurabilityTier.LOCAL | DurabilityTier.REPLICATED));
        Assert.assertFalse(DurabilityTier.hasLocal(DurabilityTier.REPLICATED));
        Assert.assertTrue(DurabilityTier.hasReplicated(DurabilityTier.REPLICATED | DurabilityTier.LEGACY_TRUE));
        Assert.assertFalse(DurabilityTier.hasReplicated(DurabilityTier.LOCAL));
        Assert.assertEquals("replicated", DurabilityTier.responseToken(DurabilityTier.REPLICATED).toString());
        Assert.assertEquals("local,replicated", DurabilityTier.responseToken(DurabilityTier.LOCAL | DurabilityTier.REPLICATED).toString());
        Assert.assertNull(DurabilityTier.responseToken(DurabilityTier.NONE));
    }
}
