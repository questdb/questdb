package io.questdb.test.cairo;

import io.questdb.cairo.CairoEngine;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class CairoEngineCompleteInitTest extends AbstractCairoTest {

    @Test
    public void testBackCompatCtorRunsCompleteInit() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine e = new CairoEngine(configuration)) {
                Assert.assertNotNull("no-arg ctor must run completeInit -- tableNameRegistry", e.getTableNameRegistry());
                Assert.assertNotNull("no-arg ctor must run completeInit -- metadataCache", e.getMetadataCache());
                Assert.assertNotNull("no-arg ctor must run completeInit -- sqlCompilerPool", e.getSqlCompilerPool());
                Assert.assertNotNull("no-arg ctor must run completeInit -- dataID", e.getDataID());
            }
        });
    }

    @Test
    public void testCompleteInitPopulatesFields() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine e = new CairoEngine(configuration, false)) {
                e.completeInit();
                Assert.assertNotNull(e.getTableNameRegistry());
                Assert.assertNotNull(e.getMetadataCache());
                Assert.assertNotNull(e.getSqlCompilerPool());
            }
        });
    }

    @Test
    public void testCtorWithCompleteInitFalseLeavesFieldsNull() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine e = new CairoEngine(configuration, false)) {
                Assert.assertNull("tableNameRegistry must be null before completeInit", e.getTableNameRegistry());
                Assert.assertNull("metadataCache must be null before completeInit", e.getMetadataCache());
                Assert.assertNull("sqlCompilerPool must be null before completeInit", e.getSqlCompilerPool());
                Assert.assertNotNull("dataID must be non-null after ctor (BackupManager dep)", e.getDataID());
            }
        });
    }

    @Test
    public void testDataIdOpenInBothCtors() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine partial = new CairoEngine(configuration, false)) {
                Assert.assertNotNull(partial.getDataID());
            }
            try (CairoEngine full = new CairoEngine(configuration)) {
                Assert.assertNotNull(full.getDataID());
            }
        });
    }
}
