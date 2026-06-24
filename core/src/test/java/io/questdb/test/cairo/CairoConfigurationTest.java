package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class CairoConfigurationTest extends AbstractCairoTest {

    @Test
    public void testSkipReplicaOnlyIndexesDefaultsFalse() {
        Assert.assertFalse(engine.getConfiguration().skipReplicaOnlyIndexes());
    }
}
