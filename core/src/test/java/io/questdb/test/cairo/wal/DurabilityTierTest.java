package io.questdb.test.cairo.wal;

import io.questdb.cairo.wal.DurabilityTier;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

public class DurabilityTierTest {
    @Test
    public void testFromHeaderValue() {
        Assert.assertEquals(DurabilityTier.DEFAULT, DurabilityTier.fromHeaderValue(new Utf8String("true")));
        Assert.assertEquals(DurabilityTier.DEFAULT, DurabilityTier.fromHeaderValue(new Utf8String("TRUE")));
        Assert.assertEquals(DurabilityTier.LOCAL, DurabilityTier.fromHeaderValue(new Utf8String("local")));
        Assert.assertEquals(DurabilityTier.REPLICATED, DurabilityTier.fromHeaderValue(new Utf8String("replicated")));
        Assert.assertEquals(DurabilityTier.NONE, DurabilityTier.fromHeaderValue(new Utf8String("bogus")));
        Assert.assertEquals(DurabilityTier.NONE, DurabilityTier.fromHeaderValue(null));
    }

    @Test
    public void testResponseToken() {
        Assert.assertEquals("local", DurabilityTier.responseToken(DurabilityTier.LOCAL).toString());
        Assert.assertEquals("replicated", DurabilityTier.responseToken(DurabilityTier.REPLICATED).toString());
        Assert.assertNull(DurabilityTier.responseToken(DurabilityTier.NONE));
    }
}
