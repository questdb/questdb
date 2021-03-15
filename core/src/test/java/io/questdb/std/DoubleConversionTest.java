package io.questdb.std;

import org.junit.Assert;
import org.junit.Test;

public class DoubleConversionTest {
    @Test
    public void append() {
        String result = DoubleConversion.append(587.0658D, Numbers.MAX_SCALE);
        Assert.assertEquals("587.0658", result);
    }
}
