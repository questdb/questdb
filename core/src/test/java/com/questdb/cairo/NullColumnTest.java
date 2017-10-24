package com.questdb.cairo;

import com.questdb.misc.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class NullColumnTest {

    @Test
    public void close() throws Exception {
        NullColumn.INSTANCE.close();
    }

    @Test
    public void getBin() throws Exception {
        Assert.assertNull(NullColumn.INSTANCE.getBin(1234));
    }

    @Test
    public void getBinLen() throws Exception {
        Assert.assertEquals(-1L, NullColumn.INSTANCE.getBinLen(1234));
    }

    @Test
    public void getBool() throws Exception {
        Assert.assertFalse(NullColumn.INSTANCE.getBool(1234));
    }

    @Test
    public void getByte() throws Exception {
        Assert.assertEquals(0, NullColumn.INSTANCE.getByte(1234));
    }

    @Test
    public void getDouble() throws Exception {
        Assert.assertTrue(Double.isNaN(NullColumn.INSTANCE.getDouble(1234)));
    }

    @Test
    public void getFd() throws Exception {
        Assert.assertEquals(-1, NullColumn.INSTANCE.getFd());
    }

    @Test
    public void getFloat() throws Exception {
        Assert.assertTrue(Float.isNaN(NullColumn.INSTANCE.getFloat(123)));
    }

    @Test
    public void getInt() throws Exception {
        Assert.assertEquals(0, NullColumn.INSTANCE.getInt(1234));
    }

    @Test
    public void getLong() throws Exception {
        Assert.assertEquals(Numbers.LONG_NaN, NullColumn.INSTANCE.getLong(1234));
    }

    @Test
    public void getShort() throws Exception {
        Assert.assertEquals(0, NullColumn.INSTANCE.getShort(1234));
    }

    @Test
    public void getStr() throws Exception {
        Assert.assertNull(NullColumn.INSTANCE.getStr(1234));
    }

    @Test
    public void getStr2() throws Exception {
        Assert.assertNull(NullColumn.INSTANCE.getStr2(1234));
    }

    @Test
    public void getStrLen() throws Exception {
        Assert.assertEquals(-1, NullColumn.INSTANCE.getStrLen(1234));
    }

    @Test
    public void trackFileSize() throws Exception {
        NullColumn.INSTANCE.trackFileSize();
    }

}