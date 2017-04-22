package com.questdb.net.http;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class DateFormatsTest {
    @Test
    public void testReadFile() throws Exception {
        DateFormats dateFormats = new DateFormats(new File(this.getClass().getResource("/date_test.formats").getFile()));
        Assert.assertEquals(4, dateFormats.size());
        Assert.assertEquals("yyyy-MM-dd HH:mm:ss", dateFormats.getQuick(0));
        Assert.assertEquals("dd/MM/y", dateFormats.getQuick(1));
        Assert.assertEquals("MM/dd/y", dateFormats.getQuick(2));
        Assert.assertEquals("MMM d yyyy", dateFormats.getQuick(3));
    }
}