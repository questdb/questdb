package com.questdb.std;

import com.questdb.ex.NumericException;
import com.questdb.misc.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class DateLocaleTest {
    @Test
    public void testRTLMonth() throws Exception {
        String s = "23مارس";
        long result = DateLocale.LOCALES.get("ar-DZ").matchMonth(s, 2, s.length());
        Assert.assertEquals(4, Numbers.decodeLen(result));
        Assert.assertEquals(2, Numbers.decodeInt(result));
    }

    @Test
    public void testShortMonth() throws Exception {
        String date = "23 Sep 2010";
        long result = DateLocale.LOCALES.get("en-GB").matchMonth(date, 3, date.length());
        Assert.assertEquals(3, Numbers.decodeLen(result));
        Assert.assertEquals(8, Numbers.decodeInt(result));
    }

    @Test
    public void testLongMonth() throws Exception {
        String date = "23 December 2010";
        long result = DateLocale.LOCALES.get("en-GB").matchMonth(date, 3, date.length());
        Assert.assertEquals(8, Numbers.decodeLen(result));
        Assert.assertEquals(11, Numbers.decodeInt(result));
    }

    @Test
    public void testLowCaseLongMonth() throws Exception {
        String date = "23 december 2010";
        long result = DateLocale.LOCALES.get("en-GB").matchMonth(date, 3, date.length());
        Assert.assertEquals(8, Numbers.decodeLen(result));
        Assert.assertEquals(11, Numbers.decodeInt(result));
    }

    @Test(expected = NumericException.class)
    public void testBadMonth() throws Exception {
        String date = "23 Dek 2010";
        DateLocale.LOCALES.get("en-GB").matchMonth(date, 3, date.length());
    }

    @Test(expected = NumericException.class)
    public void testBadMonth2() throws Exception {
        String date = "23 Zek 2010";
        DateLocale.LOCALES.get("en-GB").matchMonth(date, 3, date.length());
    }

    @Test(expected = NumericException.class)
    public void testWrongLength() throws Exception {
        String date = "23 Zek 2010";
        DateLocale.LOCALES.get("en-GB").matchMonth(date, 30, date.length());
    }
}