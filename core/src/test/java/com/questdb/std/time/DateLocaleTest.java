/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std.time;

import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import org.junit.Assert;
import org.junit.Test;

public class DateLocaleTest {
    @Test(expected = NumericException.class)
    public void testBadMonth() throws Exception {
        String date = "23 Dek 2010";
        DateLocaleFactory.INSTANCE.getDateLocale("en-GB").matchMonth(date, 3, date.length());
    }

    @Test(expected = NumericException.class)
    public void testBadMonth2() throws Exception {
        String date = "23 Zek 2010";
        DateLocaleFactory.INSTANCE.getDateLocale("en-GB").matchMonth(date, 3, date.length());
    }

    @Test
    public void testLongMonth() throws Exception {
        String date = "23 December 2010";
        long result = DateLocaleFactory.INSTANCE.getDateLocale("en-GB").matchMonth(date, 3, date.length());
        Assert.assertEquals(8, Numbers.decodeHighInt(result));
        Assert.assertEquals(11, Numbers.decodeLowInt(result));
    }

    @Test
    public void testLowCaseLongMonth() throws Exception {
        String date = "23 december 2010";
        long result = DateLocaleFactory.INSTANCE.getDateLocale("en-GB").matchMonth(date, 3, date.length());
        Assert.assertEquals(8, Numbers.decodeHighInt(result));
        Assert.assertEquals(11, Numbers.decodeLowInt(result));
    }

    @Test
    public void testRTLMonth() throws Exception {
        String s = "23مارس";
        long result = DateLocaleFactory.INSTANCE.getDateLocale("ar-DZ").matchMonth(s, 2, s.length());
        Assert.assertEquals(4, Numbers.decodeHighInt(result));
        Assert.assertEquals(2, Numbers.decodeLowInt(result));
    }

    @Test
    public void testShortMonth() throws Exception {
        String date = "23 Sep 2010";
        long result = DateLocaleFactory.INSTANCE.getDateLocale("en-GB").matchMonth(date, 3, date.length());
        Assert.assertEquals(3, Numbers.decodeHighInt(result));
        Assert.assertEquals(8, Numbers.decodeLowInt(result));
    }

    @Test(expected = NumericException.class)
    public void testWrongLength() throws Exception {
        String date = "23 Zek 2010";
        DateLocaleFactory.INSTANCE.getDateLocale("en-GB").matchMonth(date, 30, date.length());
    }
}