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

package io.questdb.test.std.datetime.millitime;

import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateLocaleFactory;
import org.junit.Assert;
import org.junit.Test;

public class DateLocaleTest {
    @Test(expected = NumericException.class)
    public void testBadMonth() throws Exception {
        String date = "23 Dek 2010";
        DateLocaleFactory.INSTANCE.getLocale("en-GB").matchMonth(date, 3, date.length());
    }

    @Test(expected = NumericException.class)
    public void testBadMonth2() throws Exception {
        String date = "23 Zek 2010";
        DateLocaleFactory.INSTANCE.getLocale("en-GB").matchMonth(date, 3, date.length());
    }

    @Test
    public void testLongMonth() throws Exception {
        String date = "23 December 2010";
        long result = DateLocaleFactory.INSTANCE.getLocale("en-GB").matchMonth(date, 3, date.length());
        Assert.assertEquals(8, Numbers.decodeHighInt(result));
        Assert.assertEquals(11, Numbers.decodeLowInt(result));
    }

    @Test
    public void testLowCaseLongMonth() throws Exception {
        String date = "23 december 2010";
        long result = DateLocaleFactory.INSTANCE.getLocale("en-GB").matchMonth(date, 3, date.length());
        Assert.assertEquals(8, Numbers.decodeHighInt(result));
        Assert.assertEquals(11, Numbers.decodeLowInt(result));
    }

    @Test
    public void testRTLMonth() throws Exception {
        String s = "23مارس";
        long result = DateLocaleFactory.INSTANCE.getLocale("ar-DZ").matchMonth(s, 2, s.length());
        Assert.assertEquals(4, Numbers.decodeHighInt(result));
        Assert.assertEquals(2, Numbers.decodeLowInt(result));
    }

    @Test
    public void testShortMonth() throws Exception {
        String date = "23 Aug 2010";
        long result = DateLocaleFactory.INSTANCE.getLocale("en-GB").matchMonth(date, 3, date.length());
        Assert.assertEquals(3, Numbers.decodeHighInt(result));
        Assert.assertEquals(7, Numbers.decodeLowInt(result));
    }

    @Test(expected = NumericException.class)
    public void testWrongLength() throws Exception {
        String date = "23 Zek 2010";
        DateLocaleFactory.INSTANCE.getLocale("en-GB").matchMonth(date, 30, date.length());
    }
}