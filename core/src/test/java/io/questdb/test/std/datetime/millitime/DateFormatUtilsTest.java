/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.NumericException;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import org.junit.Assert;
import org.junit.Test;

public class DateFormatUtilsTest {
    
    @Test
    public void testParseDate() throws NumericException {
        // PG_DATE_FORMAT: y-MM-dd
        long millis = DateFormatUtils.parseDate("2020-01-01");
        Assert.assertEquals(1577836800000L, millis);

        // pgDateTimeFormat: y-MM-dd HH:mm:ssz
        millis = DateFormatUtils.parseDate("2020-01-01 00:00:00UTC");
        Assert.assertEquals(1577836800000L, millis);

        // PG_DATE_Z_FORMAT: y-MM-dd z
        millis = DateFormatUtils.parseDate("2020-01-01 UTC");
        Assert.assertEquals(1577836800000L, millis);

        // PG_DATE_MILLI_TIME_Z_FORMAT: y-MM-dd HH:mm:ss.Sz
        millis = DateFormatUtils.parseDate("2020-01-01 00:00:00.000UTC");
        Assert.assertEquals(1577836800000L, millis);

        // PG_DATE_MILLI_TIME_Z_PRINT_FORMAT: y-MM-dd HH:mm:ss.SSSz
        millis = DateFormatUtils.parseDate("2020-01-01 00:00:00.000UTC");
        Assert.assertEquals(1577836800000L, millis);

        // PG_DATE_MILLI_TIME_Z_PRINT_FORMAT: y-MM-dd HH:mm:ss.SSSz + extra millis
        millis = DateFormatUtils.parseDate("2020-01-01 00:00:00.001UTC");
        Assert.assertEquals(1577836800001L, millis);

        // millis
        millis = DateFormatUtils.parseDate("1577836800000");
        Assert.assertEquals(1577836800000L, millis);
    }
}