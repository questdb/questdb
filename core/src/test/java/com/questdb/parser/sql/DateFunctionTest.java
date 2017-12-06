/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.parser.sql;

import com.questdb.ex.ParserException;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DateFunctionTest extends AbstractOptimiserTest {

    @Test
    public void testCannotParse() throws Exception {
        assertThat("\n", "select toDate('2017', 'MM/y') from dual");
        assertThat("\n", "select TO_DATE('2017', 'MM/y') from dual");
    }

    @Test
    public void testDefaultLocale() throws Exception {
        assertThat("2017-03-01T00:00:00.000Z\n", "select toDate('03/2017', 'MM/y') from dual");
        assertThat("2017-03-01T00:00:00.000Z\n", "select TO_DATE('03/2017', 'MM/y') from dual");
    }

    @Test
    public void testParseWithLocale() throws Exception {
        assertThat("2017-04-01T00:00:00.000Z\n", "select toDate('Abril 2017', 'MMM y', 'es') from dual");
        assertThat("2017-04-01T00:00:00.000Z\n", "select TO_DATE('Abril 2017', 'MMM y', 'es') from dual");
    }

    @Test
    public void testToChar() throws Exception {
        assertThat("20-03-2015\n", "select TO_CHAR(TO_DATE('2015-03-20T15:25:40.567Z'), 'dd-MM-y') from dual");
    }

    @Test
    public void testToCharTZ() throws Exception {
        assertThat("20-03-2015 08:25:40.567 PDT\n", "select TO_CHAR(TO_DATE('2015-03-20T15:25:40.567Z'), 'dd-MM-y HH:mm:ss.SSS Z', 'PDT') from dual");
        assertThat("20-03-2015 21:25:40.567 +0600\n", "select TO_CHAR(TO_DATE('2015-03-20T15:25:40.567Z'), 'dd-MM-y HH:mm:ss.SSS Z', '+0600') from dual");
    }

    @Test
    public void testToCharTZLocale() throws Exception {
        assertThat("Пт, 20 мар 2015 08:25:40.567 PDT\n", "select TO_CHAR(TO_DATE('2015-03-20T15:25:40.567Z'), 'E, dd MMM y HH:mm:ss.SSS Z', 'PDT', 'ru') from dual");
    }

    @Test
    public void testWrongLocale() {
        try {
            expectFailure("select toDate('Abril 2017', 'MMM y', 'wrong') from dual");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(37, QueryError.getPosition());
            TestUtils.assertEquals("Invalid locale", QueryError.getMessage());
        }
    }
}
