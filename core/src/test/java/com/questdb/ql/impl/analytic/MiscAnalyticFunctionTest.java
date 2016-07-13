/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.impl.analytic;

import org.junit.Test;

public class MiscAnalyticFunctionTest extends AbstractAnalyticRecordSourceTest {
    @Test
    public void testAnalyticAndAggregates() throws Exception {
        assertThat("0\tBZ\t2016-05-01T10:40:00.000Z\n" +
                        "1\tXX\t2016-05-01T10:37:00.000Z\n" +
                        "2\tKK\t2016-05-01T10:32:00.000Z\n" +
                        "3\tAX\t2016-05-01T10:38:00.000Z\n",
                "select dense_rank() x over(), str, max(timestamp) from 'abc'");
    }
}
