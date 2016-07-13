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

public class DenseRankAnalyticFunctionTest extends AbstractAnalyticRecordSourceTest {
    @Test
    public void testRank() throws Exception {
        assertThat("5\ttrue\tAX\t2016-05-01T10:26:00.000Z\n" +
                        "6\tfalse\tBZ\t2016-05-01T10:27:00.000Z\n" +
                        "7\ttrue\tBZ\t2016-05-01T10:28:00.000Z\n" +
                        "8\tfalse\tAX\t2016-05-01T10:29:00.000Z\n" +
                        "9\tfalse\tBZ\t2016-05-01T10:30:00.000Z\n",
                "select dense_rank() x over(), boo, str, timestamp from 'abc' limit 5,10");
    }
}
