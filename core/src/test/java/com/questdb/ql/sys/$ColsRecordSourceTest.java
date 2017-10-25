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

package com.questdb.ql.sys;

import com.questdb.parser.sql.AbstractOptimiserTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class $ColsRecordSourceTest extends AbstractOptimiserTest {
    @BeforeClass
    public static void setUp() throws Exception {
        compiler.execute(FACTORY_CONTAINER.getFactory(), "create table xy (x int, y string, ts date), index(y buckets 30) timestamp(ts) partition by YEAR");
        compiler.execute(FACTORY_CONTAINER.getFactory(), "create table abc (a symbol, b boolean, d double), index(a buckets 70)");
        $ColsRecordSource.init();
    }

    @Test
    public void testCompiled() throws Exception {
        assertThat("abc\ta\tSYMBOL\tfalse\t\ttrue\t127\n" +
                        "abc\tb\tBOOLEAN\tfalse\t\tfalse\t0\n" +
                        "abc\td\tDOUBLE\tfalse\t\tfalse\t0\n" +
                        "xy\tts\tDATE\ttrue\tYEAR\tfalse\t0\n" +
                        "xy\tx\tINT\tfalse\t\tfalse\t0\n" +
                        "xy\ty\tSTRING\tfalse\t\ttrue\t31\n",
                "$cols order by table_name, column_name");
    }
}