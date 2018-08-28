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

package com.questdb.griffin;

import org.junit.Ignore;
import org.junit.Test;

public class SampleByTest extends AbstractGriffinTest {
    @Test
    @Ignore
    public void testSample() throws Exception {
        assertQuery("",
                "select sum(a) from x sample by 3h",
                "create table x as " +
                        "(" +
                        "select * from" +
                        " random_cursor" +
                        "(20," +
                        " 'a', rnd_double(0)*100," +
                        " 'b', rnd_symbol(5,4,4,1)," +
                        " 'k', timestamp_sequence(to_timestamp(0), 3600000000)" +
                        ")" +
                        ") timestamp(k) partition by DAY",
                null);
    }
}
