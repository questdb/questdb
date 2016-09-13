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

package com.questdb.ql.impl.sys;

import com.questdb.JournalEntryWriter;
import com.questdb.JournalWriter;
import com.questdb.misc.Dates;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class $TabsRecordSourceTest extends AbstractOptimiserTest {


    @BeforeClass
    public static void setUp() throws Exception {
        try (JournalWriter w = compiler.createWriter(factory, "create table xyz(x int, y string, timestamp date) timestamp(timestamp) partition by MONTH")) {
            JournalEntryWriter ew;

            ew = w.entryWriter(Dates.parseDateTime("2016-01-02T00:00:00.000Z"));
            ew.putInt(0, 0);
            ew.append();

            ew = w.entryWriter(Dates.parseDateTime("2016-02-02T00:00:00.000Z"));
            ew.putInt(0, 1);
            ew.append();

            w.commit();
        }
    }

    @Test
    public void testCompiled() throws Exception {
        $TabsRecordSource.init();
        assertThat("xyz\tMONTH\t2\t3\n", "$tabs");
    }

    @Test
    public void testSimple() throws Exception {
        sink.clear();
        try ($TabsRecordSource rs = new $TabsRecordSource(4096)) {
            printer.print(rs, factory);
        }
        TestUtils.assertEquals("xyz\tMONTH\t2\t3\n", sink);
    }
}