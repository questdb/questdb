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

package com.questdb.ql.impl;

import com.questdb.JournalEntryWriter;
import com.questdb.JournalWriter;
import com.questdb.PartitionType;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Dates;
import com.questdb.misc.Rnd;
import com.questdb.ql.parser.AbstractOptimiserTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class JournalSourceTest extends AbstractOptimiserTest {
    @Before
    public void setUp() throws Exception {
        try (JournalWriter w = factory.bulkWriter(new JournalStructure("parent")
                .$int("i")
                .$double("d")
                .$float("f")
                .$byte("b")
                .$long("l")
                .$str("str")
                .$bool("boo")
                .$sym("sym")
                .$short("sho")
                .$date("date")
                .$ts()
                .partitionBy(PartitionType.DAY)
                .$())) {


            Rnd rnd = new Rnd();
            int n = 24 * 3; // number of records
            int timestep = 60 * 60 * 1000; // time stepping 1hr
            String[] sym = {"AX", "XX", "BZ", "KK", "PP", "UX", "LK"};
            String[] str = new String[16];
            for (int i = 0; i < str.length; i++) {
                str[i] = rnd.nextString(rnd.nextPositiveInt() % 10);
            }


            long t = Dates.toMillis(2016, 5, 1, 10, 20);
            for (int i = 0; i < n; i++) {
                JournalEntryWriter ew = w.entryWriter(t += timestep);
                ew.putInt(0, rnd.nextInt() % 15);
                ew.putDouble(1, rnd.nextDouble());
                ew.putFloat(2, rnd.nextFloat());
                ew.put(3, (byte) rnd.nextInt());
                ew.putLong(4, rnd.nextLong() % 30);
                ew.putStr(5, str[rnd.nextPositiveInt() % str.length]);
                ew.putBool(6, rnd.nextBoolean());
                ew.putSym(7, sym[rnd.nextPositiveInt() % sym.length]);
                ew.putShort(8, (short) (rnd.nextInt() % 20));
                ew.putDate(9, rnd.nextLong());
                ew.append();
            }
            w.commit();
        }
    }

    @Test
    @Ignore
    public void testSelect() throws Exception {
        assertThat("", "parent");

    }
}