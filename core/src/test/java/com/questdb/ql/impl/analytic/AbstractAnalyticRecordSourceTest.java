/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/

package com.questdb.ql.impl.analytic;

import com.questdb.JournalEntryWriter;
import com.questdb.JournalWriter;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Dates;
import com.questdb.misc.Rnd;
import com.questdb.ql.parser.AbstractOptimiserTest;
import org.junit.BeforeClass;

public abstract class AbstractAnalyticRecordSourceTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {
        try (JournalWriter w = factory.bulkWriter(new JournalStructure("xyz")
                .$int("i")
                .$str("str")
                .$ts()
                .$())) {
            int n = 100;
            String[] sym = {"AX", "XX", "BZ", "KK"};
            Rnd rnd = new Rnd();

            long t = Dates.toMillis(2016, 5, 1, 10, 20);
            for (int i = 0; i < n; i++) {
                JournalEntryWriter ew = w.entryWriter(t += 60000);
                ew.putInt(0, rnd.nextInt());
                ew.putStr(1, sym[rnd.nextPositiveInt() % sym.length]);
                ew.append();
            }
            w.commit();
        }


        try (JournalWriter w = factory.bulkWriter(new JournalStructure("abc")
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
                .$())) {
            int n = 20;
            String[] sym = {"AX", "XX", "BZ", "KK"};
            Rnd rnd = new Rnd();

            long t = Dates.toMillis(2016, 5, 1, 10, 20);
            for (int i = 0; i < n; i++) {
                JournalEntryWriter ew = w.entryWriter(t += 60000);
                ew.putInt(0, rnd.nextInt());
                ew.putDouble(1, rnd.nextDouble());
                ew.putFloat(2, rnd.nextFloat());
                ew.put(3, (byte) rnd.nextInt());
                ew.putLong(4, rnd.nextLong());
                ew.putStr(5, sym[rnd.nextPositiveInt() % sym.length]);
                ew.putBool(6, rnd.nextBoolean());
                ew.putSym(7, sym[rnd.nextPositiveInt() % sym.length]);
                ew.putShort(8, (short) rnd.nextInt());
                ew.putDate(9, rnd.nextLong());
                ew.append();
            }
            w.commit();
        }
    }
}