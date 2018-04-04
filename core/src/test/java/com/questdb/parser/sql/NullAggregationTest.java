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

package com.questdb.parser.sql;

import com.questdb.std.NumericException;
import com.questdb.std.ObjHashSet;
import com.questdb.std.Rnd;
import com.questdb.std.ex.JournalException;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.store.JournalEntryWriter;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.configuration.JournalStructure;
import org.junit.BeforeClass;
import org.junit.Test;

public class NullAggregationTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {
        createTabWithNaNs2();
    }

    @Test
    public void testNanCount() throws Exception {
        assertThat("NaN\tVEZDYHDHRFEVHKK\t5\n" +
                "NaN\t\t99\n", "(select z, a, count() from tab) where z = NaN and (a ~ 'VE' or a = null)");
    }

    @Test
    public void testNullStringCount() throws Exception {
        assertThat("\t321\t4\n", "(select a, z, count() from tab) where z = 321 and a = null");
    }

    private static void createTabWithNaNs2() throws JournalException, NumericException {
        try (JournalWriter w = FACTORY_CONTAINER.getFactory().writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $long("z").
                        $int("w").
                        $str("a").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            int n = 128;
            ObjHashSet<String> names = getNames(rnd, n);

            int mask = n - 1;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 10000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putStr(0, names.get(rnd.nextInt() & mask));
                ew.putDouble(1, rnd.nextDouble());
                if (rnd.nextPositiveInt() % 10 == 0) {
                    ew.putNull(2);
                } else {
                    ew.putDouble(2, rnd.nextDouble());
                }
                if (rnd.nextPositiveInt() % 10 == 0) {
                    ew.putNull(3);
                } else {
                    ew.putLong(3, rnd.nextLong() % 500);
                }
                if (rnd.nextPositiveInt() % 10 == 0) {
                    ew.putNull(4);
                } else {
                    ew.putInt(4, rnd.nextInt() % 500);
                }

                if (rnd.nextPositiveInt() % 10 == 0) {
                    ew.putNull(5);
                } else {
                    ew.putStr(5, names.get(rnd.nextInt() & mask));
                }

                ew.putDate(6, t += (60 * 60 * 1000));
                ew.append();
            }
            w.commit();
        }
    }

    private static ObjHashSet<String> getNames(Rnd r, int n) {
        ObjHashSet<String> names = new ObjHashSet<>();
        for (int i = 0; i < n; i++) {
            names.add(r.nextString(15));
        }
        return names;
    }

}
