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
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.ql.parser;

import com.questdb.JournalEntryWriter;
import com.questdb.JournalWriter;
import com.questdb.ex.JournalException;
import com.questdb.ex.NumericException;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Dates;
import com.questdb.misc.Rnd;
import com.questdb.std.ObjHashSet;
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
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $long("z").
                        $int("w").
                        $str("a").
                        $ts()

        );

        Rnd rnd = new Rnd();
        int n = 128;
        ObjHashSet<String> names = getNames(rnd, n);

        int mask = n - 1;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

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

    private static ObjHashSet<String> getNames(Rnd r, int n) {
        ObjHashSet<String> names = new ObjHashSet<>();
        for (int i = 0; i < n; i++) {
            names.add(r.nextString(15));
        }
        return names;
    }

}
