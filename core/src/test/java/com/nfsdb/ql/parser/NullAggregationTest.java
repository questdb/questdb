/*******************************************************************************
 * _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 * <p>
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.NumericException;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Rnd;
import com.nfsdb.std.ObjHashSet;
import org.junit.BeforeClass;
import org.junit.Test;

public class NullAggregationTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {
        createTabWithNaNs2();
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
