/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
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
import org.junit.Test;

public class QueryDateTest extends AbstractOptimiserTest {
    @Test
    public void testName() throws Exception {
        createTab();
        assertThat("10000\n", "select count() from tab where w = NaN");
    }

    private void createTab() throws JournalException, NumericException {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $date("z").
                        $date("w").
                        $ts()

        );

        Rnd rnd = new Rnd();

        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");
        long time1 = Dates.parseDateTime("2015-10-03T00:00:00.000Z");

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, rnd.nextChars(15));
            ew.putDouble(1, rnd.nextDouble());
            if (rnd.nextPositiveInt() % 10 == 0) {
                ew.putNull(2);
            } else {
                ew.putDouble(2, rnd.nextDouble());
            }
            if (rnd.nextPositiveInt() % 10 == 0) {
                ew.putNull(3);
            } else {
                ew.putDate(3, time1 + rnd.nextLong() % 5000000);
            }
            ew.putDate(5, t += 10);
            ew.append();
        }
        w.commit();
    }
}
