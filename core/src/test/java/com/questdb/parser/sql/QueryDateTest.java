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
import com.questdb.std.Rnd;
import com.questdb.std.ex.JournalException;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.store.JournalEntryWriter;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.configuration.JournalStructure;
import org.junit.Test;

public class QueryDateTest extends AbstractOptimiserTest {
    @Test
    public void testName() throws Exception {
        createTab();
        assertThat("10000\n", "select count() from tab where w = NaN");
    }

    private void createTab() throws JournalException, NumericException {
        try (JournalWriter w = FACTORY_CONTAINER.getFactory().writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $date("z").
                        $date("w").
                        $ts()

        )) {

            Rnd rnd = new Rnd();

            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");
            long time1 = DateFormatUtils.parseDateTime("2015-10-03T00:00:00.000Z");

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
}
