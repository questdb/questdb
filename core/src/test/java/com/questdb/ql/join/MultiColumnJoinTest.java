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

package com.questdb.ql.join;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.QueryError;
import com.questdb.std.Rnd;
import com.questdb.store.JournalEntryWriter;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.configuration.JournalStructure;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MultiColumnJoinTest extends AbstractTest {

    @Before
    public void setUp() throws Exception {

        getFactory().getConfiguration().exists("");
        try (JournalWriter a = getFactory().writer(new JournalStructure("a").$int("x").$str("y").$double("amount").$())) {
            try (JournalWriter b = getFactory().writer(new JournalStructure("b").$int("x").$str("y").$str("name").$())) {

                Rnd rnd = new Rnd();

                for (int i = 0; i < 10; i++) {
                    int x = rnd.nextInt();
                    String y = rnd.nextString(rnd.nextPositiveInt() % 15);

                    JournalEntryWriter ewa = a.entryWriter();
                    JournalEntryWriter ewb = b.entryWriter();

                    ewa.putInt(0, x);
                    ewa.putStr(1, y);
                    ewa.putDouble(2, rnd.nextDouble());
                    ewa.append();

                    ewb.putInt(0, x);
                    ewb.putStr(1, y);
                    ewb.putStr(2, rnd.nextChars(rnd.nextPositiveInt() % 20));
                    ewb.append();
                }
            }
//            a.commit();
//            b.commit();
        }
    }

    @Test
    public void testJoinOnThreeFields() throws Exception {
        final String expected = "-1148479920\tJWC\t-1024.000000000000\t-1148479920\tJWC\tHYRXPEHNRXG\n" +
                "339631474\tXUXIBBT\t981.018066406250\t339631474\tXUXIBBT\tWFFYU\n" +
                "-1125169127\tYYQ\t0.000006369316\t-1125169127\tYYQ\t\n" +
                "1699553881\tOWLPDXYSBEOUOJ\t0.169966913760\t1699553881\tOWLPDXYSBEOUOJ\tUEDRQQULOFJGETJR\n" +
                "326010667\tSRYR\t695.796875000000\t326010667\tSRYR\tTMHGOOZZVDZJMYI\n" +
                "1985398001\tX\t-1024.000000000000\t1985398001\tX\tICWEKGHVU\n" +
                "532665695\tDO\t632.921875000000\t532665695\tDO\tDYY\n" +
                "114747951\tGQOLYXWC\t0.000229079233\t114747951\tGQOLYXWC\t\n" +
                "1254404167\tWDSWUGSHOLNVTI\t770.359375000000\t1254404167\tWDSWUGSHOLNVTI\tXIOVIKJSMSSUQSRLTKV\n" +
                "-2080340570\tJO\t0.555824235082\t-2080340570\tJO\tHZEPIHVLTOVLJU\n";

        assertThat(expected, "a join b on (x,y)");
    }

    @Test
    public void testNonLiteral() {
        try {
            expectFailure("a join b on (1+2)");
        } catch (ParserException e) {
            Assert.assertEquals(14, QueryError.getPosition());
        }
    }
}
