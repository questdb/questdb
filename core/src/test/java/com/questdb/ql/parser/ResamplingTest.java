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
import com.questdb.ex.ParserException;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Dates;
import com.questdb.misc.Rnd;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResamplingTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {
        int recordCount = 10000;
        int employeeCount = 10;
        try (JournalWriter orders = factory.writer(
                new JournalStructure("orders").
                        $int("orderId").
                        $int("customerId").
                        $int("productId").
                        $str("employeeId").
                        $date("orderDate").
                        $int("quantity").
                        $double("price").
                        $float("rate").
                        recordCountHint(recordCount).
                        $()
        )) {


            Rnd rnd = new Rnd();

            String employees[] = new String[employeeCount];
            for (int i = 0; i < employees.length; i++) {
                employees[i] = rnd.nextString(9);
            }

            long timestamp = Dates.parseDateTime("2014-05-04T10:30:00.000Z");
            int tsIncrement = 10000;

            int orderId = 0;
            for (int i = 0; i < recordCount; i++) {
                JournalEntryWriter w = orders.entryWriter();
                w.putInt(0, ++orderId);
                w.putInt(1, rnd.nextPositiveInt() % 500);
                w.putInt(2, rnd.nextPositiveInt() % 200);
                w.putStr(3, employees[rnd.nextPositiveInt() % employeeCount]);
                w.putDate(4, timestamp += tsIncrement);
                w.putInt(5, rnd.nextPositiveInt());
                w.putDouble(6, rnd.nextDouble());
                w.putFloat(7, rnd.nextFloat());
                w.append();
            }
            orders.commit();
        }


        try (JournalWriter orders2 = factory.writer(
                new JournalStructure("orders2").
                        $int("orderId").
                        $int("customerId").
                        $int("productId").
                        $str("employeeId").
                        $date("orderDate").
                        $int("quantity").
                        $double("price").
                        $float("rate").
                        $date("basketDate").
                        recordCountHint(recordCount).
                        $()
        )) {


            Rnd rnd = new Rnd();

            String employees[] = new String[employeeCount];
            for (int i = 0; i < employees.length; i++) {
                employees[i] = rnd.nextString(9);
            }

            long timestamp = Dates.parseDateTime("2014-05-04T10:30:00.000Z");
            long timestamp2 = Dates.parseDateTime("2014-05-03T00:15:00.000Z");
            int tsIncrement = 10000;

            int orderId = 0;
            for (int i = 0; i < recordCount; i++) {
                JournalEntryWriter w = orders2.entryWriter();
                w.putInt(0, ++orderId);
                w.putInt(1, rnd.nextPositiveInt() % 500);
                w.putInt(2, rnd.nextPositiveInt() % 200);
                w.putStr(3, employees[rnd.nextPositiveInt() % employeeCount]);
                w.putDate(4, timestamp += tsIncrement);
                w.putInt(5, rnd.nextPositiveInt());
                w.putDouble(6, rnd.nextDouble());
                w.putFloat(7, rnd.nextFloat());
                w.putDate(8, timestamp2 + tsIncrement);
                w.append();
            }
            orders2.commit();
        }

        JournalWriter orders3 = factory.writer(
                new JournalStructure("orders3").
                        $int("orderId").
                        $int("customerId").
                        $int("productId").
                        $str("employeeId").
                        $int("quantity").
                        $double("price").
                        $float("rate").
                        recordCountHint(recordCount).
                        $()
        );
        orders3.close();
    }

    @Test
    public void testResampling2() throws Exception {
        assertThat("2014-05-04T08:00:00.000Z\t-18.041874103485\n" +
                        "2014-05-04T16:00:00.000Z\t-12.148285354848\n" +
                        "2014-05-05T00:00:00.000Z\t-10.773253435499\n" +
                        "2014-05-05T08:00:00.000Z\t0.750778769143\n",
                "select orderDate, vwap(price, quantity) from orders sample by 8h");
    }

    @Test
    public void testResamplingAmbiguousTimestamp() throws Exception {
        try {
            assertThat("", "select orderDate, employeeId, sum(price*quantity)/lsum(quantity), vwap(price, quantity) sum from orders2 sample by 1d");
        } catch (ParserException e) {
            Assert.assertEquals(97, QueryError.getPosition());
        }
    }

    @Test
    public void testResamplingAutoTimestamp() throws Exception {
        assertThat("2014-05-04T00:00:00.000Z\tTGPGWFFYU\t-63.253453401381\t-63.253453401381\n" +
                        "2014-05-04T00:00:00.000Z\tDEYYQEHBH\t17.232482911526\t17.232482911526\n" +
                        "2014-05-04T00:00:00.000Z\tSRYRFBVTM\t-10.581027815832\t-10.581027815832\n" +
                        "2014-05-04T00:00:00.000Z\tGZSXUXIBB\t1.191841183028\t1.191841183028\n" +
                        "2014-05-04T00:00:00.000Z\tUEDRQQULO\t-25.284387331977\t-25.284387331977\n" +
                        "2014-05-04T00:00:00.000Z\tFOWLPDXYS\t-21.110275361914\t-21.110275361914\n" +
                        "2014-05-04T00:00:00.000Z\tFJGETJRSZ\t-12.327370360108\t-12.327370360108\n" +
                        "2014-05-04T00:00:00.000Z\tBEOUOJSHR\t3.586645530510\t3.586645530510\n" +
                        "2014-05-04T00:00:00.000Z\tYRXPEHNRX\t-10.131327938006\t-10.131327938006\n" +
                        "2014-05-04T00:00:00.000Z\tVTJWCPSWH\t-24.329569665466\t-24.329569665466\n" +
                        "2014-05-05T00:00:00.000Z\tDEYYQEHBH\t-30.963486961448\t-30.963486961448\n" +
                        "2014-05-05T00:00:00.000Z\tSRYRFBVTM\t13.422138958032\t13.422138958032\n" +
                        "2014-05-05T00:00:00.000Z\tVTJWCPSWH\t0.595780540587\t0.595780540587\n" +
                        "2014-05-05T00:00:00.000Z\tBEOUOJSHR\t-31.682205368795\t-31.682205368795\n" +
                        "2014-05-05T00:00:00.000Z\tFJGETJRSZ\t-31.906856870748\t-31.906856870748\n" +
                        "2014-05-05T00:00:00.000Z\tGZSXUXIBB\t7.870801180456\t7.870801180456\n" +
                        "2014-05-05T00:00:00.000Z\tFOWLPDXYS\t-11.860556414848\t-11.860556414848\n" +
                        "2014-05-05T00:00:00.000Z\tYRXPEHNRX\t-8.573401980346\t-8.573401980346\n" +
                        "2014-05-05T00:00:00.000Z\tUEDRQQULO\t16.987375521363\t16.987375521363\n" +
                        "2014-05-05T00:00:00.000Z\tTGPGWFFYU\t17.260132823173\t17.260132823173\n",
                "select orderDate, employeeId, sum(price*quantity)/lsum(quantity), vwap(price, quantity) sum from orders sample by 1d");
    }

    @Test
    public void testResamplingExplicitTimestamp() throws Exception {
        assertThat("2014-05-04T00:00:00.000Z\tTGPGWFFYU\t-63.253453401381\t-63.253453401381\n" +
                        "2014-05-04T00:00:00.000Z\tDEYYQEHBH\t17.232482911526\t17.232482911526\n" +
                        "2014-05-04T00:00:00.000Z\tSRYRFBVTM\t-10.581027815832\t-10.581027815832\n" +
                        "2014-05-04T00:00:00.000Z\tGZSXUXIBB\t1.191841183028\t1.191841183028\n" +
                        "2014-05-04T00:00:00.000Z\tUEDRQQULO\t-25.284387331977\t-25.284387331977\n" +
                        "2014-05-04T00:00:00.000Z\tFOWLPDXYS\t-21.110275361914\t-21.110275361914\n" +
                        "2014-05-04T00:00:00.000Z\tFJGETJRSZ\t-12.327370360108\t-12.327370360108\n" +
                        "2014-05-04T00:00:00.000Z\tBEOUOJSHR\t3.586645530510\t3.586645530510\n" +
                        "2014-05-04T00:00:00.000Z\tYRXPEHNRX\t-10.131327938006\t-10.131327938006\n" +
                        "2014-05-04T00:00:00.000Z\tVTJWCPSWH\t-24.329569665466\t-24.329569665466\n" +
                        "2014-05-05T00:00:00.000Z\tDEYYQEHBH\t-30.963486961448\t-30.963486961448\n" +
                        "2014-05-05T00:00:00.000Z\tSRYRFBVTM\t13.422138958032\t13.422138958032\n" +
                        "2014-05-05T00:00:00.000Z\tVTJWCPSWH\t0.595780540587\t0.595780540587\n" +
                        "2014-05-05T00:00:00.000Z\tBEOUOJSHR\t-31.682205368795\t-31.682205368795\n" +
                        "2014-05-05T00:00:00.000Z\tFJGETJRSZ\t-31.906856870748\t-31.906856870748\n" +
                        "2014-05-05T00:00:00.000Z\tGZSXUXIBB\t7.870801180456\t7.870801180456\n" +
                        "2014-05-05T00:00:00.000Z\tFOWLPDXYS\t-11.860556414848\t-11.860556414848\n" +
                        "2014-05-05T00:00:00.000Z\tYRXPEHNRX\t-8.573401980346\t-8.573401980346\n" +
                        "2014-05-05T00:00:00.000Z\tUEDRQQULO\t16.987375521363\t16.987375521363\n" +
                        "2014-05-05T00:00:00.000Z\tTGPGWFFYU\t17.260132823173\t17.260132823173\n",
                "select orderDate, employeeId, sum(price*quantity)/lsum(quantity), vwap(price, quantity) sum from orders2 timestamp(orderDate) sample by 1d");
    }

    @Test
    public void testResamplingNoTimestamp() throws Exception {
        try {
            assertThat("", "select employeeId, sum(price*quantity)/lsum(quantity), vwap(price, quantity) sum from orders3 sample by 1d");
        } catch (ParserException e) {
            Assert.assertEquals(86, QueryError.getPosition());
        }
    }
}
