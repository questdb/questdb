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

package com.questdb.parser.sql;

import com.questdb.ex.ParserException;
import com.questdb.misc.Rnd;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.store.JournalEntryWriter;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.configuration.JournalStructure;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResamplingTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {
        int recordCount = 10000;
        int employeeCount = 10;
        try (JournalWriter orders = FACTORY_CONTAINER.getFactory().writer(
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

            long timestamp = DateFormatUtils.parseDateTime("2014-05-04T10:30:00.000Z");
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

        try (JournalWriter orders2 = FACTORY_CONTAINER.getFactory().writer(
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

            long timestamp = DateFormatUtils.parseDateTime("2014-05-04T10:30:00.000Z");
            long timestamp2 = DateFormatUtils.parseDateTime("2014-05-03T00:15:00.000Z");
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

        try (JournalWriter orders2 = FACTORY_CONTAINER.getFactory().writer(
                new JournalStructure("orders4").
                        $int("orderId").
                        $int("customerId").
                        $int("productId").
                        $sym("employeeId").
                        $date("orderDate").
                        $int("quantity").
                        $double("price").
                        $float("rate").
                        $ts("basketDate").
                        recordCountHint(recordCount).
                        $()
        )) {

            Rnd rnd = new Rnd();

            String employees[] = new String[employeeCount];
            for (int i = 0; i < employees.length; i++) {
                employees[i] = rnd.nextString(9);
            }

            long timestamp = DateFormatUtils.parseDateTime("2014-05-04T10:30:00.000Z");
            long timestamp2 = DateFormatUtils.parseDateTime("2014-05-03T00:15:00.000Z");
            int tsIncrement = 10000;

            int orderId = 0;
            for (int i = 0; i < recordCount; i++) {
                JournalEntryWriter w = orders2.entryWriter();
                w.putInt(0, ++orderId);
                w.putInt(1, rnd.nextPositiveInt() % 500);
                w.putInt(2, rnd.nextPositiveInt() % 200);
                w.putSym(3, employees[rnd.nextPositiveInt() % employeeCount]);
                w.putDate(4, timestamp += tsIncrement);
                w.putInt(5, rnd.nextPositiveInt());
                w.putDouble(6, rnd.nextDouble());
                w.putFloat(7, rnd.nextFloat());
                w.putDate(8, timestamp2 + tsIncrement);
                w.append();
            }
            orders2.commit();
        }

        JournalWriter orders3 = FACTORY_CONTAINER.getFactory().writer(
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
                "select orderDate, vwap(price, quantity) from orders timestamp (orderDate) sample by 8h");
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
                "select orderDate, employeeId, sum(price*quantity)/lsum(quantity), vwap(price, quantity) sum from orders timestamp (orderDate) sample by 1d");
    }

    @Test
    public void testResamplingAvg() throws Exception {
        assertThat("2014-05-04T00:00:00.000Z\tTGPGWFFYU\t-41.304426677644\t-41.304426677644\n" +
                        "2014-05-04T00:00:00.000Z\tDEYYQEHBH\t7.372452128965\t7.372452128965\n" +
                        "2014-05-04T00:00:00.000Z\tSRYRFBVTM\t-12.203630788820\t-12.203630788820\n" +
                        "2014-05-04T00:00:00.000Z\tGZSXUXIBB\t4.115922051504\t4.115922051504\n" +
                        "2014-05-04T00:00:00.000Z\tUEDRQQULO\t-24.365014628709\t-24.365014628709\n" +
                        "2014-05-04T00:00:00.000Z\tFOWLPDXYS\t-29.996629348333\t-29.996629348333\n" +
                        "2014-05-04T00:00:00.000Z\tFJGETJRSZ\t-4.318969422717\t-4.318969422717\n" +
                        "2014-05-04T00:00:00.000Z\tBEOUOJSHR\t7.502717136654\t7.502717136654\n" +
                        "2014-05-04T00:00:00.000Z\tYRXPEHNRX\t0.601852660663\t0.601852660663\n" +
                        "2014-05-04T00:00:00.000Z\tVTJWCPSWH\t-9.734968411374\t-9.734968411374\n" +
                        "2014-05-05T00:00:00.000Z\tDEYYQEHBH\t-16.135037390878\t-16.135037390878\n" +
                        "2014-05-05T00:00:00.000Z\tSRYRFBVTM\t14.227762047291\t14.227762047291\n" +
                        "2014-05-05T00:00:00.000Z\tVTJWCPSWH\t-21.138005372460\t-21.138005372460\n" +
                        "2014-05-05T00:00:00.000Z\tBEOUOJSHR\t-20.797434613016\t-20.797434613016\n" +
                        "2014-05-05T00:00:00.000Z\tFJGETJRSZ\t-5.417172028476\t-5.417172028476\n" +
                        "2014-05-05T00:00:00.000Z\tGZSXUXIBB\t3.389333554052\t3.389333554052\n" +
                        "2014-05-05T00:00:00.000Z\tFOWLPDXYS\t-21.249544800002\t-21.249544800002\n" +
                        "2014-05-05T00:00:00.000Z\tYRXPEHNRX\t-12.965791501497\t-12.965791501497\n" +
                        "2014-05-05T00:00:00.000Z\tUEDRQQULO\t12.717803509177\t12.717803509177\n" +
                        "2014-05-05T00:00:00.000Z\tTGPGWFFYU\t-4.258237623021\t-4.258237623021\n",
                "select orderDate, employeeId, sum(price)/count(), avg(price) sum from orders2 timestamp(orderDate) sample by 1d");
    }

    @Test
    public void testResamplingBySymbol() throws Exception {
        assertSymbol("select employeeId, vwap(price, quantity) sum from orders4 sample by 1d", 0);
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
    public void testResamplingExplicitTimestampAsFunc() throws Exception {
        try {
            expectFailure("select orderDate, employeeId, sum(price*quantity)/lsum(quantity), vwap(price, quantity) sum from orders2 timestamp(orderDate()) sample by 1d");
        } catch (ParserException e) {
            Assert.assertEquals(124, QueryError.getPosition());
        }
    }

    @Test
    public void testResamplingNoTimestamp() throws Exception {
        try {
            assertThat("", "select employeeId, sum(price*quantity)/lsum(quantity), vwap(price, quantity) sum from orders3 sample by 1d");
        } catch (ParserException e) {
            Assert.assertEquals(86, QueryError.getPosition());
        }
    }

    @Test
    public void testResamplingVariance() throws Exception {
        assertThat("2014-05-04T00:00:00.000Z\tTGPGWFFYU\t-41.304426677644\t-41.304426677644\t172487.097492439040\n" +
                        "2014-05-04T00:00:00.000Z\tDEYYQEHBH\t7.372452128965\t7.372452128965\t148077.744860982304\n" +
                        "2014-05-04T00:00:00.000Z\tSRYRFBVTM\t-12.203630788820\t-12.203630788820\t152042.353099020448\n" +
                        "2014-05-04T00:00:00.000Z\tGZSXUXIBB\t4.115922051504\t4.115922051504\t170953.021239990592\n" +
                        "2014-05-04T00:00:00.000Z\tUEDRQQULO\t-24.365014628709\t-24.365014628709\t151426.976280038816\n" +
                        "2014-05-04T00:00:00.000Z\tFOWLPDXYS\t-29.996629348333\t-29.996629348333\t138923.565569592720\n" +
                        "2014-05-04T00:00:00.000Z\tFJGETJRSZ\t-4.318969422717\t-4.318969422717\t152689.615652833824\n" +
                        "2014-05-04T00:00:00.000Z\tBEOUOJSHR\t7.502717136654\t7.502717136654\t151309.114422499264\n" +
                        "2014-05-04T00:00:00.000Z\tYRXPEHNRX\t0.601852660663\t0.601852660663\t164631.084324245504\n" +
                        "2014-05-04T00:00:00.000Z\tVTJWCPSWH\t-9.734968411374\t-9.734968411374\t163121.713218006048\n" +
                        "2014-05-05T00:00:00.000Z\tDEYYQEHBH\t-16.135037390878\t-16.135037390878\t157609.841614984256\n" +
                        "2014-05-05T00:00:00.000Z\tSRYRFBVTM\t14.227762047291\t14.227762047291\t158581.383371555040\n" +
                        "2014-05-05T00:00:00.000Z\tVTJWCPSWH\t-21.138005372460\t-21.138005372460\t159404.705680565472\n" +
                        "2014-05-05T00:00:00.000Z\tBEOUOJSHR\t-20.797434613016\t-20.797434613016\t168784.531816107744\n" +
                        "2014-05-05T00:00:00.000Z\tFJGETJRSZ\t-5.417172028476\t-5.417172028476\t159768.386499879616\n" +
                        "2014-05-05T00:00:00.000Z\tGZSXUXIBB\t3.389333554052\t3.389333554052\t149853.540099657824\n" +
                        "2014-05-05T00:00:00.000Z\tFOWLPDXYS\t-21.249544800002\t-21.249544800002\t153228.686138200064\n" +
                        "2014-05-05T00:00:00.000Z\tYRXPEHNRX\t-12.965791501497\t-12.965791501497\t174696.469778044480\n" +
                        "2014-05-05T00:00:00.000Z\tUEDRQQULO\t12.717803509177\t12.717803509177\t167741.670910154976\n" +
                        "2014-05-05T00:00:00.000Z\tTGPGWFFYU\t-4.258237623021\t-4.258237623021\t159549.212283462016\n",
                "select orderDate, employeeId, sum(price)/count(), avg(price), var(price) sum from orders2 timestamp(orderDate) sample by 1d");
    }
}
