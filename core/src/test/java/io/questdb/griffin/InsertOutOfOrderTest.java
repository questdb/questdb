/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Before;
import org.junit.Test;

public class InsertOutOfOrderTest extends AbstractGriffinTest {

    private final static long MICROS_IN_A_DAY = 86_400_000_000L;
    private final static long MICROS_IN_HALF_A_DAY = MICROS_IN_A_DAY / 2;

    @Before
    public void setup() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testInsertOutOfOrderWhenLastRowMatchesMaxTimestamp() throws Exception {

        final String expected = "a\tb\tk\n" +
                "80.43224099968394\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.4460485739401\tPEHN\t1970-01-02T00:00:00.000000Z\n" +
                "88.99286912289664\tSXUX\t1970-01-03T00:00:00.000000Z\n" +
                "42.17768841969397\tGPGW\t1970-01-04T00:00:00.000000Z\n" +
                "66.93837147631712\tDEYY\t1970-01-05T00:00:00.000000Z\n" +
                "0.35983672154330515\tHFOW\t1970-01-06T00:00:00.000000Z\n" +
                "21.583224269349387\tYSBE\t1970-01-07T00:00:00.000000Z\n" +
                "12.503042190293423\tSHRU\t1970-01-08T00:00:00.000000Z\n" +
                "67.00476391801053\tQULO\t1970-01-09T00:00:00.000000Z\n" +
                "81.0161274171258\tTJRS\t1970-01-10T00:00:00.000000Z\n";
        assertQuery(expected,
                "select * from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(0, " + MICROS_IN_A_DAY + ") k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k) partition by DAY",
                "k");

        final String expectedAfterInsert = "a\tb\tk\n" +
                "80.43224099968394\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.4460485739401\tPEHN\t1970-01-02T00:00:00.000000Z\n" +
                "88.99286912289664\tSXUX\t1970-01-03T00:00:00.000000Z\n" +
                "42.17768841969397\tGPGW\t1970-01-04T00:00:00.000000Z\n" +
                "66.93837147631712\tDEYY\t1970-01-05T00:00:00.000000Z\n" +
                "0.35983672154330515\tHFOW\t1970-01-06T00:00:00.000000Z\n" +
                "21.583224269349387\tYSBE\t1970-01-07T00:00:00.000000Z\n" +
                "12.503042190293423\tSHRU\t1970-01-08T00:00:00.000000Z\n" +
                "67.00476391801053\tQULO\t1970-01-09T00:00:00.000000Z\n" +
                "81.0161274171258\tTJRS\t1970-01-10T00:00:00.000000Z\n";

        assertQuery(expectedAfterInsert,
                "select * from x",
                "insert into x select * from " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(to_timestamp('1970-01-09T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), " + MICROS_IN_A_DAY + ") k" +
                        " from" +
                        " long_sequence(2)" +
                        ") timestamp(k) partition by DAY",
                "k");
    }

    @Test
    public void testInsertOutOfOrderToFirstPartition() throws Exception {

        final String expected = "a\tb\tk\n" +
                "80.43224099968394\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.4460485739401\tPEHN\t1970-01-02T00:00:00.000000Z\n" +
                "88.99286912289664\tSXUX\t1970-01-03T00:00:00.000000Z\n" +
                "42.17768841969397\tGPGW\t1970-01-04T00:00:00.000000Z\n" +
                "66.93837147631712\tDEYY\t1970-01-05T00:00:00.000000Z\n" +
                "0.35983672154330515\tHFOW\t1970-01-06T00:00:00.000000Z\n" +
                "21.583224269349387\tYSBE\t1970-01-07T00:00:00.000000Z\n" +
                "12.503042190293423\tSHRU\t1970-01-08T00:00:00.000000Z\n" +
                "67.00476391801053\tQULO\t1970-01-09T00:00:00.000000Z\n" +
                "81.0161274171258\tTJRS\t1970-01-10T00:00:00.000000Z\n";
        assertQuery(expected,
                "select * from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(0, " + MICROS_IN_A_DAY + ") k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k) partition by DAY",
                "k");

        final String expectedAfterInsert = "a\tb\tk\n" +
                "80.43224099968394\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.4460485739401\tPEHN\t1970-01-02T00:00:00.000000Z\n" +
                "88.99286912289664\tSXUX\t1970-01-03T00:00:00.000000Z\n" +
                "42.17768841969397\tGPGW\t1970-01-04T00:00:00.000000Z\n" +
                "66.93837147631712\tDEYY\t1970-01-05T00:00:00.000000Z\n" +
                "0.35983672154330515\tHFOW\t1970-01-06T00:00:00.000000Z\n" +
                "21.583224269349387\tYSBE\t1970-01-07T00:00:00.000000Z\n" +
                "12.503042190293423\tSHRU\t1970-01-08T00:00:00.000000Z\n" +
                "67.00476391801053\tQULO\t1970-01-09T00:00:00.000000Z\n" +
                "81.0161274171258\tTJRS\t1970-01-10T00:00:00.000000Z\n";

        assertQuery(expectedAfterInsert,
                "select * from x",
                "insert into x select * from " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(to_timestamp('1970-01-01T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), " + MICROS_IN_A_DAY + ") k" +
                        " from" +
                        " long_sequence(2)" +
                        ") timestamp(k) partition by DAY",
                "k");
    }

    @Test
    public void testInsertOutOfOrderToLastPartition() throws Exception {

        final String expected = "a\tb\tk\n" +
                "80.43224099968394\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.4460485739401\tPEHN\t1970-01-01T12:00:00.000000Z\n" +
                "88.99286912289664\tSXUX\t1970-01-02T00:00:00.000000Z\n" +
                "42.17768841969397\tGPGW\t1970-01-02T12:00:00.000000Z\n" +
                "66.93837147631712\tDEYY\t1970-01-03T00:00:00.000000Z\n" +
                "0.35983672154330515\tHFOW\t1970-01-03T12:00:00.000000Z\n" +
                "21.583224269349387\tYSBE\t1970-01-04T00:00:00.000000Z\n" +
                "12.503042190293423\tSHRU\t1970-01-04T12:00:00.000000Z\n" +
                "67.00476391801053\tQULO\t1970-01-05T00:00:00.000000Z\n" +
                "81.0161274171258\tTJRS\t1970-01-05T12:00:00.000000Z\n";
        assertQuery(expected,
                "select * from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(0, " + MICROS_IN_HALF_A_DAY + ") k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k) partition by DAY",
                "k");

        final String expectedAfterInsert = "a\tb\tk\n" +
                "80.43224099968394\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.4460485739401\tPEHN\t1970-01-01T12:00:00.000000Z\n" +
                "88.99286912289664\tSXUX\t1970-01-02T00:00:00.000000Z\n" +
                "42.17768841969397\tGPGW\t1970-01-02T12:00:00.000000Z\n" +
                "66.93837147631712\tDEYY\t1970-01-03T00:00:00.000000Z\n" +
                "0.35983672154330515\tHFOW\t1970-01-03T12:00:00.000000Z\n" +
                "21.583224269349387\tYSBE\t1970-01-04T00:00:00.000000Z\n" +
                "12.503042190293423\tSHRU\t1970-01-04T12:00:00.000000Z\n" +
                "67.00476391801053\tQULO\t1970-01-05T00:00:00.000000Z\n" +
                "81.0161274171258\tTJRS\t1970-01-05T12:00:00.000000Z\n";

        assertQuery(expectedAfterInsert,
                "select * from x",
                "insert into x select * from " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(to_timestamp('1970-01-05T01:00:00', 'yyyy-MM-ddTHH:mm:ss'), " + MICROS_IN_HALF_A_DAY + ") k" +
                        " from" +
                        " long_sequence(2)" +
                        ") timestamp(k) partition by DAY",
                "k");
    }

    private void assertContentsOfTempRows(String expected, String tableName){
        TableReader x = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName);
        //TODO
        // get path to table files
        //load each column file
        //build actual string and assertEquals
    }
}
