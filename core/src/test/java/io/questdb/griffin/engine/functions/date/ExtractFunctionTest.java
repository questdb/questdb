/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.date;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class ExtractFunctionTest extends AbstractGriffinTest {

    @Test
    public void testBeginningOfCentury() throws Exception {
        assertQuery(
                "extract\n" +
                        "20\n",
                "select extract(century from to_timestamp('2000-03-11T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testComma() throws Exception {
        assertFailure(
                "select extract(hour, to_timestamp('2022-03-11T22:00:30.555555Z'))",
                null,
                15,
                "Invalid column: hour"
        );
    }

    @Test
    public void testFirstCentury() throws Exception {
        assertQuery(
                "extract\n" +
                        "1\n",
                "select extract(century from to_timestamp('0001-01-01T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testMissingPart() throws Exception {
        assertFailure(
                "select extract(from to_timestamp('2022-03-11T22:00:30.555555Z'))",
                null,
                14,
                "unbalanced ("
        );
    }

    @Test
    public void testMultipleFrom() throws Exception {
        assertFailure(
                "select extract(hour from from to_timestamp('2022-03-11T22:00:30.555555Z'))",
                null,
                14,
                "unbalanced ("
        );
    }

    @Test
    public void testNonLiteralPart() throws Exception {
        assertFailure(
                "select extract(1+1 from to_timestamp('2022-03-11T22:00:30.555555Z'))",
                null,
                17,
                "did you mean 'hour'?"
        );
    }

    @Test
    public void testNotExtractFrom() throws Exception {
        assertFailure(
                "select something(null from to_timestamp('2022-03-11T22:00:30.555555Z'))",
                null,
                16,
                "unbalanced ("
        );
    }

    @Test
    public void testNullFrom() throws Exception {
        assertFailure(
                "select extract(null from to_timestamp('2022-03-11T22:00:30.555555Z'))",
                null,
                15,
                "did you mean 'hour'?"
        );
    }

    @Test
    public void testVanillaCentury() throws Exception {
        assertQuery(
                "extract\n" +
                        "21\n",
                "select extract(century from to_timestamp('2022-03-11T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testVanillaDay() throws Exception {
        assertQuery(
                "extract\n" +
                        "11\n",
                "select extract(day from to_timestamp('2022-03-11T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testVanillaDecade() throws Exception {
        assertQuery(
                "extract\n" +
                        "202\n",
                "select extract(decade from to_timestamp('2022-03-11T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testVanillaDoy() throws Exception {
        assertQuery(
                "extract\n" +
                        "112\n",
                "select extract(doy from to_timestamp('2022-04-22T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testVanillaEpoch() throws Exception {
        assertQuery(
                "extract\n" +
                        "1650664830\n",
                "select extract(epoch from to_timestamp('2022-04-22T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testStartOfYear() throws Exception {
        assertQuery(
                "extract\n" +
                        "1\n",
                "select extract(doy from to_timestamp('2022-01-01T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testBeforeEndOfYear() throws Exception {
        assertQuery(
                "extract\n" +
                        "364\n",
                "select extract(doy from to_timestamp('2022-12-30T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testEndOfYear() throws Exception {
        assertQuery(
                "extract\n" +
                        "365\n",
                "select extract(doy from to_timestamp('2022-12-31T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testEndOfLeapYear() throws Exception {
        assertQuery(
                "extract\n" +
                        "366\n",
                "select extract(doy from to_timestamp('2020-12-31T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testVanillaDowSaturday() throws Exception {
        assertQuery(
                "extract\n" +
                        "6\n",
                "select extract(dow from to_timestamp('2022-04-16T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testVanillaDowSunday() throws Exception {
        assertQuery(
                "extract\n" +
                        "0\n",
                "select extract(dow from to_timestamp('2022-04-10T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testVanillaHour() throws Exception {
        assertQuery(
                "extract\n" +
                        "22\n",
                "select extract(hour from to_timestamp('2022-03-11T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testZeroCentury() throws Exception {
        assertQuery(
                "extract\n" +
                        "-1\n",
                "select extract(century from to_timestamp('0000-01-01T22:00:30.555555Z'))",
                null,
                null,
                true,
                true,
                true
        );
    }
}
