/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.griffin.engine.functions.json;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.json.JsonExtractTypedFunctionFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests extracting JSON values as various SQL types.
 * <p>
 * The first column in the `scenarios` table represents the input JSON token.
 * The remaining columns represent the expected output when casting the JSON token to the corresponding SQL type.
 * <p>
 * The mechanics of the test are:
 * * setUp() creates a table with the document containing a JSON array of the first column of the `scenarios` table.
 * * Each test loops through each scenario and tests the extraction of the JSON token as a SQL type.
 * * It asserts by performing a SQL query.
 * * tearDown() drops the table.
 */
public class JsonExtractCastScenariosTest extends AbstractCairoTest {
    // N.B.: Compare these scenarios with those from `JsonParserTest`.
    private static final String[][] SCENARIOS = new String[][]{
            // json token, ::boolean, ::short, ::int, ::long, ::double, ::varchar, ::ipv4, ::date, ::timestamp, ::string
            {"null", "false", "0", "null", "null", "null", "", "", "", "", ""},
            {"true", "true", "1", "1", "1", "1.0", "true", "", "", "", "true"},
            {"false", "false", "0", "0", "0", "0.0", "false", "", "", "", "false"},
            {"1", "false", "1", "1", "1", "1.0", "1", "0.0.0.1", "1970-01-01T00:00:00.001Z", "1970-01-01T00:00:00.000001Z", "1"},
            {"0", "false", "0", "0", "0", "0.0", "0", "", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.000000Z", "0"},
            {"-1", "false", "-1", "-1", "-1", "-1.0", "-1", "255.255.255.255", "1969-12-31T23:59:59.999Z", "1969-12-31T23:59:59.999999Z", "-1"},
            {"\"true\"", "false", "0", "null", "null", "null", "true", "", "", "", "true"},
            {"\"false\"", "false", "0", "null", "null", "null", "false", "", "", "", "false"},
            {"\"null\"", "false", "0", "null", "null", "null", "null", "", "", "", "null"},
            {"\"1\"", "false", "1", "1", "1", "1.0", "1", "", "1970-01-01T00:00:00.001Z", "", "1"},
            {"\"0\"", "false", "0", "0", "0", "0.0", "0", "", "1970-01-01T00:00:00.000Z", "", "0"},
            {"\"-1\"", "false", "-1", "-1", "-1", "-1.0", "-1", "", "1969-12-31T23:59:59.999Z", "", "-1"},
            {"\"32767\"", "false", "32767", "32767", "32767", "32767.0", "32767", "", "1970-01-01T00:00:32.767Z", "", "32767"},
            {"\"32768\"", "false", "0", "32768", "32768", "32768.0", "32768", "", "1970-01-01T00:00:32.768Z", "", "32768"},
            {"\"2147483647\"", "false", "0", "2147483647", "2147483647", "2.147483647E9", "2147483647", "", "1970-01-25T20:31:23.647Z", "", "2147483647"},
            {"\"2147483648\"", "false", "0", "null", "2147483648", "2.147483648E9", "2147483648", "", "1970-01-25T20:31:23.648Z", "", "2147483648"},
            {"\"9223372036854775807\"", "false", "0", "null", "9223372036854775807", "9.223372036854776E18", "9223372036854775807", "", "292278994-08-17T07:12:55.807Z", "", "9223372036854775807"},
            {"\"9223372036854775808\"", "false", "0", "null", "null", "9.223372036854776E18", "9223372036854775808", "", "", "", "9223372036854775808"},
            {"\"\"", "false", "0", "null", "null", "null", "", "", "", "", ""},
            {"\" \"", "false", "0", "null", "null", "null", " ", "", "", "", " "},
            {"\"  \"", "false", "0", "null", "null", "null", "  ", "", "", "", "  "},
            {"\"  true\"", "false", "0", "null", "null", "null", "  true", "", "", "", "  true"},
            {"\"true  \"", "false", "0", "null", "null", "null", "true  ", "", "", "", "true  "},
            {"\"  true  \"", "false", "0", "null", "null", "null", "  true  ", "", "", "", "  true  "},
            {"\"  false\"", "false", "0", "null", "null", "null", "  false", "", "", "", "  false"},
            {"\"false  \"", "false", "0", "null", "null", "null", "false  ", "", "", "", "false  "},
            {"\"  false  \"", "false", "0", "null", "null", "null", "  false  ", "", "", "", "  false  "},
            {"\"  null\"", "false", "0", "null", "null", "null", "  null", "", "", "", "  null"},
            {"\"null  \"", "false", "0", "null", "null", "null", "null  ", "", "", "", "null  "},
            {"\"  null  \"", "false", "0", "null", "null", "null", "  null  ", "", "", "", "  null  "},
            {"\"  abc\"", "false", "0", "null", "null", "null", "  abc", "", "", "", "  abc"},
            {"\"abc  \"", "false", "0", "null", "null", "null", "abc  ", "", "", "", "abc  "},
            {"\"  abc  \"", "false", "0", "null", "null", "null", "  abc  ", "", "", "", "  abc  "},
            {"\"esc\\\"aping\"", "false", "0", "null", "null", "null", "esc\"aping", "", "", "", "esc\"aping"},
            {"\"1969-12-31T23:58:54.463Z\"", "false", "0", "null", "null", "null", "1969-12-31T23:58:54.463Z", "", "1969-12-31T23:58:54.463Z", "1969-12-31T23:58:54.463000Z", "1969-12-31T23:58:54.463Z"},
            {"\"1970-01-01T00:00:00.000Z\"", "false", "0", "null", "null", "null", "1970-01-01T00:00:00.000Z", "", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000Z"},
            {"0.0", "false", "0", "0", "0", "0.0", "0.0", "", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.000000Z", "0.0"},
            {"1.0", "false", "1", "1", "1", "1.0", "1.0", "", "1970-01-01T00:00:00.001Z", "1970-01-01T00:00:00.000001Z", "1.0"},
            {"1e1", "false", "10", "10", "10", "10.0", "1e1", "", "1970-01-01T00:00:00.010Z", "1970-01-01T00:00:00.000010Z", "1e1"},
            {"1e+1", "false", "10", "10", "10", "10.0", "1e+1", "", "1970-01-01T00:00:00.010Z", "1970-01-01T00:00:00.000010Z", "1e+1"},
            {"1e-1", "false", "0", "0", "0", "0.1", "1e-1", "", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.000000Z", "1e-1"},
            {"1e01", "false", "10", "10", "10", "10.0", "1e01", "", "1970-01-01T00:00:00.010Z", "1970-01-01T00:00:00.000010Z", "1e01"},
            {"1E1", "false", "10", "10", "10", "10.0", "1E1", "", "1970-01-01T00:00:00.010Z", "1970-01-01T00:00:00.000010Z", "1E1"},
            {"1E+1", "false", "10", "10", "10", "10.0", "1E+1", "", "1970-01-01T00:00:00.010Z", "1970-01-01T00:00:00.000010Z", "1E+1"},
            {"1E-1", "false", "0", "0", "0", "0.1", "1E-1", "", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.000000Z", "1E-1"},
            {"1E01", "false", "10", "10", "10", "10.0", "1E01", "", "1970-01-01T00:00:00.010Z", "1970-01-01T00:00:00.000010Z", "1E01"},
            {"1E+01", "false", "10", "10", "10", "10.0", "1E+01", "", "1970-01-01T00:00:00.010Z", "1970-01-01T00:00:00.000010Z", "1E+01"},
            {"0.25", "false", "0", "0", "0", "0.25", "0.25", "", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.000000Z", "0.25"},
            {"1.25", "false", "1", "1", "1", "1.25", "1.25", "", "1970-01-01T00:00:00.001Z", "1970-01-01T00:00:00.000001Z", "1.25"},
            {"1.25e2", "false", "125", "125", "125", "125.0", "1.25e2", "", "1970-01-01T00:00:00.125Z", "1970-01-01T00:00:00.000125Z", "1.25e2"},
            {"1.25e+2", "false", "125", "125", "125", "125.0", "1.25e+2", "", "1970-01-01T00:00:00.125Z", "1970-01-01T00:00:00.000125Z", "1.25e+2"},
            {"1.25e-2", "false", "0", "0", "0", "0.0125", "1.25e-2", "", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.000000Z", "1.25e-2"},
            {"1.25e02", "false", "125", "125", "125", "125.0", "1.25e02", "", "1970-01-01T00:00:00.125Z", "1970-01-01T00:00:00.000125Z", "1.25e02"},
            {"1.25e+02", "false", "125", "125", "125", "125.0", "1.25e+02", "", "1970-01-01T00:00:00.125Z", "1970-01-01T00:00:00.000125Z", "1.25e+02"},
            {"1.25e-02", "false", "0", "0", "0", "0.0125", "1.25e-02", "", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.000000Z", "1.25e-02"},
            {"1.25e+02", "false", "125", "125", "125", "125.0", "1.25e+02", "", "1970-01-01T00:00:00.125Z", "1970-01-01T00:00:00.000125Z", "1.25e+02"},
            {"2.0", "false", "2", "2", "2", "2.0", "2.0", "", "1970-01-01T00:00:00.002Z", "1970-01-01T00:00:00.000002Z", "2.0"},
            {"2.5", "false", "2", "2", "2", "2.5", "2.5", "", "1970-01-01T00:00:00.002Z", "1970-01-01T00:00:00.000002Z", "2.5"},
            {"2.75", "false", "2", "2", "2", "2.75", "2.75", "", "1970-01-01T00:00:00.002Z", "1970-01-01T00:00:00.000002Z", "2.75"},
            {"-2.0", "false", "-2", "-2", "-2", "-2.0", "-2.0", "", "1969-12-31T23:59:59.998Z", "1969-12-31T23:59:59.999998Z", "-2.0"},
            {"-2.5", "false", "-2", "-2", "-2", "-2.5", "-2.5", "", "1969-12-31T23:59:59.998Z", "1969-12-31T23:59:59.999998Z", "-2.5"},
            {"-2.75", "false", "-2", "-2", "-2", "-2.75", "-2.75", "", "1969-12-31T23:59:59.998Z", "1969-12-31T23:59:59.999998Z", "-2.75"},
            {"-1.0", "false", "-1", "-1", "-1", "-1.0", "-1.0", "", "1969-12-31T23:59:59.999Z", "1969-12-31T23:59:59.999999Z", "-1.0"},
            {"-0.25", "false", "0", "0", "0", "-0.25", "-0.25", "", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.000000Z", "-0.25"},
            {"-1.25", "false", "-1", "-1", "-1", "-1.25", "-1.25", "", "1969-12-31T23:59:59.999Z", "1969-12-31T23:59:59.999999Z", "-1.25"},
            {"-1.25e2", "false", "-125", "-125", "-125", "-125.0", "-1.25e2", "", "1969-12-31T23:59:59.875Z", "1969-12-31T23:59:59.999875Z", "-1.25e2"},
            {"-1.25e+2", "false", "-125", "-125", "-125", "-125.0", "-1.25e+2", "", "1969-12-31T23:59:59.875Z", "1969-12-31T23:59:59.999875Z", "-1.25e+2"},
            {"-1.25e-2", "false", "0", "0", "0", "-0.0125", "-1.25e-2", "", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.000000Z", "-1.25e-2"},
            {"-1.25e02", "false", "-125", "-125", "-125", "-125.0", "-1.25e02", "", "1969-12-31T23:59:59.875Z", "1969-12-31T23:59:59.999875Z", "-1.25e02"},
            {"-1.25e+02", "false", "-125", "-125", "-125", "-125.0", "-1.25e+02", "", "1969-12-31T23:59:59.875Z", "1969-12-31T23:59:59.999875Z", "-1.25e+02"},
            {"-1.25e-02", "false", "0", "0", "0", "-0.0125", "-1.25e-02", "", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.000000Z", "-1.25e-02"},
            {"-1.25e+02", "false", "-125", "-125", "-125", "-125.0", "-1.25e+02", "", "1969-12-31T23:59:59.875Z", "1969-12-31T23:59:59.999875Z", "-1.25e+02"},
            {"1e308", "false", "0", "null", "null", "1.0E308", "1e308", "", "", "", "1e308"},
            {"1E308", "false", "0", "null", "null", "1.0E308", "1E308", "", "", "", "1E308"},
            {"127", "false", "127", "127", "127", "127.0", "127", "0.0.0.127", "1970-01-01T00:00:00.127Z", "1970-01-01T00:00:00.000127Z", "127"},
            {"128", "false", "128", "128", "128", "128.0", "128", "0.0.0.128", "1970-01-01T00:00:00.128Z", "1970-01-01T00:00:00.000128Z", "128"},
            {"-128", "false", "-128", "-128", "-128", "-128.0", "-128", "255.255.255.128", "1969-12-31T23:59:59.872Z", "1969-12-31T23:59:59.999872Z", "-128"},
            {"-129", "false", "-129", "-129", "-129", "-129.0", "-129", "255.255.255.127", "1969-12-31T23:59:59.871Z", "1969-12-31T23:59:59.999871Z", "-129"},
            {"255", "false", "255", "255", "255", "255.0", "255", "0.0.0.255", "1970-01-01T00:00:00.255Z", "1970-01-01T00:00:00.000255Z", "255"},
            {"256", "false", "256", "256", "256", "256.0", "256", "0.0.1.0", "1970-01-01T00:00:00.256Z", "1970-01-01T00:00:00.000256Z", "256"},
            {"-256", "false", "-256", "-256", "-256", "-256.0", "-256", "255.255.255.0", "1969-12-31T23:59:59.744Z", "1969-12-31T23:59:59.999744Z", "-256"},
            {"-257", "false", "-257", "-257", "-257", "-257.0", "-257", "255.255.254.255", "1969-12-31T23:59:59.743Z", "1969-12-31T23:59:59.999743Z", "-257"},
            {"32767", "false", "32767", "32767", "32767", "32767.0", "32767", "0.0.127.255", "1970-01-01T00:00:32.767Z", "1970-01-01T00:00:00.032767Z", "32767"},
            {"32768", "false", "0", "32768", "32768", "32768.0", "32768", "0.0.128.0", "1970-01-01T00:00:32.768Z", "1970-01-01T00:00:00.032768Z", "32768"},
            {"-32768", "false", "-32768", "-32768", "-32768", "-32768.0", "-32768", "255.255.128.0", "1969-12-31T23:59:27.232Z", "1969-12-31T23:59:59.967232Z", "-32768"},
            {"-32769", "false", "0", "-32769", "-32769", "-32769.0", "-32769", "255.255.127.255", "1969-12-31T23:59:27.231Z", "1969-12-31T23:59:59.967231Z", "-32769"},
            {"65535", "false", "0", "65535", "65535", "65535.0", "65535", "0.0.255.255", "1970-01-01T00:01:05.535Z", "1970-01-01T00:00:00.065535Z", "65535"},
            {"65536", "false", "0", "65536", "65536", "65536.0", "65536", "0.1.0.0", "1970-01-01T00:01:05.536Z", "1970-01-01T00:00:00.065536Z", "65536"},
            {"-65536", "false", "0", "-65536", "-65536", "-65536.0", "-65536", "255.255.0.0", "1969-12-31T23:58:54.464Z", "1969-12-31T23:59:59.934464Z", "-65536"},
            {"-65537", "false", "0", "-65537", "-65537", "-65537.0", "-65537", "255.254.255.255", "1969-12-31T23:58:54.463Z", "1969-12-31T23:59:59.934463Z", "-65537"},
            {"2147483647", "false", "0", "2147483647", "2147483647", "2.147483647E9", "2147483647", "127.255.255.255", "1970-01-25T20:31:23.647Z", "1970-01-01T00:35:47.483647Z", "2147483647"},
            {"2147483648", "false", "0", "null", "2147483648", "2.147483648E9", "2147483648", "", "1970-01-25T20:31:23.648Z", "1970-01-01T00:35:47.483648Z", "2147483648"},
            {"-2147483648", "false", "0", "null", "-2147483648", "-2.147483648E9", "-2147483648", "128.0.0.0", "1969-12-07T03:28:36.352Z", "1969-12-31T23:24:12.516352Z", "-2147483648"},
            {"-2147483649", "false", "0", "null", "-2147483649", "-2.147483649E9", "-2147483649", "", "1969-12-07T03:28:36.351Z", "1969-12-31T23:24:12.516351Z", "-2147483649"},
            {"4294967295", "false", "0", "null", "4294967295", "4.294967295E9", "4294967295", "", "1970-02-19T17:02:47.295Z", "1970-01-01T01:11:34.967295Z", "4294967295"},
            {"4294967296", "false", "0", "null", "4294967296", "4.294967296E9", "4294967296", "", "1970-02-19T17:02:47.296Z", "1970-01-01T01:11:34.967296Z", "4294967296"},
            {"-4294967296", "false", "0", "null", "-4294967296", "-4.294967296E9", "-4294967296", "", "1969-11-12T06:57:12.704Z", "1969-12-31T22:48:25.032704Z", "-4294967296"},
            {"-4294967297", "false", "0", "null", "-4294967297", "-4.294967297E9", "-4294967297", "", "1969-11-12T06:57:12.703Z", "1969-12-31T22:48:25.032703Z", "-4294967297"},
            {"1000000000000", "false", "0", "null", "1000000000000", "1.0E12", "1000000000000", "", "2001-09-09T01:46:40.000Z", "1970-01-12T13:46:40.000000Z", "1000000000000"},
            {"1000000000000000", "false", "0", "null", "1000000000000000", "1.0E15", "1000000000000000", "", "33658-09-27T01:46:40.000Z", "2001-09-09T01:46:40.000000Z", "1000000000000000"},
            {"9223372036854775807", "false", "0", "null", "9223372036854775807", "9.223372036854776E18", "9223372036854775807", "", "292278994-08-17T07:12:55.807Z", "294247-01-10T04:00:54.775807Z", "9223372036854775807"},
            {"9223372036854775808", "false", "0", "null", "null", "9.223372036854776E18", "9223372036854775808", "", "", "", "9223372036854775808"},
            {"-9223372036854775808", "false", "0", "null", "null", "-9.223372036854776E18", "-9223372036854775808", "", "", "", "-9223372036854775808"},
            {"-9223372036854775809", "false", "0", "null", "null", "-9.223372036854776E18", "-9223372036854775809", "", "", "", "-9223372036854775809"},
            {"10000000000000000000000000000000000000000", "false", "0", "null", "null", "1.0E40", "10000000000000000000000000000000000000000", "", "", "", "10000000000000000000000000000000000000000"},
            {"[]", "false", "0", "null", "null", "null", "[]", "", "", "", "[]"},
            {"[true]", "false", "0", "null", "null", "null", "[true]", "", "", "", "[true]"},
            {"[false]", "false", "0", "null", "null", "null", "[false]", "", "", "", "[false]"},
            {"[null]", "false", "0", "null", "null", "null", "[null]", "", "", "", "[null]"},
            {"[1]", "false", "0", "null", "null", "null", "[1]", "", "", "", "[1]"},
            {"[0]", "false", "0", "null", "null", "null", "[0]", "", "", "", "[0]"},
            {"[\"true\"]", "false", "0", "null", "null", "null", "[\"true\"]", "", "", "", "[\"true\"]"},
            {"[\"false\"]", "false", "0", "null", "null", "null", "[\"false\"]", "", "", "", "[\"false\"]"},
            {"[1, 2]", "false", "0", "null", "null", "null", "[1, 2]", "", "", "", "[1, 2]"}
    };

    @Test
    public void testBoolean() throws Exception {
        testScenarios(ColumnType.BOOLEAN);
    }

    @Test
    public void testDate() throws Exception {
        testScenarios(ColumnType.DATE);
    }

    @Test
    public void testDouble() throws Exception {
        testScenarios(ColumnType.DOUBLE);
    }

    @Test
    public void testIPv4() throws Exception {
        testScenarios(ColumnType.IPv4);
    }

    @Test
    public void testInt() throws Exception {
        testScenarios(ColumnType.INT);
    }

    @Test
    public void testLong() throws Exception {
        testScenarios(ColumnType.LONG);
    }

    public void testScenario(int type, int index) throws Exception {
        final int scenarioColumn = selectScenarioColumn(type);
        final String json = SCENARIOS[index][0];
        final String expectedValue = SCENARIOS[index][scenarioColumn];
        final String expected = "x\n" + expectedValue + ":" + ColumnType.nameOf(type) + "\n";

        if (JsonExtractTypedFunctionFactory.isIntrusivelyOptimized(type)) {
            testScenarioVia3rdArgCall(json, type, index, expected, expectedValue);
        }

        testScenarioViaFunctionCast(json, type, index, expected, expectedValue);

        testScalarScenarioViaFunctionCast(json, type, index, expected, expectedValue);

        testScenarioViaSuffixCast(json, type, index, expected, expectedValue);
    }

    @Test
    public void testShort() throws Exception {
        testScenarios(ColumnType.SHORT);
    }

    @Test
    public void testString() throws Exception {
        testScenarios(ColumnType.STRING);
    }

    @Test
    public void testTimestamp() throws Exception {
        testScenarios(ColumnType.TIMESTAMP);
    }

    @Test
    public void testVarchar() throws Exception {
        testScenarios(ColumnType.VARCHAR);
    }

    private static int selectScenarioColumn(int type) {
        switch (type) {
            case ColumnType.BOOLEAN:
                return 1;
            case ColumnType.SHORT:
                return 2;
            case ColumnType.INT:
                return 3;
            case ColumnType.LONG:
                return 4;
            case ColumnType.DOUBLE:
                return 5;
            case ColumnType.VARCHAR:
                return 6;
            case ColumnType.IPv4:
                return 7;
            case ColumnType.DATE:
                return 8;
            case ColumnType.TIMESTAMP:
                return 9;
            case ColumnType.STRING:
                return 10;
            default:
                throw new RuntimeException("No scenario tests for type " + ColumnType.nameOf(type));
        }
    }

    private void testScalarScenarioViaFunctionCast(
            String json,
            int type,
            int index,
            String expected,
            String expectedValue
    ) throws SqlException {
        final String sql = "select cast(json_extract('" + json + "', '') as " + ColumnType.nameOf(type) +
                ") as x from long_sequence(1)";
        try {
            assertSqlWithTypes(expected, sql);
        } catch (AssertionError e) {
            throw new AssertionError(
                    "Failed cast(.. as ..) call [SCALAR]. Scenario: " + index +
                            ", SQL: `" + sql + "`" +
                            ", Cast Type: " + ColumnType.nameOf(type) +
                            ", JSON: " + SCENARIOS[index][0] +
                            ", Expected Value: " + expectedValue +
                            ", Error: " + e.getMessage(), e);
        } catch (CairoException e) {
            throw new RuntimeException(
                    "Failed cast(.. as ..) call. Scenario: " + index +
                            ", SQL: `" + sql + "`" +
                            ", Cast Type: " + ColumnType.nameOf(type) +
                            ", JSON: " + SCENARIOS[index][0] +
                            ", Expected Value: " + expectedValue +
                            ", Error: " + e.getMessage(), e);
        }
    }

    private void testScenarioVia3rdArgCall(
            String json,
            int type,
            int index,
            String expected,
            String expectedValue
    ) throws SqlException {
        final String sql = "select json_extract('{\"x\":" + json + "}', '.x', " + type + ") as x from long_sequence(1)";
        try {
            assertSqlWithTypes(expected, sql);
        } catch (AssertionError e) {
            throw new AssertionError(
                    "Failed JSON 3rd type arg call. Scenario: " + index +
                            ", SQL: `" + sql + "`" +
                            ", Cast Type: " + ColumnType.nameOf(type) +
                            ", JSON: " + SCENARIOS[index][0] +
                            ", Expected Value: " + expectedValue +
                            ", Error: " + e.getMessage(), e);
        } catch (CairoException e) {
            throw new RuntimeException(
                    "Failed JSON 3rd type arg call. Scenario: " + index +
                            ", SQL: `" + sql + "`" +
                            ", Cast Type: " + ColumnType.nameOf(type) +
                            ", JSON: " + SCENARIOS[index][0] +
                            ", Expected Value: " + expectedValue +
                            ", Error: " + e.getMessage(), e);
        }
    }

    private void testScenarioViaFunctionCast(
            String json,
            int type,
            int index,
            String expected,
            String expectedValue
    ) throws SqlException {
        final String sql = "select cast(json_extract('{\"x\":" + json + "}', '.x') as " + ColumnType.nameOf(type) +
                ") as x from long_sequence(1)";
        try {
            assertSqlWithTypes(expected, sql);
        } catch (AssertionError e) {
            throw new AssertionError(
                    "Failed cast(.. as ..) call. Scenario: " + index +
                            ", SQL: `" + sql + "`" +
                            ", Cast Type: " + ColumnType.nameOf(type) +
                            ", JSON: " + SCENARIOS[index][0] +
                            ", Expected Value: " + expectedValue +
                            ", Error: " + e.getMessage(), e);
        } catch (CairoException e) {
            throw new RuntimeException(
                    "Failed cast(.. as ..) call. Scenario: " + index +
                            ", SQL: `" + sql + "`" +
                            ", Cast Type: " + ColumnType.nameOf(type) +
                            ", JSON: " + SCENARIOS[index][0] +
                            ", Expected Value: " + expectedValue +
                            ", Error: " + e.getMessage(), e);
        }
    }

    private void testScenarioViaSuffixCast(
            String json,
            int type,
            int index,
            String expected,
            String expectedValue
    ) throws SqlException {
        final String sql = "select json_extract('{\"x\":" + json + "}', '.x')::" + ColumnType.nameOf(type) +
                " as x from long_sequence(1)";
        try {
            assertSqlWithTypes(expected, sql);
        } catch (AssertionError e) {
            throw new AssertionError(
                    "Failed suffix ::cast call. Scenario: " + index +
                            ", SQL: `" + sql + "`" +
                            ", Cast Type: " + ColumnType.nameOf(type) +
                            ", JSON: " + SCENARIOS[index][0] +
                            ", Expected Value: " + expectedValue +
                            ", Error: " + e.getMessage(), e);
        } catch (CairoException e) {
            throw new RuntimeException(
                    "Failed suffix ::cast call. Scenario: " + index +
                            ", SQL: `" + sql + "`" +
                            ", Cast Type: " + ColumnType.nameOf(type) +
                            ", JSON: " + SCENARIOS[index][0] +
                            ", Expected Value: " + expectedValue +
                            ", Error: " + e.getMessage(), e);
        }
    }

    private void testScenarios(int type) throws Exception {
        assertMemoryLeak(() -> {
            for (int index = 0; index < SCENARIOS.length; index++) {
                testScenario(type, index);
            }
        });
    }
}
