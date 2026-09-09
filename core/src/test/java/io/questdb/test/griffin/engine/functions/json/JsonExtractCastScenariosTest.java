/*+*****************************************************************************
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
    public void testBooleanWidthConsistencyAcrossWidenedReads() throws Exception {
        // A BOOLEAN json_extract expression must carry ONE value, as BooleanFunction does: every
        // other width renders the declared boolean, numerically as 1/0 and textually as true/false.
        // The base class derived each width from its own native parse, so ::boolean printed false for
        // {"a":1} while ::boolean::long re-parsed the JSON and answered 1 - one expression, two
        // values. The textual widths were worse than wrong: BOOLEAN allocates no destUtf8Sink, so
        // ::boolean::varchar tripped an assert inside the extraction path, and ::boolean::byte hit
        // the base's UnsupportedOperationException.
        // Each json column is paired with a BOOLEAN column holding the same value - the control is
        // what the declared type is required to mean.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE j (id INT, b BOOLEAN, text VARCHAR)");
            execute("""
                    INSERT INTO j VALUES
                      (1, true, '{"a":true}'),
                      (2, false, '{"a":false}'),
                      (3, false, '{"a":1}'),
                      (4, false, '{"a":"true"}'),
                      (5, false, '{"a":null}'),
                      (6, false, '{"a":"abc"}'),
                      (7, false, null)""");

            // Rows 3-7 are the discriminators: queryPointerBoolean answers false for a JSON number,
            // for a quoted "true", for JSON null and for a non-boolean string, and getBool falls back
            // to false for a NULL document, so the declared value is false and every widened read of
            // it has to be 0 / false, whatever the payload holds. BOOLEAN has no null sentinel, which
            // is why row 7 reads 0 rather than NULL - the same answer the control column gives.
            assertQuery("""
                    SELECT id,
                      json_extract(text,'$.a')::boolean v,
                      b v_control,
                      json_extract(text,'$.a')::boolean::long l,
                      b::long l_control,
                      json_extract(text,'$.a')::boolean::int i,
                      b::int i_control,
                      json_extract(text,'$.a')::boolean::short s,
                      b::short s_control,
                      json_extract(text,'$.a')::boolean::byte y,
                      b::byte y_control
                    FROM j ORDER BY id""")
                    .expectSize()
                    .returns("""
                            id\tv\tv_control\tl\tl_control\ti\ti_control\ts\ts_control\ty\ty_control
                            1\ttrue\ttrue\t1\t1\t1\t1\t1\t1\t1\t1
                            2\tfalse\tfalse\t0\t0\t0\t0\t0\t0\t0\t0
                            3\tfalse\tfalse\t0\t0\t0\t0\t0\t0\t0\t0
                            4\tfalse\tfalse\t0\t0\t0\t0\t0\t0\t0\t0
                            5\tfalse\tfalse\t0\t0\t0\t0\t0\t0\t0\t0
                            6\tfalse\tfalse\t0\t0\t0\t0\t0\t0\t0\t0
                            7\tfalse\tfalse\t0\t0\t0\t0\t0\t0\t0\t0
                            """);

            // The floating-point and temporal widths render the same 1/0. ::real reaches getFloat and
            // ::double getDouble; DATE reads the value as milliseconds and TIMESTAMP as microseconds,
            // exactly as the BOOLEAN control column does.
            assertQuery("""
                    SELECT id,
                      json_extract(text,'$.a')::boolean::double dd,
                      b::double dd_control,
                      json_extract(text,'$.a')::boolean::real rr,
                      b::real rr_control,
                      json_extract(text,'$.a')::boolean::date dt,
                      b::date dt_control,
                      json_extract(text,'$.a')::boolean::timestamp ts,
                      b::timestamp ts_control
                    FROM j ORDER BY id""")
                    .expectSize()
                    .returns("""
                            id\tdd\tdd_control\trr\trr_control\tdt\tdt_control\tts\tts_control
                            1\t1.0\t1.0\t1.0\t1.0\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.001Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000001Z
                            2\t0.0\t0.0\t0.0\t0.0\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z
                            3\t0.0\t0.0\t0.0\t0.0\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z
                            4\t0.0\t0.0\t0.0\t0.0\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z
                            5\t0.0\t0.0\t0.0\t0.0\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z
                            6\t0.0\t0.0\t0.0\t0.0\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z
                            7\t0.0\t0.0\t0.0\t0.0\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z
                            """);

            // The textual widths render the declared boolean, not the JSON token underneath it. Row 3
            // is the discriminator - the base returned the raw token 1 where the declared value is
            // false - and row 6 pins that a non-boolean string renders as false rather than as itself.
            assertQuery("""
                    SELECT id,
                      json_extract(text,'$.a')::boolean::varchar vc,
                      b::varchar vc_control,
                      json_extract(text,'$.a')::boolean::string st,
                      b::string st_control,
                      json_extract(text,'$.a')::boolean::symbol sy,
                      b::symbol sy_control,
                      json_extract(text,'$.a')::boolean::char ch,
                      b::char ch_control
                    FROM j ORDER BY id""")
                    .expectSize()
                    .returns("""
                            id\tvc\tvc_control\tst\tst_control\tsy\tsy_control\tch\tch_control
                            1\ttrue\ttrue\ttrue\ttrue\ttrue\ttrue\tT\tT
                            2\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\tF\tF
                            3\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\tF\tF
                            4\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\tF\tF
                            5\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\tF\tF
                            6\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\tF\tF
                            7\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\tF\tF
                            """);

            // The predicate path reads the widened getter too, so a filter on the 64-bit read must
            // select exactly the rows the projection shows as true. Pre-fix row 3 also matched,
            // because its independent parse of {"a":1} answered 1 at long width.
            assertQuery("SELECT count() c FROM j WHERE json_extract(text,'$.a')::boolean::long > 0")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n1\n");
        });
    }

    @Test
    public void testDate() throws Exception {
        testScenarios(ColumnType.DATE);
    }

    @Test
    public void testDateWidthConsistencyAcrossWidenedReads() throws Exception {
        // A DATE json_extract expression must carry ONE value, as DateFunction does: getTimestamp
        // converts the declared millisecond value to microseconds, getLong hands it back unchanged
        // and getDouble widens it. The base class derived each width from its own native parse, so
        // one DATE expression carried three values. A DATE taken from a JSON number read its
        // milliseconds AS microseconds through getTimestamp, so hour() saw 43.2 seconds where the
        // projection showed noon; a DATE taken from a JSON string read NULL through getLong and
        // getDouble, because those retry the JSON numeric path the string never satisfied.
        // Each json column is paired with a DATE column holding the same value - the control is what
        // the declared type is required to mean.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE j (id INT, d DATE, text VARCHAR)");
            execute("""
                    INSERT INTO j VALUES
                      (1, '1970-01-01T12:00:00.000Z'::date, '{"a":43200000}'),
                      (2, '1970-01-01T00:00:00.005Z'::date, '{"a":"1970-01-01T00:00:00.005Z"}'),
                      (3, '1970-01-01T00:00:00.002Z'::date, '{"a":2.5}'),
                      (4, null, '{"a":"abc"}')""");

            // h reads getTimestamp through hour(N) - DATE promotes to TIMESTAMP with no cast function
            // in between. l reads getLong through *(LL), which wins because * has no DATE or TIMESTAMP
            // overload. dd reads getDouble through cast(Md).
            assertQuery("""
                    SELECT id,
                      json_extract(text,'$.a')::date v,
                      hour(json_extract(text,'$.a')::date) h,
                      hour(d) h_control,
                      json_extract(text,'$.a')::date * 1 l,
                      d * 1 l_control,
                      json_extract(text,'$.a')::date::double dd,
                      d::double dd_control
                    FROM j ORDER BY id""")
                    .expectSize()
                    .returns("""
                            id\tv\th\th_control\tl\tl_control\tdd\tdd_control
                            1\t1970-01-01T12:00:00.000Z\t12\t12\t43200000\t43200000\t4.32E7\t4.32E7
                            2\t1970-01-01T00:00:00.005Z\t0\t0\t5\t5\t5.0\t5.0
                            3\t1970-01-01T00:00:00.002Z\t0\t0\t2\t2\t2.0\t2.0
                            4\t\tnull\tnull\tnull\tnull\tnull\tnull
                            """);

            // An explicit cast resolves on the argument's own tag, not by overload distance, so it
            // reaches getFloat even though FLOAT is absent from DATE's overload set. ::real is the
            // spelling that gets there - SqlParser.rewritePgCast maps ::float to DOUBLE.
            assertQuery("""
                    SELECT id,
                      json_extract(text,'$.a')::date::real rr,
                      d::real rr_control
                    FROM j ORDER BY id""")
                    .expectSize()
                    .returns("""
                            id\trr\trr_control
                            1\t4.32E7\t4.32E7
                            2\t5.0\t5.0
                            3\t2.0\t2.0
                            4\tnull\tnull
                            """);
        });
    }

    @Test
    public void testDouble() throws Exception {
        testScenarios(ColumnType.DOUBLE);
    }

    @Test
    public void testFloatWidthConsistencyAcrossWidenedReads() throws Exception {
        // A FLOAT json_extract expression must carry ONE value: getDouble widens getFloat, exactly as
        // FloatFunction does. The base class ran an independent queryPointerDouble for getDouble and
        // merely narrowed it for getFloat, so a payload with no exact float read 0.1 through getDouble
        // and 0.10000000149011612 through getFloat. One expression, two values.
        // N.B. ::float is DOUBLE here - SqlParser.rewritePgCast maps the cast keyword `float` to
        // DOUBLE for Postgres driver compatibility - so ::real is the spelling that reaches FLOAT.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE j (id INT, f FLOAT, text VARCHAR)");
            execute("""
                    INSERT INTO j VALUES
                      (1, 0.1, '{"a":0.1}'),
                      (2, 2.5, '{"a":2.5}'),
                      (3, null, '{"a":"abc"}')""");

            // r is the declared width; d reads getDouble; c reads getFloat through an explicit cast;
            // control is a FLOAT column holding the same value. All three widened reads must agree.
            assertQuery("""
                    SELECT id,
                      json_extract(text,'$.a')::real r,
                      json_extract(text,'$.a')::real + 0.0 d,
                      (json_extract(text,'$.a')::real)::double c,
                      f + 0.0 control
                    FROM j ORDER BY id""")
                    .expectSize()
                    .returns("""
                            id\tr\td\tc\tcontrol
                            1\t0.1\t0.10000000149011612\t0.10000000149011612\t0.10000000149011612
                            2\t2.5\t2.5\t2.5\t2.5
                            3\tnull\tnull\tnull\tnull
                            """);

            // The predicate path reads getDouble, so the extracted FLOAT must answer = 0.1 the same
            // way a FLOAT column holding 0.1 does. Pre-fix the extracted one matched (its getDouble
            // was exactly 0.1) while the column did not.
            assertQuery("SELECT count() c FROM j WHERE json_extract(text,'$.a')::real = 0.1")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n0\n");
            assertQuery("SELECT count() c FROM j WHERE f = 0.1")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n0\n");
        });
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
    public void testIntWidthConsistencyAcrossWidenedReads() throws Exception {
        // An INT (or SHORT) json_extract expression must carry ONE value: every widened read
        // sign-extends the declared-width getter, exactly as IntFunction/ShortFunction do. The base
        // class derived each width from its own native parse, so json_extract(t,'$.a')::int of an
        // out-of-INT payload read NULL as an INT (getInt raises NUMBER_OUT_OF_RANGE) but the full
        // number as a DOUBLE/FLOAT (independent queryPointerDouble). One expression, two values.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE j (id INT, f FLOAT, text VARCHAR)");
            execute("INSERT INTO j VALUES " +
                    "(1, 0.0, '{\"a\":42}')," +           // in range everywhere
                    "(2, 0.0, '{\"a\":32768}')," +        // out of SHORT, in INT
                    "(3, 0.0, '{\"a\":2147483648}')");    // out of INT (and SHORT)

            // INT: getInt is the source; getLong (already correct) and getDouble/getFloat (the fix)
            // must all agree. Row 3 is NULL at every width; pre-fix the + 0.0 read printed 2.147...E9.
            assertQuery("SELECT id, " +
                    "json_extract(text,'$.a')::int i, " +
                    "json_extract(text,'$.a')::int + 0L l, " +
                    "json_extract(text,'$.a')::int + 0.0 d, " +
                    "f + json_extract(text,'$.a')::int ff " +
                    "FROM j ORDER BY id")
                    .expectSize()
                    .returns("id\ti\tl\td\tff\n" +
                            "1\t42\t42\t42.0\t42.0\n" +
                            "2\t32768\t32768\t32768.0\t32768.0\n" +
                            "3\tnull\tnull\tnull\tnull\n");

            // SHORT: getShort is the source (out-of-SHORT reads 0, its only representation). The
            // widened reads must derive from it, not re-parse; pre-fix + 0L/+ 0.0 printed the full
            // number while ::short printed 0.
            assertQuery("SELECT id, " +
                    "json_extract(text,'$.a')::short s, " +
                    "json_extract(text,'$.a')::short + 0L sl, " +
                    "json_extract(text,'$.a')::short + 0.0 sd " +
                    "FROM j ORDER BY id")
                    .expectSize()
                    .returns("id\ts\tsl\tsd\n" +
                            "1\t42\t42\t42.0\n" +
                            "2\t0\t0\t0.0\n" +
                            "3\t0\t0\t0.0\n");

            // The predicate path reads getDouble too: > 1.5 must exclude the out-of-INT row (NULL),
            // matching what SELECT of ::int shows. Pre-fix it counted row 3 (2.147e9 > 1.5).
            assertQuery("SELECT count() c FROM j WHERE json_extract(text,'$.a')::int > 1.5")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n2\n");
        });
    }

    @Test
    public void testLong() throws Exception {
        testScenarios(ColumnType.LONG);
    }

    @Test
    public void testLongWidthConsistencyAcrossWidenedReads() throws Exception {
        // A LONG json_extract expression must carry ONE value, as LongFunction does: getDouble widens
        // getLong. The base class derived each width from its own native parse - getLong truncates a
        // fractional token via queryPointerLong while getDouble re-parses it via queryPointerDouble.
        // getDouble is the only widened read reachable from SQL for a LONG target; see
        // JsonExtractLongFunction's javadoc for why getFloat/getDate/getTimestamp are not.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE j (id INT, l LONG, text VARCHAR)");
            execute("""
                    INSERT INTO j VALUES
                      (1, 2, '{"a":2.5}'),
                      (2, 1, '{"a":1.5}'),
                      (3, 1, '{"a":"1"}'),
                      (4, null, '{"a":"1970-01-01T00:00:00.000002Z"}')""");

            // v is the declared width, d reads getDouble, control is a LONG column holding the same
            // value. Pre-fix the fractional rows printed 2 against 2.5 and 1 against 1.5.
            assertQuery("""
                    SELECT id,
                      json_extract(text,'$.a')::long v,
                      json_extract(text,'$.a')::long + 0.0 d,
                      l + 0.0 control
                    FROM j ORDER BY id""")
                    .expectSize()
                    .returns("""
                            id\tv\td\tcontrol
                            1\t2\t2.0\t2.0
                            2\t1\t1.0\t1.0
                            3\t1\t1.0\t1.0
                            4\tnull\tnull\tnull
                            """);

            // The predicate path reads getDouble, so a fractional token must compare as the truncated
            // long it displays. Row 2 shows 1 and must fail > 1.2; pre-fix its getDouble was 1.5.
            assertQuery("SELECT count() c FROM j WHERE json_extract(text,'$.a')::long > 1.2")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n1\n");
        });
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
    public void testTimestampWidthConsistencyAcrossWidenedReads() throws Exception {
        // A TIMESTAMP json_extract expression must carry ONE value, as TimestampFunction does:
        // getDate divides by the driver's millisecond factor, getLong hands the value back unchanged
        // and getDouble widens it. The base class derived each width from its own native parse, so a
        // TIMESTAMP taken from a JSON number read its microseconds AS milliseconds through getDate,
        // and one taken from a JSON string read NULL through getLong and getDouble because those
        // retry the JSON numeric path. Each json column is paired with a TIMESTAMP column holding the
        // same value - the control is what the declared type is required to mean.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE j (id INT, ts TIMESTAMP, tsn TIMESTAMP_NS, text VARCHAR)");
            execute("""
                    INSERT INTO j VALUES
                      (1, '1970-01-01T00:00:00.005000Z'::timestamp, '1970-01-01T00:00:00.000005000Z'::timestamp_ns, '{"a":5000}'),
                      (2, '1970-01-01T00:00:05.000000Z'::timestamp, '1970-01-01T00:00:00.005000000Z'::timestamp_ns, '{"a":5000000}'),
                      (3, '1970-01-01T00:00:00.000005Z'::timestamp, '1970-01-01T00:00:00.000005000Z'::timestamp_ns, '{"a":"1970-01-01T00:00:00.000005Z"}'),
                      (4, null, null, '{"a":"abc"}')""");

            // dt reads getDate through cast(Nm), l reads getLong through *(LL) - which wins because *
            // has no TIMESTAMP overload - and dd reads getDouble through cast(Nd).
            assertQuery("""
                    SELECT id,
                      json_extract(text,'$.a')::timestamp v,
                      json_extract(text,'$.a')::timestamp::date dt,
                      ts::date dt_control,
                      json_extract(text,'$.a')::timestamp * 1 l,
                      ts * 1 l_control,
                      json_extract(text,'$.a')::timestamp::double dd,
                      ts::double dd_control
                    FROM j ORDER BY id""")
                    .expectSize()
                    .returns("""
                            id\tv\tdt\tdt_control\tl\tl_control\tdd\tdd_control
                            1\t1970-01-01T00:00:00.005000Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.005Z\t5000\t5000\t5000.0\t5000.0
                            2\t1970-01-01T00:00:05.000000Z\t1970-01-01T00:00:05.000Z\t1970-01-01T00:00:05.000Z\t5000000\t5000000\t5000000.0\t5000000.0
                            3\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\t5\t5\t5.0\t5.0
                            4\t\t\t\tnull\tnull\tnull\tnull
                            """);

            // The nanosecond variant divides by a different factor to reach DATE, so it pins that the
            // promoted read scales the declared type's own value rather than assuming a fixed unit.
            // Row 2 is the discriminator: the same JSON number 5000000 is 5 seconds as a microsecond
            // TIMESTAMP and 5 milliseconds as a nanosecond one, so its DATE differs by 1000x.
            assertQuery("""
                    SELECT id,
                      json_extract(text,'$.a')::timestamp_ns v,
                      json_extract(text,'$.a')::timestamp_ns::date dt,
                      tsn::date dt_control,
                      json_extract(text,'$.a')::timestamp_ns::double dd,
                      tsn::double dd_control
                    FROM j ORDER BY id""")
                    .expectSize()
                    .returns("""
                            id\tv\tdt\tdt_control\tdd\tdd_control
                            1\t1970-01-01T00:00:00.000005000Z\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\t5000.0\t5000.0
                            2\t1970-01-01T00:00:00.005000000Z\t1970-01-01T00:00:00.005Z\t1970-01-01T00:00:00.005Z\t5000000.0\t5000000.0
                            3\t1970-01-01T00:00:00.000005000Z\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\t5000.0\t5000.0
                            4\t\t\t\tnull\tnull
                            """);

            // An explicit cast resolves on the argument's own tag, not by overload distance. rr goes
            // through cast(Nf) and reaches getFloat even though FLOAT is absent from TIMESTAMP's
            // overload set; tn goes through cast(Nn), the precision change, which reads getLong.
            assertQuery("""
                    SELECT id,
                      json_extract(text,'$.a')::timestamp::real rr,
                      ts::real rr_control,
                      json_extract(text,'$.a')::timestamp::timestamp_ns tn,
                      ts::timestamp_ns tn_control
                    FROM j ORDER BY id""")
                    .expectSize()
                    .returns("""
                            id\trr\trr_control\ttn\ttn_control
                            1\t5000.0\t5000.0\t1970-01-01T00:00:00.005000000Z\t1970-01-01T00:00:00.005000000Z
                            2\t5000000.0\t5000000.0\t1970-01-01T00:00:05.000000000Z\t1970-01-01T00:00:05.000000000Z
                            3\t5.0\t5.0\t1970-01-01T00:00:00.000005000Z\t1970-01-01T00:00:00.000005000Z
                            4\tnull\tnull\t\t
                            """);
        });
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
