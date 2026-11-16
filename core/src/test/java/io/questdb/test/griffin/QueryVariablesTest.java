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

package io.questdb.test.griffin;

import io.questdb.griffin.QueryVariables;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Reading the JSON of the variables parameter, see {@link QueryVariables}. The SQL side of the
 * variables is covered by {@link QueryVariablesSqlTest}.
 */
public class QueryVariablesTest {

    @Test
    public void testAssignmentsFollowTheOrderOfTheArray() throws SqlException {
        final QueryVariables variables = new QueryVariables();
        variables.of("[{\"name\":\"b\",\"value\":\"2\"},{\"name\":\"a\",\"value\":\"@b + 1\"}]");
        Assert.assertEquals(2, variables.size());
        TestUtils.assertEquals("@b := 2\n,\n@a := @b + 1", variables.getDeclarations());
        assertVariable(variables, 0, "b", "2");
        assertVariable(variables, 1, "a", "@b + 1");
    }

    @Test
    public void testControlCharacterInString() {
        assertFails("[{\"name\":\"a\",\"value\":\"1\n\"}]", 23, "control character in string");
    }

    @Test
    public void testDuplicateField() {
        assertFails("[{\"name\":\"a\",\"name\":\"b\",\"value\":\"1\"}]", 13, "duplicate field [field=name]");
        assertFails("[{\"value\":\"1\",\"name\":\"a\",\"value\":\"2\"}]", 25, "duplicate field [field=value]");
    }

    @Test
    public void testDuplicateVariableIgnoresCase() {
        assertFails("[{\"name\":\"abc\",\"value\":\"1\"},{\"name\":\"ABC\",\"value\":\"2\"}]", 36, "duplicate variable [name=ABC]");
    }

    @Test
    public void testEmptyArrayDeclaresNothing() throws SqlException {
        final QueryVariables variables = new QueryVariables();
        variables.of(" [ ] ");
        Assert.assertEquals(0, variables.size());
        TestUtils.assertEquals("", variables.getDeclarations());
    }

    @Test
    public void testEmptyName() {
        assertFails("[{\"name\":\"\",\"value\":\"1\"}]", 9, "variable name is empty");
    }

    @Test
    public void testEscapes() throws SqlException {
        final QueryVariables variables = new QueryVariables();
        variables.of("[{\"name\":\"q\",\"value\":\"\\\"col\\\" \\\\ \\/ \\b\\f\\n\\r\\t \\u0041\\u00e9\"}]");
        assertVariable(variables, 0, "q", "\"col\" \\ / \b\f\n\r\t A\u00e9");
    }

    @Test
    public void testFieldsInAnyOrderAndWhitespace() throws SqlException {
        final QueryVariables variables = new QueryVariables();
        variables.of("\n[ { \"value\" : \"'x'\" ,\t\"name\" : \"_a1\" } ]\r\n");
        assertVariable(variables, 0, "_a1", "'x'");
    }

    @Test
    public void testIndexOfMapsOffsetsToVariables() throws SqlException {
        final QueryVariables variables = new QueryVariables();
        variables.of("[{\"name\":\"a\",\"value\":\"1\"},{\"name\":\"bb\",\"value\":\"22\"}]");
        // @a := 1\n,\n@bb := 22
        TestUtils.assertEquals("@a := 1\n,\n@bb := 22", variables.getDeclarations());
        Assert.assertEquals(0, variables.indexOf(0));
        Assert.assertEquals(0, variables.indexOf(6));
        // the separator belongs to the assignment before it
        Assert.assertEquals(0, variables.indexOf(8));
        Assert.assertEquals(1, variables.indexOf(10));
        Assert.assertEquals(1, variables.indexOf(19));
        Assert.assertEquals(-1, variables.indexOf(-1));

        variables.clear();
        Assert.assertEquals(-1, variables.indexOf(0));
    }

    @Test
    public void testInvalidEscape() {
        assertFails("[{\"name\":\"a\",\"value\":\"\\x\"}]", 22, "invalid escape sequence");
        assertFails("[{\"name\":\"a\",\"value\":\"\\u00g0\"}]", 22, "invalid unicode escape");
        assertFails("[{\"name\":\"a\",\"value\":\"\\u00\"}]", 22, "invalid unicode escape");
    }

    @Test
    public void testInvalidName() {
        assertFails("[{\"name\":\"1a\",\"value\":\"1\"}]", 9, "invalid variable name [name=1a]");
        assertFails("[{\"name\":\"@a\",\"value\":\"1\"}]", 9, "invalid variable name [name=@a]");
        assertFails("[{\"name\":\"a b\",\"value\":\"1\"}]", 9, "invalid variable name [name=a b]");
        assertFails("[{\"name\":\"a-b\",\"value\":\"1\"}]", 9, "invalid variable name [name=a-b]");
    }

    @Test
    public void testMissingNameOrValue() {
        assertFails("[{\"value\":\"1\"}]", 1, "variable has no name");
        assertFails("[{\"name\":\"a\"}]", 1, "variable has no value [name=a]");
        assertFails("[{}]", 1, "variable has no name");
    }

    @Test
    public void testNameAcceptsNonAsciiLetters() throws SqlException {
        final QueryVariables variables = new QueryVariables();
        variables.of("[{\"name\":\"zaman_\u00e7\",\"value\":\"1\"}]");
        assertVariable(variables, 0, "zaman_\u00e7", "1");
    }

    @Test
    public void testNotAnArray() {
        assertFails("", 0, "variables must be a JSON array");
        assertFails("{\"name\":\"a\",\"value\":\"1\"}", 0, "variables must be a JSON array");
        assertFails("[\"a\"]", 1, "variable must be a JSON object");
    }

    @Test
    public void testNotAString() {
        assertFails("[{\"name\":\"a\",\"value\":1}]", 21, "string expected");
        assertFails("[{name:\"a\",\"value\":\"1\"}]", 2, "string expected");
    }

    @Test
    public void testReadAgainReplacesVariables() throws SqlException {
        final QueryVariables variables = new QueryVariables();
        variables.of("[{\"name\":\"a\",\"value\":\"1\"}]");
        variables.of("[{\"name\":\"b\",\"value\":\"2\"}]");
        Assert.assertEquals(1, variables.size());
        TestUtils.assertEquals("@b := 2", variables.getDeclarations());
    }

    @Test
    public void testSeparators() {
        assertFails("[{\"name\":\"a\",\"value\":\"1\"}", 25, "',' or ']' expected");
        assertFails("[{\"name\":\"a\",\"value\":\"1\"};", 25, "',' or ']' expected");
        assertFails("[{\"name\":\"a\" \"value\":\"1\"}]", 13, "',' or '}' expected");
        assertFails("[{\"name\":\"a\",\"value\":\"1\"", 24, "',' or '}' expected");
        assertFails("[{\"name\" \"a\"}]", 9, "':' expected");
        assertFails("[{\"name\":\"a\",\"value\":\"1\"}] x", 27, "unexpected text after the variables array");
    }

    @Test
    public void testUnknownField() {
        assertFails("[{\"name\":\"a\",\"value\":\"1\",\"type\":\"x\"}]", 25, "unknown field [field=type]");
    }

    @Test
    public void testUnterminatedString() {
        assertFails("[{\"name\":\"a\",\"value\":\"1}]", 21, "unterminated string");
        assertFails("[{\"name\":\"a\",\"value\":\"1\\", 21, "unterminated string");
    }

    @Test
    public void testValueBlank() {
        assertFails("[{\"name\":\"a\",\"value\":\" \\t \"}]", 21, "variable value is empty [name=a]");
    }

    @Test
    public void testValueIsKeptVerbatim() throws SqlException {
        final QueryVariables variables = new QueryVariables();
        variables.of("[{\"name\":\"day\",\"value\":\"  '2026-11-11' -- the day\"}]");
        assertVariable(variables, 0, "day", "  '2026-11-11' -- the day");
    }

    @Test
    public void testVariablesAreClearedOnError() throws SqlException {
        final QueryVariables variables = new QueryVariables();
        variables.of("[{\"name\":\"a\",\"value\":\"1\"}]");
        try {
            variables.of("[{\"name\":\"b\",\"value\":\"2\"},{\"name\":\"1c\",\"value\":\"3\"}]");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid variable name");
        }
        Assert.assertEquals(0, variables.size());
        TestUtils.assertEquals("", variables.getDeclarations());
    }

    @Test
    public void testTruncatedInput() {
        assertFails("[", 1, "variable must be a JSON object");
        assertFails("[{", 2, "string expected");
        assertFails("[{\"name\"", 8, "':' expected");
        assertFails("[{\"name\":", 9, "string expected");
        assertFails("[{\"name\":\"a\",\"value\":\"\\u00", 22, "invalid unicode escape");
        assertFails("[{\"name\":\"a\",\"value\":\"1\"},", 26, "variable must be a JSON object");
        assertFails("[{\"name\":\"a\",\"value\":\"1\"} {}]", 26, "',' or ']' expected");
    }

    @Test
    public void testNameCharacters() throws SqlException {
        final QueryVariables variables = new QueryVariables();
        variables.of("[{\"name\":\"Az_09\",\"value\":\"1\"},{\"name\":\"_\",\"value\":\"2\"}]");
        assertVariable(variables, 0, "Az_09", "1");
        assertVariable(variables, 1, "_", "2");
        assertFails("[{\"name\":\"a{\",\"value\":\"1\"}]", 9, "invalid variable name [name=a{]");
        assertFails("[{\"name\":\"a`\",\"value\":\"1\"}]", 9, "invalid variable name [name=a`]");
        assertFails("[{\"name\":\"a:\",\"value\":\"1\"}]", 9, "invalid variable name [name=a:]");
    }

    private static void assertFails(String json, int position, String message) {
        final QueryVariables variables = new QueryVariables();
        try {
            variables.of(json);
            Assert.fail("expected an error for " + json);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
            Assert.assertEquals(json, position, e.getPosition());
        }
        Assert.assertEquals(0, variables.size());
    }

    private static void assertVariable(QueryVariables variables, int index, String name, String value) {
        final CharSequence declarations = variables.getDeclarations();
        TestUtils.assertEquals(name, declarations.subSequence(variables.getNameLo(index), variables.getNameHi(index)));
        TestUtils.assertEquals(value, declarations.subSequence(variables.getValueLo(index), variables.getValueHi(index)));
        Assert.assertEquals('@', declarations.charAt(variables.getNameLo(index) - 1));
    }
}
