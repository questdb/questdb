/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.griffin.SqlKeywords;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.questdb.griffin.SqlKeywords.*;

public class SqlKeywordsTest {
    // kept protected for ent tests
    protected static final Set<String> excludedCases = new HashSet<>();
    protected static final Map<String, String> specialCases = new HashMap<>();

    @Test
    public void testIs() throws Exception {
        testIs(SqlKeywords.class.getMethods());
    }

    @Test
    public void testIsFloat4KeywordIsCaseInsensitive() {
        Assert.assertTrue(isFloat4Keyword("float4"));
        Assert.assertTrue(isFloat4Keyword("floaT4"));
        Assert.assertTrue(isFloat4Keyword("FLOAT4"));
        Assert.assertTrue(isFloat4Keyword("floAT4"));
        Assert.assertFalse(isFloat4Keyword("float"));
    }

    @Test
    public void testIsFloat8KeywordIsCaseInsensitive() {
        Assert.assertTrue(isFloat8Keyword("float8"));
        Assert.assertTrue(isFloat8Keyword("floaT8"));
        Assert.assertTrue(isFloat8Keyword("FLOAT8"));
        Assert.assertTrue(isFloat8Keyword("floAT8"));
        Assert.assertFalse(isFloat8Keyword("float"));
    }

    @Test
    public void testIsFloatKeywordIsCaseInsensitive() {
        Assert.assertTrue(isFloatKeyword("float"));
        Assert.assertTrue(isFloatKeyword("floaT"));
        Assert.assertTrue(isFloatKeyword("FLOAT"));
        Assert.assertTrue(isFloatKeyword("floAT"));
        Assert.assertFalse(isFloatKeyword("flot"));
    }

    @Test
    public void testIsFormatKeywordIsCaseInsensitive() {
        Assert.assertTrue(isFormatKeyword("format"));
        Assert.assertTrue(isFormatKeyword("formaT"));
        Assert.assertTrue(isFormatKeyword("FORMAT"));
        Assert.assertTrue(isFormatKeyword("forMAT"));
        Assert.assertFalse(isFormatKeyword("forMa"));
    }

    @Test
    public void testIsHourKeywordIsCaseInsensitive() {
        Assert.assertTrue(isHourKeyword("hour"));
        Assert.assertTrue(isHourKeyword("houR"));
        Assert.assertTrue(isHourKeyword("HOUR"));
        Assert.assertTrue(isHourKeyword("HOUr"));
        Assert.assertTrue(isHourKeyword("hoUR"));
        Assert.assertFalse(isHourKeyword("houra"));
    }

    @Test
    public void testIsMaxUncommittedRowsParamIsCaseInsensitive() {
        Assert.assertTrue(isMaxUncommittedRowsKeyword("MaxUncommittedRows"));
        Assert.assertTrue(isMaxUncommittedRowsKeyword("maxuncommittedrows"));
        Assert.assertTrue(isMaxUncommittedRowsKeyword("maxuncommittedrowS"));
        Assert.assertTrue(isMaxUncommittedRowsKeyword("MAXUNCOMMITTEDROWS"));
        Assert.assertTrue(isMaxUncommittedRowsKeyword("MAXUNCOMMITTEDROWs"));
        Assert.assertTrue(isMaxUncommittedRowsKeyword("MaxUncommittedRowS"));
        Assert.assertFalse(isMaxUncommittedRowsKeyword("MaxUncommittedRowD"));
    }

    @Test
    public void testIsMicrosecondsKeywordIsCaseInsensitive() {
        Assert.assertTrue(isMicrosecondsKeyword("microseconds"));
        Assert.assertTrue(isMicrosecondsKeyword("microsecondS"));
        Assert.assertTrue(isMicrosecondsKeyword("MICROSECONDS"));
        Assert.assertTrue(isMicrosecondsKeyword("MICROSECONDs"));
        Assert.assertTrue(isMicrosecondsKeyword("MICROseconds"));
        Assert.assertFalse(isMicrosecondsKeyword("microsecondd"));
    }

    @Test
    public void testIsMillenniumKeywordIsCaseInsensitive() {
        Assert.assertTrue(isMillenniumKeyword("millennium"));
        Assert.assertTrue(isMillenniumKeyword("millenniuM"));
        Assert.assertTrue(isMillenniumKeyword("MILLENNIUM"));
        Assert.assertTrue(isMillenniumKeyword("MILLENNIUm"));
        Assert.assertTrue(isMillenniumKeyword("MILlenNIUM"));
        Assert.assertFalse(isMillenniumKeyword("MILlenNIUn"));
    }

    @Test
    public void testIsMillisecondsKeywordIsCaseInsensitive() {
        Assert.assertTrue(isMillisecondsKeyword("milliseconds"));
        Assert.assertTrue(isMillisecondsKeyword("millisecondS"));
        Assert.assertTrue(isMillisecondsKeyword("MILLISECONDS"));
        Assert.assertTrue(isMillisecondsKeyword("MILLISECONDs"));
        Assert.assertTrue(isMillisecondsKeyword("MIlliSECONDS"));
        Assert.assertFalse(isMillisecondsKeyword("MILLISECONDD"));
    }

    @Test
    public void testIsMinuteKeywordIsCaseInsensitive() {
        Assert.assertTrue(isMinuteKeyword("minute"));
        Assert.assertTrue(isMinuteKeyword("minutE"));
        Assert.assertTrue(isMinuteKeyword("MINUTE"));
        Assert.assertTrue(isMinuteKeyword("MINUTe"));
        Assert.assertTrue(isMinuteKeyword("minUTE"));
        Assert.assertFalse(isMinuteKeyword("minutF"));
    }

    @Test
    public void testIsMonthKeywordIsCaseInsensitive() {
        Assert.assertTrue(isMonthKeyword("month"));
        Assert.assertTrue(isMonthKeyword("montH"));
        Assert.assertTrue(isMonthKeyword("MONTH"));
        Assert.assertTrue(isMonthKeyword("MONTh"));
        Assert.assertTrue(isMonthKeyword("MONth"));
        Assert.assertFalse(isMonthKeyword("MONTi"));
    }

    @Test
    public void testIsParametersKeyword() {
        Assert.assertTrue(SqlKeywords.isParametersKeyword("PARAMETERS"));
        Assert.assertTrue(SqlKeywords.isParametersKeyword("parameters"));
        Assert.assertTrue(SqlKeywords.isParametersKeyword("paraMEters"));
        Assert.assertFalse(SqlKeywords.isParametersKeyword("param3ters"));
    }

    @Test
    public void testIsPublicKeyword() {
        Assert.assertTrue(isPublicKeyword("public", 6));
        Assert.assertFalse(isPublicKeyword("private", 6));
        Assert.assertFalse(isPublicKeyword("foo", 3));
        Assert.assertTrue(isPublicKeyword("public.foobar", 6));
        Assert.assertFalse(isPublicKeyword("private.foobar", 6));
    }

    @Test
    public void testLinear() {
        Assert.assertFalse(isLinearKeyword("12345"));
        Assert.assertFalse(isLinearKeyword("123456"));
        Assert.assertFalse(isLinearKeyword("l12345"));
        Assert.assertFalse(isLinearKeyword("li1234"));
        Assert.assertFalse(isLinearKeyword("lin123"));
        Assert.assertFalse(isLinearKeyword("line12"));
        Assert.assertFalse(isLinearKeyword("linea1"));
        Assert.assertTrue(isLinearKeyword("linear"));
    }

    @Test
    public void testPrev() {
        Assert.assertFalse(isPrevKeyword("123"));
        Assert.assertFalse(isPrevKeyword("1234"));
        Assert.assertFalse(isPrevKeyword("p123"));
        Assert.assertFalse(isPrevKeyword("pr12"));
        Assert.assertFalse(isPrevKeyword("pre1"));
        Assert.assertTrue(isPrevKeyword("prev"));
    }

    protected static void testIs(Method[] methods) throws Exception {
        Arrays.sort(methods, Comparator.comparing(Method::getName));
        for (Method method : methods) {
            String name;
            int m = method.getModifiers() & Modifier.methodModifiers();
            if (Modifier.isPublic(m) && Modifier.isStatic(m) && (name = method.getName()).startsWith("is") && !excludedCases.contains(name)) {
                String methodParam = specialCases.get(name);
                if (methodParam == null) {
                    if (!name.endsWith("Keyword")) {
                        Assert.fail("if method name does not end with 'Keyword', it has to be a special case: " + name);
                    }
                    methodParam = name.substring(2, name.length() - 7).toLowerCase();
                }
                Class<?>[] argTypes = method.getParameterTypes();
                if (argTypes.length != 1) {
                    Assert.fail("keyword method must have a single argument: " + name);
                }
                if (argTypes[0] == Utf8Sequence.class) {
                    Assert.assertTrue(name, (boolean) method.invoke(null, new Utf8String(methodParam)));
                } else {
                    Assert.assertTrue(name, (boolean) method.invoke(null, methodParam));
                }
            }
        }
    }

    static {
        specialCases.put("isColonColon", "::");
        specialCases.put("isConcatOperator", "||");
        specialCases.put("isMaxIdentifierLength", "max_identifier_length");
        specialCases.put("isQuote", "'");
        specialCases.put("isSearchPath", "search_path");
        specialCases.put("isSemicolon", ";");
        specialCases.put("isStandardConformingStrings", "standard_conforming_strings");
        specialCases.put("isTextArray", "text[]");
        specialCases.put("isTransactionIsolation", "transaction_isolation");
        specialCases.put("isEmptyAlias", "''");
        specialCases.put("isKeyword", "select");
        specialCases.put("isServerVersionKeyword", "server_version");
        specialCases.put("isServerVersionNumKeyword", "server_version_num");
        specialCases.put("isUTC", "'UTC'");
        specialCases.put("isZeroOffset", "'00:00'");
        specialCases.put("isJsonExtract", "json_extract");
        specialCases.put("isRespectWord", "respect");
        specialCases.put("isIgnoreWord", "ignore");
        specialCases.put("isNullsWord", "nulls");
        specialCases.put("isTimestampNsKeyword", "timestamp_ns");

        excludedCases.add("isPublicKeyword");
    }
}
