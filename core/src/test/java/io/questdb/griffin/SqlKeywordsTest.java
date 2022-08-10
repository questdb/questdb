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

package io.questdb.griffin;

import org.junit.Assert;
import org.junit.Test;

import static io.questdb.griffin.SqlKeywords.*;

public class SqlKeywordsTest {

    @Test
    public void testPrev() {
        Assert.assertFalse(isPrevKeyword("123"));
        Assert.assertFalse(isPrevKeyword("1234"));
        Assert.assertFalse(isPrevKeyword("p123"));
        Assert.assertFalse(isPrevKeyword("pr12"));
        Assert.assertFalse(isPrevKeyword("pre1"));
        Assert.assertTrue(isPrevKeyword("prev"));
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
    public void testIsFormatKeywordIsCaseInsensitive() {
        Assert.assertTrue(isFormatKeyword("format"));
        Assert.assertTrue(isFormatKeyword("formaT"));
        Assert.assertTrue(isFormatKeyword("FORMAT"));
        Assert.assertTrue(isFormatKeyword("forMAT"));
        Assert.assertFalse(isFormatKeyword("forMa"));
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
        Assert.assertTrue(isMaxUncommittedRowsParam("MaxUncommittedRows"));
        Assert.assertTrue(isMaxUncommittedRowsParam("maxuncommittedrows"));
        Assert.assertTrue(isMaxUncommittedRowsParam("maxuncommittedrowS"));
        Assert.assertTrue(isMaxUncommittedRowsParam("MAXUNCOMMITTEDROWS"));
        Assert.assertTrue(isMaxUncommittedRowsParam("MAXUNCOMMITTEDROWs"));
        Assert.assertTrue(isMaxUncommittedRowsParam("MaxUncommittedRowS"));
        Assert.assertFalse(isMaxUncommittedRowsParam("MaxUncommittedRowD"));
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
}
