/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static io.questdb.griffin.SqlKeywords.*;

public class SqlKeywordsTest {

    @Test
    public void testAdd() {
        Assert.assertTrue(isAddKeyword("add"));
        Assert.assertTrue(isAddKeyword("aDD"));
        Assert.assertTrue(isAddKeyword("ADD"));
        Assert.assertTrue(isAddKeyword("Add"));
        Assert.assertTrue(isAddKeyword("ADd"));
        Assert.assertTrue(isAddKeyword("adD"));
        Assert.assertFalse(isAddKeyword("adds"));
        Assert.assertFalse(isAddKeyword("ad"));
        Assert.assertFalse(isAddKeyword("ade"));

    }

    @Test
    public void testAlign() {
        Assert.assertTrue(isAlignKeyword("align"));
        Assert.assertTrue(isAlignKeyword("Align"));
        Assert.assertTrue(isAlignKeyword("ALign"));
        Assert.assertTrue(isAlignKeyword("ALIgn"));
        Assert.assertTrue(isAlignKeyword("ALIGn"));
        Assert.assertTrue(isAlignKeyword("ALIGN"));
        Assert.assertFalse(isAlignKeyword("aligm"));
        Assert.assertFalse(isAlignKeyword("algin"));
        Assert.assertFalse(isAlignKeyword("aligns"));

    }

    @Test
    public void testAll() {
        Assert.assertTrue(isAllKeyword("all"));
        Assert.assertTrue(isAllKeyword("All"));
        Assert.assertTrue(isAllKeyword("ALl"));
        Assert.assertTrue(isAllKeyword("ALL"));
        Assert.assertTrue(isAllKeyword("aLL"));
        Assert.assertTrue(isAllKeyword("AlL"));
        Assert.assertFalse(isAllKeyword("al"));
        Assert.assertFalse(isAllKeyword("alll"));
        Assert.assertFalse(isAllKeyword("lal"));

    }

    @Test
    public void testAlter() {
        Assert.assertTrue(isAlterKeyword("alter"));
        Assert.assertTrue(isAlterKeyword("Alter"));
        Assert.assertTrue(isAlterKeyword("ALter"));
        Assert.assertTrue(isAlterKeyword("ALTER"));
        Assert.assertTrue(isAlterKeyword("aLTer"));
        Assert.assertTrue(isAlterKeyword("AlTER"));
        Assert.assertFalse(isAlterKeyword("alters"));
        Assert.assertFalse(isAlterKeyword("altre"));
        Assert.assertFalse(isAlterKeyword("alt"));

    }

    @Test
    public void testAnd() {
        Assert.assertTrue(isAndKeyword("and"));
        Assert.assertTrue(isAndKeyword("aNd"));
        Assert.assertTrue(isAndKeyword("AND"));
        Assert.assertTrue(isAndKeyword("anD"));
        Assert.assertTrue(isAndKeyword("And"));
        Assert.assertTrue(isAndKeyword("AnD"));
        Assert.assertFalse(isAndKeyword("ands"));
        Assert.assertFalse(isAndKeyword("nand"));
        Assert.assertFalse(isAndKeyword("adn"));

    }

    @Test
    public void testAs() {
        Assert.assertTrue(isAsKeyword("as"));
        Assert.assertTrue(isAsKeyword("aS"));
        Assert.assertTrue(isAsKeyword("AS"));
        Assert.assertTrue(isAsKeyword("As"));
        Assert.assertFalse(isAsKeyword("ads"));
        Assert.assertFalse(isAsKeyword("sas"));
        Assert.assertFalse(isAsKeyword("ad"));

    }

    @Test
    public void testConcatKeyword() {
        Assert.assertTrue(isConcatKeyword("concat"));
        Assert.assertTrue(isConcatKeyword("CONCAT"));
        Assert.assertTrue(isConcatKeyword("ConcaT"));
        Assert.assertTrue(isConcatKeyword("cONCAt"));
        Assert.assertTrue(isConcatKeyword("CONcat"));
        Assert.assertTrue(isConcatKeyword("conCAT"));
        Assert.assertFalse(isConcatKeyword("con cat"));
        Assert.assertFalse(isConcatKeyword("concatt"));
        Assert.assertFalse(isConcatKeyword("c o n c a t"));
    }

    @Test
    public void testIs() throws Exception {
        Map<String, String> specialCases = new HashMap<>();
        specialCases.put("isColonColon", "::");
        specialCases.put("isConcatOperator", "||");
        specialCases.put("isMaxIdentifierLength", "max_identifier_length");
        specialCases.put("isQuote", "'");
        specialCases.put("isSearchPath", "search_path");
        specialCases.put("isSemicolon", ";");
        specialCases.put("isStandardConformingStrings", "standard_conforming_strings");
        specialCases.put("isTextArray", "text[]");
        specialCases.put("isTransactionIsolation", "transaction_isolation");

        Method[] methods = SqlKeywords.class.getMethods();
        Arrays.sort(methods, Comparator.comparing(Method::getName));
        for (Method method : methods) {
            String name;
            int m = method.getModifiers() & Modifier.methodModifiers();
            if (Modifier.isPublic(m) && Modifier.isStatic(m) && (name = method.getName()).startsWith("is")) {
                String keyword;
                if (name.endsWith("Keyword")) {
                    keyword = name.substring(2, name.length() - 7).toLowerCase();
                } else {
                    keyword = specialCases.get(name);
                }
                Assert.assertTrue((boolean) method.invoke(null, keyword));
            }
        }
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
    public void testMaxIdentifierLength() {
        Assert.assertTrue(isMaxIdentifierLength("max_identifier_length"));
        Assert.assertTrue(isMaxIdentifierLength("Max_Identifier_Length"));
        Assert.assertTrue(isMaxIdentifierLength("MAX_IDENTIFIER_LENGTH"));
        Assert.assertTrue(isMaxIdentifierLength("MAX_Identifier_Length"));
        Assert.assertTrue(isMaxIdentifierLength("Max_IDENTIFIER_Length"));
        Assert.assertTrue(isMaxIdentifierLength("Max_Identifier_LENGTH"));
        Assert.assertFalse(isMaxIdentifierLength("Max-Identifier-Length"));
        Assert.assertFalse(isMaxIdentifierLength("Max_Identifier_Lenght"));
        Assert.assertFalse(isMaxIdentifierLength("Max_Identifier_Lengths"));
        Assert.assertFalse(isMaxIdentifierLength("Max_Length_Identifier"));

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

    @Test
    public void testStandardConformingStrings() {
        Assert.assertTrue(isStandardConformingStrings("standard_conforming_strings"));
        Assert.assertTrue(isStandardConformingStrings("Standard_Conforming_Strings"));
        Assert.assertTrue(isStandardConformingStrings("STANDARD_Conforming_Strings"));
        Assert.assertTrue(isStandardConformingStrings("Standard_CONFORMING_Strings"));
        Assert.assertTrue(isStandardConformingStrings("Standard_Conforming_STRINGS"));
        Assert.assertTrue(isStandardConformingStrings("STANDARD_CONFORMING_STRINGS"));
        Assert.assertFalse(isStandardConformingStrings("Standard-Conforming-Strings"));
        Assert.assertFalse(isStandardConformingStrings("Standard_Conforming_String"));
        Assert.assertFalse(isStandardConformingStrings("tsandard_Conforming_Strings"));
        Assert.assertFalse(isStandardConformingStrings("Conforming_standard_Strings"));

    }

    @Test
    public void testStartsWithGeoHashKeyword() {
        Assert.assertTrue(startsWithGeoHashKeyword("geohash test"));
        Assert.assertTrue(startsWithGeoHashKeyword("geohashtest"));
        Assert.assertTrue(startsWithGeoHashKeyword("GEOHASH test"));
        Assert.assertTrue(startsWithGeoHashKeyword("geoHASH test"));
        Assert.assertTrue(startsWithGeoHashKeyword("GEOhash test"));
        Assert.assertTrue(startsWithGeoHashKeyword("geohash"));
        Assert.assertFalse(startsWithGeoHashKeyword("geo test"));
        Assert.assertFalse(startsWithGeoHashKeyword("hash test"));
        Assert.assertFalse(startsWithGeoHashKeyword("geohahs test"));
    }

    @Test
    public void testTransactionIsolation() {
        Assert.assertTrue(isTransactionIsolation("transaction_isolation"));
        Assert.assertTrue(isTransactionIsolation("Transaction_Isolation"));
        Assert.assertTrue(isTransactionIsolation("TransactioN_IsolatioN"));
        Assert.assertTrue(isTransactionIsolation("TRANSACTION_ISOLATION"));
        Assert.assertTrue(isTransactionIsolation("transaction_ISOLATION"));
        Assert.assertTrue(isTransactionIsolation("TRANSACTION_isolation"));
        Assert.assertFalse(isTransactionIsolation("Transaction_Isolations"));
        Assert.assertFalse(isTransactionIsolation("TransactionIsolation"));
        Assert.assertFalse(isTransactionIsolation("transaction-isolation"));

    }

    @Test
    public void testValidateExtractPart() {
        Assert.assertTrue(validateExtractPart("microseconds"));
        Assert.assertTrue(validateExtractPart("milliseconds"));
        Assert.assertTrue(validateExtractPart("second"));
        Assert.assertTrue(validateExtractPart("minute"));
        Assert.assertTrue(validateExtractPart("hour"));
        Assert.assertTrue(validateExtractPart("hoUR"));
        Assert.assertTrue(validateExtractPart("day"));
        Assert.assertTrue(validateExtractPart("doy"));
        Assert.assertTrue(validateExtractPart("dow"));
        Assert.assertTrue(validateExtractPart("week"));
        Assert.assertTrue(validateExtractPart("month"));
        Assert.assertTrue(validateExtractPart("quarter"));
        Assert.assertTrue(validateExtractPart("year"));
        Assert.assertTrue(validateExtractPart("isoyear"));
        Assert.assertTrue(validateExtractPart("isodow"));
        Assert.assertTrue(validateExtractPart("decade"));
        Assert.assertTrue(validateExtractPart("century"));
        Assert.assertTrue(validateExtractPart("millennium"));
        Assert.assertTrue(validateExtractPart("epoch"));
        Assert.assertFalse(validateExtractPart("test"));
        Assert.assertFalse(validateExtractPart("days"));
    }

}
