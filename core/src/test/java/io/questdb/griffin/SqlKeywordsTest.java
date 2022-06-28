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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

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
    public void testIs() throws Exception {
        Map<String, String> specialCases = Map.of(
                "isColonColon", "::",
                "isConcatOperator", "||",
                "isMaxIdentifierLength", "max_identifier_length",
                "isQuote", "'",
                "isSearchPath", "search_path",
                "isSemicolon", ";",
                "isStandardConformingStrings", "standard_conforming_strings",
                "isTextArray", "text[]",
                "isTransactionIsolation", "transaction_isolation"
        );

        Method[] methods = SqlKeywords.class.getMethods();
        Arrays.sort(methods, (m1, m2) -> m1.getName().compareTo(m2.getName()));
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
}

