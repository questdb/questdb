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

package io.questdb.misc;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.catalogue.Constants;

import java.lang.reflect.Field;
import java.util.*;

public class SqlGrammarUtil {

    private static final Set<String> STATIC_SET = new TreeSet<>(Arrays.asList(
        "&", "|", "^", "~", "[]", "!=", "!~", "%", "*", "+", "-", ".", "/", "<", "<=",
        "<>", "<>all", "=", ">", ">="
    ));

    private static final Set<String> SKIP_SET = new HashSet<>(Arrays.asList(
        "unknown", "regclass", "regprocedure", "VARARG", "text[]", "CURSOR", "RECORD", "PARAMETER"
    ));

    private static void print(String header, Set<String> names) {
        System.out.printf("%s:%n", header);
        for (String name : names) {
            System.out.printf("\"%s\",%n", name);
        }
        System.out.print("\n=====\n");
    }

    public static void main(String... args) {
        print("FUNCTIONS", collectFunctionNames());
        print("KEYWORDS", collectKeywords());
        print("TYPES", collectTypes());
    }

    private static Set<String> collectFunctionNames() {
        Set<String> names = new TreeSet<>();
        for (FunctionFactory factory : ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())) {
            if (factory.getClass().getName().contains("test")) {
                continue;
            }
            String signature = factory.getSignature();
            String name = signature.substring(0, signature.indexOf('('));
            if (STATIC_SET.contains(name)) {
                continue;
            }
            names.add(name);
            if (factory.isBoolean()) {
                if (name.equals("=")) {
                    names.add("!=");
                    names.add("<>");
                } else if (name.equals("<")) {
                    names.add("<=");
                    names.add(">=");
                    names.add(">");
                }
            }
        }
        return names;
    }

    private static Set<String> collectKeywords() {
        Set<String> names = new TreeSet<>();
        try {
            Field field = Constants.class.getDeclaredField("KEYWORDS");
            field.setAccessible(true);
            for (CharSequence keyword : (CharSequence[]) field.get(null)) {
                names.add(keyword.toString());
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return names;
    }

    private static Set<String> collectTypes() {
        Set<String> names = new TreeSet<>();
        for (int type = 1; type < ColumnType.NULL; type++) {
            String name = ColumnType.nameOf(type);
            if (!SKIP_SET.contains(name)) {
                names.add(name.toLowerCase());
            }
        }
        return names;
    }
}
