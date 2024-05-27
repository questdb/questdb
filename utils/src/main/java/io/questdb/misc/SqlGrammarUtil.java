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

import java.util.*;

/**
 * Utility class for generating SQL-related grammar elements such as functions, keywords, and data types.
 */
public class SqlGrammarUtil {

    // Static set of operators and symbols
    private static final Set<String> STATIC_SET = new HashSet<>(Arrays.asList(
            "&", "|", "^", "~", "[]",
            "!=", "!~", "%", "*", "+",
            "-", ".", "/", "<", "<=",
            "<>", "<>all", "=", ">", ">="
    ));

    // Set of data types to skip
    private static final Set<String> SKIP_TYPES = new HashSet<>(Arrays.asList(
            "unknown", "regclass", "regprocedure", "VARARG", "text[]", "CURSOR", "RECORD", "PARAMETER"
    ));

    /**
     * Prints the given header and set of names, with each name enclosed in double quotes and separated by commas.
     * After printing all names, it prints a separator line.
     *
     * @param header The header string to print
     * @param names  The set of names to print
     */
    private static void printNames(String header, Set<String> names) {
        System.out.printf("%s:%n", header);
        for (String name : names) {
            System.out.printf("\"%s\",%n", name);
        }
        System.out.print("\n=====\n");
    }

    public static void main(String... args) {
        // Function names
        Set<String> functionNames = new HashSet<>();
        for (FunctionFactory factory : ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())) {
            if (!factory.getClass().getName().contains("test")) {
                String signature = factory.getSignature();
                String name = signature.substring(0, signature.indexOf('('));
                if (!STATIC_SET.contains(name)) {
                    functionNames.add(name);
                    // Add inequality function names for equality and comparison functions
                    if (factory.isBoolean()) {
                        if (name.equals("=")) {
                            functionNames.add("!=");
                            functionNames.add("<>");
                        } else if (name.equals("<")) {
                            functionNames.add("<=");
                            functionNames.add(">=");
                            functionNames.add(">");
                        }
                    }
                }
            }
        }
        printNames("FUNCTIONS", functionNames);

        // Keywords
        Set<String> keywords = new HashSet<>();
        for (CharSequence keyword : Constants.KEYWORDS) {
            keywords.add(keyword.toString());
        }
        printNames("KEYWORDS", keywords);

        // Data types
        Set<String> dataTypes = new HashSet<>();
        for (int type = 1; type < ColumnType.NULL; type++) {
            String name = ColumnType.nameOf(type).toLowerCase();
            if (!SKIP_TYPES.contains(name)) {
                dataTypes.add(name);
            }
        }
        printNames("TYPES", dataTypes);
    }
}
