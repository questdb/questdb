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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.std.ObjList;

import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * Calls group by overloads over the columns of the {@code czw_prices} table, for tests that compare what each
 * aggregate reports over no rows on two paths. {@link #PRICES_COLUMNS} declares the table's columns but its designated
 * timestamp {@code ts}, and {@link #PRICES_VALUES} selects three rows of them from {@code long_sequence(3)}: {@code sym}
 * reads 'a', 'a' and 'b', and {@code i} reads 1, 2 and 3.
 */
public final class GroupByCallSweep {
    public static final String PRICES_COLUMNS = """
            sym SYMBOL, b BOOLEAN, bt BYTE, sh SHORT, ch CHAR, i INT, l LONG, f FLOAT, d DOUBLE, dt DATE, tsx TIMESTAMP,
            tsn TIMESTAMP_NS, s STRING, v VARCHAR, l256 LONG256, u UUID, ip IPV4, g1 GEOHASH(1c), g2 GEOHASH(2c),
            g4 GEOHASH(4c), g8 GEOHASH(8c), bin BINARY, arr DOUBLE[], d8 DECIMAL(2,1), d16 DECIMAL(4,1), d32 DECIMAL(9,2),
            d64 DECIMAL(18,2), d128 DECIMAL(38,2), d256 DECIMAL(76,2)""";
    public static final String PRICES_VALUES = """
            CASE WHEN x = 3 THEN 'b' ELSE 'a' END, true, x::byte, x::short, 'c', x::int, x, x::float, x::double,
            '2024-01-01'::date, '2024-01-01'::timestamp, '2024-01-01'::timestamp_ns, 's' || x, ('v' || x)::varchar,
            rnd_long256(), rnd_uuid4(), rnd_ipv4(), rnd_geohash(5), rnd_geohash(10), rnd_geohash(20), rnd_geohash(40),
            rnd_bin(4, 4, 0), ARRAY[x::double, 2.0], x::decimal(2,1), x::decimal(4,1), x::decimal(9,2), x::decimal(18,2),
            x::decimal(38,2), x::decimal(76,2)""";

    private GroupByCallSweep() {
    }

    /**
     * Adds to {@code calls} a call of each overload whose factory {@code isIncluded} accepts, once for every column or
     * constant each of its parameters takes, and the overload's name to {@code functionNames}. A column reads as
     * {@code columnPrefix} followed by its name. The first argument is always a column: an aggregate over constants
     * folds to a constant.
     */
    public static void collectCalls(
            FunctionFactoryCache cache,
            String columnPrefix,
            Predicate<FunctionFactory> isIncluded,
            TreeSet<String> calls,
            TreeSet<String> functionNames
    ) {
        cache.getFactories().forEach((name, descriptors) -> {
            for (int i = 0, n = descriptors.size(); i < n; i++) {
                final FunctionFactoryDescriptor descriptor = descriptors.getQuick(i);
                if (!isIncluded.test(descriptor.getFactory())) {
                    continue;
                }
                functionNames.add(descriptor.getName());
                final ObjList<ObjList<String>> argsPerPosition = new ObjList<>();
                for (int p = 0, m = descriptor.getSigArgCount(); p < m; p++) {
                    final int typeWithFlags = descriptor.getArgTypeWithFlags(p);
                    final ObjList<String> args = new ObjList<>();
                    addArgs(
                            columnPrefix,
                            FunctionFactoryDescriptor.toTypeTag(typeWithFlags),
                            FunctionFactoryDescriptor.isArray(typeWithFlags),
                            FunctionFactoryDescriptor.isConstant(typeWithFlags),
                            p > 0,
                            args
                    );
                    argsPerPosition.add(args);
                }
                addCalls(descriptor.getName(), argsPerPosition, 0, new StringBuilder(), calls);
            }
        });
    }

    private static void addArgs(String columnPrefix, short typeTag, boolean isArray, boolean isConstant, boolean isAfterFirst, ObjList<String> args) {
        if (!isConstant) {
            if (isArray) {
                args.add(columnPrefix + "arr");
            } else {
                switch (typeTag) {
                    case ColumnType.BOOLEAN -> args.add(columnPrefix + "b");
                    case ColumnType.BYTE -> args.add(columnPrefix + "bt");
                    case ColumnType.SHORT -> args.add(columnPrefix + "sh");
                    case ColumnType.CHAR -> args.add(columnPrefix + "ch");
                    case ColumnType.INT -> args.add(columnPrefix + "i");
                    case ColumnType.LONG -> args.add(columnPrefix + "l");
                    case ColumnType.FLOAT -> args.add(columnPrefix + "f");
                    case ColumnType.DOUBLE -> args.add(columnPrefix + "d");
                    case ColumnType.DATE -> args.add(columnPrefix + "dt");
                    case ColumnType.TIMESTAMP -> {
                        args.add(columnPrefix + "tsx");
                        args.add(columnPrefix + "tsn");
                        args.add(columnPrefix + "ts");
                    }
                    case ColumnType.STRING -> args.add(columnPrefix + "s");
                    case ColumnType.VARCHAR -> args.add(columnPrefix + "v");
                    case ColumnType.SYMBOL -> args.add(columnPrefix + "sym");
                    case ColumnType.LONG256 -> args.add(columnPrefix + "l256");
                    case ColumnType.UUID -> args.add(columnPrefix + "u");
                    case ColumnType.IPv4 -> args.add(columnPrefix + "ip");
                    case ColumnType.GEOHASH -> {
                        args.add(columnPrefix + "g1");
                        args.add(columnPrefix + "g2");
                        args.add(columnPrefix + "g4");
                        args.add(columnPrefix + "g8");
                    }
                    case ColumnType.BINARY -> args.add(columnPrefix + "bin");
                    case ColumnType.DECIMAL -> {
                        args.add(columnPrefix + "d8");
                        args.add(columnPrefix + "d16");
                        args.add(columnPrefix + "d32");
                        args.add(columnPrefix + "d64");
                        args.add(columnPrefix + "d128");
                        args.add(columnPrefix + "d256");
                    }
                    // A VAR_ARG parameter takes a column of any type, so one column stands for all of them.
                    case ColumnType.VAR_ARG -> args.add(columnPrefix + "i");
                    default -> throw new AssertionError("no czw_prices column of type " + ColumnType.nameOf(typeTag));
                }
            }
        }
        if (isConstant || isAfterFirst) {
            if (isArray) {
                args.add("ARRAY[0.5]");
            } else {
                switch (typeTag) {
                    case ColumnType.BOOLEAN -> args.add("true");
                    case ColumnType.CHAR -> args.add("','");
                    case ColumnType.INT, ColumnType.LONG -> args.add("4");
                    case ColumnType.DOUBLE -> {
                        args.add("0.5");
                        args.add("1.0");
                    }
                    case ColumnType.STRING -> args.add("'1.5'");
                    default -> {
                        if (isConstant) {
                            throw new AssertionError("no constant of type " + ColumnType.nameOf(typeTag));
                        }
                    }
                }
            }
        }
    }

    private static void addCalls(
            String functionName,
            ObjList<ObjList<String>> argsPerPosition,
            int position,
            StringBuilder argList,
            TreeSet<String> calls
    ) {
        if (position == argsPerPosition.size()) {
            calls.add(functionName + '(' + argList + ')');
            return;
        }
        final ObjList<String> args = argsPerPosition.getQuick(position);
        final int length = argList.length();
        for (int i = 0, n = args.size(); i < n; i++) {
            if (position > 0) {
                argList.append(", ");
            }
            argList.append(args.getQuick(i));
            addCalls(functionName, argsPerPosition, position + 1, argList, calls);
            argList.setLength(length);
        }
    }
}
