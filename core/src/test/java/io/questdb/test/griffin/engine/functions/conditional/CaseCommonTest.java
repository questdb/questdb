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

package io.questdb.test.griffin.engine.functions.conditional;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.functions.conditional.CaseCommon;
import io.questdb.test.griffin.BaseFunctionFactoryTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class CaseCommonTest extends BaseFunctionFactoryTest {
    private static final short MAX_COLUMN_TYPE_ID = ColumnType.VARCHAR;
    private static final short MIN_COLUMN_TYPE_ID = ColumnType.BOOLEAN;
    private static final Map<Short, Set<Short>> IGNORED_TYPE = buildIgnoredTypeMap();
    private static final int NO_COMMON_TYPE = -1;
    private FunctionParser functionParser;

    @Before
    public void setUpParser() {
        functionParser = new FunctionParser(configuration, new FunctionFactoryCache(
                configuration,
                new FunctionFactoryCacheBuilder().scan(LOG).build()
        ));
    }

    @Test
    public void testCastToCommonTypeOfBranchSequence() throws Exception {
        // CASE, SWITCH and coalesce fold the branch types left to right and then cast every
        // branch to the final type. The final type can be one that no pair with a given branch
        // type produces (SYMBOL, STRING, IPv4 -> IPv4), so this walks every type the fold can
        // reach once a branch of a given type took part, not only the pairwise common type.
        // Each branch must be cast to that type or be readable as it, else the query must fail
        // to compile.
        assertMemoryLeak(() -> {
            SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1);
            final List<Integer> types = getFoldTypes();
            final List<String> rejected = new ArrayList<>();
            for (int branchType : types) {
                for (int commonType : getReachableCommonTypes(branchType, types)) {
                    final String pair = ColumnType.nameOf(branchType) + " -> " + ColumnType.nameOf(commonType);
                    final Function branch = getRndFunction(branchType);
                    branch.init(null, context);
                    final Function castFunction;
                    try {
                        castFunction = CaseCommon.getCastToCommonTypeFunction(branch, 0, commonType, configuration, null);
                    } catch (SqlException e) {
                        Assert.assertEquals("[0] inconvertible types: " + pair + " [from=" + ColumnType.nameOf(branchType)
                                + ", to=" + ColumnType.nameOf(commonType) + ']', e.getMessage());
                        rejected.add(pair);
                        continue;
                    }
                    // an accepted branch gets the same function as without the check
                    Assert.assertSame(pair, CaseCommon.getCastFunction(branch, 0, commonType, configuration, null).getClass(), castFunction.getClass());
                    assertReadsAs(castFunction, commonType);
                }
            }
            Assert.assertEquals("[CHAR -> UUID, CHAR -> IPv4, SYMBOL -> UUID, SYMBOL -> IPv4]", rejected.toString());
        });
    }

    @Test
    public void testCharOrSymbolBranchWithIPv4OrUuidCommonTypeFailsToCompile() throws Exception {
        // a STRING branch takes the common type of a CHAR or SYMBOL branch to IPv4 or UUID; the
        // CHAR or SYMBOL branch cannot be read as either, so every branch order must fail to
        // compile in CASE, CASE x WHEN and coalesce, as the pair without the STRING branch does
        final String[][] forms = {
                {"SELECT CASE WHEN k < 1 THEN ", " WHEN k < 2 THEN ", " ELSE ", " END FROM t"},
                {"SELECT CASE k WHEN 0 THEN ", " WHEN 1 THEN ", " ELSE ", " END FROM t"},
                {"SELECT coalesce(", ", ", ", ", ") FROM t"}
        };
        // the order in which each form folds its branch types: SWITCH starts with ELSE
        final int[][] foldOrders = {{0, 1, 2}, {2, 0, 1}, {0, 1, 2}};
        final String[][] branchPairs = {
                // CHAR or SYMBOL column and type, IPv4 or UUID column and type, STRING literal of the latter
                {"ch", "CHAR", "ip", "IPv4", "'5.6.7.8'"},
                {"sym", "SYMBOL", "ip", "IPv4", "'5.6.7.8'"},
                {"ch", "CHAR", "uid", "UUID", "'11111111-1111-1111-1111-111111111111'"},
                {"sym", "SYMBOL", "uid", "UUID", "'11111111-1111-1111-1111-111111111111'"}
        };
        final int[][] orders = {{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}};
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, ch CHAR, sym SYMBOL, ip IPv4, uid UUID)");
            execute("INSERT INTO t VALUES (0, 'a', 'x', '1.2.3.4', '22222222-2222-2222-2222-222222222222')");
            for (int f = 0; f < forms.length; f++) {
                final String[] form = forms[f];
                final int[] foldOrder = foldOrders[f];
                for (String[] branchPair : branchPairs) {
                    // branch 0 is CHAR or SYMBOL, branch 1 is the STRING literal, branch 2 is IPv4 or UUID
                    final String[] branches = {branchPair[0], branchPair[4], branchPair[2]};
                    for (int[] order : orders) {
                        final StringBuilder sql = new StringBuilder(form[0]);
                        final int[] positions = new int[3];
                        for (int i = 0; i < 3; i++) {
                            positions[order[i]] = sql.length();
                            sql.append(branches[order[i]]).append(form[i + 1]);
                        }
                        if (order[foldOrder[0]] == 0 && order[foldOrder[1]] == 2) {
                            // the fold meets the pair first, as in getCommonType(CHAR, IPv4)
                            assertQuery(sql.toString())
                                    .noLeakCheck()
                                    .fails(positions[2], "inconvertible types: " + branchPair[3] + " -> " + branchPair[1]);
                        } else {
                            assertQuery(sql.toString())
                                    .noLeakCheck()
                                    .fails(positions[0], "inconvertible types: " + branchPair[1] + " -> " + branchPair[3]);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCommonType() throws Exception {
        // this test that:
        // 1. for each pair of types getCommonType() returns a valid type, unless it's explicitly ignored.
        //    this protects us against a missing entry in CaseFunction.typeEscalationMap
        // 2. getCastFunction() returns a function that can read the common type
        //    this protects us against a missing entry in CaseFunction.castFactories

        // this test is relatively invasive, it tests against internals thus do not be afraid to change it if you need to

        assertMemoryLeak(() -> {
            SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1);
            for (short l = MIN_COLUMN_TYPE_ID; l <= MAX_COLUMN_TYPE_ID; l++) {
                for (short r = MIN_COLUMN_TYPE_ID; r <= MAX_COLUMN_TYPE_ID; r++) {
                    try {
                        int commonType = CaseCommon.getCommonType(l, r, 0, "UNDEFINED is not expected");
                        Function upstreamFunction = getRndFunction(l);
                        upstreamFunction.init(null, context);
                        Function commonFunction = CaseCommon.getCastFunction(upstreamFunction, 0, commonType, configuration, null);
                        Assert.assertNotNull(commonFunction);

                        assertReadsAs(commonFunction, commonType);
                    } catch (SqlException e) {
                        Assert.assertTrue("retType = " + l + ", r = " + r, isIgnoredType(l, r));
                    }
                }
            }
        });
    }

    @Test
    public void testCommonTypeIsSymmetric() {
        // CASE, SWITCH and coalesce fold their branch types left to right, so the common
        // type of a pair must not depend on which branch comes first
        final List<Integer> types = getFoldTypes();
        for (int l : types) {
            for (int r : types) {
                Assert.assertEquals(
                        "l = " + ColumnType.nameOf(l) + ", r = " + ColumnType.nameOf(r),
                        getCommonTypeOrNone(l, r),
                        getCommonTypeOrNone(r, l)
                );
            }
        }
        Assert.assertEquals(ColumnType.STRING, getCommonTypeOrNone(ColumnType.CHAR, ColumnType.SYMBOL));
    }

    private static void addGlobalExceptions(Map<Short, Set<Short>> map) {
        for (short i = MIN_COLUMN_TYPE_ID; i <= MAX_COLUMN_TYPE_ID; i++) {
            Set<Short> ignored = map.get(i);
            boolean ignoreAll = isGeoType(i) || isAuxType(i) || i == ColumnType.BINARY || i == ColumnType.LONG128;
            for (short j = MIN_COLUMN_TYPE_ID; j <= MAX_COLUMN_TYPE_ID; j++) {
                if (ignoreAll || isGeoType(j) || isAuxType(j) || j == ColumnType.BINARY || j == ColumnType.LONG128) {
                    ignored.add(j);
                }
            }
        }
    }

    private static void addIgnored(Map<Short, Set<Short>> map, short from, short... to) {
        Set<Short> ignored = map.get(from);
        for (short t : to) {
            ignored.add(t);
        }
    }

    private static Map<Short, Set<Short>> buildIgnoredTypeMap() {
        Map<Short, Set<Short>> res = new HashMap<>();
        for (short i = MIN_COLUMN_TYPE_ID; i <= MAX_COLUMN_TYPE_ID; i++) {
            res.put(i, new HashSet<>());
        }
        addGlobalExceptions(res);
        addIgnored(
                res,
                ColumnType.BOOLEAN,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.INT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.CHAR,
                ColumnType.SYMBOL,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        );
        addIgnored(
                res,
                ColumnType.BYTE,
                ColumnType.BOOLEAN,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.CHAR,
                ColumnType.SYMBOL,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        );
        addIgnored(
                res,
                ColumnType.SHORT,
                ColumnType.BOOLEAN,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.CHAR,
                ColumnType.SYMBOL,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        );
        addIgnored(
                res,
                ColumnType.CHAR,
                ColumnType.BOOLEAN,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.INT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        );

        addIgnored(
                res,
                ColumnType.INT,
                ColumnType.BOOLEAN,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.CHAR,
                ColumnType.SYMBOL,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        );

        addIgnored(
                res,
                ColumnType.LONG,
                ColumnType.BOOLEAN,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.CHAR,
                ColumnType.SYMBOL,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        );

        addIgnored(
                res,
                ColumnType.DATE,
                ColumnType.BOOLEAN,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.INT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.CHAR,
                ColumnType.SYMBOL,
                ColumnType.TIMESTAMP
        );

        addIgnored(
                res,
                ColumnType.TIMESTAMP,
                ColumnType.BOOLEAN,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.INT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.CHAR,
                ColumnType.SYMBOL,
                ColumnType.DATE
        );

        addIgnored(
                res,
                ColumnType.FLOAT,
                ColumnType.BOOLEAN,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.CHAR,
                ColumnType.SYMBOL,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        );

        addIgnored(
                res,
                ColumnType.DOUBLE,
                ColumnType.BOOLEAN,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.CHAR,
                ColumnType.SYMBOL,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        );

        addIgnored(
                res,
                ColumnType.STRING,
                ColumnType.BOOLEAN,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.INT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        ); // we can find a common type for String and IPv4 -> String

        addIgnored(
                res,
                ColumnType.SYMBOL,
                ColumnType.BOOLEAN,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.INT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        );

        // string types do not have common type with numerics and exotics
        addIgnored(
                res,
                ColumnType.CHAR,
                ColumnType.BOOLEAN,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.INT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        );

        addIgnored(
                res,
                ColumnType.VARCHAR,
                ColumnType.BOOLEAN,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.INT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        );

        addIgnored(
                res,
                ColumnType.LONG256,
                ColumnType.BOOLEAN,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.INT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.LONG128,
                ColumnType.IPv4,
                ColumnType.UUID,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.CHAR,
                ColumnType.SYMBOL,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        );

        addIgnored(
                res,
                ColumnType.UUID,
                ColumnType.BOOLEAN,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.INT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.IPv4,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.CHAR,
                ColumnType.SYMBOL,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        ); // UUID and Long128 do not have common type, but they should


        // IPv4 has very little common types implemented
        addIgnored(
                res,
                ColumnType.IPv4,
                ColumnType.BOOLEAN,
                ColumnType.BOOLEAN,
                ColumnType.BYTE,
                ColumnType.SHORT,
                ColumnType.INT,
                ColumnType.LONG,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.LONG128,
                ColumnType.LONG256,
                ColumnType.UUID,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.CHAR,
                ColumnType.SYMBOL,
                ColumnType.DATE,
                ColumnType.TIMESTAMP
        );
        return res;
    }

    private static void assertReadsAs(Function function, int type) {
        switch (type) {
            case ColumnType.BOOLEAN:
                function.getBool(null);
                break;
            case ColumnType.BYTE:
                function.getByte(null);
                break;
            case ColumnType.SHORT:
                function.getShort(null);
                break;
            case ColumnType.CHAR:
                function.getChar(null);
                break;
            case ColumnType.INT:
                function.getInt(null);
                break;
            case ColumnType.LONG:
                function.getLong(null);
                break;
            case ColumnType.DATE:
                function.getDate(null);
                break;
            case ColumnType.TIMESTAMP:
            case ColumnType.TIMESTAMP_NANO:
                function.getTimestamp(null);
                break;
            case ColumnType.FLOAT:
                function.getFloat(null);
                break;
            case ColumnType.DOUBLE:
                function.getDouble(null);
                break;
            case ColumnType.STRING:
                function.getStrA(null);
                break;
            case ColumnType.SYMBOL:
                function.getSymbol(null);
                break;
            case ColumnType.LONG256:
                function.getLong256A(null);
                break;
            case ColumnType.UUID:
                function.getLong128Lo(null);
                function.getLong128Hi(null);
                break;
            case ColumnType.VARCHAR:
                function.getVarcharA(null);
                break;
            case ColumnType.BINARY:
                function.getBin(null);
                break;
            case ColumnType.IPv4:
                function.getIPv4(null);
                break;
            default:
                Assert.fail("Unsupported type: " + type);
        }
    }

    private static List<Integer> getFoldTypes() {
        final List<Integer> types = new ArrayList<>();
        for (short t = MIN_COLUMN_TYPE_ID; t <= MAX_COLUMN_TYPE_ID; t++) {
            if (!isGeoType(t) && !isAuxType(t) && t != ColumnType.BINARY && t != ColumnType.LONG128) {
                types.add((int) t);
            }
        }
        types.add(ColumnType.TIMESTAMP_NANO);
        return types;
    }

    private static int getCommonTypeOrNone(int l, int r) {
        try {
            return CaseCommon.getCommonType(l, r, 0, "UNDEFINED is not expected");
        } catch (SqlException e) {
            return NO_COMMON_TYPE;
        }
    }

    private static Set<Integer> getReachableCommonTypes(int branchType, List<Integer> types) {
        // the branch can come first or follow any common type
        final Set<Integer> reachable = new TreeSet<>();
        reachable.add(branchType);
        for (int type : types) {
            final int commonType = getCommonTypeOrNone(type, branchType);
            if (commonType != NO_COMMON_TYPE) {
                reachable.add(commonType);
            }
        }
        // later branches of any type escalate the common type further
        final Deque<Integer> pending = new ArrayDeque<>(reachable);
        while (!pending.isEmpty()) {
            final int commonType = pending.pop();
            for (int type : types) {
                final int nextType = getCommonTypeOrNone(commonType, type);
                if (nextType != NO_COMMON_TYPE && reachable.add(nextType)) {
                    pending.push(nextType);
                }
            }
        }
        return reachable;
    }

    private static boolean isAuxType(short type) {
        return type == ColumnType.CURSOR || type == ColumnType.VAR_ARG || type == ColumnType.RECORD;
    }

    private static boolean isGeoType(short type) {
        return type == ColumnType.GEOBYTE || type == ColumnType.GEOSHORT || type == ColumnType.GEOINT || type == ColumnType.GEOLONG || type == ColumnType.GEOHASH;
    }

    private static boolean isIgnoredType(short lType, short rType) {
        return IGNORED_TYPE.get(lType).contains(rType);
    }

    private Function getRndFunction(int type) throws SqlException {
        switch (type) {
            case ColumnType.BOOLEAN:
                return parseFunction("rnd_boolean()", null, functionParser);
            case ColumnType.BYTE:
                return parseFunction("rnd_byte()", null, functionParser);
            case ColumnType.SHORT:
                return parseFunction("rnd_short()", null, functionParser);
            case ColumnType.CHAR:
                return parseFunction("rnd_char()", null, functionParser);
            case ColumnType.INT:
                return parseFunction("rnd_int()", null, functionParser);
            case ColumnType.LONG:
                return parseFunction("rnd_long()", null, functionParser);
            case ColumnType.DATE:
                return parseFunction("rnd_date()", null, functionParser);
            case ColumnType.TIMESTAMP:
                return parseFunction("rnd_long()::timestamp", null, functionParser);
            case ColumnType.TIMESTAMP_NANO:
                return parseFunction("rnd_long()::timestamp_ns", null, functionParser);
            case ColumnType.FLOAT:
                return parseFunction("rnd_float()", null, functionParser);
            case ColumnType.DOUBLE:
                return parseFunction("rnd_double()", null, functionParser);
            case ColumnType.STRING:
                return parseFunction("rnd_str()", null, functionParser);
            case ColumnType.SYMBOL:
                return parseFunction("rnd_symbol('AA')", null, functionParser);
            case ColumnType.LONG256:
                return parseFunction("rnd_long256()", null, functionParser);
            case ColumnType.UUID:
                return parseFunction("rnd_uuid4()", null, functionParser);
            case ColumnType.VARCHAR:
                return parseFunction("rnd_varchar()", null, functionParser);
            case ColumnType.BINARY:
                return parseFunction("rnd_bin(10)", null, functionParser);
            case ColumnType.IPv4:
                return parseFunction("rnd_ipv4()", null, functionParser);

        }
        throw new AssertionError("Unsupported type: " + type);
    }
}
