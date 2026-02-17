/*******************************************************************************
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
    private FunctionParser functionParser;

    @Before
    public void setUpParser() {
        functionParser = new FunctionParser(configuration, new FunctionFactoryCache(
                configuration,
                new FunctionFactoryCacheBuilder().scan(LOG).build()
        ));
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

                        // check we can read the expected type
                        switch (commonType) {
                            case ColumnType.BOOLEAN:
                                commonFunction.getBool(null);
                                break;
                            case ColumnType.BYTE:
                                commonFunction.getByte(null);
                                break;
                            case ColumnType.SHORT:
                                commonFunction.getShort(null);
                                break;
                            case ColumnType.CHAR:
                                commonFunction.getChar(null);
                                break;
                            case ColumnType.INT:
                                commonFunction.getInt(null);
                                break;
                            case ColumnType.LONG:
                                commonFunction.getLong(null);
                                break;
                            case ColumnType.DATE:
                                commonFunction.getDate(null);
                                break;
                            case ColumnType.TIMESTAMP:
                                commonFunction.getTimestamp(null);
                                break;
                            case ColumnType.FLOAT:
                                commonFunction.getFloat(null);
                                break;
                            case ColumnType.DOUBLE:
                                commonFunction.getDouble(null);
                                break;
                            case ColumnType.STRING:
                                commonFunction.getStrA(null);
                                break;
                            case ColumnType.SYMBOL:
                                commonFunction.getSymbol(null);
                                break;
                            case ColumnType.LONG256:
                                commonFunction.getLong256A(null);
                                break;
                            case ColumnType.UUID:
                                commonFunction.getLong128Lo(null);
                                commonFunction.getLong128Hi(null);
                                break;
                            case ColumnType.VARCHAR:
                                commonFunction.getVarcharA(null);
                                break;
                            case ColumnType.BINARY:
                                commonFunction.getBin(null);
                                break;
                            case ColumnType.IPv4:
                                commonFunction.getIPv4(null);
                                break;
                            default:
                                Assert.fail("Unsupported type: " + commonType);
                        }

                    } catch (SqlException e) {
                        Assert.assertTrue("retType = " + l + ", r = " + r, isIgnoredType(l, r));
                    }
                }
            }
        });
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

    private static boolean isAuxType(short type) {
        return type == ColumnType.CURSOR || type == ColumnType.VAR_ARG || type == ColumnType.RECORD;
    }

    private static boolean isGeoType(short type) {
        return type == ColumnType.GEOBYTE || type == ColumnType.GEOSHORT || type == ColumnType.GEOINT || type == ColumnType.GEOLONG || type == ColumnType.GEOHASH;
    }

    private static boolean isIgnoredType(short lType, short rType) {
        return IGNORED_TYPE.get(lType).contains(rType);
    }

    private Function getRndFunction(short type) throws SqlException {
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
