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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Coverage of the type an untyped bind variable takes under a CAST ({@code $1::T}), which
 * {@code FunctionParser.createFunction} answers per CAST target (F34 keeps it per type). Text
 * targets give STRING, number targets DOUBLE, array and decimal targets themselves; any other
 * target leaves the variable to the cast overload that matches first. The table pins every type's
 * answer, the overload picks included, so a new CAST target shows up here with the type its
 * variable takes. ARRAY_STRING's error is a defect, pinned until fixed (issue
 * sql-cast-bind-text-array): the cast factory reads the unbound variable at compile time.
 */
public class CastBindVariableTypeTest extends AbstractCairoTest {

    @Test
    public void testBindVariableTypeByCastTarget() throws Exception {
        assertMemoryLeak(() -> {
            final StringSink sink = new StringSink();
            for (int t = 0; t < QueryEngineTypeFactsTest.TYPES.length; t++) {
                final String label = QueryEngineTypeFactsTest.LABELS[t];
                final String name = ColumnType.nameOf(QueryEngineTypeFactsTest.TYPES[t]);
                sink.put(label);
                for (int k = label.length(); k < 16; k++) {
                    sink.put(' ');
                }
                bindVariableService.clear();
                try (RecordCursorFactory ignored = select("select $1::" + name)) {
                    final Function variable = bindVariableService.getFunction(0);
                    sink.put(variable != null ? ColumnType.nameOf(variable.getType()) : "-");
                } catch (SqlException e) {
                    sink.put("error: [").put(e.getFlyweightMessage()).put(']');
                }
                sink.put('\n');
            }
            bindVariableService.clear();
            TestUtils.assertEquals(
                    """
                            UNDEFINED       error: [invalid constant: unknown]
                            BOOLEAN         DOUBLE
                            BYTE            DOUBLE
                            SHORT           DOUBLE
                            CHAR            STRING
                            INT             DOUBLE
                            LONG            DOUBLE
                            DATE            DOUBLE
                            TIMESTAMP       DOUBLE
                            FLOAT           DOUBLE
                            DOUBLE          DOUBLE
                            STRING          STRING
                            SYMBOL          DOUBLE
                            LONG256         DOUBLE
                            GEOBYTE         error: [invalid constant: unknown]
                            GEOSHORT        error: [invalid constant: unknown]
                            GEOINT          error: [invalid constant: unknown]
                            GEOLONG         error: [invalid constant: unknown]
                            BINARY          STRING
                            UUID            STRING
                            CURSOR          error: [invalid constant: CURSOR]
                            VAR_ARG         error: [invalid constant: VARARG]
                            RECORD          error: [invalid constant: RECORD]
                            GEOHASH         error: [invalid GEOHASH size, must be number followed by 'C' or 'B' character]
                            LONG128         error: [invalid constant: LONG128]
                            IPv4            STRING
                            VARCHAR         STRING
                            ARRAY           error: [ARRAY not followed by '[']
                            DECIMAL8        error: [invalid constant: unknown]
                            DECIMAL16       error: [invalid constant: unknown]
                            DECIMAL32       error: [invalid constant: unknown]
                            DECIMAL64       error: [invalid constant: unknown]
                            DECIMAL128      error: [invalid constant: unknown]
                            DECIMAL256      error: [invalid constant: unknown]
                            DECIMAL         DECIMAL(18,3)
                            REGCLASS        STRING
                            REGPROCEDURE    STRING
                            ARRAY_STRING    error: [exception in function factory: ]
                            PARAMETER       error: [invalid constant: PARAMETER]
                            INTERVAL        error: [there is no matching function `cast` with the argument types: (unknown, INTERVAL)]
                            VARCHAR_SLICE   error: [invalid constant: VARCHAR_SLICE]
                            NULL            error: [there is no matching function `cast` with the argument types: (unknown, NULL)]
                            TIMESTAMP_NS    DOUBLE
                            GEOHASH(1c)     STRING
                            GEOHASH(8b)     STRING
                            GEOHASH(31b)    STRING
                            GEOHASH(12c)    STRING
                            DECIMAL(5,2)    DECIMAL(5,2)
                            DECIMAL(18,3)   DECIMAL(18,3)
                            DOUBLE[]        DOUBLE[]
                            DOUBLE[][]      DOUBLE[][]
                            INTERVAL(us)    error: [there is no matching function `cast` with the argument types: (unknown, INTERVAL)]
                            INTERVAL(ns)    error: [there is no matching function `cast` with the argument types: (unknown, INTERVAL)]
                            """,
                    sink
            );
        });
    }
}
