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

package io.questdb.test.griffin.engine.functions;

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.CursorFunction;
import org.junit.Assert;
import org.junit.Test;

public class CursorFunctionTest {
    private static final CursorFunction function = new CursorFunction(null);

    @Test(expected = UnsupportedOperationException.class)
    public void testChar() {
        function.getChar(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetArray() {
        function.getArray(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBin() {
        function.getBin(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBinLen() {
        function.getBinLen(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBool() {
        function.getBool(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetByte() {
        function.getByte(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDate() {
        function.getDate(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal128() {
        function.getDecimal128(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal16() {
        function.getDecimal16(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal256() {
        function.getDecimal256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal32() {
        function.getDecimal32(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal64() {
        function.getDecimal64(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal8() {
        function.getDecimal8(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDouble() {
        function.getDouble(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetFloat() {
        function.getFloat(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoByte() {
        function.getGeoByte(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoInt() {
        function.getGeoInt(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoLong() {
        function.getGeoLong(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoShort() {
        function.getGeoShort(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetIPv4() {
        function.getInt(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetInt() {
        function.getInt(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong() {
        function.getLong(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong128Hi() {
        function.getLong128Hi(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong128Lo() {
        function.getLong128Lo(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong256() {
        function.getLong256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong256A() {
        function.getLong256A(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong256B() {
        function.getLong256B(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetShort() {
        function.getShort(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStr() {
        function.getStrA(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStrB() {
        function.getStrB(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStrLen() {
        function.getStrLen(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetSym() {
        function.getSymbol(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetSymbolB() {
        function.getSymbolB(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetTimestamp() {
        function.getTimestamp(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetVarcharA() {
        function.getVarcharA(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetVarcharB() {
        function.getVarcharB(null);
    }

    /**
     * {@code RecordCursorFactory#isNonDeterministic()} is a fail-safe optimizer hint that defaults to
     * true for the ~97 of 114 factories that never override it. {@code Function#isNonDeterministic()}
     * is a fail-open legality flag, read by the materialized-view guard in {@code FunctionParser},
     * that defaults to false. Delegating across that polarity boundary makes {@code BinaryFunction}
     * OR the fail-safe true up through any enclosing operator, so a predicate such as
     * {@code n > (SELECT count() FROM t)} is rejected as a non-deterministic use of the operator.
     * CursorFunction must therefore keep the fail-open Function default.
     */
    @Test
    public void testDoesNotInheritFactoryFailSafeNonDeterminism() {
        try (CursorFunction cursorFunction = new CursorFunction(new EmptyTableRecordCursorFactory(new GenericRecordMetadata()))) {
            // the factory reports the fail-safe optimizer default...
            Assert.assertTrue(cursorFunction.getRecordCursorFactory().isNonDeterministic());
            // ...which must not leak into the legality flag consulted by the mat-view guard
            Assert.assertFalse(cursorFunction.isNonDeterministic());
        }
    }

    /**
     * External-source reporting is the fail-open, opt-in property the materialized-view guard is
     * allowed to reject on, and it must propagate from the factory tree.
     * <p>
     * The guard in {@code FunctionParser} reads the flag as
     * {@code function.getRecordCursorFactory().usesExternalDataSource()}, and the interface default
     * resolves it by walking {@code getBaseFactory()}. The case that matters is therefore a wrapper
     * that never overrides the flag sitting above a leaf that does: asserting on a factory that
     * overrides it directly would only re-read a value this test hard-coded, and would exercise no
     * production logic beyond a getter.
     */
    @Test
    public void testUsesExternalDataSourcePropagatesThroughFactoryTree() {
        try (
                RecordCursorFactory externalLeaf = new EmptyTableRecordCursorFactory(new GenericRecordMetadata()) {
                    @Override
                    public boolean usesExternalDataSource() {
                        return true;
                    }
                }
        ) {
            try (
                    CursorFunction internal = new CursorFunction(new EmptyTableRecordCursorFactory(new GenericRecordMetadata()));
                    // a wrapper that only exposes a base and never overrides the flag - the shape of
                    // every projection/filter/sort factory the guard walks through
                    CursorFunction external = new CursorFunction(new EmptyTableRecordCursorFactory(new GenericRecordMetadata()) {
                        @Override
                        public RecordCursorFactory getBaseFactory() {
                            return externalLeaf;
                        }
                    })
            ) {
                Assert.assertFalse(
                        "a tree with no external scan beneath it must report false",
                        internal.getRecordCursorFactory().usesExternalDataSource()
                );
                Assert.assertTrue(
                        "a wrapper that never overrides the flag must still surface the external scan beneath it",
                        external.getRecordCursorFactory().usesExternalDataSource()
                );
            }
        }
    }
}
