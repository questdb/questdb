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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.engine.functions.cast.CastBinaryToVarcharFunctionFactory;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * End-to-end SQL test of cast(BINARY as VARCHAR).
 * <p>
 * Each test goes through the SQL parser, FunctionParser overload
 * resolution, the function factory cache, and the cast function
 * itself. Catches two failure modes: registration (factory not in
 * function_list.txt) and overload routing (signature flag bits
 * preventing the matcher from binding a BINARY column to the value
 * slot). Byte-level UTF-8 validator behaviour has no SQL surface
 * for constructing arbitrary-byte BINARY values, so that surface
 * is covered separately.
 */
public class CastBinaryToVarcharFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastNullBinaryReturnsNullVarchar() throws Exception {
        assertQuery(
                "cast\n",
                "select cast(b as varchar) from t",
                "create table t (b binary)",
                null,
                "insert into t select cast(null as binary) from long_sequence(3)",
                "cast\n\n\n\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCastRecognisedBySqlCompiler() throws Exception {
        // Smoke: the compiler must produce a plan for `cast(b as varchar)`
        // where b is a BINARY column. Failure mode this guards against:
        // factory not in function_list.txt, or the matcher rejecting the
        // BINARY column before the factory is considered.
        assertQuery(
                "cast\n",
                "select cast(b as varchar) from t",
                "create table t (b binary)",
                null,
                true,
                false
        );
    }

    @Test
    public void testFactoryIsRegisteredAsCastOverload() {
        // The classpath scanner must pick up CastBinaryToVarcharFunctionFactory
        // and register it under the token "cast". If this fails the factory
        // is missing from function_list.txt or the scanner is misconfigured.
        // The flag asserts pin the matcher contract: arg0 is the BINARY value
        // (non-constant), arg1 is the VARCHAR type literal (constant). Drop
        // either flag and the matcher falls into the wrong overload bucket.
        ObjList<FunctionFactoryDescriptor> overloads = engine.getFunctionFactoryCache().getOverloadList("cast");
        Assert.assertNotNull("`cast` overload list missing", overloads);
        boolean found = false;
        for (int i = 0, n = overloads.size(); i < n; i++) {
            if (overloads.getQuick(i).getFactory() instanceof CastBinaryToVarcharFunctionFactory) {
                FunctionFactoryDescriptor d = overloads.getQuick(i);
                Assert.assertEquals("arg count", 2, d.getSigArgCount());
                Assert.assertEquals("arg0 is BINARY",
                        ColumnType.BINARY,
                        FunctionFactoryDescriptor.toTypeTag(d.getArgTypeWithFlags(0)));
                Assert.assertFalse("arg0 must NOT be const (BINARY column must bind to it)",
                        FunctionFactoryDescriptor.isConstant(d.getArgTypeWithFlags(0)));
                Assert.assertEquals("arg1 is VARCHAR",
                        ColumnType.VARCHAR,
                        FunctionFactoryDescriptor.toTypeTag(d.getArgTypeWithFlags(1)));
                Assert.assertTrue("arg1 must be const (type literal)",
                        FunctionFactoryDescriptor.isConstant(d.getArgTypeWithFlags(1)));
                found = true;
                break;
            }
        }
        Assert.assertTrue("CastBinaryToVarcharFunctionFactory not in cast overload list", found);
    }
}
