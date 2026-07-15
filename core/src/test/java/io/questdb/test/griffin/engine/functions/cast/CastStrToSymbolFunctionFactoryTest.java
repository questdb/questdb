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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class CastStrToSymbolFunctionFactoryTest extends AbstractCairoTest {

    // A lazy cast-to-symbol only needs a dictionary to answer the integer-key API
    // (getInt/valueOf). getSymbol()/getSymbolB() are pure pass-throughs: the wire
    // serializers, ORDER BY and the non-static GROUP BY key sink all read the column
    // through getSymbol on every row, so interning there is dead work that grows an
    // unbounded, unaccounted on-heap dictionary for values no consumer ever looks up by
    // key. This pins the contract: a value seen only through getSymbol consumes no symbol
    // key, so the first value routed through getInt is assigned key 0.
    @Test
    public void testGetSymbolIsPassThroughAndConsumesNoSymbolKey() {
        final FeedFunction arg = new FeedFunction();
        final CastStrToSymbolFunctionFactory.Func func = new CastStrToSymbolFunctionFactory.Func(arg);

        // getSymbol reads the A-slot and getSymbolB the B-slot; both pass their argument
        // through verbatim without touching the dictionary. Distinct A/B feed values prove
        // getSymbolB reads getStrB, not getStrA.
        arg.valueA = "a_via_getSymbol";
        arg.valueB = "b_via_getSymbolB";
        Assert.assertEquals("a_via_getSymbol", func.getSymbol(null));
        Assert.assertEquals("b_via_getSymbolB", func.getSymbolB(null));
        arg.valueA = null;
        Assert.assertNull(func.getSymbol(null));

        // Those getSymbol calls must have consumed no symbol keys, so the first value
        // routed through getInt is assigned key 0.
        arg.valueA = "seen_via_getInt";
        Assert.assertEquals(0, func.getInt(null));
        arg.valueA = "second_via_getInt";
        Assert.assertEquals(1, func.getInt(null));

        // getInt/valueOf round-trip, and a repeated value reuses its key.
        Assert.assertEquals("seen_via_getInt", func.valueOf(0));
        Assert.assertEquals("second_via_getInt", func.valueOf(1));
        arg.valueA = "seen_via_getInt";
        Assert.assertEquals(0, func.getInt(null));

        // NULL maps to the null sentinel and resolves back to null.
        arg.valueA = null;
        Assert.assertEquals(SymbolTable.VALUE_IS_NULL, func.getInt(null));
        Assert.assertNull(func.valueOf(SymbolTable.VALUE_IS_NULL));
    }

    private static class FeedFunction extends StrFunction {
        CharSequence valueA;
        CharSequence valueB;

        @Override
        public CharSequence getStrA(Record rec) {
            return valueA;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return valueB;
        }
    }
}
