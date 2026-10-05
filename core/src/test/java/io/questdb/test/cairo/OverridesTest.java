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


package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class OverridesTest extends AbstractCairoTest {

    @Test
    public void testNoOpRemoveKeepsPendingChange() {
        setProperty(PropertyKey.CAIRO_SQL_SYMBOL_IN_BITSET_ENABLED, "false");
        Assert.assertFalse(configuration().isSqlSymbolInBitsetEnabled());
        setProperty(PropertyKey.CAIRO_SQL_SYMBOL_IN_BITSET_ENABLED, "true");
        // Removing a property that was never set changes nothing, and must not drop the change above.
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_SQL_JIT_SYMBOL_IN_BITSET_ENABLED, (String) null);
        Assert.assertTrue(configuration().isSqlSymbolInBitsetEnabled());
    }

    @Test
    public void testNoOpSetKeepsPendingChange() {
        // The sequence that made SymbolInBitSetTest#testLargeSymbolTableFallsBack vacuous: a set to
        // the value a property already holds used to clear the change pending from the sets before it.
        setProperty(PropertyKey.CAIRO_SQL_SYMBOL_IN_BITSET_MAX_KEYS, 100);
        setProperty(PropertyKey.CAIRO_SQL_SYMBOL_IN_BITSET_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_SQL_JIT_SYMBOL_IN_BITSET_ENABLED, "false");
        Assert.assertFalse(configuration().isSqlJitSymbolInBitsetEnabled());
        Assert.assertFalse(configuration().isSqlSymbolInBitsetEnabled());

        setProperty(PropertyKey.CAIRO_SQL_SYMBOL_IN_BITSET_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_JIT_SYMBOL_IN_BITSET_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_SYMBOL_IN_BITSET_MAX_KEYS, 100);
        final CairoConfiguration configuration = configuration();
        Assert.assertTrue(configuration.isSqlJitSymbolInBitsetEnabled());
        Assert.assertTrue(configuration.isSqlSymbolInBitsetEnabled());
        Assert.assertEquals(100, configuration.getSqlSymbolInBitsetMaxKeys());
    }

    private static CairoConfiguration configuration() {
        // The engine's configuration reads the overrides on every call.
        return engine.getConfiguration();
    }
}
