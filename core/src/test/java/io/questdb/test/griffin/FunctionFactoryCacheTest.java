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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionFactoryCacheBuilder;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionFactoryScanner;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.scan.LoadableFunctionFactory;
import io.questdb.test.griffin.scan.ThrowingConstructorFunctionFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * The function library reaches the parser only through {@link FunctionFactoryScanner} and
 * {@link FunctionFactoryCache}, and both skip what they cannot load, with a log line as the only
 * trace. A factory for a new type that names it with an unknown signature character would vanish
 * that way, and the function would resolve to another type's overload. These tests pin that the
 * production scan skips nothing and that a broken factory is skipped, not fatal.
 */
public class FunctionFactoryCacheTest extends AbstractCairoTest {

    @Test
    public void testBadSignatureFactoryIsSkipped() throws Exception {
        assertMemoryLeak(() -> {
            // 'y' is not a signature character
            final ArrayList<FunctionFactory> factories = new ArrayList<>();
            factories.add(factoryOf("f(y)"));
            factories.add(factoryOf("g(I)"));
            final FunctionFactoryCache cache = new FunctionFactoryCache(configuration, factories);
            Assert.assertNull(cache.getOverloadList("f"));
            Assert.assertEquals(1, cache.getOverloadList("g").size());
        });
    }

    @Test
    public void testScanDropsNoFactory() throws Exception {
        assertMemoryLeak(() -> {
            // the engine's startup already filled the static cache; without this the builder returns it without scanning
            FunctionFactoryCacheBuilder.clearCache();
            final FunctionFactoryCacheBuilder builder = new FunctionFactoryCacheBuilder().scan(LOG);
            final Set<FunctionFactory> scanned = Collections.newSetFromMap(new IdentityHashMap<>());
            for (FunctionFactory factory : builder.build()) {
                scanned.add(factory);
            }
            Assert.assertFalse(scanned.isEmpty());
            Assert.assertEquals("scanner dropped: " + builder.getDroppedClassNames(), 0, builder.getDroppedClassNames().size());

            // the test configuration enables the test factories, so every scanned factory is admitted
            Assert.assertTrue(configuration.enableTestFactories());
            final FunctionFactoryCache cache = new FunctionFactoryCache(configuration, scanned);
            final Set<FunctionFactory> cached = Collections.newSetFromMap(new IdentityHashMap<>());
            cache.getFactories().forEach((name, overloads) -> {
                for (int j = 0, m = overloads.size(); j < m; j++) {
                    cached.add(overloads.getQuick(j).getFactory());
                }
            });
            // the cache adds negating and swapping wrappers on top; nothing scanned may be missing
            for (FunctionFactory factory : scanned) {
                Assert.assertTrue("dropped: " + factory.getClass().getName() + " " + factory.getSignature(), cached.contains(factory));
            }
        });
    }

    @Test
    public void testThrowingConstructorFactoryIsSkipped() throws Exception {
        assertMemoryLeak(() -> {
            final ArrayList<FunctionFactory> factories = new ArrayList<>();
            final ObjList<String> dropped = new ObjList<>();
            FunctionFactoryScanner.scan(
                    factories,
                    ThrowingConstructorFunctionFactory.class.getPackageName(),
                    "function_list.txt",
                    ThrowingConstructorFunctionFactory.class,
                    "io.questdb",
                    LOG,
                    dropped
            );
            Assert.assertEquals(1, factories.size());
            Assert.assertTrue(factories.get(0) instanceof LoadableFunctionFactory);
            Assert.assertEquals(1, dropped.size());
            Assert.assertEquals(ThrowingConstructorFunctionFactory.class.getName(), dropped.get(0));
        });
    }

    private static FunctionFactory factoryOf(String signature) {
        return new FunctionFactory() {
            @Override
            public String getSignature() {
                return signature;
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
