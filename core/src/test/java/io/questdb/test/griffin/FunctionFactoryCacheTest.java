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
import io.questdb.test.griffin.scan.ThrowingConstructorFunctionFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * The function library reaches the parser only through {@link FunctionFactoryScanner} and
 * {@link FunctionFactoryCache}, and both used to skip what they could not load with a log line
 * as the only trace. A factory for a new type that names it with an unknown signature
 * character would vanish that way, and the function would resolve to another type's overload.
 * These tests pin that nothing is skipped today and that, with assertions on (the test
 * configuration), a skip is an error.
 */
public class FunctionFactoryCacheTest extends AbstractCairoTest {

    @Test
    public void testDroppedFactoryFailsFastWithAssertionsOn() throws Exception {
        Assume.assumeTrue("assertions are off", FunctionFactoryCache.class.desiredAssertionStatus());
        assertMemoryLeak(() -> {
            // 'y' is not a signature character
            final ArrayList<FunctionFactory> factories = new ArrayList<>();
            factories.add(factoryOf("f(y)"));
            try {
                new FunctionFactoryCache(configuration, factories);
                Assert.fail();
            } catch (AssertionError e) {
                TestUtils.assertContains(e.getMessage(), "function factory dropped: ");
                TestUtils.assertContains(e.getMessage(), "[signature=f(y), reason=illegal argument type: `y`]");
            }
        });
    }

    @Test
    public void testNotScannedFactoryFailsFastWithAssertionsOn() throws Exception {
        Assume.assumeTrue("assertions are off", FunctionFactoryScanner.class.desiredAssertionStatus());
        assertMemoryLeak(() -> {
            final ArrayList<FunctionFactory> factories = new ArrayList<>();
            try {
                FunctionFactoryScanner.scan(
                        factories,
                        ThrowingConstructorFunctionFactory.class.getPackageName(),
                        "function_list.txt",
                        ThrowingConstructorFunctionFactory.class,
                        "io.questdb",
                        null
                );
                Assert.fail();
            } catch (AssertionError e) {
                TestUtils.assertContains(e.getMessage(), "function factory not scanned: " + ThrowingConstructorFunctionFactory.class.getName());
                TestUtils.assertContains(e.getMessage(), "error: java.lang.reflect.InvocationTargetException");
            }
        });
    }

    @Test
    public void testScanDropsNoFactory() throws Exception {
        assertMemoryLeak(() -> {
            final Set<FunctionFactory> scanned = Collections.newSetFromMap(new IdentityHashMap<>());
            for (FunctionFactory factory : new FunctionFactoryCacheBuilder().scan(LOG).build()) {
                scanned.add(factory);
            }
            Assert.assertFalse(scanned.isEmpty());

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
