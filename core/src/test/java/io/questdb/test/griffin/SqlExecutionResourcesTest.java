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

import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlExecutionResource;
import io.questdb.griffin.SqlExecutionResources;
import io.questdb.griffin.engine.functions.cast.CastToSymbolDictionary;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.text.SqlExecutionContextStub;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlExecutionResourcesTest extends AbstractCairoTest {
    @Test
    public void testCleanupFailureDoesNotSkipOtherResources() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionResources resources = new SqlExecutionResources();
            resources.begin();
            resources.bind(sqlExecutionContext);
            final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(true, sqlExecutionContext);
            dictionary.put(42, "42");
            new FailingResource("first");
            new FailingResource("second");
            resources.unbind(sqlExecutionContext);
            try {
                resources.close();
                Assert.fail("resource cleanup must throw");
            } catch (IllegalStateException e) {
                Assert.assertEquals("second", e.getMessage());
                Assert.assertEquals(1, e.getSuppressed().length);
                Assert.assertEquals("first", e.getSuppressed()[0].getMessage());
            }
            Assert.assertEquals(0, dictionary.size());
            Assert.assertEquals(0, resources.getResourceCount());
            Assert.assertFalse(resources.isOpen());
            resources.close();
        });
    }

    @Test
    public void testContextWithoutExecutionScopeUsesExplicitCleanup() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionContextStub context = new SqlExecutionContextStub(engine);
            final SqlExecutionResources resources = new SqlExecutionResources();
            context.setExecutionResources(resources);
            Assert.assertNull(context.getExecutionResources());
            final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(true, context);
            try {
                dictionary.put(42, "42");
                TestUtils.assertEquals("42", dictionary.valueOf(0, new DirectString()));
                Assert.assertEquals(0, resources.getResourceCount());
            } finally {
                dictionary.clear();
            }
            Assert.assertEquals(0, dictionary.size());
        });
    }

    @Test
    public void testInheritedBackgroundTrackerReleasesOnlyInnerDictionaries() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            try (MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                    sqlExecutionContext.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH
            )) {
                sqlExecutionContext.setMemoryTracker(tracker);
                try {
                    final long id = registry.register("inner SELECT", sqlExecutionContext);
                    final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(true, sqlExecutionContext);
                    dictionary.put(42, "42");
                    Assert.assertTrue(tracker.getUsed() > 0);
                    registry.unregister(id, sqlExecutionContext);
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(0, dictionary.size());
                    Assert.assertSame(tracker, sqlExecutionContext.getMemoryTracker());
                    Assert.assertNull(sqlExecutionContext.getExecutionResources());
                } finally {
                    sqlExecutionContext.setMemoryTracker(null);
                }
            }
        });
    }

    @Test
    public void testNestedScopesRestoreOnlyLiveBindings() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionResources outer = new SqlExecutionResources();
            final SqlExecutionResources inner = new SqlExecutionResources();
            outer.begin();
            outer.bind(sqlExecutionContext);
            inner.begin();
            inner.bind(sqlExecutionContext);
            inner.unbind(sqlExecutionContext);
            inner.close();
            Assert.assertSame(outer, sqlExecutionContext.getExecutionResources());
            inner.begin();
            inner.bind(sqlExecutionContext);
            outer.unbind(sqlExecutionContext);
            outer.close();
            // Reusing the same scope object must not restore the old execution's binding.
            outer.begin();
            inner.unbind(sqlExecutionContext);
            Assert.assertNull(sqlExecutionContext.getExecutionResources());
            inner.close();
            outer.close();
        });
    }

    @Test
    public void testReboundResourceSurvivesOldScopeRelease() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionResources compilation = new SqlExecutionResources();
            final SqlExecutionResources execution = new SqlExecutionResources();
            compilation.begin();
            compilation.bind(sqlExecutionContext);
            final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(true, sqlExecutionContext);
            dictionary.put(1, "compile");
            dictionary.clear();
            execution.begin();
            execution.bind(sqlExecutionContext);
            dictionary.init(sqlExecutionContext);
            dictionary.put(2, "execute");
            compilation.unbind(sqlExecutionContext);
            compilation.close();
            TestUtils.assertEquals("execute", dictionary.valueOf(0, new DirectString()));
            Assert.assertSame(execution, sqlExecutionContext.getExecutionResources());
            execution.unbind(sqlExecutionContext);
            execution.close();
            Assert.assertEquals(0, dictionary.size());
            Assert.assertNull(sqlExecutionContext.getExecutionResources());
        });
    }

    @Test
    public void testRegistrationFailureReleasesDictionaries() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            registry.setListener((query, id, context) -> {
                final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(true, context);
                dictionary.put(42, "42");
                throw new IllegalStateException("registration failed");
            });
            try {
                registry.register("SELECT", sqlExecutionContext);
                Assert.fail("registration must fail");
            } catch (IllegalStateException e) {
                Assert.assertEquals("registration failed", e.getMessage());
            } finally {
                registry.setListener(null);
            }
            Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
            Assert.assertNull(sqlExecutionContext.getExecutionResources());
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
            assertQuery("SELECT 42").noLeakCheck().expectSize().returns("42\n42\n");
        });
    }

    @Test
    public void testRepeatedInitRegistersOnceAndScopeCanBeReused() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionResources resources = new SqlExecutionResources();
            final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(true);
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            for (int run = 0; run < 3; run++) {
                resources.begin();
                resources.bind(sqlExecutionContext);
                try (resources) {
                    for (int init = 0; init < 100; init++) {
                        dictionary.init(sqlExecutionContext);
                    }
                    Assert.assertEquals(1, resources.getResourceCount());
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                    dictionary.put(42, "42");
                    Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS) > baseline);
                } finally {
                    resources.unbind(sqlExecutionContext);
                }
                Assert.assertEquals(0, dictionary.size());
                Assert.assertEquals(0, resources.getResourceCount());
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                Assert.assertNull(sqlExecutionContext.getExecutionResources());
            }
        });
    }

    private static class FailingResource extends SqlExecutionResource {
        private final String message;

        private FailingResource(String message) {
            this.message = message;
            registerWith(sqlExecutionContext);
        }

        @Override
        public void clear() {
            throw new IllegalStateException(message);
        }
    }
}
