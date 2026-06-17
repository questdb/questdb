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

package io.questdb.test.cairo.security;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContextFactory;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.security.PrincipalContext;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.security.ReadOnlySecurityContextFactory;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.pgwire.ReadOnlyUsersAwareSecurityContextFactory;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class SecurityContextFactoryPrincipalTest {

    @Test
    public void testAllowAllFactoryAnonymousReturnsSingleton() {
        // a null principal (anonymous, no http.user configured) keeps the shared singleton and the default name
        SecurityContext context = AllowAllSecurityContextFactory.INSTANCE.getInstance(principal(null), SecurityContextFactory.HTTP);
        Assert.assertSame(AllowAllSecurityContext.INSTANCE, context);
        TestUtils.assertEquals("admin", context.getPrincipal());
    }

    @Test
    public void testAllowAllFactoryCopiesTransientPrincipal() {
        // the principal is annotated @Transient, so the factory must copy it rather than retain the source buffer
        StringSink mutable = new StringSink();
        mutable.put("foo");
        SecurityContext context = AllowAllSecurityContextFactory.INSTANCE.getInstance(principal(mutable), SecurityContextFactory.HTTP);
        mutable.clear();
        mutable.put("somethingelse");
        TestUtils.assertEquals("foo", context.getPrincipal());
    }

    @Test
    public void testAllowAllFactoryDefaultPrincipalReturnsSingleton() {
        // an explicit "admin" principal matches the default and avoids allocating a new context
        SecurityContext context = AllowAllSecurityContextFactory.INSTANCE.getInstance(principal("admin"), SecurityContextFactory.HTTP);
        Assert.assertSame(AllowAllSecurityContext.INSTANCE, context);
    }

    @Test
    public void testAllowAllFactoryReportsConfiguredPrincipal() {
        SecurityContext context = AllowAllSecurityContextFactory.INSTANCE.getInstance(principal("foo"), SecurityContextFactory.HTTP);
        Assert.assertNotSame(AllowAllSecurityContext.INSTANCE, context);
        TestUtils.assertEquals("foo", context.getPrincipal());
        // it still allows everything
        Assert.assertTrue(context.isSystemAdmin());
        context.authorizeHttp();
    }

    @Test
    public void testAllowAllForPrincipalCachesDerivedContext() {
        // the HTTP path re-derives the context per request, so the same principal must reuse the
        // cached context instead of allocating a new one (and copying the principal) every time
        SecurityContext first = AllowAllSecurityContext.INSTANCE.forPrincipal("cacheduser");
        SecurityContext second = AllowAllSecurityContext.INSTANCE.forPrincipal("cacheduser");
        Assert.assertSame(first, second);
        Assert.assertNotSame(AllowAllSecurityContext.INSTANCE, first);
        TestUtils.assertEquals("cacheduser", first.getPrincipal());
    }

    @Test
    public void testDenyAllForPrincipalStaysDenyAll() {
        // forPrincipal must never downgrade a deny-all context to a read-allowing one
        SecurityContext context = DenyAllSecurityContext.INSTANCE.forPrincipal("foo");
        Assert.assertSame(DenyAllSecurityContext.INSTANCE, context);
        try {
            context.authorizeHttp();
            Assert.fail("expected permission denied");
        } catch (CairoException e) {
            Assert.assertTrue(e.getFlyweightMessage().toString().contains("permission denied"));
        }
    }

    @Test
    public void testForPrincipalConcurrentAlternatingPrincipalsNeverLeak() throws Exception {
        // every thread alternates between two principals, forcing constant eviction of the single-entry
        // cache on the shared singleton. Even with a context for the other principal mid-publish in the
        // cache slot, every call must return a context reporting exactly its own requested principal.
        final int threadCount = 4;
        final int iterations = 50_000;
        final CyclicBarrier barrier = new CyclicBarrier(threadCount);
        final AtomicInteger errors = new AtomicInteger();
        final ObjList<Thread> threads = new ObjList<>();
        for (int t = 0; t < threadCount; t++) {
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < iterations; i++) {
                        final String principal = (i & 1) == 0 ? "alice" : "bob";
                        SecurityContext context = AllowAllSecurityContext.INSTANCE.forPrincipal(principal);
                        if (!Chars.equals(principal, context.getPrincipal())) {
                            errors.incrementAndGet();
                        }
                    }
                } catch (Throwable th) {
                    errors.incrementAndGet();
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (int t = 0; t < threadCount; t++) {
            threads.getQuick(t).join();
        }
        Assert.assertEquals(0, errors.get());
    }

    @Test
    public void testForPrincipalConcurrentReportsOwnPrincipal() throws Exception {
        // the single-entry cache on the shared singleton is updated without a lock; under contention
        // every caller must still get a context reporting its own principal, never another thread's
        final int threadCount = 4;
        final int iterations = 50_000;
        final CyclicBarrier barrier = new CyclicBarrier(threadCount);
        final AtomicInteger errors = new AtomicInteger();
        final ObjList<Thread> threads = new ObjList<>();
        for (int t = 0; t < threadCount; t++) {
            final String principal = "user" + t;
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < iterations; i++) {
                        SecurityContext context = AllowAllSecurityContext.INSTANCE.forPrincipal(principal);
                        if (!Chars.equals(principal, context.getPrincipal())) {
                            errors.incrementAndGet();
                        }
                    }
                } catch (Throwable th) {
                    errors.incrementAndGet();
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (int t = 0; t < threadCount; t++) {
            threads.getQuick(t).join();
        }
        Assert.assertEquals(0, errors.get());
    }

    @Test
    public void testForPrincipalConcurrentSamePrincipalReusesCachedContext() throws Exception {
        // all threads request the same principal: once the single-entry cache is warmed it is never evicted
        // (no other principal is ever requested), so every concurrent caller must hit the cache and get back
        // the very same derived instance, which must always report that principal. This exercises the
        // cache-hit path under contention, which the distinct-principal test never takes.
        final String principal = "shared";
        final SecurityContext warmed = AllowAllSecurityContext.INSTANCE.forPrincipal(principal);
        Assert.assertNotSame(AllowAllSecurityContext.INSTANCE, warmed);

        final int threadCount = 4;
        final int iterations = 50_000;
        final CyclicBarrier barrier = new CyclicBarrier(threadCount);
        final AtomicInteger errors = new AtomicInteger();
        final ObjList<Thread> threads = new ObjList<>();
        for (int t = 0; t < threadCount; t++) {
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < iterations; i++) {
                        SecurityContext context = AllowAllSecurityContext.INSTANCE.forPrincipal(principal);
                        // the warmed entry is never evicted, so the same cached instance must come back
                        if (context != warmed || !Chars.equals(principal, context.getPrincipal())) {
                            errors.incrementAndGet();
                        }
                    }
                } catch (Throwable th) {
                    errors.incrementAndGet();
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (int t = 0; t < threadCount; t++) {
            threads.getQuick(t).join();
        }
        Assert.assertEquals(0, errors.get());
    }

    @Test
    public void testForPrincipalDoesNotLeakAcrossPrincipals() {
        // the single-entry cache on the shared singleton must never hand one principal's context to
        // another: every call returns a context reporting its own principal, regardless of cache state
        SecurityContext alice = AllowAllSecurityContext.INSTANCE.forPrincipal("alice");
        SecurityContext bob = AllowAllSecurityContext.INSTANCE.forPrincipal("bob");
        SecurityContext aliceAgain = AllowAllSecurityContext.INSTANCE.forPrincipal("alice");
        TestUtils.assertEquals("alice", alice.getPrincipal());
        TestUtils.assertEquals("bob", bob.getPrincipal());
        TestUtils.assertEquals("alice", aliceAgain.getPrincipal());
        Assert.assertNotSame(alice, bob);
        // "bob" evicted "alice" from the single-entry cache, so the second "alice" is a fresh derivation
        Assert.assertNotSame(alice, aliceAgain);
    }

    @Test
    public void testForPrincipalEmptyStringKeepsSingleton() {
        // an empty principal is treated as anonymous, like null: it keeps the shared singleton and the
        // default name rather than deriving a context that reports an empty principal
        SecurityContext context = AllowAllSecurityContext.INSTANCE.forPrincipal("");
        Assert.assertSame(AllowAllSecurityContext.INSTANCE, context);
        TestUtils.assertEquals("admin", context.getPrincipal());
    }

    @Test
    public void testForPrincipalWithNullCurrentPrincipalDoesNotThrow() {
        // forPrincipal compares the requested principal against getPrincipal(); a subclass that reports
        // a null principal must not NPE (Chars.equals is @NotNull). It derives a context for the
        // requested principal instead of matching the singleton.
        AllowAllSecurityContext nullPrincipal = new AllowAllSecurityContext() {
            @Override
            public CharSequence getPrincipal() {
                return null;
            }
        };
        SecurityContext derived = nullPrincipal.forPrincipal("foo");
        Assert.assertNotSame(nullPrincipal, derived);
        TestUtils.assertEquals("foo", derived.getPrincipal());
    }

    @Test
    public void testReadOnlyFactoryReportsConfiguredPrincipal() {
        SecurityContext context = ReadOnlySecurityContextFactory.INSTANCE.getInstance(principal("foo"), SecurityContextFactory.HTTP);
        Assert.assertNotSame(ReadOnlySecurityContext.INSTANCE, context);
        TestUtils.assertEquals("foo", context.getPrincipal());
        // the derived context is still read-only: writes are denied
        try {
            context.authorizeInsert(null);
            Assert.fail("expected write to be denied");
        } catch (CairoException e) {
            Assert.assertTrue(e.getFlyweightMessage().toString().contains("Write permission denied"));
        }
    }

    @Test
    public void testReadOnlyUsersAwareFactoryDefaultInterfaceReportsConfiguredPrincipal() {
        // the default branch (e.g. ILP, interface id other than HTTP/PGWIRE) yields an allow-all context
        // that still reports the authenticated user, and is allow-all regardless of httpReadOnly
        ReadOnlyUsersAwareSecurityContextFactory factory = new ReadOnlyUsersAwareSecurityContextFactory(false, null, true);
        SecurityContext context = factory.getInstance(principal("foo"), SecurityContextFactory.ILP);
        TestUtils.assertEquals("foo", context.getPrincipal());
        Assert.assertTrue(context.isSystemAdmin());
        context.authorizeInsert(null);
        // a null principal on the default branch keeps the shared singleton
        Assert.assertSame(AllowAllSecurityContext.INSTANCE, factory.getInstance(principal(null), SecurityContextFactory.ILP));
    }

    @Test
    public void testReadOnlyUsersAwareFactoryReportsConfiguredPrincipal() {
        ReadOnlyUsersAwareSecurityContextFactory factory = new ReadOnlyUsersAwareSecurityContextFactory(false, null, false);

        SecurityContext http = factory.getInstance(principal("foo"), SecurityContextFactory.HTTP);
        TestUtils.assertEquals("foo", http.getPrincipal());
        Assert.assertTrue(http.isSystemAdmin());

        SecurityContext pgWire = factory.getInstance(principal("foo"), SecurityContextFactory.PGWIRE);
        TestUtils.assertEquals("foo", pgWire.getPrincipal());

        // anonymous/default keeps the shared singleton
        Assert.assertSame(AllowAllSecurityContext.INSTANCE, factory.getInstance(principal(null), SecurityContextFactory.HTTP));
    }

    @Test
    public void testReadOnlyUsersAwareFactoryReportsReadOnlyPgWireUser() {
        // the read-only pgwire user gets a read-only context that still reports its own name
        ReadOnlyUsersAwareSecurityContextFactory factory = new ReadOnlyUsersAwareSecurityContextFactory(false, "ro_user", false);
        SecurityContext context = factory.getInstance(principal("ro_user"), SecurityContextFactory.PGWIRE);
        TestUtils.assertEquals("ro_user", context.getPrincipal());
        Assert.assertFalse(context.isQueryCancellationAllowed());
    }

    @Test
    public void testSettingsReadOnlyFactoryHttpAllowAllReportsConfiguredPrincipal() {
        // the allow-all settings-read-only HTTP branch (httpReadOnly=false) derives from
        // AllowAllSecurityContext.SETTINGS_READ_ONLY: it allows everything except writing settings,
        // while reporting the configured principal
        ReadOnlyUsersAwareSecurityContextFactory factory = new ReadOnlyUsersAwareSecurityContextFactory(false, null, false, true);
        SecurityContext context = factory.getInstance(principal("foo"), SecurityContextFactory.HTTP);
        TestUtils.assertEquals("foo", context.getPrincipal());
        // allow-all: cancellation is allowed and it is a system admin, and writes are permitted
        Assert.assertTrue(context.isQueryCancellationAllowed());
        Assert.assertTrue(context.isSystemAdmin());
        context.authorizeHttp();
        context.authorizeInsert(null);
        // but the settings endpoint stays read-only
        try {
            context.authorizeSettings();
            Assert.fail("expected settings to be read-only");
        } catch (CairoException e) {
            Assert.assertTrue(e.getFlyweightMessage().toString().contains("read-only"));
        }
    }

    @Test
    public void testSettingsReadOnlyFactoryHttpReportsConfiguredPrincipal() {
        // the read-only settings-read-only HTTP branch (httpReadOnly=true) derives from
        // ReadOnlySecurityContext.SETTINGS_READ_ONLY, keeping both the read-only and settings-read-only
        // restrictions while reporting the configured principal
        ReadOnlyUsersAwareSecurityContextFactory factory = new ReadOnlyUsersAwareSecurityContextFactory(false, null, true, true);
        SecurityContext context = factory.getInstance(principal("foo"), SecurityContextFactory.HTTP);
        TestUtils.assertEquals("foo", context.getPrincipal());
        Assert.assertFalse(context.isQueryCancellationAllowed());
        try {
            context.authorizeSettings();
            Assert.fail("expected settings to be read-only");
        } catch (CairoException e) {
            Assert.assertTrue(e.getFlyweightMessage().toString().contains("read-only"));
        }
    }

    @Test
    public void testSettingsReadOnlyForPrincipalStaysSettingsReadOnly() {
        // forPrincipal on the settings-read-only singleton must keep the settings restriction
        SecurityContext context = AllowAllSecurityContext.SETTINGS_READ_ONLY.forPrincipal("foo");
        Assert.assertNotSame(AllowAllSecurityContext.SETTINGS_READ_ONLY, context);
        TestUtils.assertEquals("foo", context.getPrincipal());
        try {
            context.authorizeSettings();
            Assert.fail("expected settings to be read-only");
        } catch (CairoException e) {
            Assert.assertTrue(e.getFlyweightMessage().toString().contains("read-only"));
        }
        // it still allows everything else
        context.authorizeHttp();
    }

    private static PrincipalContext principal(CharSequence name) {
        return new PrincipalContext() {
            @Override
            public byte getAuthType() {
                return SecurityContext.AUTH_TYPE_CREDENTIALS;
            }

            @Override
            public ReadOnlyObjList<CharSequence> getGroups() {
                return new ObjList<>();
            }

            @Override
            public CharSequence getPrincipal() {
                return name;
            }
        };
    }
}
