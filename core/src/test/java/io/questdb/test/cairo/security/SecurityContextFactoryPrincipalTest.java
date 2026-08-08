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
import io.questdb.cairo.security.AbstractPrincipalAwareSecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContextFactory;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.security.PrincipalContext;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.security.ReadOnlySecurityContextFactory;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.pgwire.ReadOnlyUsersAwareSecurityContextFactory;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SecurityContextFactoryPrincipalTest {

    // read from production, not copied: a silently drifting copy would make every boundary test below
    // pass while testing the wrong number
    private static final int CACHE_CAP = AbstractPrincipalAwareSecurityContext.MAX_CACHED_PRINCIPALS;

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(60, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testAllowAllFactoryAnonymousReturnsSingleton() {
        // a null principal (anonymous, no http.user configured) keeps the shared singleton and the default name
        SecurityContext context = AllowAllSecurityContextFactory.INSTANCE.getInstance(principal(null), SecurityContextFactory.HTTP);
        Assert.assertSame(AllowAllSecurityContext.INSTANCE, context);
        TestUtils.assertEquals("admin", context.getPrincipal());
    }

    @Test
    public void testAllowAllFactoryCopiesTransientPrincipal() {
        // The principal is @Transient, so the factory must copy it rather than retain the caller's buffer.
        //
        // The name has to be one no other test derives on AllowAllSecurityContext.INSTANCE. That cache is a
        // JVM-lifetime static with no eviction and no reset hook, and surefire reuses the fork, so a name
        // another test already seeded (HttpSecurityTest runs with http.user=foo) turns this into a cache HIT
        // -- and the hit path does not copy. It hands back the context that other test derived, whose
        // principal is already an immutable String, and the assertion below then holds whether or not
        // forPrincipal copies anything at all.
        StringSink mutable = new StringSink();
        mutable.put("allowallfactorytransientprobe");
        SecurityContext context = AllowAllSecurityContextFactory.INSTANCE.getInstance(principal(mutable), SecurityContextFactory.HTTP);
        Assert.assertNotSame("must be a derived context, not the singleton", AllowAllSecurityContext.INSTANCE, context);
        mutable.clear();
        mutable.put("somethingelse");
        TestUtils.assertEquals("allowallfactorytransientprobe", context.getPrincipal());
    }

    @Test
    public void testAllowAllFactoryDefaultPrincipalReturnsSingleton() {
        // an explicit "admin" principal matches the default and avoids allocating a new context
        SecurityContext context = AllowAllSecurityContextFactory.INSTANCE.getInstance(principal("admin"), SecurityContextFactory.HTTP);
        Assert.assertSame(AllowAllSecurityContext.INSTANCE, context);
    }

    @Test
    public void testAllowAllFactoryIlpDoesNotRouteTransportCredential() {
        // ILP's principal is a JWK key id (a transport credential, not an ACL identity), so the factory must
        // hand back the bare singleton rather than route it through the process-lifetime per-principal cache.
        SecurityContext context = AllowAllSecurityContextFactory.INSTANCE.getInstance(principal("keyid"), SecurityContextFactory.ILP);
        Assert.assertSame(AllowAllSecurityContext.INSTANCE, context);
        TestUtils.assertEquals("admin", context.getPrincipal());
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
        AllowAllSecurityContext root = freshAllowAll();
        SecurityContext first = root.forPrincipal("cacheduser");
        SecurityContext second = root.forPrincipal("cacheduser");
        Assert.assertSame(first, second);
        Assert.assertNotSame(root, first);
        TestUtils.assertEquals("cacheduser", first.getPrincipal());
    }

    @Test
    public void testDenyAllForPrincipalStaysDenyAll() {
        // forPrincipal must never downgrade a deny-all context to a read-allowing one
        final ReadOnlySecurityContext legacyReadOnlyContext = DenyAllSecurityContext.INSTANCE;
        SecurityContext context = legacyReadOnlyContext.forPrincipal("foo");
        Assert.assertSame(DenyAllSecurityContext.INSTANCE, context);
        try {
            context.authorizeHttp();
            Assert.fail("expected permission denied");
        } catch (CairoException e) {
            Assert.assertTrue(e.getFlyweightMessage().toString().contains("permission denied"));
        }
    }

    @Test
    public void testForPrincipalCachesExactLastAdmission() {
        final AllowAllSecurityContext root = freshAllowAll();
        SecurityContext last = null;
        for (int i = 0; i < CACHE_CAP; i++) {
            last = root.forPrincipal("p" + i);
        }

        Assert.assertNotNull(last);
        Assert.assertSame("the exact cache-capacity admission must remain cached",
                last, root.forPrincipal("p" + (CACHE_CAP - 1)));
    }

    @Test
    public void testForPrincipalCapCopiesTransientPrincipal() {
        // The over-cap leg has its own Chars.toString, and nothing pinned it: deleting the copy from the
        // saturated branch left the whole suite green. testForPrincipalCopiesTransientPrincipal only covers
        // the CACHED leg (fresh root, so it never saturates), and the cap tests only pass String literals,
        // where a missing copy is a no-op.
        //
        // It is the leg that matters most. Once a singleton saturates, every subsequent request for an
        // uncached principal re-derives here -- and on the HTTP path the principal is a flyweight over the
        // reused request buffer, which is exactly why the parameter is @Transient (HttpConnectionContext
        // re-derives the security context per request). Retaining it aliases that buffer into a long-lived
        // SecurityContext, and current_user() / session_user() / SHOW CREATE ... OWNED BY would then read
        // whatever the next request left in it.
        final AllowAllSecurityContext root = freshAllowAll();
        for (int i = 0; i < CACHE_CAP; i++) {
            root.forPrincipal("p" + i);
        }

        final StringSink mutable = new StringSink();
        mutable.put("overcapuser");
        final SecurityContext context = root.forPrincipal(mutable);
        // saturated, so this is a fresh derivation and not a cache hit
        Assert.assertNotSame(root, context);
        Assert.assertNotSame(context, root.forPrincipal(mutable));

        mutable.clear();
        mutable.put("someoneelse");
        TestUtils.assertEquals("overcapuser", context.getPrincipal());
    }

    @Test
    public void testForPrincipalCapDegradesToAllocatePerCall() {
        // beyond the cache cap, additional principals must degrade to allocate-per-call (the pre-cache
        // behavior) rather than growing the cache without bound, while staying correct
        final AllowAllSecurityContext root = freshAllowAll();
        for (int i = 0; i < CACHE_CAP; i++) {
            root.forPrincipal("p" + i);
        }
        // a principal cached before the cap was reached is still retained
        Assert.assertSame(root.forPrincipal("p0"), root.forPrincipal("p0"));
        // a brand-new principal beyond the cap is re-derived every call (not cached), yet correct
        SecurityContext a = root.forPrincipal("overflow");
        SecurityContext b = root.forPrincipal("overflow");
        TestUtils.assertEquals("overflow", a.getPrincipal());
        TestUtils.assertEquals("overflow", b.getPrincipal());
        Assert.assertNotSame(a, b);
        Assert.assertTrue(a.isSystemAdmin());
    }

    @Test
    public void testForPrincipalConcurrentAlternatingPrincipalsNeverLeak() throws Exception {
        // every thread alternates between two principals. Even while another thread is mid-publish swapping
        // in a freshly grown cache, every call must return a context reporting exactly its own principal.
        final int iterations = 50_000;
        final AllowAllSecurityContext root = freshAllowAll();
        TestUtils.runConcurrently(4, t -> {
            for (int i = 0; i < iterations; i++) {
                final String principal = (i & 1) == 0 ? "alice" : "bob";
                TestUtils.assertEquals(principal, root.forPrincipal(principal).getPrincipal());
            }
        });
    }

    @Test
    public void testForPrincipalConcurrentCacheCapDoesNotOvershoot() throws Exception {
        final int racingPrincipalCount = 8;
        final AllowAllSecurityContext root = freshAllowAll();
        for (int i = 0; i < CACHE_CAP - 1; i++) {
            root.forPrincipal("p" + i);
        }

        // runConcurrently releases all workers from a single barrier, so they enter forPrincipal together,
        // clear the outer `cache.size() < MAX` check at the same size, and then contend on the admission CAS.
        // Without atomic admission they would all pass the soft check at size()==MAX-1 and each get cached,
        // overshooting the bound by racingPrincipalCount - 1; with the hard cap exactly one reserves the
        // remaining slot. The barrier stays OUTSIDE forPrincipal on purpose -- blocking inside
        // newPrincipalContext would run under computeIfAbsent's bin lock and could wedge a same-bin racer.
        final String[] principals = new String[racingPrincipalCount];
        final SecurityContext[] first = new SecurityContext[racingPrincipalCount];
        TestUtils.runConcurrently(racingPrincipalCount, t -> {
            principals[t] = "last-slot-racer-" + t;
            first[t] = root.forPrincipal(principals[t]);
            TestUtils.assertEquals(principals[t], first[t].getPrincipal());
        });

        int retainedCount = 0;
        for (int i = 0; i < racingPrincipalCount; i++) {
            final SecurityContext again = root.forPrincipal(principals[i]);
            TestUtils.assertEquals(principals[i], again.getPrincipal());
            if (again == first[i]) {
                retainedCount++;
            }
        }
        Assert.assertEquals("only one racing principal may claim the final cache slot", 1, retainedCount);
    }

    @Test
    public void testForPrincipalConcurrentDistinctPrincipalsRetainedNoThrash() throws Exception {
        // the core M1 guarantee under contention: each thread owns a distinct principal and, after the
        // cache is warmed, every one of its repeated calls must return the *same* cached instance. The
        // old single-entry cache thrashed here (each thread's context evicted by the others); the
        // per-principal cache must keep them all live with zero eviction.
        final int iterations = 20_000;
        final AllowAllSecurityContext root = freshAllowAll();
        TestUtils.runConcurrently(6, t -> {
            final String principal = "tenant" + t;
            final SecurityContext mine = root.forPrincipal(principal);
            TestUtils.assertEquals(principal, mine.getPrincipal());
            for (int i = 0; i < iterations; i++) {
                // no eviction: this thread's cached context must come back unchanged every time
                Assert.assertSame(mine, root.forPrincipal(principal));
            }
        });
    }

    @Test
    public void testForPrincipalConcurrentFirstDerivationConverges() throws Exception {
        // many threads race to derive the SAME new principal for the first time; computeIfAbsent
        // must converge them all onto a single cached instance (no duplicate cached
        // contexts, no observation of a half-published map).
        final int threadCount = 8;
        final AllowAllSecurityContext root = freshAllowAll();
        final String principal = "racy";
        final SecurityContext[] results = new SecurityContext[threadCount];
        TestUtils.runConcurrently(threadCount, t -> results[t] = root.forPrincipal(principal));

        for (int t = 0; t < threadCount; t++) {
            TestUtils.assertEquals(principal, results[t].getPrincipal());
            Assert.assertSame("all racing callers must converge on one cached instance", results[0], results[t]);
        }
        // the converged instance is the one now cached
        Assert.assertSame(results[0], root.forPrincipal(principal));
    }

    @Test
    public void testForPrincipalConcurrentLastAdmissionConverges() throws Exception {
        final AllowAllSecurityContext root = freshAllowAll();
        for (int i = 0; i < CACHE_CAP - 1; i++) {
            root.forPrincipal("p" + i);
        }

        final int threadCount = 8;
        final String principal = "last-admission";
        final SecurityContext[] results = new SecurityContext[threadCount];
        TestUtils.runConcurrently(threadCount, t -> results[t] = root.forPrincipal(principal));

        for (int t = 0; t < threadCount; t++) {
            Assert.assertSame("all callers racing for the last cache slot must converge",
                    results[0], results[t]);
        }
        Assert.assertSame("the concurrent last-slot winner must remain cached",
                results[0], root.forPrincipal(principal));
    }

    @Test
    public void testForPrincipalConcurrentReportsOwnPrincipal() throws Exception {
        // the cache is read lock-free; under contention every caller must still get a
        // context reporting its own principal, never another thread's
        final int iterations = 50_000;
        final AllowAllSecurityContext root = freshAllowAll();
        TestUtils.runConcurrently(4, t -> {
            final String principal = "user" + t;
            for (int i = 0; i < iterations; i++) {
                TestUtils.assertEquals(principal, root.forPrincipal(principal).getPrincipal());
            }
        });
    }

    @Test
    public void testForPrincipalConcurrentSamePrincipalReusesCachedContext() throws Exception {
        // all threads request the same principal: once the cache is warmed the entry is never evicted,
        // so every concurrent caller must hit the cache and get back the very same derived instance,
        // which must always report that principal. This exercises the cache-hit path under contention,
        // which the distinct-principal test never takes.
        final AllowAllSecurityContext root = freshAllowAll();
        final String principal = "shared";
        final SecurityContext warmed = root.forPrincipal(principal);
        Assert.assertNotSame(root, warmed);

        final int iterations = 50_000;
        TestUtils.runConcurrently(4, t -> {
            for (int i = 0; i < iterations; i++) {
                // the warmed entry is never evicted, so the same cached instance must come back
                Assert.assertSame(warmed, root.forPrincipal(principal));
            }
        });
    }

    @Test
    public void testForPrincipalCopiesTransientPrincipal() {
        // forPrincipal must copy the @Transient principal, never retain the caller's mutable buffer,
        // and a later lookup with an equal-content flyweight must still hit the cached entry
        AllowAllSecurityContext root = freshAllowAll();
        StringSink mutable = new StringSink();
        mutable.put("foo");
        SecurityContext context = root.forPrincipal(mutable);
        mutable.clear();
        mutable.put("bar");
        TestUtils.assertEquals("foo", context.getPrincipal());
        StringSink probe = new StringSink();
        probe.put("foo");
        Assert.assertSame(context, root.forPrincipal(probe));
    }

    @Test
    public void testForPrincipalDoesNotLeakAcrossPrincipals() {
        // the per-principal cache must never hand one principal's context to another: every call
        // returns a context reporting its own principal, regardless of cache state
        AllowAllSecurityContext root = freshAllowAll();
        SecurityContext alice = root.forPrincipal("alice");
        SecurityContext bob = root.forPrincipal("bob");
        SecurityContext aliceAgain = root.forPrincipal("alice");
        TestUtils.assertEquals("alice", alice.getPrincipal());
        TestUtils.assertEquals("bob", bob.getPrincipal());
        TestUtils.assertEquals("alice", aliceAgain.getPrincipal());
        Assert.assertNotSame(alice, bob);
        // both principals stay cached (the M1 fix): "bob" does not evict "alice", so the second
        // "alice" returns the very same cached instance rather than a fresh derivation
        Assert.assertSame(alice, aliceAgain);
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
    public void testForPrincipalFailedDerivationReleasesCacheAdmission() {
        final FailingOnceTestAllowAllSecurityContext root = new FailingOnceTestAllowAllSecurityContext();
        try {
            root.forPrincipal("failing-principal");
            Assert.fail("expected the injected derivation failure");
        } catch (IllegalStateException e) {
            Assert.assertEquals("injected derivation failure", e.getMessage());
        }

        SecurityContext last = null;
        for (int i = 0; i < CACHE_CAP; i++) {
            last = root.forPrincipal("p" + i);
        }
        Assert.assertNotNull(last);
        Assert.assertSame(
                "a failed construction must not consume one of the process-lifetime cache admissions",
                last,
                root.forPrincipal("p" + (CACHE_CAP - 1))
        );
    }

    @Test
    public void testForPrincipalNeverSerializesOnTheInstanceMonitor() throws Exception {
        // No derivation may serialize callers on the instance monitor, cached or not. That monitor belongs to
        // a process-wide static singleton shared by every protocol and every IO worker, so anything taking it
        // is a scalability cliff dressed up as graceful degradation: a cache miss would stall every other
        // miss in the process, and a saturated cache would stall every subsequent request.
        //
        // Hold the monitor from another thread and require both derivations to complete anyway -- the WRITE
        // path (a fresh principal, which inserts) and the SATURATED path (past the cap, which does not).
        // The write half is the one that discriminates: a cache that publishes under `synchronized` -- as a
        // copy-on-write map keyed off the instance lock would -- cannot get past it.
        //
        // Both derivations run on their own thread with a bounded wait, so a cache that does take the monitor
        // fails on the timeout instead of deadlocking the test.
        final TestAllowAllSecurityContext root = freshAllowAll();
        // one slot short of the cap, so the first derivation below still inserts
        for (int i = 0; i < CACHE_CAP - 1; i++) {
            root.forPrincipal("p" + i);
        }

        final CountDownLatch monitorHeld = new CountDownLatch(1);
        final CountDownLatch releaseMonitor = new CountDownLatch(1);
        final Thread holder = new Thread(() -> {
            synchronized (root) {
                monitorHeld.countDown();
                try {
                    releaseMonitor.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "security-context-monitor-holder");
        holder.setDaemon(true);
        holder.start();

        final ObjList<Thread> derivers = new ObjList<>();
        try {
            Assert.assertTrue("the holder thread must acquire the monitor", monitorHeld.await(10, TimeUnit.SECONDS));

            deriveWhileMonitorHeld(derivers, root, "uncached",
                    "a cache-miss derivation must not block on the instance monitor");
            // the cache is full now, so this one derives without caching
            deriveWhileMonitorHeld(derivers, root, "overflow",
                    "an over-cap derivation must not block on the instance monitor");
        } finally {
            releaseMonitor.countDown();
            final Thread[] threads = new Thread[derivers.size() + 1];
            threads[0] = holder;
            for (int i = 0, n = derivers.size(); i < n; i++) {
                threads[i + 1] = derivers.getQuick(i);
            }
            TestUtils.joinThreads(threads);
        }
    }

    @Test
    public void testForPrincipalPreservesRuntimeType() {
        // A subclass that overrides newPrincipalContext must come back as ITSELF. Asserting `instanceof
        // AllowAllSecurityContext` cannot detect a downgrade -- a plain AllowAllSecurityContext satisfies it
        // too -- so compare the exact runtime class.
        final TestAllowAllSecurityContext allowAll = freshAllowAll();
        Assert.assertSame(TestAllowAllSecurityContext.class, allowAll.forPrincipal("u").getClass());

        final TestReadOnlySecurityContext readOnly = freshReadOnly();
        Assert.assertSame(TestReadOnlySecurityContext.class, readOnly.forPrincipal("u").getClass());

        // the roots themselves derive to their own concrete type
        Assert.assertSame(AllowAllSecurityContext.class, AllowAllSecurityContext.INSTANCE.forPrincipal("u").getClass());
        Assert.assertSame(ReadOnlySecurityContext.class, ReadOnlySecurityContext.INSTANCE.forPrincipal("u").getClass());
    }

    @Test
    public void testForPrincipalRejectsSubclassThatDropsItsOverrides() {
        // A subclass that overrides authorize* but forgets newPrincipalContext inherits the supertype's
        // implementation, so forPrincipal hands back a plain AllowAllSecurityContext and every override is
        // dropped -- the derived context ALLOWS what the base DENIES. forPrincipal must reject that rather
        // than silently downgrade a security check. QuestDB runs with -ea (see the surefire argLine).
        Assert.assertTrue(
                "this test requires assertions to be enabled (-ea)",
                AbstractPrincipalAwareSecurityContext.class.desiredAssertionStatus()
        );
        final AllowAllSecurityContext denyingHttp = new AllowAllSecurityContext() {
            @Override
            public void authorizeHttp() {
                throw CairoException.authorization().put("HTTP denied");
            }
        };
        // the override is honoured on the base context...
        try {
            denyingHttp.authorizeHttp();
            Assert.fail("expected the subclass override to deny");
        } catch (CairoException e) {
            Assert.assertTrue(e.getFlyweightMessage().toString().contains("HTTP denied"));
        }
        // ...and deriving it must not be allowed to throw that away
        try {
            denyingHttp.forPrincipal("alice");
            Assert.fail("expected forPrincipal to reject a subclass that does not override newPrincipalContext");
        } catch (AssertionError e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("must override newPrincipalContext"));
        }
    }

    @Test
    public void testForPrincipalRetainsManyDistinctPrincipals() {
        // the M1 fix: many distinct principals are retained concurrently in the cache instead of
        // evicting one another. Derive a batch, then re-request each: every one must return the very
        // same cached instance and report its own principal (the old single-slot cache failed this).
        final int n = 32;
        final AllowAllSecurityContext root = freshAllowAll();
        final SecurityContext[] first = new SecurityContext[n];
        for (int i = 0; i < n; i++) {
            first[i] = root.forPrincipal("user" + i);
            TestUtils.assertEquals("user" + i, first[i].getPrincipal());
            Assert.assertNotSame(root, first[i]);
        }
        for (int i = 0; i < n; i++) {
            SecurityContext again = root.forPrincipal("user" + i);
            Assert.assertSame("user" + i + " must stay cached", first[i], again);
        }
    }

    @Test
    public void testForPrincipalShortCircuitsReturnThis() {
        // null, empty, and the context's own principal all short-circuit to `this` without deriving
        AllowAllSecurityContext root = freshAllowAll();
        Assert.assertSame(root, root.forPrincipal(null));
        Assert.assertSame(root, root.forPrincipal(""));
        Assert.assertSame(root, root.forPrincipal("admin")); // the default seeded principal
    }

    @Test
    public void testForPrincipalWithNullCurrentPrincipalDoesNotThrow() {
        // forPrincipal compares the requested principal against getPrincipal(); a context that reports a
        // null principal must not NPE (Chars.equals is @NotNull). It derives a context for the requested
        // principal instead of matching the root.
        final TestAllowAllSecurityContext nullPrincipal = new TestAllowAllSecurityContext(false, null);
        Assert.assertNull(nullPrincipal.getPrincipal());
        SecurityContext derived = nullPrincipal.forPrincipal("foo");
        Assert.assertNotSame(nullPrincipal, derived);
        TestUtils.assertEquals("foo", derived.getPrincipal());
    }

    @Test
    public void testReadOnlyFactoryIlpDoesNotRouteTransportCredential() {
        // ILP's principal is a JWK key id (a transport credential, not an ACL identity), so the factory must
        // hand back the bare singleton rather than route it through the process-lifetime per-principal cache.
        SecurityContext context = ReadOnlySecurityContextFactory.INSTANCE.getInstance(principal("keyid"), SecurityContextFactory.ILP);
        Assert.assertSame(ReadOnlySecurityContext.INSTANCE, context);
        TestUtils.assertEquals("admin", context.getPrincipal());
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
    public void testReadOnlyForPrincipalRetainsAndStaysReadOnly() {
        // ReadOnly mirrors AllowAll: distinct principals are retained, and every derived context keeps
        // the read-only restriction (writes denied) while reporting its own principal
        final ReadOnlySecurityContext root = freshReadOnly();
        SecurityContext alice = root.forPrincipal("alice");
        SecurityContext bob = root.forPrincipal("bob");
        Assert.assertSame(alice, root.forPrincipal("alice"));
        Assert.assertSame(bob, root.forPrincipal("bob"));
        Assert.assertNotSame(alice, bob);
        TestUtils.assertEquals("alice", alice.getPrincipal());
        try {
            alice.authorizeInsert(null);
            Assert.fail("expected write to be denied");
        } catch (CairoException e) {
            Assert.assertTrue(e.getFlyweightMessage().toString().contains("Write permission denied"));
        }
    }

    @Test
    public void testReadOnlyUsersAwareFactoryGlobalPgWireReadOnlyReportsConfiguredPrincipal() {
        final ReadOnlyUsersAwareSecurityContextFactory factory =
                new ReadOnlyUsersAwareSecurityContextFactory(true, null, false);
        final SecurityContext context = factory.getInstance(principal("alice"), SecurityContextFactory.PGWIRE);

        TestUtils.assertEquals("alice", context.getPrincipal());
        Assert.assertNotSame(ReadOnlySecurityContext.INSTANCE, context);
        try {
            context.authorizeInsert(null);
            Assert.fail("expected globally read-only PGWire context to deny writes");
        } catch (CairoException e) {
            Assert.assertTrue(e.getFlyweightMessage().toString().contains("Write permission denied"));
        }
    }

    @Test
    public void testReadOnlyUsersAwareFactoryIlpDoesNotRouteTransportCredential() {
        // The ILP branch must NOT route its principal through forPrincipal: with a line auth db configured
        // the ILP principal is a JWK key id -- a transport credential, not an ACL identity -- so the factory
        // hands back the bare shared allow-all singleton reporting the default name, never a per-principal
        // derived context. Routing it would retain every key id in the process-lifetime per-principal cache
        // that hangs off the singleton shared by all three protocols, saturating its bound and pushing the
        // HTTP/PGWire users onto the uncached, allocate-per-call path. ILP has no query surface to read
        // current_user() back anyway. Mirrors EntReadOnlyUsersAwareSecurityContextFactory.
        ReadOnlyUsersAwareSecurityContextFactory factory = new ReadOnlyUsersAwareSecurityContextFactory(false, null, true);
        SecurityContext context = factory.getInstance(principal("keyid"), SecurityContextFactory.ILP);
        Assert.assertSame(AllowAllSecurityContext.INSTANCE, context);
        TestUtils.assertEquals("admin", context.getPrincipal());
        Assert.assertTrue(context.isSystemAdmin());
        context.authorizeInsert(null);
        // a null principal likewise keeps the shared singleton
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

    /**
     * Derives {@code principal} on its own thread and requires it to finish while the caller holds
     * {@code root}'s monitor. Registers the thread before waiting, so the caller can still join it if the
     * wait times out.
     */
    private static void deriveWhileMonitorHeld(
            ObjList<Thread> derivers,
            AbstractPrincipalAwareSecurityContext root,
            String principal,
            String message
    ) throws Exception {
        final AtomicReference<SecurityContext> derived = new AtomicReference<>();
        final AtomicReference<Throwable> firstError = new AtomicReference<>();
        final CountDownLatch done = new CountDownLatch(1);
        final Thread deriver = new Thread(() -> {
            try {
                derived.set(root.forPrincipal(principal));
            } catch (Throwable th) {
                firstError.compareAndSet(null, th);
            } finally {
                done.countDown();
            }
        }, "principal-deriver-" + principal);
        deriver.setDaemon(true);
        derivers.add(deriver);
        deriver.start();
        Assert.assertTrue(message, done.await(5, TimeUnit.SECONDS));
        TestUtils.rethrowFirst(firstError);
        TestUtils.assertEquals(principal, derived.get().getPrincipal());
    }

    private static TestAllowAllSecurityContext freshAllowAll() {
        // a fresh instance (not the shared singleton) so each test gets an isolated, empty principal cache.
        // It is a named subclass that overrides newPrincipalContext, i.e. it honours the contract forPrincipal
        // asserts on -- an anonymous `new AllowAllSecurityContext() {}` would inherit the supertype's
        // newPrincipalContext and be downgraded away from itself on the first derivation.
        return new TestAllowAllSecurityContext();
    }

    private static TestReadOnlySecurityContext freshReadOnly() {
        // see freshAllowAll()
        return new TestReadOnlySecurityContext();
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

    private static final class FailingOnceTestAllowAllSecurityContext extends AllowAllSecurityContext {
        private boolean failNextDerivation;

        private FailingOnceTestAllowAllSecurityContext() {
            failNextDerivation = true;
        }

        private FailingOnceTestAllowAllSecurityContext(boolean settingsReadOnly, CharSequence principal) {
            super(settingsReadOnly, principal);
        }

        @Override
        protected SecurityContext newPrincipalContext(CharSequence principal) {
            if (failNextDerivation) {
                failNextDerivation = false;
                throw new IllegalStateException("injected derivation failure");
            }
            return new FailingOnceTestAllowAllSecurityContext(settingsReadOnly, principal);
        }
    }

    /**
     * A well-behaved subclass: it overrides {@code newPrincipalContext} to return its own type, so
     * {@code forPrincipal} preserves it. Used as an isolated (non-singleton) cache root by most tests, and
     * as the positive case in {@link #testForPrincipalPreservesRuntimeType()}.
     */
    private static class TestAllowAllSecurityContext extends AllowAllSecurityContext {
        private TestAllowAllSecurityContext() {
            super();
        }

        private TestAllowAllSecurityContext(boolean settingsReadOnly, CharSequence principal) {
            super(settingsReadOnly, principal);
        }

        @Override
        protected SecurityContext newPrincipalContext(CharSequence principal) {
            return new TestAllowAllSecurityContext(settingsReadOnly, principal);
        }
    }

    /**
     * The read-only counterpart of {@link TestAllowAllSecurityContext}.
     */
    private static class TestReadOnlySecurityContext extends ReadOnlySecurityContext {
        private TestReadOnlySecurityContext() {
            super();
        }

        private TestReadOnlySecurityContext(boolean settingsReadOnly, CharSequence principal) {
            super(settingsReadOnly, principal);
        }

        @Override
        protected SecurityContext newPrincipalContext(CharSequence principal) {
            return new TestReadOnlySecurityContext(settingsReadOnly, principal);
        }
    }
}
