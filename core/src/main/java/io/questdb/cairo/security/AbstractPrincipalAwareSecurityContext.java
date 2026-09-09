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

package io.questdb.cairo.security;

import io.questdb.cairo.SecurityContext;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiFunction;

/**
 * Base for the identity-only security contexts ({@link AbstractAllowAllSecurityContext} and
 * {@link AbstractReadOnlySecurityContext}). It owns the reported principal and the per-principal
 * context cache behind {@link #forPrincipal(CharSequence)}; subclasses supply the authorization
 * behavior and the concrete derived type via {@link #newPrincipalContext(CharSequence)}.
 */
public abstract class AbstractPrincipalAwareSecurityContext implements SecurityContext {
    // upper bound on the number of distinct principals cached by forPrincipal. The cache never evicts: the
    // first MAX_CACHED_PRINCIPALS distinct principals hold it for the process lifetime, and once it is full
    // every further principal re-derives (allocate-per-call) on each request instead of the cache growing
    // without bound. Not an LRU -- saturation degrades the uncached tail, it does not reshuffle who is cached.
    public static final int MAX_CACHED_PRINCIPALS = 256;
    // A constant, so it captures nothing and the JVM hands back the same instance every time. A method
    // reference to an instance method would allocate a capturing lambda on each miss; computeIfAbsent's token
    // overload exists precisely to take `this` as a parameter instead.
    private static final BiFunction<CharSequence, Object, SecurityContext> NEW_PRINCIPAL_CONTEXT =
            (principal, self) -> ((AbstractPrincipalAwareSecurityContext) self).newCachedPrincipalContext(principal);
    private static final long PRINCIPAL_CONTEXT_CACHE_COUNT_OFFSET =
            Unsafe.getFieldOffset(AbstractPrincipalAwareSecurityContext.class, "principalContextCacheCount");
    private static final long PRINCIPAL_CONTEXT_CACHE_OFFSET =
            Unsafe.getFieldOffset(AbstractPrincipalAwareSecurityContext.class, "principalContextCache");

    protected final boolean settingsReadOnly;

    // the reported principal; the singletons seed it with Constants.USER_NAME ("admin"), which is also
    // the value forPrincipal treats as the default/anonymous case (it returns the shared singleton)
    private final CharSequence principal;
    // Contexts derived for non-default principals, keyed by principal. These contexts model identity only
    // (pure functions of the principal), so caching one per distinct principal lets concurrently active
    // principals coexist instead of evicting each other. Reads are lock-free and hash the incoming
    // @Transient flyweight by content, so a hit allocates nothing.
    //
    // Lazily allocated by principalContextCache(): only a context that actually SERVES forPrincipal (a shared
    // singleton) ever needs one. A derived per-principal context and a validation context never call
    // forPrincipal, so leaving this null on them avoids allocating a ConcurrentHashMap -- plus the CarrierLocal
    // and process-global counter bump its constructor does -- once per derivation and, for the validation
    // context, once per /validate request.
    private volatile ConcurrentHashMap<SecurityContext> principalContextCache;
    // Reserved and established cache entries. Admission is reserved inside computeIfAbsent's per-key atomic
    // mapping step, so distinct principals racing for the last slot cannot overshoot the hard cap. Keeping the
    // count as a primitive field avoids allocating an AtomicInteger on every derived context.
    private volatile int principalContextCacheCount;

    protected AbstractPrincipalAwareSecurityContext(boolean settingsReadOnly, CharSequence principal) {
        this.settingsReadOnly = settingsReadOnly;
        this.principal = principal;
    }

    /**
     * Returns a context that keeps this context's authorization behavior but reports the given
     * principal, so that {@code current_user()} and session handling reflect the authenticated user
     * rather than the hardcoded default. Returns {@code this} when the principal is null (anonymous)
     * or already matches, to keep the singleton path allocation-free.
     * <p>
     * The HTTP authentication path re-derives the security context on every request (see
     * {@code HttpConnectionContext.configureSecurityContext}), and PGWire/LineTCP derive once per
     * connection, so derived contexts are cached by principal to avoid allocating a context and
     * copying the principal on every call. The cache keeps one context per distinct principal, so
     * concurrently active principals coexist instead of evicting each other. It never evicts and is bounded
     * at {@value #MAX_CACHED_PRINCIPALS} entries: the first {@value #MAX_CACHED_PRINCIPALS} distinct
     * principals hold the cache for the process lifetime, and once it is full every further principal
     * re-derives (allocate-per-call) on each request rather than the cache growing without bound.
     * <p>
     * The method is {@code final} and routes instance creation through the overridable
     * {@link #newPrincipalContext(CharSequence)}. That does not by itself preserve a subclass's
     * runtime type: a subclass of a concrete context that does not override
     * {@code newPrincipalContext} inherits its supertype's implementation and is silently
     * downgraded, dropping every {@code authorize*} and identity override it declared -- turning a
     * context that DENIES an operation into one that ALLOWS it. An assertion in
     * {@link #newCheckedPrincipalContext(CharSequence)} rejects that, so the requirement is enforced
     * rather than merely documented (QuestDB runs with {@code -ea}).
     */
    public final SecurityContext forPrincipal(@Transient @Nullable CharSequence principal) {
        // compare against getPrincipal(), not the raw field, so a subclass that overrides getPrincipal()
        // is matched consistently; equalsNc tolerates a null getPrincipal() (e.g. a validation context
        // delegating to a null-principal delegate)
        if (principal == null || principal.isEmpty() || Chars.equalsNc(principal, getPrincipal())) {
            return this;
        }
        final ConcurrentHashMap<SecurityContext> cache = principalContextCache();
        // lock-free, and get() hashes the incoming flyweight by content, so a hit allocates nothing
        final SecurityContext hit = cache.get(principal);
        if (hit != null) {
            return hit;
        }
        // Gate on the primitive reservation counter, not cache.size(): size() sums ConcurrentHashMap's striped
        // counter cells (O(number of cores)), and a burst of first-time authentications -- distinct principals
        // populating those cells -- is exactly what drives this miss path hardest, so the costly size() and the
        // hot case coincide. principalContextCacheCount is a single volatile read, and it is the same value the
        // hard cap CASes against in newCachedPrincipalContext, so the fast-path gate and the real cap now agree
        // on "full". It is >= size() (a reservation precedes its entry), so it can only ever trip the gate a
        // hair early while a construction is in flight -- which the recheck below absorbs.
        if (principalContextCacheCount >= MAX_CACHED_PRINCIPALS) {
            // Another caller may have filled the final slot with this same principal after our first get().
            // Recheck before taking the uncached overflow path so callers racing for that slot still converge
            // on the admitted context rather than some of them receiving throwaway duplicates.
            final SecurityContext lastAdmission = cache.get(principal);
            if (lastAdmission != null) {
                return lastAdmission;
            }
            // saturated: derive and hand back without caching, rather than retaining contexts without bound
            return newCheckedPrincipalContext(Chars.toString(principal));
        }
        // Copy the principal before it may be stored: the parameter is @Transient (a flyweight over a reused
        // request buffer), and ConcurrentHashMap.computeIfAbsent does NOT clone the key on every insert path,
        // so the copy is required rather than defensive -- without it a stored key could later alias the next
        // request's buffer. Racing first-derivations of the same principal converge on one instance inside
        // computeIfAbsent. The mapping returns null when another principal reserved the final cache slot; in
        // that case derive from the same stable copy without retaining the context.
        final String stablePrincipal = Chars.toString(principal);
        final SecurityContext cached = cache.computeIfAbsent(stablePrincipal, this, NEW_PRINCIPAL_CONTEXT);
        return cached != null ? cached : newCheckedPrincipalContext(stablePrincipal);
    }

    @Override
    public CharSequence getPrincipal() {
        return principal;
    }

    /**
     * Reserves one cache admission and derives its context. Called only by the map's atomic per-key
     * {@code computeIfAbsent} step, so callers racing for the same principal share one reservation while
     * distinct principals compete through the CAS counter. Returning null tells the map not to retain the
     * context when the hard cap is already reserved.
     */
    private SecurityContext newCachedPrincipalContext(CharSequence principal) {
        int count;
        do {
            count = principalContextCacheCount;
            if (count >= MAX_CACHED_PRINCIPALS) {
                return null;
            }
        } while (!Unsafe.cas(this, PRINCIPAL_CONTEXT_CACHE_COUNT_OFFSET, count, count + 1));

        try {
            return newCheckedPrincipalContext(principal);
        } catch (RuntimeException | Error e) {
            // computeIfAbsent leaves the mapping absent when construction fails, so release its reservation.
            Unsafe.getAndAddInt(this, PRINCIPAL_CONTEXT_CACHE_COUNT_OFFSET, -1);
            throw e;
        }
    }

    /**
     * Derives a context via {@link #newPrincipalContext(CharSequence)} and asserts that the subclass
     * did not let itself be downgraded. A subclass of a concrete context that forgets to override
     * {@code newPrincipalContext} inherits its supertype's implementation, so the derived context comes
     * back as the supertype and every {@code authorize*} / identity override it declared is silently
     * dropped -- a context that DENIES an operation would start ALLOWING it. Returning {@code this} is
     * the other legal answer: an identity-invariant context (deny-all, SQL validation, mat view refresh)
     * ignores the principal by design.
     */
    private SecurityContext newCheckedPrincipalContext(CharSequence principal) {
        final SecurityContext context = newPrincipalContext(principal);
        assert context == this || context.getClass() == getClass()
                : getClass().getName() + " must override newPrincipalContext(): forPrincipal() would "
                + "silently downgrade it to " + context.getClass().getName()
                + ", dropping its authorize*/identity overrides";
        return context;
    }

    /**
     * Returns the per-principal cache, allocating it on first use. Only a context that actually serves
     * {@code forPrincipal} (a shared singleton) reaches this; a derived or validation context never does, so
     * it keeps a null cache and allocates no map. Lock-free: a racing first call may build a throwaway map,
     * but the CAS publishes exactly one and every racer converges on it. It must not take the instance
     * monitor -- that monitor belongs to a process-wide singleton, so serialising on it would be a
     * scalability cliff (both properties are pinned by the concurrency tests).
     */
    private ConcurrentHashMap<SecurityContext> principalContextCache() {
        final ConcurrentHashMap<SecurityContext> existing = principalContextCache;
        if (existing != null) {
            return existing;
        }
        final ConcurrentHashMap<SecurityContext> created = new ConcurrentHashMap<>();
        if (Unsafe.cas(this, PRINCIPAL_CONTEXT_CACHE_OFFSET, null, created)) {
            return created;
        }
        return principalContextCache;
    }

    /**
     * Creates the concrete context returned by {@link #forPrincipal(CharSequence)} for a new
     * principal. The {@code principal} is already a stable copy. Every subclass MUST override this to
     * return its own type, or return {@code this} if it is identity-invariant; an assertion in
     * {@link #newCheckedPrincipalContext(CharSequence)} rejects anything else, because an inherited
     * implementation silently downgrades the subclass and drops its overrides.
     * <p>
     * The derived context overrides only the reported principal; {@code getAuthType()} and
     * {@code isExternal()} keep their defaults ({@code AUTH_TYPE_NONE} / not external). These
     * contexts model identity only and are used when ACL is not enforced; the full authentication
     * metadata is modelled by the ACL-enforcing security contexts.
     */
    protected abstract SecurityContext newPrincipalContext(CharSequence principal);
}
