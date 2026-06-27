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
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Base for the identity-only security contexts ({@link AbstractAllowAllSecurityContext} and
 * {@link AbstractReadOnlySecurityContext}). It owns the reported principal and the per-principal
 * context cache behind {@link #forPrincipal(CharSequence)}; subclasses supply the authorization
 * behavior and the concrete derived type via {@link #newPrincipalContext(CharSequence)}.
 */
public abstract class AbstractPrincipalAwareSecurityContext implements SecurityContext {
    // upper bound on the number of distinct principals cached by forPrincipal; beyond it, additional
    // principals degrade to allocate-per-call instead of growing the cache without bound
    private static final int MAX_CACHED_PRINCIPALS = 256;

    protected final boolean settingsReadOnly;

    // the reported principal; the singletons seed it with Constants.USER_NAME ("admin"), which is also
    // the value forPrincipal treats as the default/anonymous case (it returns the shared singleton)
    private final CharSequence principal;
    // contexts derived for non-default principals, keyed by principal. Published copy-on-write: the
    // (rare) write path builds a fresh immutable map under the instance lock and swaps this reference,
    // so the lock-free reader in forPrincipal only ever sees a fully built map. These contexts model
    // identity only (pure functions of the principal), so caching one per distinct principal lets
    // concurrently active principals coexist instead of evicting each other.
    private volatile CharSequenceObjHashMap<SecurityContext> principalContextCache;

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
     * concurrently active principals coexist instead of evicting each other; it is populated
     * copy-on-write under the instance lock and read lock-free. It is bounded at
     * {@value #MAX_CACHED_PRINCIPALS} entries, beyond which further principals degrade to
     * allocate-per-call rather than growing without bound.
     * <p>
     * The method is {@code final} and routes instance creation through
     * {@link #newPrincipalContext(CharSequence)} so subclasses preserve their runtime type
     * instead of being silently downgraded.
     */
    public final SecurityContext forPrincipal(@Transient @Nullable CharSequence principal) {
        // compare against getPrincipal(), not the raw field, so a subclass that overrides getPrincipal()
        // is matched consistently; equalsNc tolerates a null getPrincipal() (e.g. a validation context
        // delegating to a null-principal delegate)
        if (principal == null || principal.isEmpty() || Chars.equalsNc(principal, getPrincipal())) {
            return this;
        }
        // lock-free read of the published (immutable) cache; get() is side-effect-free and hashes the
        // incoming flyweight principal by content, so a cache hit allocates nothing
        final CharSequenceObjHashMap<SecurityContext> cache = principalContextCache;
        if (cache != null) {
            final SecurityContext hit = cache.get(principal);
            if (hit != null) {
                return hit;
            }
        }
        return addPrincipalContext(principal);
    }

    @Override
    public CharSequence getPrincipal() {
        return principal;
    }

    /**
     * Derives and caches the context for a principal not yet present in the cache. Synchronized so
     * concurrent callers for new principals serialize on this (rare) write path; the common cache-hit
     * path in {@link #forPrincipal(CharSequence)} never reaches here. The cache is published
     * copy-on-write: a fresh map is built and stored into the volatile field, so the lock-free reader
     * only ever observes a fully built, immutable map.
     */
    private synchronized SecurityContext addPrincipalContext(@Transient @NotNull CharSequence principal) {
        // re-read under the lock; all writers hold this lock, so this is the latest published cache
        final CharSequenceObjHashMap<SecurityContext> cache = principalContextCache;
        if (cache != null) {
            // another thread may have derived this principal while we waited for the lock
            final SecurityContext hit = cache.get(principal);
            if (hit != null) {
                return hit;
            }
            if (cache.size() >= MAX_CACHED_PRINCIPALS) {
                // pathological principal cardinality: stop growing and fall back to allocate-per-call
                // (the pre-cache behavior) instead of retaining contexts without bound
                return newPrincipalContext(Chars.toString(principal));
            }
        }
        final String key = Chars.toString(principal);
        final SecurityContext context = newPrincipalContext(key);
        final CharSequenceObjHashMap<SecurityContext> next = new CharSequenceObjHashMap<>();
        if (cache != null) {
            next.putAll(cache);
        }
        next.put(key, context);
        principalContextCache = next;
        return context;
    }

    /**
     * Creates the concrete context returned by {@link #forPrincipal(CharSequence)} for a new
     * principal. The {@code principal} is already a stable copy. Subclasses must override this
     * to return their own type so {@code forPrincipal} does not downgrade them.
     * <p>
     * The derived context overrides only the reported principal; {@code getAuthType()} and
     * {@code isExternal()} keep their defaults ({@code AUTH_TYPE_NONE} / not external). These
     * contexts model identity only and are used when ACL is not enforced; the full authentication
     * metadata is modelled by the ACL-enforcing security contexts.
     */
    protected abstract SecurityContext newPrincipalContext(CharSequence principal);
}
