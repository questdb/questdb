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

package io.questdb.test.cutlass.http;

import io.questdb.DefaultServerConfiguration;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.PrincipalContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpSessionCredential;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.cutlass.http.HttpSessionStoreImpl;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class HttpSessionStoreImplTest extends AbstractCairoTest {
    private static final long REVALIDATION_INTERVAL = 30_000_000L;
    private static final long SESSION_TIMEOUT = 1_800_000_000L;
    private static final long START_MICROS = 1_760_743_438_000_000L;

    @Test
    public void testCredentialNeverVerifiedWithoutRevalidationInterval() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(START_MICROS);
            final HttpSessionStore store = newStore(0);
            try (HttpConnectionContext context = newContext()) {
                final HttpSessionStore.SessionInfo session = createSession(store, context, "alice", null, fingerprint(1));
                // every request carrying a credential verifies it again
                Assert.assertFalse(store.isCredentialVerified(session, fingerprint(1)));
                store.setVerifiedCredential(session, null, fingerprint(1));
                Assert.assertFalse(store.isCredentialVerified(session, fingerprint(1)));
            }
        });
    }

    @Test
    public void testCredentialVerifiedWithinRevalidationInterval() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(START_MICROS);
            final HttpSessionStore store = newStore(REVALIDATION_INTERVAL);
            try (HttpConnectionContext context = newContext()) {
                final HttpSessionStore.SessionInfo session = createSession(store, context, "alice", null, fingerprint(1));

                // the credential the session was opened with
                Assert.assertTrue(store.isCredentialVerified(session, fingerprint(1)));
                // any other credential
                Assert.assertFalse(store.isCredentialVerified(session, fingerprint(2)));

                setCurrentMicros(START_MICROS + REVALIDATION_INTERVAL - 1);
                Assert.assertTrue(store.isCredentialVerified(session, fingerprint(1)));

                // the interval has passed, the credential has to be verified again
                setCurrentMicros(START_MICROS + REVALIDATION_INTERVAL);
                Assert.assertFalse(store.isCredentialVerified(session, fingerprint(1)));

                // a credential verified for the session replaces the previous one, and restarts the interval
                store.setVerifiedCredential(session, null, fingerprint(2));
                Assert.assertTrue(store.isCredentialVerified(session, fingerprint(2)));
                Assert.assertFalse(store.isCredentialVerified(session, fingerprint(1)));
                setCurrentMicros(START_MICROS + 2 * REVALIDATION_INTERVAL - 1);
                Assert.assertTrue(store.isCredentialVerified(session, fingerprint(2)));
                setCurrentMicros(START_MICROS + 2 * REVALIDATION_INTERVAL);
                Assert.assertFalse(store.isCredentialVerified(session, fingerprint(2)));
            }
        });
    }

    @Test
    public void testCredentialWithoutReusableVerification() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(START_MICROS);
            final HttpSessionStore store = newStore(REVALIDATION_INTERVAL);
            try (HttpConnectionContext context = newContext()) {
                // e.g. a REST token: verifying it slides its TTL, so the session never vouches for it
                final AtomicBoolean tokenValid = new AtomicBoolean(true);
                final HttpSessionCredential token = newNonReusableCredential(tokenValid);
                final HttpSessionStore.SessionInfo session = createSession(store, context, "alice", token, fingerprint(1));
                final String sessionId = session.getSessionId();
                Assert.assertFalse(store.isCredentialVerified(session, fingerprint(1)));
                store.setVerifiedCredential(session, token, fingerprint(1));
                Assert.assertFalse(store.isCredentialVerified(session, fingerprint(1)));

                // a credential the session can vouch for is reused as usual
                store.setVerifiedCredential(session, () -> true, fingerprint(2));
                Assert.assertTrue(store.isCredentialVerified(session, fingerprint(2)));

                // the session still ends when the token it was opened with is revoked
                tokenValid.set(false);
                Assert.assertNull(store.verifySessionId(sessionId, context));
                Assert.assertTrue(session.isInvalid());
            }
        });
    }

    @Test
    public void testDestroySessions() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(START_MICROS);
            final HttpSessionStore store = newStore(REVALIDATION_INTERVAL);
            try (HttpConnectionContext context = newContext()) {
                final HttpSessionStore.SessionInfo alice1 = createSession(store, context, "alice", null, null);
                final String alice1OldId = alice1.getSessionId();
                final HttpSessionStore.SessionInfo alice2 = createSession(store, context, "alice", null, null);
                final HttpSessionStore.SessionInfo bob = createSession(store, context, "bob", null, null);

                // rotate the first session, it is mapped under two ids until the old one is evicted
                setCurrentMicros(alice1.getRotateAt() + 1);
                store.extendSession(alice1, context);
                final String alice1NewId = alice1.getSessionId();
                Assert.assertNotEquals(alice1OldId, alice1NewId);
                Assert.assertEquals(3, store.size("alice"));

                store.destroySessions("alice");

                Assert.assertEquals(0, store.size("alice"));
                Assert.assertTrue(alice1.isInvalid());
                Assert.assertTrue(alice2.isInvalid());
                Assert.assertNull(store.verifySessionId(alice1OldId, context));
                Assert.assertNull(store.verifySessionId(alice1NewId, context));
                Assert.assertNull(store.verifySessionId(alice2.getSessionId(), context));

                Assert.assertFalse(bob.isInvalid());
                Assert.assertSame(bob, store.verifySessionId(bob.getSessionId(), context));
            }
        });
    }

    @Test
    public void testRevokedCredentialEndsSession() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(START_MICROS);
            final HttpSessionStore store = newStore(REVALIDATION_INTERVAL);
            try (HttpConnectionContext context = newContext()) {
                final AtomicBoolean valid = new AtomicBoolean(true);
                final HttpSessionStore.SessionInfo session = createSession(store, context, "alice", valid::get, fingerprint(1));
                final String sessionId = session.getSessionId();
                Assert.assertSame(session, store.verifySessionId(sessionId, context));

                // the credential the session was opened with is revoked
                valid.set(false);
                Assert.assertNull(store.verifySessionId(sessionId, context));
                Assert.assertTrue(session.isInvalid());
                Assert.assertNull(store.getSession(sessionId));

                // reinstating the credential does not bring the session back
                valid.set(true);
                Assert.assertNull(store.verifySessionId(sessionId, context));
            }
        });
    }

    @Test
    public void testRevokedOpeningCredentialEndsSessionAfterAnotherIsVerified() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(START_MICROS);
            final HttpSessionStore store = newStore(REVALIDATION_INTERVAL);
            try (HttpConnectionContext context = newContext()) {
                final AtomicBoolean openedWithValid = new AtomicBoolean(true);
                final HttpSessionStore.SessionInfo session = createSession(store, context, "alice", openedWithValid::get, fingerprint(1));
                final String sessionId = session.getSessionId();
                store.setVerifiedCredential(session, () -> true, fingerprint(2));

                // e.g. the password the session was opened with is changed, the session ends,
                // even though its requests carry a token that is still valid
                openedWithValid.set(false);
                Assert.assertNull(store.verifySessionId(sessionId, context));
                Assert.assertTrue(session.isInvalid());
            }
        });
    }

    @Test
    public void testRevokedVerifiedCredentialEndsSession() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(START_MICROS);
            final HttpSessionStore store = newStore(REVALIDATION_INTERVAL);
            try (HttpConnectionContext context = newContext()) {
                final AtomicBoolean openedWithValid = new AtomicBoolean(true);
                final AtomicBoolean verifiedValid = new AtomicBoolean(true);
                final HttpSessionStore.SessionInfo session = createSession(store, context, "alice", openedWithValid::get, fingerprint(1));
                final String sessionId = session.getSessionId();

                // the principal authenticates with another credential, e.g. the session was opened with
                // a password, and its requests carry a REST token
                store.setVerifiedCredential(session, verifiedValid::get, fingerprint(2));
                Assert.assertSame(session, store.verifySessionId(sessionId, context));

                // revoking the second credential ends the session, even though the first one is valid
                verifiedValid.set(false);
                Assert.assertNull(store.verifySessionId(sessionId, context));
                Assert.assertTrue(session.isInvalid());
            }
        });
    }

    @Test
    public void testVerifySessionIdDoesNotExtendSession() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(START_MICROS);
            final HttpSessionStore store = newStore(REVALIDATION_INTERVAL);
            try (HttpConnectionContext context = newContext()) {
                final HttpSessionStore.SessionInfo session = createSession(store, context, "alice", null, null);
                final String sessionId = session.getSessionId();
                Assert.assertEquals(START_MICROS + SESSION_TIMEOUT, session.getExpiresAt());

                // looking the session up, e.g. for a request that is refused later, neither extends nor rotates it
                setCurrentMicros(session.getRotateAt() + 1);
                context.getSessionIdSink().clear();
                Assert.assertSame(session, store.verifySessionId(sessionId, context));
                Assert.assertEquals(START_MICROS + SESSION_TIMEOUT, session.getExpiresAt());
                Assert.assertEquals(sessionId, session.getSessionId());
                Assert.assertEquals(0, context.getSessionIdSink().length());

                // extending it, for a request that has been authorized, does both
                final long now = session.getRotateAt() + 1;
                store.extendSession(session, context);
                Assert.assertEquals(now + SESSION_TIMEOUT, session.getExpiresAt());
                Assert.assertNotEquals(sessionId, session.getSessionId());
                TestUtils.assertEquals(session.getSessionId(), context.getSessionIdSink());

                // a session not extended times out
                setCurrentMicros(now + SESSION_TIMEOUT + 1);
                Assert.assertNull(store.verifySessionId(session.getSessionId(), context));
            }
        });
    }

    private static HttpSessionStore.SessionInfo createSession(
            HttpSessionStore store,
            HttpConnectionContext context,
            String principal,
            HttpSessionCredential credential,
            byte[] fingerprint
    ) {
        store.createSession(new TestPrincipal(principal), credential, fingerprint, context);
        final HttpSessionStore.SessionInfo session = store.getSession(context.getSessionIdSink());
        Assert.assertNotNull(session);
        return session;
    }

    private static byte[] fingerprint(int value) {
        final byte[] fingerprint = new byte[32];
        fingerprint[0] = (byte) value;
        return fingerprint;
    }

    private static HttpConnectionContext newContext() {
        return new HttpConnectionContext(new DefaultHttpServerConfiguration(configuration), PlainSocketFactory.INSTANCE);
    }

    private static HttpSessionCredential newNonReusableCredential(AtomicBoolean valid) {
        return new HttpSessionCredential() {
            @Override
            public boolean isValid() {
                return valid.get();
            }

            @Override
            public boolean isVerificationReusable() {
                return false;
            }
        };
    }

    private static HttpSessionStore newStore(long revalidationInterval) {
        return new HttpSessionStoreImpl(
                new DefaultServerConfiguration(root) {
                    @Override
                    public CairoConfiguration getCairoConfiguration() {
                        return configuration;
                    }
                },
                revalidationInterval
        );
    }

    private record TestPrincipal(String principal) implements PrincipalContext {
        @Override
        public byte getAuthType() {
            return SecurityContext.AUTH_TYPE_CREDENTIALS;
        }

        @Override
        public ReadOnlyObjList<CharSequence> getGroups() {
            return null;
        }

        @Override
        public CharSequence getPrincipal() {
            return principal;
        }
    }
}
