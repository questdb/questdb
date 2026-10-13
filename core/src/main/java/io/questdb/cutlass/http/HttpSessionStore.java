package io.questdb.cutlass.http;

import io.questdb.cairo.security.PrincipalContext;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicBoolean;

public interface HttpSessionStore {

    /**
     * Create a new session
     *
     * @param principalContext      Principal context, such as an HTTP authenticator used to log the user in
     * @param credential            the credential that authenticated the principal, the session ends when it is
     *                              revoked; null if it cannot be revoked by this server
     * @param credentialFingerprint fingerprint of the Authorization header that carried the credential, null
     *                              if the request carried none, or the store does not reuse verified credentials
     * @param httpContext           HTTP context associated with the user's connection
     */
    void createSession(
            @NotNull PrincipalContext principalContext,
            @Nullable HttpSessionCredential credential,
            byte @Nullable [] credentialFingerprint,
            @NotNull HttpConnectionContext httpContext
    );

    /**
     * Close the session associated with the session id
     *
     * @param sessionId   id of the session to be closed
     * @param httpContext HTTP context associated with the user's connection
     */
    void destroySession(@NotNull CharSequence sessionId, @NotNull HttpConnectionContext httpContext);

    /**
     * Close every session of a principal, such as when the user is disabled or dropped.
     *
     * @param principal entity name
     */
    void destroySessions(@NotNull CharSequence principal);

    /**
     * Extend the lifetime of the session, and rotate its id if due.
     * Called only for requests that have been authenticated and authorized, so a refused request
     * never keeps a session alive.
     *
     * @param session     session used by the request
     * @param httpContext HTTP context associated with the user's connection
     */
    void extendSession(@NotNull SessionInfo session, @NotNull HttpConnectionContext httpContext);

    /**
     * Returns how long, in microseconds, a credential verified for a session stays verified for the
     * requests of the session that carry it again. When 0, such requests verify the credential every time,
     * and the fingerprints of credentials are of no use to the store.
     *
     * @return the credential revalidation interval in microseconds, 0 if verified credentials are not reused
     */
    long getCredentialRevalidationInterval();

    /**
     * Lookup session by id
     *
     * @param sessionId session id of the session
     * @return session associated with the session id, or null if the session does not exist
     */
    @TestOnly
    @Nullable
    default SessionInfo getSession(@NotNull CharSequence sessionId) {
        return null;
    }

    /**
     * Check if the credential carried by a request has already been verified for the session recently,
     * that is, within the credential revalidation interval. If it has, the request can use the session
     * without verifying the credential again. A credential whose verification cannot be reused, see
     * {@link HttpSessionCredential#isVerificationReusable()}, is never verified for the session this way.
     *
     * @param session               session sent with the request
     * @param credentialFingerprint fingerprint of the Authorization header of the request
     * @return true if the session verified the same credential within the revalidation interval
     */
    boolean isCredentialVerified(@NotNull SessionInfo session, byte @NotNull [] credentialFingerprint);

    /**
     * Overrides the token generator used to generate session ids.
     * Useful for testing.
     */
    @TestOnly
    default void setTokenGenerator(TokenGenerator tokenGenerator) {
    }

    /**
     * Record that a credential has just authenticated the principal of the session. The session
     * ends when the credential is revoked, and the credential needs no verification for the session
     * again until the revalidation interval passes.
     *
     * @param session               session sent with the request
     * @param credential            the credential, or null if it cannot be revoked by this server
     * @param credentialFingerprint fingerprint of the Authorization header that carried the credential,
     *                              null if the store does not reuse verified credentials
     */
    void setVerifiedCredential(
            @NotNull SessionInfo session,
            @Nullable HttpSessionCredential credential,
            byte @Nullable [] credentialFingerprint
    );

    /**
     * Returns the number of active sessions for a user
     *
     * @param principal entity name
     * @return Number of active sessions associated with the principal
     */
    @TestOnly
    default int size(@NotNull CharSequence principal) {
        return 0;
    }

    /**
     * Update the external groups of the user
     *
     * @param principal entity name
     * @param groups    external groups of entity
     */
    void updateUserGroups(@NotNull CharSequence principal, @NotNull ObjList<CharSequence> groups);

    /**
     * Lookup the session associated with the session id.
     * If the session is valid, it will be returned.
     * If no valid session exists for the session id, the session already expired, or a credential
     * the session was authenticated with has been revoked, there is no session returned.
     * Does not extend the lifetime of the session, see {@link #extendSession(SessionInfo, HttpConnectionContext)}.
     *
     * @param sessionId   session id to verify
     * @param httpContext HTTP context associated with the user's connection
     * @return session associated with the session id, or null if the session does not exist, expired or was revoked
     */
    @Nullable
    SessionInfo verifySessionId(@NotNull CharSequence sessionId, @NotNull HttpConnectionContext httpContext);

    class SessionInfo implements PrincipalContext {
        private static final ObjList<CharSequence> EMPTY_LIST = new ObjList<>();
        private final byte authType;
        // the credential the session was opened with, null if it cannot be revoked by this server
        private final HttpSessionCredential credential;
        private final ConcurrentHashMap<ReadOnlyObjList<CharSequence>> groupsByEntity;
        private final AtomicBoolean lock = new AtomicBoolean();
        private final String principal;
        private volatile long expiresAt;
        private volatile boolean invalid = false;
        private volatile RotationInfo rotationInfo;
        // the credential that authenticated the principal of the session last, null if none did yet
        private volatile VerifiedCredential verifiedCredential;

        public SessionInfo(
                @NotNull String sessionId,
                String principal,
                @NotNull ConcurrentHashMap<ReadOnlyObjList<CharSequence>> groupsByEntity,
                byte authType,
                long expiresAt,
                long rotateAt,
                @Nullable HttpSessionCredential credential
        ) {
            this.rotationInfo = new RotationInfo(sessionId, rotateAt);
            this.principal = principal;
            this.groupsByEntity = groupsByEntity;
            this.authType = authType;
            this.expiresAt = expiresAt;
            this.credential = credential;
        }

        @Override
        public byte getAuthType() {
            return authType;
        }

        public long getExpiresAt() {
            return expiresAt;
        }

        @Override
        public ReadOnlyObjList<CharSequence> getGroups() {
            final ReadOnlyObjList<CharSequence> groups = groupsByEntity.get(principal);
            return groups != null ? groups : EMPTY_LIST;
        }

        @Override
        public String getPrincipal() {
            return principal;
        }

        public long getRotateAt() {
            return rotationInfo.rotateAt;
        }

        public String getSessionId() {
            return rotationInfo.sessionId;
        }

        public void invalidate() {
            invalid = true;
        }

        /**
         * Checks the credentials the session was opened and last authenticated with.
         *
         * @return true if any of them has been revoked, such as a password changed or a token dropped
         */
        public boolean isCredentialRevoked() {
            if (credential != null && !credential.isValid()) {
                return true;
            }
            final VerifiedCredential verified = verifiedCredential;
            return verified != null
                    && verified.credential != null
                    && verified.credential != credential
                    && !verified.credential.isValid();
        }

        public boolean isInvalid() {
            return invalid;
        }

        public void setExpiresAt(long expiresAt) {
            this.expiresAt = expiresAt;
        }

        @Override
        public String toString() {
            final RotationInfo rotationInfo = this.rotationInfo;
            final StringSink sink = Misc.getThreadLocalSink();
            sink.put("SessionInfo [principal=").put(principal)
                    .put(", groups=").put(getGroups())
                    .put(", authType=").put(authType)
                    .put(", expiresAt=").put(expiresAt)
                    .put(", rotateAt=").put(rotationInfo.rotateAt)
                    .put(", sessionId=").put(rotationInfo.sessionId)
                    .put(", invalid=").put(invalid)
                    .put("]");
            return sink.toString();
        }

        boolean isCredentialVerified(byte @NotNull [] credentialFingerprint, long verifiedAfter) {
            final VerifiedCredential verified = verifiedCredential;
            return verified != null
                    && verified.fingerprint != null
                    && verified.verifiedAt > verifiedAfter
                    && MessageDigest.isEqual(verified.fingerprint, credentialFingerprint);
        }

        boolean isEvictableOldSessionId(@NotNull CharSequence mappedSessionId, long evictionGracePeriod, long currentMicros) {
            final RotationInfo rotationInfo = this.rotationInfo;
            return !Chars.equals(mappedSessionId, rotationInfo.sessionId)
                    && rotationInfo.rotateAt - evictionGracePeriod < currentMicros;
        }

        void rotate(String newSessionId, long nextRotationAt) {
            rotationInfo = new RotationInfo(newSessionId, nextRotationAt);
        }

        void setVerifiedCredential(@Nullable HttpSessionCredential credential, byte @Nullable [] credentialFingerprint, long verifiedAt) {
            // without a fingerprint, no request matches the credential, and each one that carries it verifies it again
            final boolean isReusable = credentialFingerprint != null && (credential == null || credential.isVerificationReusable());
            verifiedCredential = new VerifiedCredential(
                    credential,
                    isReusable ? credentialFingerprint.clone() : null,
                    verifiedAt
            );
        }

        boolean tryLock() {
            return lock.compareAndSet(false, true);
        }

        void unlock() {
            lock.set(false);
        }

        /**
         * Immutable (sessionId, rotateAt) pair. The two values are logically coupled:
         * rotation must replace them in a single volatile store, so concurrent readers
         * never observe the id of one rotation generation with the deadline of another.
         */
        private record RotationInfo(String sessionId, long rotateAt) {
        }

        /**
         * Immutable record of the credential that authenticated the principal of the session last:
         * the credential itself, the fingerprint of the Authorization header that carried it, if the store
         * reuses verified credentials and the credential allows it, and the time of the authentication.
         * Replaced in a single volatile store, like {@link RotationInfo}.
         */
        private record VerifiedCredential(HttpSessionCredential credential, byte[] fingerprint, long verifiedAt) {
        }
    }
}
