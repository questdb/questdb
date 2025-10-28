package io.questdb.cutlass.http;

import io.questdb.cairo.security.PrincipalContext;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;

public interface HttpSessionStore {

    /**
     * Create a new session
     *
     * @param principalContext Principal context, such as an HTTP authenticator used to log the user in
     * @param httpContext      HTTP context associated with the user's connection
     */
    void createSession(@NotNull PrincipalContext principalContext, @NotNull HttpConnectionContext httpContext);

    /**
     * Close the session associated with the session id
     *
     * @param sessionId   id of the session to be closed
     * @param httpContext HTTP context associated with the user's connection
     */
    void destroySession(@NotNull CharSequence sessionId, @NotNull HttpConnectionContext httpContext);

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
     * Overrides the token generator used to generate session ids.
     * Useful for testing.
     */
    @TestOnly
    default void setTokenGenerator(TokenGenerator tokenGenerator) {
    }

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
     * If no valid session exists for the session id, or the session already expired, there is no session returned.
     *
     * @param sessionId   session id to verify
     * @param httpContext HTTP context associated with the user's connection
     * @return session associated with the session id, or null if the session does not exist or expired
     */
    @Nullable
    SessionInfo verifySessionId(@NotNull CharSequence sessionId, @NotNull HttpConnectionContext httpContext);

    class SessionInfo implements PrincipalContext {
        private static final ObjList<CharSequence> EMPTY_LIST = new ObjList<>();
        private final byte authType;
        private final ConcurrentHashMap<ReadOnlyObjList<CharSequence>> groupsByEntity;
        private final AtomicBoolean lock = new AtomicBoolean();
        private final String principal;
        private volatile long expiresAt;
        private volatile boolean invalid = false;
        private volatile long rotateAt;
        private volatile String sessionId;

        public SessionInfo(@NotNull String sessionId, String principal, @NotNull ConcurrentHashMap<ReadOnlyObjList<CharSequence>> groupsByEntity, byte authType, long expiresAt, long rotateAt) {
            this.sessionId = sessionId;
            this.principal = principal;
            this.groupsByEntity = groupsByEntity;
            this.authType = authType;
            this.expiresAt = expiresAt;
            this.rotateAt = rotateAt;
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
            return rotateAt;
        }

        public String getSessionId() {
            return sessionId;
        }

        public void invalidate() {
            invalid = true;
        }

        public boolean isInvalid() {
            return invalid;
        }

        public void setExpiresAt(long expiresAt) {
            this.expiresAt = expiresAt;
        }

        @Override
        public String toString() {
            final StringSink sink = Misc.getThreadLocalSink();
            sink.put("SessionInfo [principal=").put(principal)
                    .put(", groups=").put(getGroups())
                    .put(", authType=").put(authType)
                    .put(", expiresAt=").put(expiresAt)
                    .put(", rotateAt=").put(rotateAt)
                    .put(", sessionId=").put(sessionId)
                    .put(", invalid=").put(invalid)
                    .put("]");
            return sink.toString();
        }

        void rotate(String newSessionId, long nextRotationAt) {
            this.sessionId = newSessionId;
            this.rotateAt = nextRotationAt;
        }

        boolean tryLock() {
            return lock.compareAndSet(false, true);
        }

        void unlock() {
            lock.set(false);
        }
    }
}
