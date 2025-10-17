package io.questdb.cutlass.http;

import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface HttpSessionStore {

    /**
     * Create a new session
     *
     * @param context       Principal context, such as an HTTP authenticator used to log the user in
     * @param sessionIdSink Populated with the id of the newly created session
     * @param fd            fd of connection the session belongs to
     */
    void createSession(@NotNull PrincipalContext context, StringSink sessionIdSink, long fd);

    /**
     * Closes a session
     *
     * @param sessionId id of the session to be closed
     * @param fd        fd of connection the session belongs to
     */
    void destroySession(@NotNull CharSequence sessionId, long fd);

    /**
     * Session lookup by principal
     *
     * @param principal entity name
     * @return List of sessions associated with the principal, or null if no active sessions exist
     */
    @Nullable
    ObjList<SessionInfo> getSessions(@NotNull CharSequence principal);

    /**
     * Overrides the token generator used to generate session ids.
     * Useful for testing.
     */
    default void setTokenGenerator(TokenGenerator tokenGenerator) {
    }

    /**
     * Verify session id and return the associated session if the session is valid.
     *
     * @param sessionId     session id to verify
     * @param sessionIdSink Populated with the new session id if the session has been rotated
     * @param fd            fd of connection the session belongs to
     * @return session associated with the session id, or null if the session does not exist
     */
    SessionInfo verifySession(@NotNull CharSequence sessionId, StringSink sessionIdSink, long fd);

    class SessionInfo implements PrincipalContext {
        private final byte authType;
        private final ObjList<CharSequence> groupsA = new ObjList<>();
        private final ObjList<CharSequence> groupsB = new ObjList<>();
        private final String principal;
        private volatile long expiresAt;
        private volatile ObjList<CharSequence> groups = groupsB;
        private volatile long rotateAt;
        private volatile String sessionId;

        public SessionInfo(@NotNull String sessionId, String principal, @Nullable ObjList<CharSequence> groups, byte authType, long expiresAt, long rotateAt) {
            this.sessionId = sessionId;
            this.principal = principal;
            this.authType = authType;
            this.expiresAt = expiresAt;
            this.rotateAt = rotateAt;

            setGroups(groups);
        }

        @Override
        public byte getAuthType() {
            return authType;
        }

        public long getExpiresAt() {
            return expiresAt;
        }

        @Override
        public ObjList<CharSequence> getGroups() {
            return groups;
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

        public void rotate(String newSessionId, long nextRotationAt) {
            this.sessionId = newSessionId;
            this.rotateAt = nextRotationAt;
        }

        public void setExpiresAt(long expiresAt) {
            this.expiresAt = expiresAt;
        }

        public synchronized void setGroups(@Nullable ObjList<CharSequence> source) {
            // ideally these would be compared as sets, but it is ok
            // unlikely that the order of groups changing constantly
            if (groups.equals(source)) {
                return;
            }

            // select non-active list as target
            final ObjList<CharSequence> target = groups == groupsA ? groupsB : groupsA;

            // populate target
            target.clear();
            if (source != null) {
                for (int i = 0, n = source.size(); i < n; i++) {
                    target.add(Chars.toString(source.getQuick(i)));
                }
            }

            // publish new groups
            groups = target;
        }
    }
}
