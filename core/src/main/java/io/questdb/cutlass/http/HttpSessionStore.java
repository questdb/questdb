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
     * @param context Principal context, such as an HTTP authenticator used to log the user in
     */
    void createSession(@NotNull PrincipalContext context, StringSink sink);

    /**
     * Closes a session
     *
     * @param sessionId id of the session to be closed
     */
    void destroySession(@NotNull CharSequence sessionId);

    /**
     * Session lookup by principal
     *
     * @param principal Entity name
     * @return List of sessions associated with the principal, or null if no active sessions exist
     */
    @Nullable
    ObjList<SessionInfo> getSessions(@NotNull CharSequence principal);

    default void setTokenGenerator(TokenGenerator tokenGenerator) {
    }

    /**
     * Verify session id and return the associated session if the session is valid.
     *
     * @param sessionId session id to verify
     * @return session info associated with the session id, or null if the session does not exist
     */
    SessionInfo verifySession(@NotNull CharSequence sessionId);

    class SessionInfo implements PrincipalContext {
        private final byte authType;
        private final ObjList<CharSequence> groupsA = new ObjList<>();
        private final ObjList<CharSequence> groupsB = new ObjList<>();
        private final String principal;
        private final String sessionId;
        private volatile long expiresAt;
        private volatile ObjList<CharSequence> groups = groupsB;

        public SessionInfo(@NotNull CharSequence sessionId, CharSequence principal, @Nullable ObjList<CharSequence> groups, byte authType, long expiresAt) {
            this.sessionId = Chars.toString(sessionId);
            this.principal = Chars.toString(principal);
            this.authType = authType;
            this.expiresAt = expiresAt;

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

        public String getSessionId() {
            return sessionId;
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
