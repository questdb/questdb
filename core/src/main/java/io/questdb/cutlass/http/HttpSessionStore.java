package io.questdb.cutlass.http;

import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface HttpSessionStore {

    /**
     * Create a new session
     *
     * @param authenticator HTTP authenticator used to log the user in
     * @return session id
     */
    String createSession(@NotNull HttpAuthenticator authenticator);

    /**
     * Verify session id and return the associated session if the session is valid.
     *
     * @param sessionId session id to verify
     * @return session info associated with the session id, or null if the session does not exist
     */
    SessionInfo verifySession(@NotNull CharSequence sessionId);

    class SessionInfo {
        private final byte authType;
        private final ObjList<CharSequence> groups = new ObjList<>();
        private final String principal;
        private volatile long expiresAt;
        private volatile String sessionId;

        public SessionInfo(String sessionId, CharSequence principal, @Nullable ObjList<CharSequence> groups, byte authType, long expiresAt) {
            this.sessionId = sessionId;
            this.principal = principal.toString();
            this.authType = authType;
            this.expiresAt = expiresAt;

            if (groups != null) {
                for (int i = 0, n = groups.size(); i < n; i++) {
                    this.groups.add(groups.getQuick(i).toString());
                }
            }
        }

        public byte getAuthType() {
            return authType;
        }

        public long getExpiresAt() {
            return expiresAt;
        }

        public ObjList<CharSequence> getGroups() {
            return groups;
        }

        public String getPrincipal() {
            return principal;
        }
    }
}
