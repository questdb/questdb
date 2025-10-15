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
     * @param authenticator HTTP authenticator used to log the user in
     */
    void createSession(@NotNull HttpAuthenticator authenticator, StringSink sink);

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
        private final ObjList<CharSequence> groups = new ObjList<>();
        private final String principal;
        private volatile long expiresAt;

        public SessionInfo(CharSequence principal, @Nullable ObjList<CharSequence> groups, byte authType, long expiresAt) {
            this.principal = Chars.toString(principal);
            this.authType = authType;
            this.expiresAt = expiresAt;

            if (groups != null) {
                for (int i = 0, n = groups.size(); i < n; i++) {
                    this.groups.add(Chars.toString(groups.getQuick(i)));
                }
            }
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
    }
}
