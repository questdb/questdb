package io.questdb.cutlass.http;

import org.jetbrains.annotations.NotNull;

public class DefaultHttpSessionStore implements HttpSessionStore {
    public static final DefaultHttpSessionStore INSTANCE = new DefaultHttpSessionStore();

    private DefaultHttpSessionStore() {
    }

    @Override
    public String createSession(@NotNull HttpAuthenticator authenticator) {
        return null;
    }

    @Override
    public SessionInfo verifySession(@NotNull CharSequence sessionId) {
        return null;
    }
}
