package io.questdb.cutlass.http;

import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class DefaultHttpSessionStore implements HttpSessionStore {
    public static final DefaultHttpSessionStore INSTANCE = new DefaultHttpSessionStore();

    private DefaultHttpSessionStore() {
    }

    @Override
    public void createSession(@NotNull HttpAuthenticator authenticator, StringSink sessionID) {
        sessionID.clear();
    }

    @Override
    public SessionInfo verifySession(@NotNull CharSequence sessionId) {
        return null;
    }
}
