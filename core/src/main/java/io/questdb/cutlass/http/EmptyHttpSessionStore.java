package io.questdb.cutlass.http;

import io.questdb.cairo.security.PrincipalContext;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class EmptyHttpSessionStore implements HttpSessionStore {
    public static final EmptyHttpSessionStore INSTANCE = new EmptyHttpSessionStore();

    protected EmptyHttpSessionStore() {
    }

    @Override
    public void createSession(@NotNull PrincipalContext principalContext, @NotNull HttpConnectionContext httpContext) {
    }

    @Override
    public void destroySession(@NotNull CharSequence sessionId, @NotNull HttpConnectionContext httpContext) {
    }

    @Override
    public SessionInfo getSession(@NotNull CharSequence sessionId) {
        return null;
    }

    @Override
    public ObjList<SessionInfo> getSessions(@NotNull CharSequence principal) {
        return null;
    }

    @Override
    public SessionInfo verifySessionId(@NotNull CharSequence sessionId, @NotNull HttpConnectionContext httpContext) {
        return null;
    }
}
