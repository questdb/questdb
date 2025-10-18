package io.questdb.cutlass.http;

import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cutlass.http.HttpSessionStore.SessionInfo.NO_SESSION;

public class DefaultHttpSessionStore implements HttpSessionStore {
    public static final DefaultHttpSessionStore INSTANCE = new DefaultHttpSessionStore();

    private DefaultHttpSessionStore() {
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
    public @NotNull SessionInfo verifySession(@NotNull CharSequence sessionId, @NotNull HttpConnectionContext httpContext) {
        return NO_SESSION;
    }
}
