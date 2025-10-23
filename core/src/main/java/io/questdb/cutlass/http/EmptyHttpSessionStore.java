package io.questdb.cutlass.http;

import io.questdb.cairo.security.PrincipalContext;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class EmptyHttpSessionStore implements HttpSessionStore {
    public static final EmptyHttpSessionStore INSTANCE = new EmptyHttpSessionStore();

    private EmptyHttpSessionStore() {
    }

    @Override
    public void createSession(@NotNull PrincipalContext principalContext, @NotNull HttpConnectionContext httpContext) {
    }

    @Override
    public void destroySession(@NotNull CharSequence sessionId, @NotNull HttpConnectionContext httpContext) {
    }

    @Override
    public void updateUserGroups(@NotNull CharSequence principal, @NotNull ObjList<CharSequence> groups) {
    }

    @Override
    public SessionInfo verifySessionId(@NotNull CharSequence sessionId, @NotNull HttpConnectionContext httpContext) {
        return null;
    }
}
