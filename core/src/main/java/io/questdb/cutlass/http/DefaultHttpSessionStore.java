package io.questdb.cutlass.http;

import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class DefaultHttpSessionStore implements HttpSessionStore {
    public static final DefaultHttpSessionStore INSTANCE = new DefaultHttpSessionStore();

    private DefaultHttpSessionStore() {
    }

    @Override
    public void createSession(@NotNull PrincipalContext context, StringSink sessionIdSink, long fd) {
        sessionIdSink.clear();
    }

    @Override
    public void destroySession(@NotNull CharSequence sessionId, long fd) {
    }

    @Override
    public ObjList<SessionInfo> getSessions(@NotNull CharSequence principal) {
        return null;
    }

    @Override
    public SessionInfo verifySession(@NotNull CharSequence sessionId, StringSink sessionIdSink, long fd) {
        return null;
    }
}
