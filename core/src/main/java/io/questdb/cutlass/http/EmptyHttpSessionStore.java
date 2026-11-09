package io.questdb.cutlass.http;

import io.questdb.cairo.security.PrincipalContext;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Predicate;

public class EmptyHttpSessionStore implements HttpSessionStore {
    public static final EmptyHttpSessionStore INSTANCE = new EmptyHttpSessionStore();

    private EmptyHttpSessionStore() {
    }

    @Override
    public void createSession(
            @NotNull PrincipalContext principalContext,
            @Nullable HttpSessionCredential credential,
            byte @Nullable [] credentialFingerprint,
            @NotNull HttpConnectionContext httpContext
    ) {
    }

    @Override
    public void destroySession(@NotNull CharSequence sessionId, @NotNull HttpConnectionContext httpContext) {
    }

    @Override
    public void destroySessions(@NotNull CharSequence principal, @NotNull Predicate<SessionInfo> filter) {
    }

    @Override
    public void extendSession(@NotNull SessionInfo session, @NotNull HttpConnectionContext httpContext) {
    }

    @Override
    public long getCredentialRevalidationInterval() {
        return 0;
    }

    @Override
    public boolean isCredentialVerified(@NotNull SessionInfo session, byte @NotNull [] credentialFingerprint) {
        return false;
    }

    @Override
    public void setVerifiedCredential(
            @NotNull SessionInfo session,
            @Nullable HttpSessionCredential credential,
            byte @Nullable [] credentialFingerprint
    ) {
    }

    @Override
    public void updateUserGroups(@NotNull CharSequence principal, @NotNull ObjList<CharSequence> groups) {
    }

    @Override
    public SessionInfo verifySessionId(@NotNull CharSequence sessionId, @NotNull HttpConnectionContext httpContext) {
        return null;
    }
}
