package io.questdb.cairo.security;

import io.questdb.cairo.SecurityContext;
import io.questdb.std.Chars;

public final class ReadOnlySecurityContextFactory implements SecurityContextFactory {
    public static final ReadOnlySecurityContextFactory INSTANCE = new ReadOnlySecurityContextFactory();

    @Override
    public SecurityContext getInstance(CharSequence principal, int interfaceId) {
        if (principal != null && Chars.equals(ReadOnlySecurityContext.INSTANCE.getEntityName(), principal)) {
            return ReadOnlySecurityContext.INSTANCE;
        }
        return new ReadOnlySecurityContext(principal);
    }
}
