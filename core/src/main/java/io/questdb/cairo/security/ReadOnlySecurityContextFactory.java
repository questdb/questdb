package io.questdb.cairo.security;

import io.questdb.cairo.SecurityContext;
import io.questdb.std.ObjList;

public final class ReadOnlySecurityContextFactory implements SecurityContextFactory {
    public static final ReadOnlySecurityContextFactory INSTANCE = new ReadOnlySecurityContextFactory();

    @Override
    public SecurityContext getInstance(CharSequence principal, ObjList<CharSequence> groups, byte authType, byte interfaceId) {
        return ReadOnlySecurityContext.INSTANCE;
    }
}
