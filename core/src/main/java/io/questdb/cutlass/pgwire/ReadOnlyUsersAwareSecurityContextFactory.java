package io.questdb.cutlass.pgwire;

import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.std.Chars;

public final class ReadOnlyUsersAwareSecurityContextFactory implements SecurityContextFactory {
    private final boolean httpReadOnly;
    private final boolean pgWireReadOnly;
    private final String pgWireReadOnlyUser;

    public ReadOnlyUsersAwareSecurityContextFactory(boolean pgWireReadOnly, String pgWireReadOnlyUser, boolean httpReadOnly) {
        this.pgWireReadOnly = pgWireReadOnly;
        this.pgWireReadOnlyUser = pgWireReadOnlyUser;
        this.httpReadOnly = httpReadOnly;
    }

    @Override
    public SecurityContext getInstance(CharSequence principal, int interfaceId) {
        String principalStr = toString(principal);
        switch (interfaceId) {
            case SecurityContextFactory.HTTP:
                return httpReadOnly ? new ReadOnlySecurityContext(principalStr) : new AllowAllSecurityContext(principalStr);
            case SecurityContextFactory.PGWIRE:
                return isReadOnlyPgWireUser(principal) ? new ReadOnlySecurityContext(principalStr) : new AllowAllSecurityContext(principalStr);
            default:
                return new AllowAllSecurityContext(principalStr);
        }
    }

    private boolean isReadOnlyPgWireUser(CharSequence principal) {
        return pgWireReadOnly || (pgWireReadOnlyUser != null && principal != null && Chars.equals(pgWireReadOnlyUser, principal));
    }

    private String toString(CharSequence s) {
        if (s == null) {
            return "";
        } else {
            return s.toString();
        }
    }
}
