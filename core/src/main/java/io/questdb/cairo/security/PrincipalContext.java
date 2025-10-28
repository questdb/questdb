package io.questdb.cairo.security;

import io.questdb.std.ReadOnlyObjList;

public interface PrincipalContext {
    byte getAuthType();

    ReadOnlyObjList<CharSequence> getGroups();

    CharSequence getPrincipal();
}
