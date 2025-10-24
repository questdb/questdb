package io.questdb.cairo.security;

import io.questdb.std.ObjList;

public interface PrincipalContext {
    byte getAuthType();

    ObjList<CharSequence> getGroups();

    CharSequence getPrincipal();
}
