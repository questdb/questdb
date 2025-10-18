package io.questdb.cutlass.http;

import io.questdb.std.ObjList;

public interface PrincipalContext {
    byte getAuthType();

    ObjList<CharSequence> getGroups();

    CharSequence getPrincipal();
}
