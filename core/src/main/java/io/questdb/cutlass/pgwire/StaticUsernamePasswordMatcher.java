package io.questdb.cutlass.pgwire;

import io.questdb.std.Chars;
import io.questdb.std.Vect;

public class StaticUsernamePasswordMatcher implements UsernamePasswordMatcher {
    private final int passwordLen;
    private final long passwordPtr;
    private final String username;

    public StaticUsernamePasswordMatcher(String username, long passwordPtr, int passwordLen) {
        assert !Chars.empty(username);
        assert passwordPtr != 0;
        assert passwordLen > 0;

        this.username = username;
        this.passwordPtr = passwordPtr;
        this.passwordLen = passwordLen;
    }

    @Override
    public boolean verifyPassword(CharSequence username, long passwordPtr, int passwordLen) {
        return username != null
                && this.username != null
                && Chars.equals(this.username, username)
                && this.passwordLen == passwordLen
                && Vect.memeq(this.passwordPtr, passwordPtr, passwordLen);
    }
}
