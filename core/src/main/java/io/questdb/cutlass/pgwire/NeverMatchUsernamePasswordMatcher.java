package io.questdb.cutlass.pgwire;

public final class NeverMatchUsernamePasswordMatcher implements UsernamePasswordMatcher {
    public static final NeverMatchUsernamePasswordMatcher INSTANCE = new NeverMatchUsernamePasswordMatcher();

    @Override
    public boolean verifyPassword(CharSequence username, long passwordPtr, int passwordLen) {
        return false;
    }
}
