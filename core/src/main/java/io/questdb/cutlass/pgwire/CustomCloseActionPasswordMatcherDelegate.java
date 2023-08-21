package io.questdb.cutlass.pgwire;

import io.questdb.std.QuietCloseable;

public final class CustomCloseActionPasswordMatcherDelegate implements UsernamePasswordMatcher, QuietCloseable {

    private final Runnable closeAction;
    private final UsernamePasswordMatcher delegate;

    public CustomCloseActionPasswordMatcherDelegate(UsernamePasswordMatcher delegate, Runnable closeAction) {
        this.delegate = delegate;
        this.closeAction = closeAction;
    }

    @Override
    public void close() {
        closeAction.run();
    }

    @Override
    public boolean verifyPassword(CharSequence username, long passwordPtr, int passwordLen) {
        return delegate.verifyPassword(username, passwordPtr, passwordLen);
    }
}
