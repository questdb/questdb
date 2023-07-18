package io.questdb.cutlass.pgwire;

public final class CustomCloseActionPasswordMatcherDelegate implements UsernamePasswordMatcher {

    private final Runnable closeAction;
    private final UsernamePasswordMatcher delegate;

    public CustomCloseActionPasswordMatcherDelegate(UsernamePasswordMatcher delegate, Runnable closeAction) {
        this.delegate = delegate;
        this.closeAction = closeAction;
    }

    @Override
    public void close() {
        try {
            delegate.close();
        } finally {
            closeAction.run();
        }
    }

    @Override
    public boolean verifyPassword(CharSequence username, long passwordPtr, int passwordLen) {
        return delegate.verifyPassword(username, passwordPtr, passwordLen);
    }
}
