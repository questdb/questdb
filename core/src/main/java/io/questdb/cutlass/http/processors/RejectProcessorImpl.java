package io.questdb.cutlass.http.processors;

import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;

import static io.questdb.cairo.SecurityContext.AUTH_TYPE_NONE;

public class RejectProcessorImpl implements RejectProcessor {
    protected final HttpConnectionContext httpConnectionContext;
    protected byte authenticationType = AUTH_TYPE_NONE;
    protected int rejectCode = 0;
    protected CharSequence rejectCookieName = null;
    protected CharSequence rejectCookieValue = null;
    protected CharSequence rejectMessage = null;

    public RejectProcessorImpl(HttpConnectionContext httpConnectionContext) {
        this.httpConnectionContext = httpConnectionContext;
    }

    @Override
    public void clear() {
        rejectCode = 0;
        authenticationType = AUTH_TYPE_NONE;
        rejectCookieName = null;
        rejectCookieValue = null;
        rejectMessage = null;
    }

    @Override
    public boolean isErrorProcessor() {
        return true;
    }

    @Override
    public boolean isRequestBeingRejected() {
        return rejectCode != 0;
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        httpConnectionContext.simpleResponse().sendStatusWithCookie(rejectCode, rejectMessage, rejectCookieName, rejectCookieValue);
        httpConnectionContext.reset();
    }

    @Override
    public HttpRequestProcessor rejectRequest(int code, CharSequence userMessage, CharSequence cookieName, CharSequence cookieValue, byte authenticationType) {
        this.rejectCode = code;
        this.rejectMessage = userMessage;
        this.rejectCookieName = cookieName;
        this.rejectCookieValue = cookieValue;
        this.authenticationType = authenticationType;
        return this;
    }
}
