package io.questdb.cutlass.http;

import io.questdb.cairo.SecurityContext;

public class DefaultHttpCookieHandler implements HttpCookieHandler {
    public static final DefaultHttpCookieHandler INSTANCE = new DefaultHttpCookieHandler();

    private DefaultHttpCookieHandler() {
    }

    @Override
    public boolean processCookies(HttpConnectionContext contexts, SecurityContext securityContext) {
        return true;
    }

    @Override
    public void setCookie(HttpResponseHeader header, SecurityContext securityContext) {
    }
}
