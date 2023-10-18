package io.questdb.cutlass.http;

import io.questdb.cairo.SecurityContext;

public class DefaultHttpCookieHandler implements HttpCookieHandler {
    public static final DefaultHttpCookieHandler INSTANCE = new DefaultHttpCookieHandler();

    private DefaultHttpCookieHandler() {
    }

    @Override
    public void processCookies(CharSequence cookies, SecurityContext securityContext) {
    }

    @Override
    public void setCookie(HttpResponseHeader header, SecurityContext securityContext) {
    }
}
