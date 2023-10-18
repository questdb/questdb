package io.questdb.cutlass.http;

import io.questdb.cairo.SecurityContext;

public interface HttpCookieHandler {
    void processCookies(CharSequence cookies, SecurityContext securityContext);

    void setCookie(HttpResponseHeader header, SecurityContext securityContext);
}
