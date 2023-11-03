package io.questdb.cutlass.http;

import io.questdb.cairo.SecurityContext;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;

public interface HttpCookieHandler {
    boolean processCookies(HttpConnectionContext context, SecurityContext securityContext) throws PeerIsSlowToReadException, PeerDisconnectedException;

    void setCookie(HttpResponseHeader header, SecurityContext securityContext);
}
