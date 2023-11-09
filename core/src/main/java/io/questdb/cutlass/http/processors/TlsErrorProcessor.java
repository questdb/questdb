package io.questdb.cutlass.http.processors;

import io.questdb.cutlass.http.HttpChunkedResponseSocket;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.network.ServerDisconnectException;

public final class TlsErrorProcessor implements HttpRequestProcessor {
    public static final TlsErrorProcessor INSTANCE = new TlsErrorProcessor();
    public static final String URL = "/tls-handshake-failed";

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
        r.status(400, "text/plain");
        r.sendHeader();

        r.putAscii("Use HTTPS to connect to this server.\n");
        r.sendChunk(true);

        // force disconnects
        throw ServerDisconnectException.INSTANCE;
    }

    @Override
    public boolean requiresAuthentication() {
        return false;
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        context.resumeResponseSend();
        throw ServerDisconnectException.INSTANCE;
    }
}
