package io.questdb.cutlass.http.processors;

import io.questdb.cutlass.http.HttpChunkedResponseSocket;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.network.ServerDisconnectException;

public class TlsErrorProcessor implements HttpRequestProcessor {
    public final static TlsErrorProcessor INSTANCE = new TlsErrorProcessor();

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
        r.status(400, "text/plain");
        r.sendHeader();
        r.putAscii("Use HTTPS to connect to this server");
        r.sendChunk(true);
    }
}
