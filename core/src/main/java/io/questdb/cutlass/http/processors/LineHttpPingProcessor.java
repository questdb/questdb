package io.questdb.cutlass.http.processors;

import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.network.ServerDisconnectException;

public class LineHttpPingProcessor implements HttpRequestProcessor {
    private final String header;

    public LineHttpPingProcessor(CharSequence version) {
        this.header = "X-Influxdb-Version: " + version;
    }

    @Override
    public byte getRequiredAuthType() {
        return SecurityContext.AUTH_TYPE_NONE;
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        context.simpleResponse().sendStatusNoContent(204, header);
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerIsSlowToReadException, PeerDisconnectedException {
        context.simpleResponse().sendStatusNoContent(204, header);
    }
}
