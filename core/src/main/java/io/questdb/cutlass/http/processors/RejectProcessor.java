package io.questdb.cutlass.http.processors;

import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpMultipartContentListener;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.network.ServerDisconnectException;

public interface RejectProcessor extends HttpRequestProcessor, HttpMultipartContentListener {

    void clear();

    boolean isRequestBeingRejected();

    default void onChunk(long lo, long hi) {
    }

    default void onPartBegin(HttpRequestHeader partHeader) {
    }

    default void onPartEnd() {
    }

    HttpRequestProcessor rejectRequest(int code, CharSequence userMessage, CharSequence cookieName, CharSequence cookieValue, byte authenticationType);

    default void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        onRequestComplete(context);
    }
}
