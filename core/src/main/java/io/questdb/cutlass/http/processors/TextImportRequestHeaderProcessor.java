package io.questdb.cutlass.http.processors;

import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;

public interface TextImportRequestHeaderProcessor {
    TextImportRequestHeaderProcessor DEFAULT = new TextImportRequestHeaderProcessorImpl();

    void processRequestHeader(
            HttpRequestHeader partHeader,
            HttpConnectionContext transientContext,
            TextImportProcessorState transientState
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException;
}
