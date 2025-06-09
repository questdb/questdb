package io.questdb.cutlass.http.processors;

import io.questdb.cairo.PartitionBy;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpKeywords;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.text.Atomicity;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.cutlass.http.processors.TextImportProcessor.sendErrorAndThrowDisconnect;

public class TextImportRequestHeaderProcessorImpl implements TextImportRequestHeaderProcessor {
    private static final Utf8String PARTITION_BY_NONE = new Utf8String("NONE");

    @Override
    public void processRequestHeader(HttpRequestHeader partHeader, HttpConnectionContext transientContext, TextImportProcessorState transientState)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        final HttpRequestHeader rh = transientContext.getRequestHeader();
        DirectUtf8Sequence name = rh.getUrlParam(URL_PARAM_NAME);
        if (name == null) {
            name = partHeader.getContentDispositionFilename();

        }
        if (name == null) {
            sendErrorAndThrowDisconnect("no file name given", transientContext, transientState);
        }

        assert name != null;

        Utf8Sequence partitionedBy = rh.getUrlParam(URL_PARAM_PARTITION_BY);
        if (partitionedBy == null) {
            partitionedBy = PARTITION_BY_NONE;
        }
        int partitionBy = PartitionBy.fromUtf8String(partitionedBy);
        if (partitionBy == -1) {
            sendErrorAndThrowDisconnect("invalid partitionBy", transientContext, transientState);
        }

        DirectUtf8Sequence timestampColumn = rh.getUrlParam(URL_PARAM_TIMESTAMP);
        if (PartitionBy.isPartitioned(partitionBy) && timestampColumn == null) {
            sendErrorAndThrowDisconnect("when specifying partitionBy you must also specify timestamp", transientContext, transientState);
        }

        transientState.analysed = false;

        transientState.textLoader.configureDestination(
                name,
                HttpKeywords.isTrue(rh.getUrlParam(URL_PARAM_OVERWRITE)),
                getAtomicity(rh.getUrlParam(URL_PARAM_ATOMICITY)),
                partitionBy,
                timestampColumn,
                null
        );

        DirectUtf8Sequence o3MaxLagChars = rh.getUrlParam(URL_PARAM_O3_MAX_LAG);
        if (o3MaxLagChars != null) {
            try {
                long o3MaxLag = Numbers.parseLong(o3MaxLagChars);
                if (o3MaxLag >= 0) {
                    transientState.textLoader.setO3MaxLag(o3MaxLag);
                }
            } catch (NumericException e) {
                sendErrorAndThrowDisconnect("invalid o3MaxLag value, must be a long", transientContext, transientState);
            }
        }

        DirectUtf8Sequence maxUncommittedRowsChars = rh.getUrlParam(URL_PARAM_MAX_UNCOMMITTED_ROWS);
        if (maxUncommittedRowsChars != null) {
            try {
                int maxUncommittedRows = Numbers.parseInt(maxUncommittedRowsChars);
                if (maxUncommittedRows >= 0) {
                    transientState.textLoader.setMaxUncommittedRows(maxUncommittedRows);
                }
            } catch (NumericException e) {
                sendErrorAndThrowDisconnect("invalid maxUncommittedRows, must be an int", transientContext, transientState);
            }
        }

        boolean create = !HttpKeywords.isFalse(rh.getUrlParam(URL_PARAM_CREATE));
        transientState.textLoader.setCreate(create);

        boolean forceHeader = HttpKeywords.isTrue(rh.getUrlParam(URL_PARAM_FORCE_HEADER));
        transientState.textLoader.setForceHeaders(forceHeader);
        transientState.textLoader.setSkipLinesWithExtraValues(Utf8s.equalsNcAscii("true", rh.getUrlParam(URL_PARAM_SKIP_LEV)));
        DirectUtf8Sequence delimiter = rh.getUrlParam(URL_PARAM_DELIMITER);
        if (delimiter != null && delimiter.size() == 1) {
            transientState.textLoader.configureColumnDelimiter(delimiter.byteAt(0));
        }
        transientState.textLoader.setState(TextLoader.ANALYZE_STRUCTURE);

        transientState.forceHeader = forceHeader;
    }

    private static int getAtomicity(Utf8Sequence name) {
        if (name == null) {
            return Atomicity.SKIP_COL;
        }

        if (HttpKeywords.isSkipRow(name)) {
            return Atomicity.SKIP_ROW;
        }

        if (HttpKeywords.isAbort(name)) {
            return Atomicity.SKIP_ALL;
        }

        return Atomicity.SKIP_COL;
    }
}
