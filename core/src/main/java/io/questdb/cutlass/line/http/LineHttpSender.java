/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.line.http;

import io.questdb.BuildInformationHolder;
import io.questdb.ClientTlsConfiguration;
import io.questdb.HttpClientConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.client.Sender;
import io.questdb.cutlass.http.HttpConstants;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientException;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.json.JsonParser;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.NanosecondClockImpl;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public final class LineHttpSender implements Sender {
    private static final String PATH = "/write?precision=n";
    private static final int RETRY_BACKOFF_MULTIPLIER = 2;
    private static final int RETRY_INITIAL_BACKOFF_MS = 10;
    private static final int RETRY_MAX_BACKOFF_MS = 1000;
    private static final int RETRY_MAX_JITTER_MS = 10;
    private final String authToken;
    private final int autoFlushRows;
    private final int baseTimeoutMillis;
    private final long flushIntervalNanos;
    private final String host;
    private final long maxRetriesNanos;
    private final long minRequestThroughput;
    private final String password;
    private final String path;
    private final int port;
    private final CharSequence questdbVersion;
    private final Rnd rnd = new Rnd(NanosecondClockImpl.INSTANCE.getTicks(), MicrosecondClockImpl.INSTANCE.getTicks());
    private final StringSink sink = new StringSink();
    private final String url;
    private final String username;
    private HttpClient client;
    private boolean closed;
    private long flushAfterNanos = Long.MAX_VALUE;
    private JsonErrorParser jsonErrorParser;
    private long pendingRows;
    private HttpClient.Request request;
    private int rowBookmark;
    private RequestState state = RequestState.EMPTY;

    public LineHttpSender(
            String host,
            int port,
            HttpClientConfiguration clientConfiguration,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            String authToken,
            String username,
            String password,
            long maxRetriesNanos,
            long minRequestThroughput,
            long flushIntervalNanos
    ) {
        this(
                host,
                port,
                PATH,
                clientConfiguration,
                tlsConfig,
                autoFlushRows,
                authToken,
                username,
                password,
                maxRetriesNanos,
                minRequestThroughput,
                flushIntervalNanos
        );
    }

    public LineHttpSender(
            String host,
            int port,
            String path,
            HttpClientConfiguration clientConfiguration,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            String authToken,
            String username,
            String password,
            long maxRetriesNanos,
            long minRequestThroughput,
            long flushIntervalNanos
    ) {
        assert authToken == null || (username == null && password == null);
        this.maxRetriesNanos = maxRetriesNanos;
        this.host = host;
        this.port = port;
        this.path = path != null ? path : PATH;
        this.autoFlushRows = autoFlushRows;
        this.authToken = authToken;
        this.username = username;
        this.password = password;
        this.minRequestThroughput = minRequestThroughput;
        this.flushIntervalNanos = flushIntervalNanos;
        this.baseTimeoutMillis = clientConfiguration.getTimeout();
        if (tlsConfig != null) {
            this.client = HttpClientFactory.newTlsInstance(clientConfiguration, tlsConfig);
            this.url = "https://" + host + ":" + port + this.path;
        } else {
            this.client = HttpClientFactory.newPlainTextInstance(clientConfiguration);
            this.url = "http://" + host + ":" + port + this.path;
        }
        this.questdbVersion = new BuildInformationHolder().getSwVersion();
        this.request = newRequest();
    }

    @Override
    public void at(long timestamp, ChronoUnit unit) {
        request.putAscii(' ').put(Timestamps.toMicros(timestamp, unit)).put('t');
        atNow();
    }

    @Override
    public void at(Instant timestamp) {
        long micros = timestamp.getEpochSecond() * Timestamps.SECOND_MICROS + timestamp.getNano() / 1_000;
        request.putAscii(' ').put(micros).put('t');
        atNow();
    }

    @Override
    public void atNow() {
        switch (state) {
            case EMPTY:
                throw new LineSenderException("no table name was provided");
            case TABLE_NAME_SET:
                throw new LineSenderException("no symbols or columns were provided");
            case ADDING_SYMBOLS:
            case ADDING_COLUMNS:
                request.put('\n');
                state = RequestState.EMPTY;
                break;
        }
        if (rowAdded()) {
            flush();
        }
        rowBookmark = request.getContentLength();
    }

    @Override
    public Sender boolColumn(CharSequence name, boolean value) {
        writeFieldName(name);
        request.put(value ? 't' : 'f');
        return this;
    }

    @Override
    public void cancelRow() {
        validateNotClosed();
        request.trimContentToLen(rowBookmark);
        state = RequestState.EMPTY;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            if (autoFlushRows != 0 || flushIntervalNanos != Long.MAX_VALUE) {
                // either row-based or time-based auto flushing is enabled
                // => let's auto-flush on close
                flush0(true);
            }
        } finally {
            Misc.free(jsonErrorParser);
            closed = true;
            client = Misc.free(client);
        }
    }

    @Override
    public Sender doubleColumn(CharSequence name, double value) {
        writeFieldName(name);
        request.put(value);
        return this;
    }

    @Override
    public void flush() {
        flush0(false);
    }

    @Override
    public Sender longColumn(CharSequence name, long value) {
        writeFieldName(name);
        request.put(value);
        request.put('i');
        return this;
    }

    @TestOnly
    public void putRawMessage(CharSequence msg) {
        request.put(msg); // message must include trailing \n
        state = RequestState.EMPTY;
        if (rowAdded()) {
            flush();
        }
    }

    @Override
    public Sender stringColumn(CharSequence name, CharSequence value) {
        writeFieldName(name);
        request.put('"');
        escapeString(value);
        request.put('"');
        return this;
    }

    @Override
    public Sender symbol(CharSequence name, CharSequence value) {
        switch (state) {
            case EMPTY:
                throw new LineSenderException("table name must be set first");
            case ADDING_COLUMNS:
                throw new LineSenderException("symbols must be written before any other column types");
            case TABLE_NAME_SET:
                state = RequestState.ADDING_SYMBOLS;
                // fall through
            case ADDING_SYMBOLS:
                validateColumnName(name);
                request.putAscii(',');
                escapeQuotedString(name);
                request.putAscii('=');
                escapeQuotedString(value);
                state = RequestState.ADDING_SYMBOLS;
                break;
            default:
                throw new LineSenderException("unexpected state: ").put(state.name());
        }
        return this;
    }

    @Override
    public Sender table(CharSequence table) {
        assert request != null;
        validateNotClosed();
        validateTableName(table);
        if (state != RequestState.EMPTY) {
            throw new LineSenderException("duplicated table. call sender.at() or sender.atNow() to finish the current row first");
        }
        if (table.length() == 0) {
            throw new LineSenderException("table name cannot be empty");
        }
        state = RequestState.TABLE_NAME_SET;
        escapeQuotedString(table);
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, long value, ChronoUnit unit) {
        // micros
        writeFieldName(name).put(Timestamps.toMicros(value, unit)).put('t');
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, Instant value) {
        // micros
        writeFieldName(name).put((value.getEpochSecond() * Timestamps.SECOND_MICROS + value.getNano() / 1000L)).put('t');
        return this;
    }

    private static void chunkedResponseToSink(HttpClient.ResponseHeaders response, StringSink sink) {
        if (!response.isChunked()) {
            return;
        }
        Response chunkedRsp = response.getResponse();
        Fragment fragment;
        while ((fragment = chunkedRsp.recv()) != null) {
            sink.putNonAscii(fragment.lo(), fragment.hi());
        }
    }

    private static boolean isSuccessResponse(DirectUtf8Sequence statusCode) {
        return statusCode != null && statusCode.size() == 3 && statusCode.byteAt(0) == '2';
    }

    private static boolean keepAliveDisabled(HttpClient.ResponseHeaders response) {
        DirectUtf8Sequence connectionHeader = response.getHeader(HttpConstants.HEADER_CONNECTION);
        return connectionHeader != null && Utf8s.equalsAscii("close", connectionHeader);
    }

    private int backoff(int retryBackoff) {
        int jitter = rnd.nextInt(RETRY_MAX_JITTER_MS);
        int backoff = retryBackoff + jitter;
        Os.sleep(backoff);
        return Math.min(RETRY_MAX_BACKOFF_MS, backoff * RETRY_BACKOFF_MULTIPLIER);
    }

    private void consumeChunkedResponse(HttpClient.ResponseHeaders response) {
        if (!response.isChunked()) {
            return;
        }
        Response chunkedRsp = response.getResponse();
        while ((chunkedRsp.recv()) != null) {
            // we don't care about the response, just consume it, so it won't stay in the socket receive buffer
        }
    }

    private void escapeQuotedString(CharSequence name) {
        for (int i = 0, n = name.length(); i < n; i++) {
            char c = name.charAt(i);
            switch (c) {
                case ' ':
                case ',':
                case '=':
                case '\n':
                case '\r':
                case '\\':
                    request.put((byte) '\\').put((byte) c);
                    break;
                default:
                    request.put(c);
                    break;
            }
        }
    }

    private void escapeString(CharSequence value) {
        for (int i = 0, n = value.length(); i < n; i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\n':
                case '\r':
                case '"':
                case '\\':
                    request.put((byte) '\\').put((byte) c);
                    break;
                default:
                    request.put(c);
                    break;
            }
        }
    }

    private void flush0(boolean closing) {
        if (state != RequestState.EMPTY && !closing) {
            throw new LineSenderException("Cannot flush buffer while row is in progress. Use sender.at() or sender.atNow() to finish the current row first.");
        }
        if (pendingRows == 0) {
            return;
        }

        long retryingDeadlineNanos = Long.MIN_VALUE;
        int retryBackoff = RETRY_INITIAL_BACKOFF_MS;
        int contentLen = request.getContentLength();
        int actualTimeoutMillis = baseTimeoutMillis;
        if (minRequestThroughput > 0) {
            long throughputTimeoutBonusMillis = (contentLen * 1_000L / minRequestThroughput);
            if (throughputTimeoutBonusMillis + actualTimeoutMillis > Integer.MAX_VALUE) {
                actualTimeoutMillis = Integer.MAX_VALUE;
            } else {
                actualTimeoutMillis += (int) throughputTimeoutBonusMillis;
            }
        }
        for (; ; ) {
            try {
                long beforeRequest = System.nanoTime();
                HttpClient.ResponseHeaders response = request.send(actualTimeoutMillis);
                long elapsedNanos = System.nanoTime() - beforeRequest;
                int remainingMillis = actualTimeoutMillis - (int) (elapsedNanos / 1_000_000L);
                if (remainingMillis <= 0) {
                    throw new HttpClientException("Request timed out");
                }

                response.await(remainingMillis);
                DirectUtf8Sequence statusCode = response.getStatusCode();
                if (isSuccessResponse(statusCode)) {
                    consumeChunkedResponse(response); // if any
                    if (keepAliveDisabled(response)) {
                        // Server has HTTP keep-alive disabled and it's closing this TCP connection.
                        client.disconnect();
                    }
                    break;
                }
                assert response.isChunked();
                if (isRetryableHttpStatus(statusCode)) {
                    long nowNanos = System.nanoTime();
                    retryingDeadlineNanos = (retryingDeadlineNanos == Long.MIN_VALUE && !closing) ? nowNanos + maxRetriesNanos : retryingDeadlineNanos;
                    if (nowNanos >= retryingDeadlineNanos) {
                        throwOnHttpErrorResponse(statusCode, response);
                    }
                    client.disconnect(); // forces reconnect, just in case
                    retryBackoff = backoff(retryBackoff);
                    continue;
                }
                throwOnHttpErrorResponse(statusCode, response);
            } catch (HttpClientException e) {
                // this is a network error, we can retry
                client.disconnect(); // forces reconnect
                long nowNanos = System.nanoTime();
                retryingDeadlineNanos = (retryingDeadlineNanos == Long.MIN_VALUE && !closing) ? nowNanos + maxRetriesNanos : retryingDeadlineNanos;
                if (nowNanos >= retryingDeadlineNanos) {
                    // we did our best, give up
                    pendingRows = 0;
                    flushAfterNanos = Long.MAX_VALUE;
                    request = newRequest();
                    throw new LineSenderException("Could not flush buffer: ").put(url).put(" Connection Failed").put(": ").put(e.getMessage());
                }
                retryBackoff = backoff(retryBackoff);
            }
        }
        pendingRows = 0;
        flushAfterNanos = System.nanoTime() + flushIntervalNanos;
        request = newRequest();
    }

    private boolean isRetryableHttpStatus(DirectUtf8Sequence statusCode) {
        if (statusCode == null || statusCode.size() != 3 || statusCode.byteAt(0) != '5') {
            return false;
        }

        /*
        We are retrying on the following response codes (copied from the Rust client):
        500:  Internal Server Error
        503:  Service Unavailable
        504:  Gateway Timeout

        // Unofficial extensions
        507:  Insufficient Storage
        509:  Bandwidth Limit Exceeded
        523:  Origin is Unreachable
        524:  A Timeout Occurred
        529:  Site is overloaded
        599:  Network Connect Timeout Error
        */

        byte middle = statusCode.byteAt(1);
        byte last = statusCode.byteAt(2);
        return (middle == '0' && (last == '0' || last == '3' || last == '4' || last == '7' || last == '9'))
                || (middle == '2' && (last == '3' || last == '4' || last == '9'))
                || (middle == '9' && last == '9');
    }

    private HttpClient.Request newRequest() {
        HttpClient.Request r = client.newRequest(host, port)
                .POST()
                .url(path)
                .header("User-Agent", "QuestDB/java/" + questdbVersion);
        if (username != null) {
            r.authBasic(username, password);
        } else if (authToken != null) {
            r.authToken(null, authToken);
        }
        r.withContent();
        rowBookmark = r.getContentLength();
        return r;
    }

    /**
     * @return true if flush is required
     */
    private boolean rowAdded() {
        pendingRows++;
        long nowNanos = System.nanoTime();
        if (flushAfterNanos == Long.MAX_VALUE) {
            flushAfterNanos = nowNanos + flushIntervalNanos;
        } else if (flushAfterNanos - nowNanos < 0) {
            return true;
        }
        return pendingRows == autoFlushRows;
    }

    private void throwOnHttpErrorResponse(DirectUtf8Sequence statusCode, HttpClient.ResponseHeaders response) {
        // be ready for next request
        flushAfterNanos = Long.MAX_VALUE;
        pendingRows = 0;
        request = newRequest();

        CharSequence statusAscii = statusCode.asAsciiCharSequence();
        if (Chars.equals("404", statusAscii)) {
            consumeChunkedResponse(response);
            client.disconnect();
            throw new LineSenderException("Could not flush buffer: HTTP endpoint does not support ILP. [http-status=404]");
        }
        if (Chars.equals("401", statusAscii) || Chars.equals("403", statusAscii)) {
            sink.clear();
            chunkedResponseToSink(response, sink);
            LineSenderException ex = new LineSenderException("Could not flush buffer: HTTP endpoint authentication error");
            if (sink.length() > 0) {
                ex = ex.put(": ").put(sink);
            }
            ex.put(" [http-status=").put(statusAscii).put(']');
            client.disconnect();
            throw ex;
        }
        DirectUtf8Sequence contentType = response.getContentType();
        if (contentType != null && Utf8s.equalsAscii("application/json", contentType)) {
            if (jsonErrorParser == null) {
                jsonErrorParser = new JsonErrorParser();
            }
            jsonErrorParser.reset();
            LineSenderException ex = jsonErrorParser.toException(response.getResponse(), statusCode);
            client.disconnect();
            throw ex;
        }
        // ok, no JSON, let's do something more generic
        sink.clear();
        sink.put("Could not flush buffer: ");
        chunkedResponseToSink(response, sink);
        sink.put(" [http-status=").put(statusCode).put(']');
        client.disconnect();
        throw new LineSenderException(sink);
    }

    private void validateColumnName(CharSequence name) {
        if (!TableUtils.isValidColumnName(name, Integer.MAX_VALUE)) {
            throw new LineSenderException("column name contains an illegal char: '\\n', '\\r', '?', '.', ','" +
                    ", ''', '\"', '\\', '/', ':', ')', '(', '+', '-', '*' '%%', '~', or a non-printable char: ").putAsPrintable(name);
        }
    }

    private void validateNotClosed() {
        if (closed) {
            throw new LineSenderException("sender already closed");
        }
    }

    private void validateTableName(CharSequence name) {
        if (!TableUtils.isValidTableName(name, Integer.MAX_VALUE)) {
            throw new LineSenderException("table name contains an illegal char: '\\n', '\\r', '?', ',', ''', " +
                    "'\"', '\\', '/', ':', ')', '(', '+', '*' '%%', '~', or a non-printable char: ").putAsPrintable(name);
        }
    }

    private HttpClient.Request writeFieldName(CharSequence name) {
        validateColumnName(name);
        switch (state) {
            case EMPTY:
                throw new LineSenderException("table name must be set first");
            case ADDING_SYMBOLS:
                // fall through
            case TABLE_NAME_SET:
                request.putAscii(' ');
                state = RequestState.ADDING_COLUMNS;
                break;
            case ADDING_COLUMNS:
                request.putAscii(',');
                break;
        }
        escapeQuotedString(name);
        request.put('=');
        return request;
    }

    enum RequestState {
        EMPTY,
        TABLE_NAME_SET,
        ADDING_SYMBOLS,
        ADDING_COLUMNS,
    }

    private static class JsonErrorParser implements JsonParser, Closeable {
        private final StringSink codeSink = new StringSink();
        private final StringSink errorIdSink = new StringSink();
        private final StringSink jsonSink = new StringSink();
        private final JsonLexer lexer = new JsonLexer(1024, 1024);
        private final StringSink lineSink = new StringSink();
        private final StringSink messageSink = new StringSink();
        private State state = State.INIT;

        @Override
        public void close() {
            Misc.free(lexer);
        }

        @Override
        public void onEvent(int code, CharSequence tag, int position) throws JsonException {
            switch (state) {
                case INIT:
                    if (code == JsonLexer.EVT_OBJ_START) {
                        state = State.NEXT_KEY_NAME;
                    } else {
                        throw JsonException.$(position, "expected '{'");
                    }
                    break;
                case NEXT_KEY_NAME:
                    if (code == JsonLexer.EVT_OBJ_END) {
                        state = State.INIT;
                    } else if (code == JsonLexer.EVT_NAME) {
                        if (Chars.equals("code", tag)) {
                            state = State.NEXT_CODE_VALUE;
                        } else if (Chars.equals("message", tag)) {
                            state = State.NEXT_MESSAGE_VALUE;
                        } else if (Chars.equals("line", tag)) {
                            state = State.NEXT_LINE_NUMBER_VALUE;
                        } else if (Chars.equals("errorId", tag)) {
                            state = State.NEXT_ERROR_ID_VALUE;
                        } else {
                            throw JsonException.$(position, "expected 'code', 'message', 'line' or 'error'");
                        }
                    } else {
                        throw JsonException.$(position, "expected 'error' or 'message'");
                    }
                    break;
                case NEXT_CODE_VALUE:
                    if (code == JsonLexer.EVT_VALUE) {
                        codeSink.put(tag);
                        state = State.NEXT_KEY_NAME;
                    } else {
                        throw JsonException.$(position, "expected number");
                    }
                    break;
                case NEXT_MESSAGE_VALUE:
                    if (code == JsonLexer.EVT_VALUE) {
                        messageSink.put(tag);
                        state = State.NEXT_KEY_NAME;
                    } else {
                        throw JsonException.$(position, "expected string");
                    }
                    break;
                case NEXT_LINE_NUMBER_VALUE:
                    if (code == JsonLexer.EVT_VALUE) {
                        lineSink.put(tag);
                        state = State.NEXT_KEY_NAME;
                    } else {
                        throw JsonException.$(position, "expected number");
                    }
                    break;
                case NEXT_ERROR_ID_VALUE:
                    if (code == JsonLexer.EVT_VALUE) {
                        errorIdSink.put(tag);
                        state = State.NEXT_KEY_NAME;
                    } else {
                        throw JsonException.$(position, "expected string");
                    }
                    break;
                case DONE:
                    break;
            }
        }

        private void drainAndReset(LineSenderException sink, DirectUtf8Sequence httpStatus) {
            assert state == State.INIT;

            sink.put(messageSink).put(" [http-status=").put(httpStatus.asAsciiCharSequence());
            if (codeSink.length() > 0 || errorIdSink.length() > 0 || lineSink.length() > 0) {
                if (errorIdSink.length() > 0) {
                    sink.put(", id: ").put(errorIdSink);
                }
                if (codeSink.length() > 0) {
                    sink.put(", code: ").put(codeSink);
                }
                if (lineSink.length() > 0) {
                    sink.put(", line: ").put(lineSink);
                }
            }
            sink.put(']');
            reset();
        }

        private void reset() {
            state = State.INIT;
            codeSink.clear();
            errorIdSink.clear();
            lineSink.clear();
            messageSink.clear();
            lexer.clear();
            jsonSink.clear();
        }

        LineSenderException toException(Response chunkedRsp, DirectUtf8Sequence httpStatus) {
            Fragment fragment;
            LineSenderException exception = new LineSenderException("Could not flush buffer: ");
            while ((fragment = chunkedRsp.recv()) != null) {
                try {
                    jsonSink.putNonAscii(fragment.lo(), fragment.hi());
                    lexer.parse(fragment.lo(), fragment.hi(), this);
                } catch (JsonException e) {
                    // we failed to parse JSON, but we still want to show the error message.
                    // if we cannot parse it then we show the whole response as is.
                    // let's make sure we have the whole message - there might be more chunks
                    while ((fragment = chunkedRsp.recv()) != null) {
                        jsonSink.putNonAscii(fragment.lo(), fragment.hi());
                    }
                    exception.put(jsonSink).put(" [http-status=").put(httpStatus.asAsciiCharSequence()).put(']');
                    reset();
                    return exception;
                }
            }
            drainAndReset(exception, httpStatus);
            return exception;
        }

        enum State {
            INIT,
            NEXT_KEY_NAME,
            NEXT_CODE_VALUE,
            NEXT_MESSAGE_VALUE,
            NEXT_LINE_NUMBER_VALUE,
            NEXT_ERROR_ID_VALUE,
            DONE
        }
    }
}
