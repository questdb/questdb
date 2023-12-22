package io.questdb.cutlass.line.http;

import io.questdb.cairo.TableUtils;
import io.questdb.client.Sender;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.DirectUtf8Sequence;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public final class LineHttpSender implements Sender {
    private static final String URL = "/write";
    private final String host;
    private final int port;
    private HttpClient client;
    private boolean closed;
    private HttpClient.Request request;
    private RequestState state = RequestState.EMPTY;

    public LineHttpSender(String host, int port, int initialBufferCapacity, boolean tls) {
        this.host = host;
        this.port = port;
        this.client = tls ? HttpClientFactory.newTlsInstance() : HttpClientFactory.newPlainTextInstance();
        this.request = newRequest();
    }

    @Override
    public void at(long timestamp, ChronoUnit unit) {
        request.putAscii(' ').put(timestamp * unitToNanos(unit));
    }

    @Override
    public void at(Instant timestamp) {
        long nanos = timestamp.getEpochSecond() * Timestamps.SECOND_NANOS + timestamp.getNano();
        request.putAscii(' ').put(nanos);
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
    }

    @Override
    public Sender boolColumn(CharSequence name, boolean value) {
        writeFieldName(name);
        request.put(value ? 't' : 'f');
        return this;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            flush();
        } finally {
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
        if (state != RequestState.EMPTY) {
            //todo: maybe try to send everything up until the last end of row?
            throw new LineSenderException("cannot flush while row is in progress");
        }
        HttpClient.ResponseHeaders response = request.send(host, port);
        response.await();
        if (!isSuccessResponse(response)) {
            response.getStatusCode();
            throw new LineSenderException("unexpected status code: "); //todo: add status code
        }
        request = newRequest();
    }

    @Override
    public Sender longColumn(CharSequence name, long value) {
        writeFieldName(name);
        request.put(value);
        request.put('i');
        return this;
    }

    @Override
    public Sender stringColumn(CharSequence name, CharSequence value) {
        writeFieldName(name);
        request.put('"');
        writeEscapedString_insideQuotes(value);
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
                writeEscapedString_notInQuotes(name);
                request.putAscii('=');
                writeEscapedString_notInQuotes(value);
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
        writeEscapedString_notInQuotes(table);
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, long value, ChronoUnit unit) {
        // micros
        writeFieldName(name).put(value * unitToNanos(unit) / 1000).put('t');
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, Instant value) {
        // micros
        writeFieldName(name).put((value.getEpochSecond() * Timestamps.SECOND_NANOS + value.getNano()) / 1000).put('t');
        return this;
    }

    private static boolean isSuccessResponse(HttpClient.ResponseHeaders response) {
        DirectUtf8Sequence statusCode = response.getStatusCode();
        return statusCode.size() == 3 && statusCode.byteAt(0) == '2';
    }

    private static long unitToNanos(ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return 1;
            case MICROS:
                return 1_000;
            case MILLIS:
                return 1_000_000;
            case SECONDS:
                return 1_000_000_000;
            default:
                return unit.getDuration().toNanos();
        }
    }

    private HttpClient.Request newRequest() {
        return client.newRequest().POST().url(URL).withContent();
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

    private void writeEscapedString_insideQuotes(CharSequence value) {
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

    private void writeEscapedString_notInQuotes(CharSequence name) {
        for (int i = 0, n = name.length(); i < n; i++) {
            char c = name.charAt(i);
            switch (c) {
                case ' ':
                case ',':
                case '=':
                case '\n':
                case '\r':
                case '\\':
                    request.put((byte) '\\');
                    request.put((byte) c);
                    break;
                default:
                    request.put(c);
                    break;
            }
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
        writeEscapedString_notInQuotes(name);
        request.put('=');
        return request;
    }

    enum RequestState {
        EMPTY,
        TABLE_NAME_SET,
        ADDING_SYMBOLS,
        ADDING_COLUMNS,
    }
}
