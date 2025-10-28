package io.questdb.cutlass.http;

import io.questdb.cairo.CairoException;
import io.questdb.cutlass.http.processors.RejectProcessor;
import io.questdb.griffin.engine.functions.str.TrimType;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cutlass.http.HttpConstants.*;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;

public class HttpCookieHandlerImpl implements HttpCookieHandler {
    protected static final ThreadLocal<StringSink> tlSink1 = new ThreadLocal<>(StringSink::new);
    protected static final ThreadLocal<StringSink> tlSink2 = new ThreadLocal<>(StringSink::new);

    @Override
    public boolean parseCookies(HttpConnectionContext context) {
        final CharSequenceObjHashMap<CharSequence> parsedCookies = context.getParsedCookiesMap();
        parsedCookies.clear();
        try {
            final DirectUtf8Sequence cookies = context.getRequestHeader().getHeader(HEADER_COOKIE);
            final StringSink cookiesSink = tlSink1.get();
            cookiesSink.clear();
            cookiesSink.put(cookies);
            if (!Chars.isBlank(cookiesSink)) {
                int cookieStartIndex = 0;
                while (cookieStartIndex < cookiesSink.length()) {
                    int cookieEndIndex = Chars.indexOf(cookiesSink, cookieStartIndex, COOKIE_SEPARATOR);
                    if (cookieEndIndex < 0) {
                        cookieEndIndex = cookiesSink.length();
                    }
                    final CharSequence cookie = cookiesSink.subSequence(cookieStartIndex, cookieEndIndex);
                    final StringSink cookieSink = tlSink2.get();
                    Chars.trim(TrimType.LTRIM, cookie, cookieSink);
                    parseCookie(cookieSink, parsedCookies);
                    cookieStartIndex = cookieEndIndex + 1;
                }
            }
        } catch (CairoException e) {
            rejectWithCookies(context.getRejectProcessor()).reject(HTTP_BAD_REQUEST, e.getFlyweightMessage());
            return false;
        }
        return true;
    }

    @Override
    public CharSequence processSessionCookie(HttpConnectionContext context) {
        final CharSequence sessionId = context.getParsedCookiesMap().get(SESSION_COOKIE_NAME);
        if (Chars.empty(sessionId) || !Chars.startsWith(sessionId, SESSION_ID_PREFIX)) {
            return null;
        }
        return sessionId;
    }

    @Override
    public void setSessionCookie(HttpResponseHeader header, CharSequence sessionId) {
        final StringSink cookieValueSink = tlSink1.get();
        cookieValueSink.clear();

        if (sessionId != null) {
            cookieValueSink.put(sessionId);
            appendSessionCookieAttributes(cookieValueSink);
        } else {
            cookieValueSink.put(DELETED_COOKIE);
        }

        header.setCookie(SESSION_COOKIE_NAME, cookieValueSink);
    }

    private void parseCookie(@NotNull CharSequence cookie, CharSequenceObjHashMap<CharSequence> parsedCookies) {
        final int separatorIndex = Chars.indexOf(cookie, COOKIE_VALUE_SEPARATOR);
        if (separatorIndex < 0) {
            throw CairoException.nonCritical().put("Invalid cookie [").put(cookie).put(']');
        } else {
            final CharSequence cookieValue = cookie.subSequence(separatorIndex + 1, cookie.length()).toString();
            if (Chars.isBlank(cookieValue)) {
                throw CairoException.nonCritical().put("Empty cookie value [").put(cookie).put(']');
            }
            final CharSequence cookieName = cookie.subSequence(0, separatorIndex);
            recordCookie(cookieName, cookieValue, parsedCookies);
        }
    }

    protected void appendSessionCookieAttributes(StringSink cookieValueSink) {
        cookieValueSink.put(SESSION_COOKIE_ATTRIBUTES);
    }

    protected void recordCookie(@NotNull CharSequence cookieName, @NotNull CharSequence cookieValue, CharSequenceObjHashMap<CharSequence> parsedCookies) {
        if (Chars.equals(cookieName, SESSION_COOKIE_NAME)) {
            parsedCookies.put(SESSION_COOKIE_NAME, cookieValue);
        }
    }

    protected RejectProcessor rejectWithCookies(RejectProcessor rejectProcessor) {
        return rejectProcessor.withCookie(SESSION_COOKIE_NAME, DELETED_COOKIE);
    }
}
