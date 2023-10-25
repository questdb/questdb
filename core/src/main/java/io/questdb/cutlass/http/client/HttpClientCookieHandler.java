package io.questdb.cutlass.http.client;

public interface HttpClientCookieHandler {
    void processCookies(HttpClient.ResponseHeaders response);

    void setCookies(HttpClient.Request request, CharSequence username);
}
