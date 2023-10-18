package io.questdb.cutlass.http;

public class HttpConstants {
    public static final String CONTENT_DISPOSITION_HEADER = "Content-Disposition";
    public static final String CONTENT_TYPE_CSV = "text/csv; charset=utf-8";
    public static final String CONTENT_TYPE_HEADER = "Content-Type";
    public static final String CONTENT_TYPE_HTML = "text/html; charset=utf-8";
    public static final String CONTENT_TYPE_JSON = "application/json; charset=utf-8";
    public static final String CONTENT_TYPE_TEXT = "text/plain; charset=utf-8";
    public static final String COOKIE_HEADER = "Cookie: ";
    public static final char COOKIE_SEPARATOR = ';';
    public static final char COOKIE_VALUE_SEPARATOR = '=';
    public static final String DDL_OK = "0c\r\n{\"ddl\":\"OK\"}";
    public static final String EOL_EOL = "\r\n\r\n";
    public static final String HTTP_BAD_RESPONSE_STATUS = "HTTP/1.1 400 Bad request\r\n";
    public static final String HTTP_OK_RESPONSE_STATUS = "HTTP/1.1 200 OK\r\n";
    public static final String HTTP_RESPONSE_HEADERS_NO_COOKIE = "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: application/json; charset=utf-8\r\n" +
            "Keep-Alive: timeout=5, max=10000";
    public static final String HTTP_RESPONSE_SUFFIX = "\r\n00" + EOL_EOL;
    public static final String SET_COOKIE_HEADER = "Set-Cookie: ";
}
