package io.questdb.cutlass.http;

import io.questdb.cutlass.http.processors.RejectProcessor;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.http.HttpConstants.*;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

public class HttpRequestValidator {
    public static final byte INVALID = 1;
    public static final byte METHOD_GET = 2;
    public static final byte METHOD_MULTIPART_POST = 4;
    public static final byte METHOD_MULTIPART_PUT = 8;
    public static final byte METHOD_POST = 16;
    public static final byte METHOD_PUT = 32;
    public static final byte ALL = METHOD_GET | METHOD_MULTIPART_POST | METHOD_MULTIPART_PUT | METHOD_POST | METHOD_PUT;
    private boolean chunked;
    private long contentLength;
    private boolean multipart;
    private HttpRequestHeader requestHeader;
    private byte requestType = INVALID;

    HttpRequestValidator() {
    }

    void of(HttpRequestHeader requestHeader) {
        this.requestHeader = requestHeader;

        contentLength = requestHeader.getContentLength();
        chunked = Utf8s.equalsNcAscii(HEADER_TRANSFER_ENCODING_CHUNKED, requestHeader.getHeader(HEADER_TRANSFER_ENCODING));
        multipart = Utf8s.equalsNcAscii(CONTENT_TYPE_MULTIPART_FORM_DATA, requestHeader.getContentType())
                || Utf8s.equalsNcAscii(CONTENT_TYPE_MULTIPART_MIXED, requestHeader.getContentType());
        requestType = INVALID;
    }

    void validateRequestHeader(RejectProcessor rejectProcessor) {
        if (requestHeader.getUrl() == null) {
            throw HttpException.instance("missing URL");
        }

        // We could throw HttpException for the below problems too,
        // because they are protocol violations as well.
        // However, a HttpException results in disconnect,
        // it is more convenient for the user to handle a reject.
        if (requestHeader.isGetRequest()) {
            if (chunked || multipart || contentLength > 0) {
                rejectProcessor.reject(HTTP_BAD_REQUEST, "GET request method cannot have content");
                return;
            }
            requestType = METHOD_GET;
        } else if (requestHeader.isPostRequest() || requestHeader.isPutRequest()) {
            if (chunked && contentLength > 0) {
                rejectProcessor.reject(HTTP_BAD_REQUEST, "Invalid chunked request; content-length specified");
                return;
            }
            if (!chunked && contentLength < 0) {
                rejectProcessor.reject(HTTP_BAD_REQUEST, "Content-length not specified for POST/PUT request");
                return;
            }
            if (!multipart) {
                requestType = requestHeader.isPostRequest() ? METHOD_POST : METHOD_PUT;
            } else {
                requestType = requestHeader.isPostRequest() ? METHOD_MULTIPART_POST : METHOD_MULTIPART_PUT;
            }
        }
    }

    HttpRequestProcessor validateRequestType(HttpRequestProcessor processor, RejectProcessor rejectProcessor) {
        if ((processor.getSupportedRequestTypes() & requestType) == 0) {
            final Utf8Sequence method = requestHeader.getMethod();
            rejectProcessor.getMessageSink()
                    .put(requestHeader.isPostRequest() || requestHeader.isPutRequest()
                            ? (multipart ? "Multipart " : "Non-multipart ")
                            : "Method ")
                    .put(method)
                    .put(!Utf8s.equalsAscii("", method) ? " not supported" : "not supported");
            return rejectProcessor.reject(HTTP_NOT_FOUND);
        }
        return processor;
    }
}
