package io.questdb.cutlass.http;

import io.questdb.cutlass.http.processors.RejectProcessor;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.http.HttpConstants.HEADER_TRANSFER_ENCODING;
import static java.net.HttpURLConnection.HTTP_BAD_METHOD;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;

// request type is encoded in a short
// bits of the lower byte correspond to HTTP methods:
// 0 bit: GET (1), 1 bit: POST (2), 2 bit: PUT (4)
// we can assign more method types in the future, if necessary
// bits of the higher byte correspond to multipart/non-multipart content types:
// 0 bit: non-multipart (256), 1 bit: multipart (512)
// in the future these groups could be broken up into multiple, more specific content types
public class HttpRequestValidator {
    public static final short ALL_CONTENT_TYPES = 0x7F00;
    public static final short ALL_METHODS = 0x00FF;
    public static final short ALL = ALL_METHODS | ALL_CONTENT_TYPES;
    public static final short INVALID = Short.MIN_VALUE;
    public static final short METHOD_GET = 0x0001;
    public static final short METHOD_POST = METHOD_GET << 1;
    public static final short METHOD_PUT = METHOD_POST << 1;
    public static final short NON_MULTIPART_REQUEST = 0x0100;
    public static final short MULTIPART_REQUEST = NON_MULTIPART_REQUEST << 1;

    private boolean chunked;
    private long contentLength;
    private boolean multipart;
    private HttpRequestHeader requestHeader;
    private short requestType = INVALID;

    HttpRequestValidator() {
    }

    void of(HttpRequestHeader requestHeader) {
        this.requestHeader = requestHeader;

        contentLength = requestHeader.getContentLength();
        chunked = HttpKeywords.isChunked(requestHeader.getHeader(HEADER_TRANSFER_ENCODING));
        multipart = HttpKeywords.isContentTypeMultipartFormData(requestHeader.getContentType())
                || HttpKeywords.isContentTypeMultipartMixed(requestHeader.getContentType());
        requestType = INVALID;
    }

    void validateRequestHeader(RejectProcessor rejectProcessor) {
        if (rejectProcessor.isRequestBeingRejected()) {
            return;
        }
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
            requestType = multipart ? MULTIPART_REQUEST : NON_MULTIPART_REQUEST;
            requestType |= requestHeader.isPostRequest() ? METHOD_POST : METHOD_PUT;
        }
    }

    HttpRequestProcessor validateRequestType(HttpRequestProcessor processor, RejectProcessor rejectProcessor) {
        if ((processor.getSupportedRequestTypes() & requestType) == requestType) {
            // request type is supported, check passed
            return processor;
        }

        // request type is not supported
        // we need to work out the right error code and message
        final Utf8Sequence method = requestHeader.getMethod();
        if (method == null || Utf8s.equalsAscii("", method)) {
            rejectProcessor.getMessageSink().put("Method is not set in HTTP request");
            return rejectProcessor.reject(HTTP_BAD_REQUEST);
        }
        if ((processor.getSupportedRequestTypes() & ALL_METHODS & requestType) == 0) {
            rejectProcessor.getMessageSink().put("Method ").put(method).put(" not supported");
            return rejectProcessor.reject(HTTP_BAD_METHOD);
        }
        rejectProcessor.getMessageSink().put(multipart ? "Multipart " : "Non-multipart ").put(method).put(" not supported");
        return rejectProcessor.reject(HTTP_BAD_REQUEST);
    }
}
