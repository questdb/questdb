package io.questdb.test.cutlass.http.line;

import io.questdb.cutlass.http.*;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.tools.TestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

final class MockHttpProcessor implements HttpRequestProcessor, HttpMultipartContentListener {
    private static final long MAX_DELIVERY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(10);
    private final Queue<ExpectedRequest> expectedRequests = new ConcurrentLinkedQueue<>();
    private final Queue<ActualRequest> recordedRequests = new ConcurrentLinkedQueue<>();
    private final Queue<Response> responses = new ConcurrentLinkedQueue<>();
    private ActualRequest actualRequest = new ActualRequest();
    private ExpectedRequest expectedRequest = new ExpectedRequest();
    private Response lastResortResponse;

    public MockHttpProcessor delayedReplyWithStatus(int statusCode, CountDownLatch delayLatch) {
        Response response = new Response();
        response.responseStatusCode = statusCode;
        response.delayLatch = delayLatch;
        responses.add(response);

        expectedRequests.add(expectedRequest);
        expectedRequest = new ExpectedRequest();

        return this;
    }

    public MockHttpProcessor keepReplyingWithContent(int statusCode, String responseContent, String contentType) {
        Response response = new Response();
        response.responseStatusCode = statusCode;
        response.responseContent = responseContent;
        response.contentType = contentType;
        lastResortResponse = response;

        expectedRequests.add(expectedRequest);
        expectedRequest = new ExpectedRequest();

        return this;
    }

    @Override
    public void onChunk(long lo, long hi) {
        actualRequest.bodyContent.putUtf8(lo, hi);
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) {
        ObjList<? extends Utf8Sequence> headerNames = context.getRequestHeader().getHeaderNames();
        for (int i = 0, n = headerNames.size(); i < n; i++) {
            Utf8Sequence headerNameUtf8 = headerNames.getQuick(i);
            String headerName = headerNameUtf8.toString();
            String headerValue = context.getRequestHeader().getHeader(headerNameUtf8).toString();
            actualRequest.headers.put(headerName, headerValue);
        }
    }

    @Override
    public void onPartBegin(HttpRequestHeader partHeader) {

    }

    @Override
    public void onPartEnd() {

    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        recordedRequests.add(actualRequest);
        actualRequest = new ActualRequest();

        Response response = responses.poll();
        if (response == null) {
            response = lastResortResponse;
        }
        if (response == null) {
            throw new AssertionError("No response configured for request: " + actualRequest);
        }
        if (response.delayLatch != null) {
            TestUtils.await(response.delayLatch);
        }
        if (response.responseContent != null) {
            HttpChunkedResponse chunkedResponseSocket = context.getChunkedResponse();
            chunkedResponseSocket.status(response.responseStatusCode, response.contentType);
            chunkedResponseSocket.sendHeader();
            chunkedResponseSocket.putAscii(response.responseContent);
            chunkedResponseSocket.sendChunk(true);
        } else {
            context.simpleResponse().sendStatus(response.responseStatusCode);
        }
    }

    public MockHttpProcessor replyWithContent(int statusCode, String responseContent, String contentType) {
        Response response = new Response();
        response.responseStatusCode = statusCode;
        response.responseContent = responseContent;
        response.contentType = contentType;
        responses.add(response);

        expectedRequests.add(expectedRequest);
        expectedRequest = new ExpectedRequest();

        return this;
    }

    public MockHttpProcessor replyWithStatus(int statusCode) {
        Response response = new Response();
        response.responseStatusCode = statusCode;
        responses.add(response);

        expectedRequests.add(expectedRequest);
        expectedRequest = new ExpectedRequest();

        return this;
    }

    @Override
    public boolean requiresAuthentication() {
        return false;
    }

    public void verify() {
        for (int i = 0; !expectedRequests.isEmpty(); i++) {
            ExpectedRequest expectedRequest = expectedRequests.poll();
            ActualRequest actualRequest;
            long deadline = System.nanoTime() + MAX_DELIVERY_DELAY_NANOS;
            do {
                actualRequest = recordedRequests.poll();
            } while (actualRequest == null && System.nanoTime() < deadline);
            verifyInteraction(expectedRequest, actualRequest, i);
        }
        if (!recordedRequests.isEmpty() && lastResortResponse == null) {
            throw new AssertionError("Unexpected requests: " + recordedRequests);
        }
    }

    public MockHttpProcessor withExpectedContent(String expectedContent) {
        expectedRequest.content = expectedContent;
        return this;
    }

    public MockHttpProcessor withExpectedHeader(String headerName, String headerValue) {
        expectedRequest.headers.put(headerName, headerValue);
        return this;
    }

    private void verifyInteraction(ExpectedRequest expectedRequest, ActualRequest actualRequest, int interactionIndex) {
        if (actualRequest == null) {
            throw new AssertionError("Expected request: " + expectedRequest + ", actual: null. Interaction index: " + interactionIndex);
        }
        if (expectedRequest.content != null) {
            if (!Chars.equals(expectedRequest.content, actualRequest.bodyContent.toString())) {
                throw new AssertionError("Expected content: " + expectedRequest.content + ", actual: " + actualRequest.bodyContent + ". Interaction index: " + interactionIndex);
            }
        }
        for (Map.Entry<String, String> header : expectedRequest.headers.entrySet()) {
            String actualHeaderValue = actualRequest.headers.get(header.getKey());
            if (actualHeaderValue == null || !Chars.equals(header.getValue(), actualHeaderValue)) {
                throw new AssertionError("Expected header: " + header.getKey() + "=" + header.getValue() + ", actual: " + actualHeaderValue + ". Interaction index: " + interactionIndex);
            }
        }
    }

    private static class ActualRequest {
        private final StringSink bodyContent = new StringSink();
        private final Map<String, String> headers = new HashMap<>();

        @Override
        public String toString() {
            return "ActualRequest{" +
                    "bodyContent=" + bodyContent +
                    ", headers=" + headers +
                    '}';
        }
    }

    private static class ExpectedRequest {
        private final Map<String, String> headers = new HashMap<>();
        private String content;
    }

    private static class Response {
        private String contentType;
        private CountDownLatch delayLatch;
        private String responseContent;
        private int responseStatusCode;
    }

}
