/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cutlass.http;

import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.http.HttpConstants.HEADER_CONTENT_DISPOSITION;
import static io.questdb.test.cutlass.http.SendAndReceiveRequestBuilder.RequestHeaders;

/**
 * The /exp endpoint copies its filename URL parameter into the Content-Disposition response
 * header. Most tests here send raw requests and compare the raw response, so a header line that
 * the file name splits off, or a quote that ends the file name early, shows up as a byte difference.
 */
public class ExpContentDispositionTest extends AbstractTest {
    private static final String BAD_UTF8_RESPONSE = """
            HTTP/1.1 400 Bad request\r
            Server: questDB/1.0\r
            Date: Thu, 1 Jan 1970 00:00:00 GMT\r
            Transfer-Encoding: chunked\r
            Content-Type: application/json; charset=utf-8\r
            Keep-Alive: timeout=5, max=10000\r
            \r
            4b\r
            {"query":"select 1 x","error":"Bad UTF8 encoding in filename","position":0}\r
            00\r
            \r
            """;
    private static final String CONTROL_CHAR_RESPONSE = """
            HTTP/1.1 400 Bad request\r
            Server: questDB/1.0\r
            Date: Thu, 1 Jan 1970 00:00:00 GMT\r
            Transfer-Encoding: chunked\r
            Content-Type: application/json; charset=utf-8\r
            Keep-Alive: timeout=5, max=10000\r
            \r
            5a\r
            {"query":"select 1 x","error":"filename must not contain control characters","position":0}\r
            00\r
            \r
            """;
    private static final String DEFAULT_FILE_NAME = "questdb-query-0.csv";
    private static final String NO_FILE_NAME_REQUEST = "GET /exp?query=select+1+x HTTP/1.1\r\n" + RequestHeaders;

    @Test
    public void testBadUtf8IsRejected() throws Exception {
        getSimpleTester().run((_, _) -> new SendAndReceiveRequestBuilder().executeMany(executor -> {
            final String[] fileNames = {
                    // a lead byte followed by a byte that does not continue it
                    "a%C3%28",
                    // a continuation byte without a lead byte
                    "a%80",
                    // a byte that never occurs in UTF-8
                    "a%FF",
                    // a 4-byte sequence cut short by the end of the name
                    "a%F0%9F%98",
                    // a UTF-16 surrogate encoded as UTF-8
                    "a%ED%A0%80",
                    // overlong encodings of CR, which a lenient decoder turns into a line break
                    "a%C0%8D",
                    "a%E0%80%8D",
                    "a%F0%80%80%8D",
                    // invalid UTF-8 together with a control character
                    "%0A%FF"
            };
            for (String fileName : fileNames) {
                executor.execute(csvRequest(fileName), BAD_UTF8_RESPONSE);
                executor.execute(parquetRequest(fileName), BAD_UTF8_RESPONSE);
            }
            executor.execute(csvRequest("report"), csvResponse("report.csv"));
        }));
    }

    @Test
    public void testControlCharactersAreRejected() throws Exception {
        getSimpleTester().run((_, _) -> new SendAndReceiveRequestBuilder().executeMany(executor -> {
            // every RFC 5234 CTL, 0x00 to 0x1F and 0x7F, alone and at the start, middle and end
            for (int c = 0; c < 0x80; c = c == 0x1F ? 0x7F : c + 1) {
                final String ctl = String.format("%%%02X", c);
                for (String fileName : new String[]{ctl, ctl + "a", "a" + ctl + "b", "a" + ctl}) {
                    executor.execute(csvRequest(fileName), CONTROL_CHAR_RESPONSE);
                    executor.execute(parquetRequest(fileName), CONTROL_CHAR_RESPONSE);
                }
            }
            executor.execute(csvRequest("report"), csvResponse("report.csv"));
        }));
    }

    @Test
    public void testEmptyFileNameGetsDefaultName() throws Exception {
        getSimpleTester().run((_, _) -> new SendAndReceiveRequestBuilder().executeMany(executor -> {
            executor.execute(csvRequest(""), csvResponse(DEFAULT_FILE_NAME));
            executor.execute(NO_FILE_NAME_REQUEST, csvResponse(DEFAULT_FILE_NAME));
        }));
    }

    @Test
    public void testHeaderInjectionIsRejected() throws Exception {
        getSimpleTester().run((_, _) -> new SendAndReceiveRequestBuilder().executeMany(executor -> {
            final String[] fileNames = {
                    // the reported payload
                    "x%0D%0ASet-Cookie:%20session%3Dattacker;%20Path%3D/%0D%0AX-Injected:%20yes",
                    "x%0d%0aSet-Cookie:%20a%3Db",
                    // bare CR and bare LF, which lenient HTTP parsers also take as line breaks
                    "x%0DSet-Cookie:%20a%3Db",
                    "x%0ASet-Cookie:%20a%3Db",
                    // an empty line, which would end the headers and start the body early
                    "x%0D%0A%0D%0A%3Chtml%3E"
            };
            for (String fileName : fileNames) {
                executor.execute(csvRequest(fileName), CONTROL_CHAR_RESPONSE);
                executor.execute(parquetRequest(fileName), CONTROL_CHAR_RESPONSE);
            }
        }));
    }

    @Test
    public void testNonAsciiFileName() throws Exception {
        assertCsvFileNames(
                // 2-byte UTF-8
                "%C3%A9t%C3%A9", "été.csv",
                // 3-byte UTF-8
                "%E5%A0%B1%E5%91%8A", "報告.csv",
                // 4-byte UTF-8, a surrogate pair in UTF-16
                "%F0%9F%98%80", "\uD83D\uDE00.csv",
                // escapes next to multibyte characters
                "%C3%A9%22%F0%9F%98%80%5C", "é\\\"\uD83D\uDE00\\\\.csv",
                // C1 controls, U+0080 and U+009F, encode to bytes above 0x7F, which cannot end a header line
                "%C2%80%C2%9F", "\u0080\u009F.csv"
        );
    }

    @Test
    public void testParquetFileNameIsEscaped() throws Exception {
        getSimpleTester().run((_, _) -> {
            try (TestHttpClient client = new TestHttpClient()) {
                // with rmode=nodelay the header goes out before the export starts, without it with the first data
                for (boolean isNoDelay : new boolean[]{false, true}) {
                    assertParquetContentDisposition(client.getHttpClient(), "report", isNoDelay, "attachment; filename=\"report.parquet\"");
                    assertParquetContentDisposition(client.getHttpClient(), "a\"b\\c", isNoDelay, "attachment; filename=\"a\\\"b\\\\c.parquet\"");
                    assertParquetContentDisposition(client.getHttpClient(), "", isNoDelay, "attachment; filename=\"questdb-query-0.parquet\"");
                }
            }
        });
    }

    @Test
    public void testPrintableBoundaries() throws Exception {
        assertCsvFileNames(
                // 0x20, the first character after the control range, and '+', which decodes to it
                "%20", " .csv",
                "a+b", "a b.csv",
                // 0x7E, the last character before DEL
                "%7E", "~.csv",
                // characters that delimit header parameters are plain text inside the quoted string
                "a;b%3Dc,d", "a;b=c,d.csv"
        );
    }

    @Test
    public void testQuoteAndBackslashAreEscaped() throws Exception {
        assertCsvFileNames(
                // on the wire: a\"b.csv
                "a%22b", "a\\\"b.csv",
                // on the wire: a\\b.csv
                "a%5Cb", "a\\\\b.csv",
                // on the wire: \".csv
                "%22", "\\\".csv",
                // on the wire: \\.csv
                "%5C", "\\\\.csv",
                // a backslash at the end must not escape the quote that closes the string,
                // on the wire: a\\.csv
                "a%5C", "a\\\\.csv",
                // on the wire: \\\".csv
                "%5C%22", "\\\\\\\".csv",
                // a quote must not end the file name and push the extension out of it,
                // on the wire: evil.bat\"; x=\".csv
                "evil.bat%22;%20x%3D%22", "evil.bat\\\"; x=\\\".csv",
                // every character escaped, which doubles the name in the header
                "%22".repeat(400), "\\\"".repeat(400) + ".csv"
        );
    }

    @Test
    public void testRandomFileNames() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        getSimpleTester()
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .run((_, _) -> new SendAndReceiveRequestBuilder().executeMany(executor -> {
                    for (int i = 0; i < 300; i++) {
                        final byte[] fileName = randomFileName(rnd);
                        executor.execute(csvRequest(percentEncode(fileName)), expectedCsvResponse(fileName));
                    }
                }));
    }

    @Test
    public void testRejectedFileNameDoesNotReachNextRequest() throws Exception {
        getSimpleTester().run((_, _) -> new SendAndReceiveRequestBuilder().executeMany(executor -> {
            executor.execute(csvRequest("a%0D%0Ab"), CONTROL_CHAR_RESPONSE);
            executor.execute(NO_FILE_NAME_REQUEST, csvResponse(DEFAULT_FILE_NAME));
            executor.execute(csvRequest("a%FF"), BAD_UTF8_RESPONSE);
            executor.execute(csvRequest(""), csvResponse(DEFAULT_FILE_NAME));
            executor.execute(csvRequest("a%22b"), csvResponse("a\\\"b.csv"));
            executor.execute(NO_FILE_NAME_REQUEST, csvResponse(DEFAULT_FILE_NAME));
        }));
    }

    private static void assertCsvFileNames(String... urlParamAndHeaderFileName) throws Exception {
        getSimpleTester().run((_, _) -> new SendAndReceiveRequestBuilder().executeMany(executor -> {
            for (int i = 0; i < urlParamAndHeaderFileName.length; i += 2) {
                executor.execute(csvRequest(urlParamAndHeaderFileName[i]), csvResponse(urlParamAndHeaderFileName[i + 1]));
            }
        }));
    }

    private static void assertParquetContentDisposition(
            HttpClient client,
            String fileName,
            boolean isNoDelay,
            String expectedContentDisposition
    ) {
        final HttpClient.Request request = client.newRequest("localhost", 9001);
        request.GET().url("/exp")
                .query("query", "select 1 x")
                .query("fmt", "parquet")
                .query("filename", fileName);
        if (isNoDelay) {
            request.query("rmode", "nodelay");
        }
        try (HttpClient.ResponseHeaders response = request.send()) {
            response.await();
            TestUtils.assertEquals("200", response.getStatusCode());
            TestUtils.assertEquals(expectedContentDisposition, response.getHeader(HEADER_CONTENT_DISPOSITION));
            response.getResponse().discard();
        }
    }

    private static String csvRequest(String fileNameUrlParam) {
        return "GET /exp?query=select+1+x&filename=" + fileNameUrlParam + " HTTP/1.1\r\n" + RequestHeaders;
    }

    private static String csvResponse(String headerFileName) {
        return """
                HTTP/1.1 200 OK\r
                Server: questDB/1.0\r
                Date: Thu, 1 Jan 1970 00:00:00 GMT\r
                Transfer-Encoding: chunked\r
                Content-Type: text/csv; charset=utf-8\r
                Content-Disposition: attachment; filename="%s"\r
                Keep-Alive: timeout=5, max=10000\r
                \r
                08\r
                "x"\r
                1\r
                \r
                00\r
                \r
                """.formatted(headerFileName);
    }

    // The JDK decoder and plain string replacement stand in for the server: invalid UTF-8 comes
    // first, then control characters, then the backslash escapes of an RFC 9110 quoted-string.
    private static String expectedCsvResponse(byte[] fileName) {
        final String decoded;
        try {
            decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(fileName))
                    .toString();
        } catch (CharacterCodingException e) {
            return BAD_UTF8_RESPONSE;
        }
        if (decoded.isEmpty()) {
            return csvResponse(DEFAULT_FILE_NAME);
        }
        for (int i = 0, n = decoded.length(); i < n; i++) {
            final char c = decoded.charAt(i);
            if (c < 0x20 || c == 0x7F) {
                return CONTROL_CHAR_RESPONSE;
            }
        }
        return csvResponse(decoded.replace("\\", "\\\\").replace("\"", "\\\"") + ".csv");
    }

    private static String parquetRequest(String fileNameUrlParam) {
        return "GET /exp?query=select+1+x&fmt=parquet&filename=" + fileNameUrlParam + " HTTP/1.1\r\n" + RequestHeaders;
    }

    private static String percentEncode(byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 3);
        for (byte b : bytes) {
            sb.append(String.format("%%%02X", b & 0xFF));
        }
        return sb.toString();
    }

    private static byte[] randomFileName(Rnd rnd) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        // a quarter of the names get one random byte, which may break UTF-8, and a quarter get one
        // control character, the rest are valid; the expected response comes from the bytes alone
        final int kind = rnd.nextInt(4);
        final int length = rnd.nextInt(16);
        final int specialPos = length > 0 ? rnd.nextInt(length) : -1;
        for (int i = 0; i < length; i++) {
            if (i == specialPos && kind == 0) {
                out.write(rnd.nextInt(256));
                continue;
            }
            if (i == specialPos && kind == 1) {
                final int c = rnd.nextInt(33);
                out.write(c == 32 ? 0x7F : c);
                continue;
            }
            final int codePoint = switch (rnd.nextInt(8)) {
                case 0 -> '"';
                case 1 -> '\\';
                case 2 -> 0x80 + rnd.nextInt(0x800 - 0x80);
                case 3 -> {
                    final int cp = 0x800 + rnd.nextInt(0x10000 - 0x800);
                    yield Character.isSurrogate((char) cp) ? 'x' : cp;
                }
                case 4 -> 0x10000 + rnd.nextInt(0x110000 - 0x10000);
                default -> 0x20 + rnd.nextInt(0x7F - 0x20);
            };
            out.writeBytes(new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8));
        }
        return out.toByteArray();
    }
}
