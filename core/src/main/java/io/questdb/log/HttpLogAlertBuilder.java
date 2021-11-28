/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.log;

import io.questdb.VisibleForTesting;
import io.questdb.std.*;


public class HttpLogAlertBuilder extends LogRecordSink {

    static final String CRLF = "\r\n";
    private static final String CL_MARKER = "#######";
    private static final int CL_MARKER_LEN = CL_MARKER.length(); // number of digits available for contentLength
    private static final int CL_MARKER_MAX_LEN = (int) Math.pow(10, CL_MARKER_LEN) - 1;
    private static final int MARK_NOT_SET = -1;


    private long mark = MARK_NOT_SET;
    private boolean hasContentLengthMarker;
    private long contentLengthEnd;
    private long bodyStart;
    private Sinkable footer;

    public HttpLogAlertBuilder(LogAlertSocket alertSkt) {
        this(alertSkt.getOutBufferPtr(), alertSkt.getOutBufferSize());
    }

    public HttpLogAlertBuilder(long address, long addressSize) {
        super(address, addressSize);
        contentLengthEnd = _wptr;
        bodyStart = _wptr;
    }

    public HttpLogAlertBuilder putHeader(CharSequence localHostIp) {
        clear();
        put("POST /api/v1/alerts HTTP/1.1").put(CRLF)
                .put("Host: ").put(localHostIp).put(CRLF)
                .put("User-Agent: QuestDB/LogAlert").put(CRLF)
                .put("Accept: */*").put(CRLF)
                .put("Content-Type: application/json").put(CRLF)
                .putContentLengthMarker();
        put(CRLF); // header/body separator
        bodyStart = _wptr;
        return this;
    }

    public int $() {
        if (footer != null) {
            footer.toSink(this);
        }

        if (hasContentLengthMarker) {
            // take the body length and format it into the ###### contentLength marker
            int bodyLen = (int) (_wptr - bodyStart);
            if (bodyLen > CL_MARKER_MAX_LEN) {
                throw new LogError("Content too long");
            }

            long p = contentLengthEnd;
            if (bodyLen == 0) {
                Unsafe.getUnsafe().putByte(p--, (byte) '0');
            } else {
                int rem = bodyLen % 10;
                while (bodyLen > 0) {
                    Unsafe.getUnsafe().putByte(p--, (byte) (('0' + rem) & 0x000000FF));
                    bodyLen /= 10;
                    rem = bodyLen % 10;
                }
            }
            int lpadLen = (int) (CL_MARKER_LEN - contentLengthEnd + p);
            for (int lpad = 0; lpad < lpadLen; lpad++) {
                Unsafe.getUnsafe().putByte(p--, (byte) ' ');
            }
        }
        return length();
    }


    public HttpLogAlertBuilder setFooter(Sinkable footer) {
        this.footer = footer;
        return this;
    }

    public HttpLogAlertBuilder setMark() {
        mark = _wptr;
        return this;
    }

    public long getMark() {
        return mark;
    }

    public HttpLogAlertBuilder rewindToMark() {
        _wptr = mark == MARK_NOT_SET ? address : mark;
        return this;
    }

    @Override
    public void clear() {
        super.clear();
        mark = MARK_NOT_SET;
        contentLengthEnd = _wptr;
        bodyStart = _wptr;
        footer = null;
        hasContentLengthMarker = false;
    }

    public HttpLogAlertBuilder put(LogRecordSink logRecord) {
        final int len = logRecord.length();
        final long address = logRecord.getAddress();
        for (long p = address, limit = address + len; p < limit; p++) {
            byte c = Unsafe.getUnsafe().getByte(p);
            switch (c) {
                case '\b': // ignore chars
                case '\f':
                case '\r':
                case '\n':
                    break;

                case '\t': // replace tab
                    put(' ');
                    break;

                case '$': // escape
                    put("\\$");
                    break;

                case '"': // escape
                    put("\\\"");
                    break;

                default:
                    put((char) c);
                    break;
            }
        }
        return this;
    }

    @Override
    public HttpLogAlertBuilder put(CharSequence cs) {
        super.put(cs);
        return this;
    }

    @Override
    public HttpLogAlertBuilder put(CharSequence cs, int lo, int hi) {
        super.put(cs, lo, hi);
        return this;
    }

    @Override
    public HttpLogAlertBuilder put(Sinkable sinkable) {
        super.put(sinkable);
        return this;
    }

    @Override
    public HttpLogAlertBuilder put(char c) {
        super.put(c);
        return this;
    }

    @Override
    public String toString() {
        return Chars.stringFromUtf8Bytes(address, _wptr);
    }

    @VisibleForTesting
    void putContentLengthMarker() {
        put("Content-Length:").put(CL_MARKER);
        contentLengthEnd = _wptr - 1; // will scan backwards from here
        put(CRLF);
        hasContentLengthMarker = true;
    }
}
