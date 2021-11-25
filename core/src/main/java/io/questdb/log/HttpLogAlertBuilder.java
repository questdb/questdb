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

import io.questdb.std.*;


public class HttpLogAlertBuilder extends LogRecordSink {

    private static final String HEADER_BODY_SEPARATOR = "\r\n\r\n";
    private static final String CL_MARKER = "######"; // 999999 / (1024*2) == 488 half a Gb payload max
    private static final int CL_MARKER_LEN = CL_MARKER.length();
    private static final int CL_MARKER_MAX_LEN = (int) Math.pow(10, CL_MARKER_LEN) - 1;
    private static final int NOT_SET = -1;


    private long mark = NOT_SET;
    private long contentLenStartLimit;
    private long contentLenEnd;
    private long bodyStart;
    private Sinkable footer;

    public HttpLogAlertBuilder(LogAlertSocket laSkt) {
        this(laSkt.getOutBufferPtr(), laSkt.getOutBufferSize());
    }

    public HttpLogAlertBuilder(long address, long addressSize) {
        super(address, addressSize);
        contentLenStartLimit = _wptr;
        contentLenEnd = _wptr;
        bodyStart = _wptr;
    }

    public HttpLogAlertBuilder putHeader(CharSequence localHostIp) {
        clear();
        put("POST /api/v1/alerts HTTP/1.1\r\n")
                .put("Host: ").put(localHostIp).put("\r\n")
                .put("User-Agent: QuestDB/LogAlert").put("\r\n")
                .put("Accept: */*\r\n")
                .put("Content-Type: application/json\r\n")
                .put("Content-Length: ");
        contentLenStartLimit = _wptr - 1;
        put(CL_MARKER);
        contentLenEnd = _wptr - 1;
        put(HEADER_BODY_SEPARATOR);
        bodyStart = _wptr;
        return this;
    }

    public int $() {
        if (footer != null) {
            footer.toSink(this);
        }

        // take the body length and format it into the ###### contentLength marker
        int bodyLen = (int) (_wptr - bodyStart);
        if (bodyLen > CL_MARKER_MAX_LEN) {
            throw new LogError("Content too long");
        }
        long p = contentLenEnd;
        int n = bodyLen, rem = n % 10;
        while (n > 10) {
            Unsafe.getUnsafe().putByte(p--, (byte) ('0' + rem));
            n /= 10;
            rem = n % 10;
        }
        Unsafe.getUnsafe().putByte(p--, (byte) ('0' + rem));
        while (p > contentLenStartLimit) {
            Unsafe.getUnsafe().putByte(p--, (byte) ' ');
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
        this._wptr = mark == NOT_SET ? address : mark;
        return this;
    }

    @Override
    public void clear() {
        super.clear();
        mark = NOT_SET;
        contentLenEnd = _wptr;
        bodyStart = _wptr;
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
                    put("\\\\$");
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
}
