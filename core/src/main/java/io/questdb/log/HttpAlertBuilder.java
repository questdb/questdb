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

import io.questdb.std.Chars;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;

public class HttpAlertBuilder implements CharSink {
    private static final String HEADER_BODY_SEPARATOR = "\r\n\r\n";
    private static final String CL_MARKER = "######"; // 999999 / (1024*2) == 488 half a Gb payload max
    private static final int CL_MARKER_LEN = 6;
    private static final int NO_MARK = -1;

    private long lo;
    private long hi;
    private long p;
    private long mark = NO_MARK;
    private long contentLenStart;
    private long bodyStart;

    public HttpAlertBuilder of(long address, long addressLimit, CharSequence localHostIp) {
        this.lo = address;
        this.hi = addressLimit;
        this.p = address;
        this.mark = NO_MARK;
        put("POST /api/v1/alerts HTTP/1.1\r\n")
                .put("Host: ").put(localHostIp).put("\r\n")
                .put("User-Agent: QuestDB/7.71.1\r\n")
                .put("Accept: */*\r\n")
                .put("Content-Type: application/json\r\n")
                .put("Content-Length: ");
        contentLenStart = p;
        put(CL_MARKER);
        put(HEADER_BODY_SEPARATOR);
        bodyStart = p;
        return this;
    }

    public void $() {
        char[] len = Long.toString(p - bodyStart).toCharArray();
        long q = contentLenStart;
        for (int i = 0, limit = CL_MARKER_LEN - len.length; i < limit; i++) {
            Chars.asciiPut(' ', q++);
        }
        Chars.asciiCopyTo(len, 0, len.length, q);
    }

    public int length() {
        return (int) (p - lo);
    }

    public void setMark() {
        mark = p;
    }

    public long getMark() {
        return mark;
    }

    public HttpAlertBuilder rewindToMark() {
        this.p = mark == NO_MARK ? lo : mark;
        return this;
    }

    public HttpAlertBuilder put(LogRecordSink logRecord) {
        final long address = logRecord.getAddress();
        final int logRecordLen = logRecord.length();
        checkFits(logRecordLen);
        for (long p = address, limit = address + logRecordLen; p < limit; p++) {
            char c = (char) Unsafe.getUnsafe().getByte(p);
            switch (c) {
                case '\b': // scape chars
                case '\f':
                case '\t':
                case '$':
                case '"':
                case '\\':
                    put('\\');
                    break;
                case '\r': // ignore chars
                case '\n':
                    continue;
            }
            put(c);
        }
        return this;
    }

    @Override
    public HttpAlertBuilder put(char c) {
        checkFits(1);
        Chars.asciiPut(c, p++);
        return this;
    }

    @Override
    public HttpAlertBuilder put(CharSequence cs) {
        int len = cs.length();
        checkFits(len);
        Chars.asciiStrCpy(cs, len, p);
        p += len;
        return this;
    }

    @Override
    public HttpAlertBuilder put(CharSequence cs, int lo, int hi) {
        int len = hi - lo;
        checkFits(len);
        Chars.asciiStrCpy(cs, len, p);
        p += len;
        return this;
    }

    @Override
    public String toString() {
        return Chars.stringFromUtf8Bytes(lo, p);
    }

    private void checkFits(int len) {
        if (p + len >= hi) {
            throw new LogError("not enough out buffer size");
        }
    }
}
