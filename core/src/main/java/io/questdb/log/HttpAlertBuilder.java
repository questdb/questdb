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
import io.questdb.std.Sinkable;
import io.questdb.std.Unsafe;


public class HttpAlertBuilder extends LogRecordSink {

    private static final String QDB_VERSION = "7.71.1";
    private static final String HEADER_BODY_SEPARATOR = "\r\n\r\n";
    private static final String CL_MARKER = "######"; // 999999 / (1024*2) == 488 half a Gb payload max
    private static final int CL_MARKER_LEN = 6;
    private static final int NOT_SET = -1;


    private long mark = NOT_SET;
    private long contentLenStart;
    private long bodyStart;

    public HttpAlertBuilder(long address, long addressSize) {
        super(address, addressSize);
        contentLenStart = _wptr;
        bodyStart = _wptr;
    }

    public HttpAlertBuilder putHeader(CharSequence localHostIp) {
        clear();
        put("POST /api/v1/alerts HTTP/1.1\r\n")
                .put("Host: ").put(localHostIp).put("\r\n")
                .put("User-Agent: QuestDB/").put(QDB_VERSION).put("\r\n")
                .put("Accept: */*\r\n")
                .put("Content-Type: application/json\r\n")
                .put("Content-Length: ");
        contentLenStart = _wptr;
        put(CL_MARKER);
        put(HEADER_BODY_SEPARATOR);
        bodyStart = _wptr;
        return this;
    }

    public void $() {
        char[] len = Long.toString(_wptr - bodyStart).toCharArray();
        long q = contentLenStart;
        for (int i = 0, limit = CL_MARKER_LEN - len.length; i < limit; i++) {
            Chars.asciiPut(' ', q++);
        }
        Chars.asciiCopyTo(len, 0, len.length, q);
    }

    public HttpAlertBuilder setMark() {
        mark = _wptr;
        return this;
    }

    public long getMark() {
        return mark;
    }

    public HttpAlertBuilder rewindToMark() {
        this._wptr = mark == NOT_SET ? address : mark;
        return this;
    }

    @Override
    public void clear() {
        super.clear();
        mark = NOT_SET;
        contentLenStart = _wptr;
        bodyStart = _wptr;
    }

    public HttpAlertBuilder put(LogRecordSink logRecord) {
        final int len = logRecord.length();
        final long address = logRecord.getAddress();
        for (long p = address, limit = address + len; p < limit; p++) {
            char c = (char) Unsafe.getUnsafe().getByte(p);
            switch (c) {
                case '\t':
                    put(' ');
                    continue;
                case '$':
                case '"':
                case '\\':
                    put('\\');
                    break;
                case '\b': // ignore chars
                case '\f':
                case '\r':
                case '\n':
                    continue;
            }
            put(c);
        }
        return this;
    }

    @Override
    public HttpAlertBuilder put(CharSequence cs) {
        super.put(cs);
        return this;
    }

    @Override
    public HttpAlertBuilder put(CharSequence cs, int lo, int hi) {
        super.put(cs, lo, hi);
        return this;
    }

    @Override
    public HttpAlertBuilder put(Sinkable sinkable) {
        sinkable.toSink(this);
        return this;
    }

    @Override
    public HttpAlertBuilder put(char c) {
        super.put(c);
        return this;
    }

    @Override
    public String toString() {
        return Chars.stringFromUtf8Bytes(address, _wptr);
    }
}
