/*******************************************************************************
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

package io.questdb.cutlass.line;

import io.questdb.network.Net;
import io.questdb.std.str.StringSink;

/**
 * Exception thrown by QuestDB's ILP (InfluxDB Line Protocol) receiver implementations to indicate
 * failures during data transmission or protocol violations.
 */
public class LineReceiverException extends RuntimeException {

    private final StringSink message = new StringSink();

    private int errno = Integer.MIN_VALUE;

    @SuppressWarnings("unused")
    public LineReceiverException(CharSequence message) {
        this.message.put(message);
    }

    @SuppressWarnings("unused")
    public LineReceiverException(CharSequence message, boolean retryable) {
        this.message.put(message);
    }

    @SuppressWarnings("unused")
    public LineReceiverException(Throwable t) {
        super(t);
    }

    @SuppressWarnings("unused")
    public LineReceiverException(String message, Throwable cause) {
        super(message, cause);
        this.message.put(message);
    }

    @SuppressWarnings("unused")
    public LineReceiverException appendIPv4(int ip) {
        Net.appendIP4(message, ip);
        return this;
    }

    @SuppressWarnings("unused")
    public LineReceiverException errno(int errno) {
        this.errno = errno;
        return this;
    }

    @Override
    public String getMessage() {
        if (errno == Integer.MIN_VALUE) {
            return message.toString();
        }
        String errNoRender = "[" + errno + "]";
        if (message.isEmpty()) {
            return errNoRender;
        }
        return errNoRender + " " + message;
    }

    @SuppressWarnings("unused")
    public LineReceiverException put(char ch) {
        message.put(ch);
        return this;
    }

    @SuppressWarnings("unused")
    public LineReceiverException put(long value) {
        message.put(value);
        return this;
    }

    @SuppressWarnings("unused")
    public LineReceiverException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    @SuppressWarnings("unused")
    public LineReceiverException putAsPrintable(CharSequence nonPrintable) {
        message.putAsPrintable(nonPrintable);
        return this;
    }
}
