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
import io.questdb.std.str.Utf8Sequence;

/**
 * Exception thrown by QuestDB's ILP (InfluxDB Line Protocol) sender implementations to indicate
 * failures during data transmission or protocol violations.
 *
 * <h2>Buffer Preservation on Error</h2>
 * <strong>Important:</strong> When a Sender throws a {@code LineSenderException}, it does NOT clear
 * its internal buffer. This design allows for retry strategies:
 * <ul>
 *   <li>For transient errors: Retry by calling {@code flush()} again on the same Sender instance</li>
 *   <li>For permanent errors: Either close and recreate the Sender, or call {@code reset()} to clear
 *       the buffer and continue with new data</li>
 * </ul>
 *
 * <h2>Retryability</h2>
 * The {@link #isRetryable()} method provides a best-effort indication of whether the error
 * might be resolved by retrying at the application level. This is particularly important
 * because this exception is only thrown after the sender has exhausted its own internal
 * retry attempts. The retryability flag helps applications decide whether to implement
 * additional retry logic with longer delays or different strategies.
 *
 * @see io.questdb.client.Sender
 * @see io.questdb.client.Sender#flush()
 * @see io.questdb.client.Sender#reset()
 */
public class LineSenderException extends RuntimeException {

    private final StringSink message = new StringSink();
    private final boolean retryable;
    private int errno = Integer.MIN_VALUE;

    public LineSenderException(CharSequence message) {
        this.message.put(message);
        this.retryable = false;
    }

    public LineSenderException(CharSequence message, boolean retryable) {
        this.message.put(message);
        this.retryable = retryable;
    }

    public LineSenderException(Throwable t) {
        super(t);
        this.retryable = false;
    }

    public LineSenderException(String message, Throwable cause) {
        super(message, cause);
        this.message.put(message);
        this.retryable = false;
    }

    public LineSenderException appendIPv4(int ip) {
        Net.appendIP4(message, ip);
        return this;
    }

    public LineSenderException errno(int errno) {
        this.errno = errno;
        return this;
    }

    @Override
    public String getMessage() {
        if (errno == Integer.MIN_VALUE) {
            return message.toString();
        }
        String errNoRender = "[" + errno + "]";
        if (message.length() == 0) {
            return errNoRender;
        }
        return errNoRender + " " + message;
    }

    /**
     * Returns a best-effort indication of whether the error condition might be resolved by retrying
     * at the application level.
     *
     * <p><strong>Important:</strong> This exception is thrown only after the Sender has exhausted its
     * own internal retry attempts. The {@code retryable} flag indicates whether additional retries
     * at a higher level might succeed.
     *
     * <p>This is a heuristic determination and should not be considered definitive. Even after internal
     * retries have failed, the retryability remains ambiguous:
     * <ul>
     *   <li>Network issues that persisted through internal retries might resolve with more time</li>
     *   <li>A server that was restarting might now be available</li>
     *   <li>However, some failures (authentication, malformed data) will never succeed</li>
     * </ul>
     *
     * <p>Callers should use this value as a hint for application-level retry strategies:
     * <ul>
     *   <li>Consider implementing exponential backoff with longer delays than the Sender used internally</li>
     *   <li>Set a reasonable maximum retry limit at the application level</li>
     *   <li>For {@code false} values, consider the error permanent and handle accordingly (e.g., dead-letter queue)</li>
     * </ul>
     *
     * @return {@code true} if the error appears transient and application-level retries with longer delays might succeed;
     * {@code false} if the error appears permanent and will likely never succeed regardless of retries.
     * When uncertain, this method errs on the side of returning {@code true}.
     */
    public boolean isRetryable() {
        return retryable;
    }

    public LineSenderException put(char ch) {
        message.put(ch);
        return this;
    }

    public LineSenderException put(long value) {
        message.put(value);
        return this;
    }

    public LineSenderException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    public LineSenderException put(Utf8Sequence cs) {
        message.put(cs);
        return this;
    }

    public LineSenderException putAsPrintable(CharSequence nonPrintable) {
        message.putAsPrintable(nonPrintable);
        return this;
    }
}
