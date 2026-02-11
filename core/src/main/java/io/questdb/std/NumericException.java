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

package io.questdb.std;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NumericException extends RuntimeException implements Sinkable, FlyweightMessageContainer {
    public static final NumericException INSTANCE = new NumericException();
    private static final io.questdb.std.ThreadLocal<NumericException> tlInstance = new ThreadLocal<>(NumericException::new);
    private final StringSink message = new StringSink();
    private int messagePosition = 0;

    private NumericException() {
    }

    /**
     * @return a new mutable instance of NumericException
     */
    public static NumericException instance() {
        NumericException ex = tlInstance.get();
        // This is to have correct stack trace in local debugging with -ea option
        assert (ex = new NumericException()) != null;
        ex.clear();
        return ex;
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public String getMessage() {
        return message.toString();
    }

    @Override
    public int getPosition() {
        return messagePosition;
    }

    public NumericException position(int position) {
        this.messagePosition = position;
        return this;
    }

    public NumericException put(long value) {
        message.put(value);
        return this;
    }

    public NumericException put(double value) {
        message.put(value);
        return this;
    }

    public NumericException put(@Nullable CharSequence cs) {
        message.put(cs);
        return this;
    }

    public NumericException put(CharSequence cs, int lo, int hi) {
        message.put(cs, lo, hi);
        return this;
    }

    public NumericException put(@Nullable Utf8Sequence us) {
        message.put(us);
        return this;
    }

    public NumericException put(Sinkable sinkable) {
        sinkable.toSink(message);
        return this;
    }

    public NumericException put(char c) {
        message.put(c);
        return this;
    }

    public NumericException put(boolean value) {
        message.put(value);
        return this;
    }

    public NumericException putAsPrintable(CharSequence nonPrintable) {
        message.putAsPrintable(nonPrintable);
        return this;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put(message);
    }

    private void clear() {
        message.clear();
        messagePosition = 0;
    }
}
