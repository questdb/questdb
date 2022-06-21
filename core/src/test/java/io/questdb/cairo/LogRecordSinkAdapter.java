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

package io.questdb.cairo;

import io.questdb.log.LogRecord;
import io.questdb.std.Sinkable;
import io.questdb.std.str.AbstractCharSink;
import io.questdb.std.str.CharSink;

public class LogRecordSinkAdapter extends AbstractCharSink {

    private LogRecord line;

    @Override
    public CharSink put(CharSequence cs) {
        line.$(cs);
        return this;
    }

    @Override
    public CharSink encodeUtf8(CharSequence cs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSink encodeUtf8(CharSequence cs, int lo, int hi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public char[] getDoubleDigitsBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSink put(char c) {
        line.$(c);
        return this;
    }

    @Override
    public CharSink put(int value) {
        line.$(value);
        return this;
    }

    @Override
    public CharSink put(long value) {
        line.$(value);
        return this;
    }

    @Override
    public CharSink put(float value, int scale) {
        line.$(value);
        return this;
    }

    @Override
    public CharSink put(double value) {
        line.$(value);
        return this;
    }

    @Override
    public CharSink put(double value, int scale) {
        line.$(value);
        return this;
    }

    @Override
    public CharSink put(boolean value) {
        line.$(value);
        return this;
    }

    @Override
    public CharSink put(Throwable e) {
        line.$(e);
        return this;
    }

    @Override
    public CharSink put(Sinkable sinkable) {
        line.$(sinkable);
        return this;
    }

    @Override
    public CharSink putISODate(long value) {
        line.$ts(value);
        return this;
    }

    @Override
    public CharSink putISODateMillis(long value) {
        line.$ts(value * 1000);
        return this;
    }

    @Override
    public CharSink putMem(long bytes) {
        line.$mem(bytes);
        return this;
    }

    @Override
    public CharSink putQuoted(CharSequence cs) {
        line.$('\"').$(cs).I$();
        return this;
    }

    @Override
    public CharSink put(char[] chars, int start, int len) {
        throw new UnsupportedOperationException();
    }

    public LogRecordSinkAdapter of(LogRecord line) {
        this.line = line;
        return this;
    }
}
