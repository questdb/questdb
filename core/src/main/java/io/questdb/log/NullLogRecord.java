/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.TimestampDriver;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

final class NullLogRecord implements LogRecord {

    public static final NullLogRecord INSTANCE = new NullLogRecord();

    private NullLogRecord() {
    }

    @Override
    public void $() {
    }

    @Override
    public LogRecord $(@Nullable CharSequence sequence) {
        return this;
    }

    @Override
    public LogRecord $(@Nullable Utf8Sequence sequence) {
        return this;
    }

    @Override
    public LogRecord $(@Nullable DirectUtf8Sequence sequence) {
        return this;
    }

    @Override
    public LogRecord $(int x) {
        return this;
    }

    @Override
    public LogRecord $(double x) {
        return this;
    }

    @Override
    public LogRecord $(long l) {
        return this;
    }

    @Override
    public LogRecord $(boolean x) {
        return this;
    }

    @Override
    public LogRecord $(char c) {
        return this;
    }

    @Override
    public LogRecord $(@Nullable Throwable e) {
        return this;
    }

    @Override
    public LogRecord $(@Nullable File x) {
        return this;
    }

    @Override
    public LogRecord $(@Nullable Object x) {
        return this;
    }

    @Override
    public LogRecord $(@Nullable Sinkable x) {
        return this;
    }

    @Override
    public LogRecord $256(long a, long b, long c, long d) {
        return this;
    }

    @Override
    public LogRecord $hex(long value) {
        return this;
    }

    @Override
    public LogRecord $hexPadded(long value) {
        return this;
    }

    @Override
    public LogRecord $ip(long ip) {
        return this;
    }

    @Override
    public LogRecord $safe(@NotNull CharSequence sequence, int lo, int hi) {
        return this;
    }

    @Override
    public LogRecord $safe(@Nullable DirectUtf8Sequence sequence) {
        return this;
    }

    @Override
    public LogRecord $safe(@Nullable Utf8Sequence sequence) {
        return this;
    }

    @Override
    public LogRecord $safe(long lo, long hi) {
        return this;
    }

    @Override
    public LogRecord $safe(@Nullable CharSequence sequence) {
        return this;
    }

    @Override
    public LogRecord $size(long memoryBytes) {
        return this;
    }

    @Override
    public LogRecord $substr(int from, @Nullable DirectUtf8Sequence sequence) {
        return this;
    }

    @Override
    public LogRecord $ts(long x) {
        return this;
    }

    @Override
    public LogRecord $ts(TimestampDriver driver, long x) {
        return this;
    }

    @Override
    public LogRecord $uuid(long lo, long hi) {
        return this;
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public LogRecord microTime(long x) {
        return this;
    }

    @Override
    public Utf8Sink put(@Nullable Utf8Sequence us) {
        return this;
    }

    @Override
    public Utf8Sink put(byte b) {
        return this;
    }

    @Override
    public LogRecord put(char c) {
        return this;
    }

    @Override
    public Utf8Sink putNonAscii(long lo, long hi) {
        return this;
    }

    @Override
    public LogRecord ts() {
        return this;
    }
}
