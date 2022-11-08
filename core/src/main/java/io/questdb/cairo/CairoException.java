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

import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Sinkable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class CairoException extends RuntimeException implements Sinkable, FlyweightMessageContainer {
    public static final int ERRNO_FILE_DOES_NOT_EXIST = 2;
    public static final int ILLEGAL_OPERATION = -101;
    public static final int METADATA_VALIDATION = -100;
    public static final int NON_CRITICAL = -1;
    private static final StackTraceElement[] EMPTY_STACK_TRACE = {};
    private static final ThreadLocal<CairoException> tlException = new ThreadLocal<>(CairoException::new);
    protected final StringSink message = new StringSink();
    protected int errno;
    private boolean cacheable;
    private boolean interruption; // used when a query times out

    public static CairoException critical(int errno) {
        CairoException ex = tlException.get();
        // This is to have correct stack trace in local debugging with -ea option
        assert (ex = new CairoException()) != null;
        ex.message.clear();
        ex.errno = errno;
        ex.cacheable = false;
        ex.interruption = false;
        return ex;
    }

    public static CairoException detachedColumnMetadataMismatch(int columnIndex, CharSequence columnName, CharSequence attribute) {
        return critical(METADATA_VALIDATION)
                .put("Detached column [index=")
                .put(columnIndex)
                .put(", name=")
                .put(columnName)
                .put(", attribute=")
                .put(attribute)
                .put("] does not match current table metadata");
    }

    public static CairoException detachedMetadataMismatch(CharSequence attribute) {
        return critical(METADATA_VALIDATION)
                .put("Detached partition metadata [")
                .put(attribute)
                .put("] is not compatible with current table metadata");
    }

    public static CairoException duplicateColumn(CharSequence columnName, CharSequence columnAlias) {
        CairoException exception = invalidMetadata("Duplicate column", columnName);
        if (columnAlias != null) {
            exception.put(", [alias=").put(columnAlias).put(']');
        }
        return exception;
    }

    public static CairoException duplicateColumn(CharSequence columnName) {
        return duplicateColumn(columnName, null);
    }

    public static CairoException invalidMetadata(@NotNull CharSequence msg, @NotNull CharSequence columnName) {
        return critical(METADATA_VALIDATION).put(msg).put(" [name=").put(columnName).put(']');
    }

    public static CairoException nonCritical() {
        return critical(NON_CRITICAL);
    }

    public int getErrno() {
        return errno;
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public String getMessage() {
        return "[" + errno + "] " + message;
    }

    @Override
    public StackTraceElement[] getStackTrace() {
        StackTraceElement[] result = EMPTY_STACK_TRACE;
        // This is to have correct stack trace reported in CI 
        assert (result = super.getStackTrace()) != null;
        return result;
    }

    public boolean isCacheable() {
        return cacheable;
    }

    public boolean isCritical() {
        return errno != NON_CRITICAL;
    }

    public boolean isInterruption() {
        return interruption;
    }

    public CairoException put(long value) {
        message.put(value);
        return this;
    }

    public CairoException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    public CairoException put(char c) {
        message.put(c);
        return this;
    }

    public CairoException putAsPrintable(CharSequence nonPrintable) {
        message.putAsPrintable(nonPrintable);
        return this;
    }

    public CairoException setCacheable(boolean cacheable) {
        this.cacheable = cacheable;
        return this;
    }

    public CairoException setInterruption(boolean interruption) {
        this.interruption = interruption;
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('[').put(errno).put("]: ").put(message);
    }

    public CairoException ts(long timestamp) {
        TimestampFormatUtils.appendDateTime(message, timestamp);
        return this;
    }
}
