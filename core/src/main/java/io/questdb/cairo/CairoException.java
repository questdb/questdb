/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Os;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.CharSinkBase;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CairoException extends RuntimeException implements Sinkable, FlyweightMessageContainer {

    public static final int ERRNO_FILE_DOES_NOT_EXIST = 2;
    public static final int ERRNO_FILE_DOES_NOT_EXIST_WIN = 3;

    public static final int METADATA_VALIDATION = -100;
    public static final int ILLEGAL_OPERATION = METADATA_VALIDATION - 1;
    private static final int TABLE_DROPPED = ILLEGAL_OPERATION - 1;
    public static final int METADATA_VALIDATION_RECOVERABLE = METADATA_VALIDATION - 1;
    public static final int PARTITION_MANIPULATION_RECOVERABLE = METADATA_VALIDATION_RECOVERABLE - 1;
    public static final int NON_CRITICAL = -1;
    private static final StackTraceElement[] EMPTY_STACK_TRACE = {};
    public static final int ERRNO_ACCESS_DENIED_WIN = 5;
    private static final ThreadLocal<CairoException> tlException = new ThreadLocal<>(CairoException::new);
    protected final StringSink message = new StringSink();
    protected int errno;
    private boolean authorizationError = false;
    private boolean cacheable;
    private boolean cancellation; // when query is explicitly cancelled by user
    private boolean entityDisabled; // used when account is disabled and connection should be dropped
    private boolean interruption; // used when a query times out
    private int messagePosition;

    public static CairoException authorization() {
        CairoException e = nonCritical();
        e.authorizationError = true;
        return e;
    }

    public static CairoException critical(int errno) {
        return instance(errno);
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

    public static CairoException duplicateColumn(CharSequence column, CharSequence columnAlias) {
        CairoException exception = critical(METADATA_VALIDATION).put("Duplicate column [name=").put(column);
        if (columnAlias != null) {
            exception.put(", alias=").put(columnAlias);
        }
        return exception.put(']');
    }

    public static CairoException duplicateColumn(CharSequence columnName) {
        return duplicateColumn(columnName, null);
    }

    @SuppressWarnings("unused")
    public static CairoException entityIsDisabled(CharSequence entityName) {
        return nonCritical().setEntityDisabled(true).put("entity is disabled [name=").put(entityName).put(']');
    }

    public static boolean errnoReadPathDoesNotExist(int errno) {
        return errnoRemovePathDoesNotExist(errno) || (Os.type == Os.WINDOWS && errno == ERRNO_ACCESS_DENIED_WIN);
    }

    public static boolean errnoRemovePathDoesNotExist(int errno) {
        return errno == ERRNO_FILE_DOES_NOT_EXIST || (Os.type == Os.WINDOWS && errno == ERRNO_FILE_DOES_NOT_EXIST_WIN);
    }

    public static CairoException invalidMetadataRecoverable(@NotNull CharSequence msg, @NotNull CharSequence columnName) {
        return critical(METADATA_VALIDATION_RECOVERABLE).put(msg).put(" [column=").put(columnName).put(']');
    }

    public static CairoException nonCritical() {
        return instance(NON_CRITICAL);
    }

    public static CairoException partitionManipulationRecoverable() {
        return instance(PARTITION_MANIPULATION_RECOVERABLE);
    }

    public static CairoException queryCancelled(int fd) {
        CairoException exception = nonCritical().put("cancelled by user").setInterruption(true).setCancellation(true);
        if (fd > -1) {
            exception.put(" [fd=").put(fd).put(']');
        }
        return exception;
    }

    public static CairoException queryCancelled() {
        return nonCritical().put("cancelled by user").setInterruption(true).setCancellation(true);
    }

    public static CairoException queryTimedOut(int fd) {
        return nonCritical().put("timeout, query aborted [fd=").put(fd).put(']').setInterruption(true);
    }

    public static CairoException queryTimedOut() {
        return nonCritical().put("timeout, query aborted").setInterruption(true);
    }

    public static CairoException tableDoesNotExist(CharSequence tableName) {
        return nonCritical().put("table does not exist [table=").put(tableName).put(']');
    }

    public static CairoException tableDropped(TableToken tableToken) {
        return critical(TABLE_DROPPED)
                .put("table is dropped [dirName=").put(tableToken.getDirName())
                .put(", tableName=").put(tableToken.getTableName())
                .put(']');
    }

    public boolean errnoReadPathDoesNotExist() {
        return errnoReadPathDoesNotExist(errno);
    }

    public int getErrno() {
        return errno;
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return message;
    }

    public int getInterruptionReason() {
        if (isCancellation()) {
            return SqlExecutionCircuitBreaker.STATE_CANCELLED;
        } else if (isInterruption()) {
            return SqlExecutionCircuitBreaker.STATE_TIMEOUT;
        } else {
            return SqlExecutionCircuitBreaker.STATE_OK;
        }
    }

    @Override
    public String getMessage() {
        return "[" + errno + "] " + message;
    }

    @Override
    public int getPosition() {
        return messagePosition;
    }

    @Override
    public StackTraceElement[] getStackTrace() {
        StackTraceElement[] result = EMPTY_STACK_TRACE;
        // This is to have correct stack trace reported in CI
        assert (result = super.getStackTrace()) != null;
        return result;
    }

    public boolean isAuthorizationError() {
        return authorizationError;
    }

    public boolean isCacheable() {
        return cacheable;
    }

    public boolean isCancellation() {
        return cancellation;
    }

    public boolean isCritical() {
        return errno != NON_CRITICAL && errno != PARTITION_MANIPULATION_RECOVERABLE && errno != METADATA_VALIDATION_RECOVERABLE;
    }

    public boolean isEntityDisabled() {
        return entityDisabled;
    }

    public boolean isInterruption() {
        return interruption;
    }

    public boolean isTableDropped() {
        return errno == TABLE_DROPPED;
    }

    // logged and skipped by WAL applying code
    public boolean isWALTolerable() {
        return errno == PARTITION_MANIPULATION_RECOVERABLE || errno == METADATA_VALIDATION_RECOVERABLE;
    }

    public CairoException position(int position) {
        this.messagePosition = position;
        return this;
    }

    public CairoException put(long value) {
        message.put(value);
        return this;
    }

    public CairoException put(double value) {
        message.put(value);
        return this;
    }

    public CairoException put(@Nullable CharSequence cs) {
        message.put(cs);
        return this;
    }

    public CairoException put(@Nullable Utf8Sequence us) {
        message.put(us);
        return this;
    }

    public CairoException put(Sinkable sinkable) {
        sinkable.toSink(message);
        return this;
    }

    public CairoException put(char c) {
        message.put(c);
        return this;
    }

    public CairoException put(boolean value) {
        message.put(value);
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

    public CairoException setCancellation(boolean cancellation) {
        this.cancellation = cancellation;
        return this;
    }

    public CairoException setEntityDisabled(boolean disabled) {
        this.entityDisabled = disabled;
        return this;
    }

    public CairoException setInterruption(boolean interruption) {
        this.interruption = interruption;
        return this;
    }

    @Override
    public void toSink(@NotNull CharSinkBase<?> sink) {
        sink.putAscii('[').put(errno).putAscii("]: ").put(message);
    }

    public CairoException ts(long timestamp) {
        TimestampFormatUtils.appendDateTime(message, timestamp);
        return this;
    }

    private static CairoException instance(int errno) {
        CairoException ex = tlException.get();
        // This is to have correct stack trace in local debugging with -ea option
        assert (ex = new CairoException()) != null;
        ex.clear(errno);
        return ex;
    }

    protected void clear(int errno) {
        message.clear();
        this.errno = errno;
        cacheable = false;
        interruption = false;
        authorizationError = false;
        entityDisabled = false;
        messagePosition = 0;
    }
}
