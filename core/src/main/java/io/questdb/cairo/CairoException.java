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

package io.questdb.cairo;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.Files;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Os;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CairoException extends RuntimeException implements Sinkable, FlyweightMessageContainer {

    public static final int ERRNO_ACCESS_DENIED_WIN = 5;
    public static final int ERRNO_FILE_DOES_NOT_EXIST = 2;
    public static final int ERRNO_FILE_DOES_NOT_EXIST_WIN = 3;
    // psync_cvcontinue sets two bits in the error code to indicate whether the wait timed out (0x100) or there were no waiters (0x200).
    // Error #316 (0x13C) is the timed out bit bitwise OR'd with ETIMEDOUT (60).
    public static final int ERRNO_FILE_READ_TIMEOUT_MACOS = 316;
    public static final int ERRNO_INVALID_PARAMETER = 22;
    public static final int ERRNO_INVALID_PARAMETER_WIN = 87;
    public static final int METADATA_VALIDATION = -100;
    public static final int ILLEGAL_OPERATION = METADATA_VALIDATION - 1;
    private static final int TABLE_DROPPED = ILLEGAL_OPERATION - 1;
    public static final int METADATA_VALIDATION_RECOVERABLE = TABLE_DROPPED - 1;
    public static final int PARTITION_MANIPULATION_RECOVERABLE = METADATA_VALIDATION_RECOVERABLE - 1;
    public static final int TABLE_DOES_NOT_EXIST = PARTITION_MANIPULATION_RECOVERABLE - 1;
    public static final int MAT_VIEW_DOES_NOT_EXIST = TABLE_DOES_NOT_EXIST - 1;
    public static final int TXN_BLOCK_APPLY_FAILED = MAT_VIEW_DOES_NOT_EXIST - 1;
    public static final int NON_CRITICAL = -1;
    private static final StackTraceElement[] EMPTY_STACK_TRACE = {};
    private static final ThreadLocal<CairoException> tlException = new ThreadLocal<>(CairoException::new);
    protected final StringSink message = new StringSink();
    protected final StringSink nativeBacktrace = new StringSink();
    protected int errno;
    private boolean authorizationError = false;
    private boolean cacheable;
    private boolean cancellation; // when query is explicitly cancelled by user
    private boolean housekeeping;
    private boolean interruption; // used when a query times out
    private int messagePosition;
    private boolean outOfMemory;
    private boolean preferencesOutOfDateError = false;

    public static CairoException authorization() {
        return nonCritical().setAuthorizationError();
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
        CairoException exception = critical(METADATA_VALIDATION).put("duplicate column [name=").put(column);
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
        return nonCritical().put("entity is disabled [name=").put(entityName).put(']');
    }

    public static CairoException fileNotFound() {
        return instance(Os.errno());
    }

    public static CairoException invalidMetadataRecoverable(@NotNull CharSequence msg, @NotNull CharSequence columnName) {
        return critical(METADATA_VALIDATION_RECOVERABLE).put(msg).put(" [column=").put(columnName).put(']');
    }

    public static boolean isCairoOomError(Throwable t) {
        return t instanceof CairoException && ((CairoException) t).isOutOfMemory();
    }

    public static CairoException matViewDoesNotExist(CharSequence matViewName) {
        return critical(MAT_VIEW_DOES_NOT_EXIST).put("materialized view does not exist [view=").put(matViewName).put(']');
    }

    public static CairoException nonCritical() {
        return instance(NON_CRITICAL);
    }

    public static CairoException partitionManipulationRecoverable() {
        return instance(PARTITION_MANIPULATION_RECOVERABLE);
    }

    public static CairoException preferencesOutOfDate(long currentVersion, long expectedVersion) {
        return nonCritical().setPreferencesOutOfDateError()
                .put("preferences view is out of date [currentVersion=")
                .put(currentVersion)
                .put(", expectedVersion=")
                .put(expectedVersion)
                .put(']');
    }

    public static CairoException queryCancelled(long fd) {
        CairoException exception = nonCritical().put("cancelled by user").setInterruption(true).setCancellation(true);
        if (fd > -1) {
            exception.put(" [fd=").put(fd).put(']');
        }
        return exception;
    }

    public static CairoException queryCancelled() {
        return nonCritical().put("cancelled by user").setInterruption(true).setCancellation(true);
    }

    public static CairoException queryTimedOut(long fd, long runtime, long timeout) {
        return nonCritical()
                .put("timeout, query aborted [fd=").put(fd)
                .put(", runtime=").put(runtime).put("ms")
                .put(", timeout=").put(timeout).put("ms")
                .put(']').setInterruption(true);
    }

    public static CairoException queryTimedOut() {
        return nonCritical().put("timeout, query aborted").setInterruption(true);
    }

    public static CairoException tableDoesNotExist(CharSequence tableName) {
        return critical(TABLE_DOES_NOT_EXIST).put("table does not exist [table=").put(tableName).put(']');
    }

    public static CairoException tableDropped(TableToken tableToken) {
        return critical(TABLE_DROPPED)
                .put("table is dropped [dirName=").put(tableToken.getDirName())
                .put(", tableName=").put(tableToken.getTableName())
                .put(']');
    }

    public static CairoException txnApplyBlockError(TableToken tableToken) {
        return critical(TXN_BLOCK_APPLY_FAILED)
                .put("sorting transaction block failed, need to be re-run in 1 by 1 apply mode [dirName=").put(tableToken.getDirName())
                .put(", tableName=").put(tableToken.getTableName()).put(']');
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

    public boolean isBlockApplyError() {
        return errno == TXN_BLOCK_APPLY_FAILED;
    }

    public boolean isCacheable() {
        return cacheable;
    }

    public boolean isCancellation() {
        return cancellation;
    }

    public boolean isCritical() {
        return errno != NON_CRITICAL
                && errno != PARTITION_MANIPULATION_RECOVERABLE
                && errno != METADATA_VALIDATION_RECOVERABLE
                && errno != TABLE_DROPPED
                && errno != MAT_VIEW_DOES_NOT_EXIST
                && errno != TABLE_DOES_NOT_EXIST;
    }

    public boolean isFileCannotRead() {
        return Files.isErrnoFileCannotRead(errno);
    }

    public boolean isHousekeeping() {
        return housekeeping;
    }

    public boolean isInterruption() {
        return interruption;
    }

    public boolean isMetadataValidation() {
        return errno == METADATA_VALIDATION || errno == METADATA_VALIDATION_RECOVERABLE;
    }

    public boolean isOutOfMemory() {
        return outOfMemory;
    }

    public boolean isPreferencesOutOfDateError() {
        return preferencesOutOfDateError;
    }

    public boolean isTableDoesNotExist() {
        return errno == TABLE_DOES_NOT_EXIST;
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

    public void setHousekeeping(boolean housekeeping) {
        this.housekeeping = housekeeping;
    }

    public CairoException setInterruption(boolean interruption) {
        this.interruption = interruption;
        return this;
    }

    public CairoException setOutOfMemory(boolean outOfMemory) {
        this.outOfMemory = outOfMemory;
        return this;
    }

    public boolean tableDoesNotExist() {
        return errno == TABLE_DOES_NOT_EXIST;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii('[').put(errno).putAscii("]: ").put(message);
    }

    public CairoException ts(int timestampType, long timestamp) {
        ColumnType.getTimestampDriver(timestampType).append(message, timestamp);
        return this;
    }

    public CairoException ts(TimestampDriver driver, long timestamp) {
        driver.append(message, timestamp);
        return this;
    }

    private static CairoException instance(int errno) {
        CairoException ex = tlException.get();
        // This is to have correct stack trace in local debugging with -ea option
        assert (ex = new CairoException()) != null;
        ex.clear(errno);
        return ex;
    }

    // N.B.: Change the API with care! This method is called from native code via JNI.
    // See `struct CairoException` in the `qdbr` Rust crate.
    @SuppressWarnings("unused")
    private static CairoException paramInstance(
            int errno, // pass `NON_CRITICAL` (-1) to create a non-critical exception
            boolean outOfMemory,
            CharSequence message,
            @Nullable CharSequence nativeBacktrace
    ) {
        CairoException ex = instance(errno)
                .setOutOfMemory(outOfMemory)
                .put(message);
        ex.nativeBacktrace.put(nativeBacktrace);
        return ex;
    }

    private CairoException setAuthorizationError() {
        this.authorizationError = true;
        return this;
    }

    private CairoException setPreferencesOutOfDateError() {
        this.preferencesOutOfDateError = true;
        return this;
    }

    protected void clear(int errno) {
        message.clear();
        nativeBacktrace.clear();
        this.errno = errno;
        cacheable = false;
        interruption = false;
        authorizationError = false;
        messagePosition = 0;
        outOfMemory = false;
        housekeeping = false;
    }
}
