/*+*****************************************************************************
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

package io.questdb.cairo;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.Files;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Os;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CairoException extends RuntimeException implements Sinkable, FlyweightMessageContainer {

    public static final int ERRNO_ACCESS_DENIED_WIN = 5;
    public static final int ERRNO_EACCES_LINUX = 13;
    public static final int ERRNO_EACCES_MACOS = 13;
    public static final int ERRNO_EPERM_LINUX = 1;
    public static final int ERRNO_EPERM_MACOS = 1;
    public static final int ERRNO_FILE_DOES_NOT_EXIST = 2;
    public static final int ERRNO_FILE_DOES_NOT_EXIST_WIN = 3;
    // psync_cvcontinue sets two bits in the error code to indicate whether the wait timed out (0x100) or there were no waiters (0x200).
    // Error #316 (0x13C) is the timed out bit bitwise OR'd with ETIMEDOUT (60).
    public static final int ERRNO_FILE_READ_TIMEOUT_MACOS = 316;
    public static final int METADATA_VALIDATION = -100;
    public static final int ILLEGAL_OPERATION = METADATA_VALIDATION - 1;
    private static final int TABLE_DROPPED = ILLEGAL_OPERATION - 1;
    public static final int METADATA_VALIDATION_RECOVERABLE = TABLE_DROPPED - 1;
    public static final int PARTITION_MANIPULATION_RECOVERABLE = METADATA_VALIDATION_RECOVERABLE - 1;
    public static final int TABLE_DOES_NOT_EXIST = PARTITION_MANIPULATION_RECOVERABLE - 1;
    public static final int VIEW_DOES_NOT_EXIST = TABLE_DOES_NOT_EXIST - 1;
    public static final int MAT_VIEW_DOES_NOT_EXIST = VIEW_DOES_NOT_EXIST - 1;
    public static final int TXN_BLOCK_APPLY_FAILED = MAT_VIEW_DOES_NOT_EXIST - 1;
    public static final int METADATA_VERSION_MISMATCH = TXN_BLOCK_APPLY_FAILED - 1;
    public static final int FILE_TOO_SMALL = METADATA_VERSION_MISMATCH - 1;
    public static final int SEQUENCER_METADATA_OPEN_FAILED = FILE_TOO_SMALL - 1;
    private static final int TABLE_SUSPENDED = SEQUENCER_METADATA_OPEN_FAILED - 1;
    public static final int NON_CRITICAL = -1;
    // Single source of truth for the write-refusal message a read-only node emits. Both a static
    // read-only OSS instance and an enterprise node acting as a read-only replica reach this
    // message, so the wording stays in one place to keep every emitter consistent and to make a
    // future role-neutral reword a one-line change. The wording is retained as-is because the
    // string is asserted across roughly twenty OSS/enterprise/e2e test files; centralizing the
    // literal first lets any later reword land without scattering the change.
    public static final String READ_ONLY_ACCESS_MESSAGE = "replica access is read-only";
    private static final StackTraceElement[] EMPTY_STACK_TRACE = {};
    private static final int FLAG_AUTHORIZATION_ERROR = 1;
    private static final int FLAG_CACHEABLE = 1 << 1;
    private static final int FLAG_CANCELLATION = 1 << 2; // query was explicitly cancelled by the user
    private static final int FLAG_HOUSEKEEPING = 1 << 3;
    private static final int FLAG_INTERRUPTION = 1 << 4; // query timed out
    private static final int FLAG_OUT_OF_MEMORY = 1 << 5;
    private static final int FLAG_PREFERENCES_OUT_OF_DATE_ERROR = 1 << 6;
    private static final int FLAG_READ_ONLY_ACCESS_REFUSAL = 1 << 7;
    private static final int FLAG_SCHEMA_MISMATCH = 1 << 8;
    protected final StringSink message = new StringSink();
    protected final StringSink nativeBacktrace = new StringSink();
    protected int errno;
    private int flags;
    private int messagePosition;

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
        return authorization().put("entity is disabled [name=").put(entityName).put(']');
    }

    public static CairoException fileNotFound() {
        return instance(Os.errno());
    }

    public static CairoException fileTooSmall(long size, long required) {
        return critical(FILE_TOO_SMALL).put("File is too small, size=").put(size).put(", required=").put(required);
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

    public static CairoException metadataVersionMismatch(Utf8Sequence metaPath, int expectedVersion, int actualVersion) {
        return critical(METADATA_VERSION_MISMATCH)
                .put("metadata version does not match runtime version [path=").put(metaPath)
                .put(", expectedVersion=").put(expectedVersion)
                .put(", actualVersion=").put(actualVersion)
                .put(']');
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

    /**
     * A write refused BECAUSE the node is read-only -- statically
     * ({@code readonly=true} in the server configuration) or dynamically (the
     * role-derived read-only of an enterprise replica, including a demote in
     * flight). Carries the authorization flag, the canonical
     * {@link #READ_ONLY_ACCESS_MESSAGE} and the read-only-refusal marker
     * ({@link #isReadOnlyAccessRefusal()}) in lockstep, so the refusal's CAUSE
     * travels with the exception.
     * <p>
     * Protocol layers that must tell a transient role-demote refusal apart from
     * a genuine ACL denial (e.g. the QWP NACK classification) key on the marker.
     * They must NOT re-read live engine state at catch time -- that races with a
     * demote revert (the drain-timeout PRIMARY restore can land between the
     * throw and the catch; nothing fences the propagation) -- and must NOT match
     * the message text, which is brittle. Every "node is read-only" refusal site
     * uses this factory so the marker is correct by construction; sites must not
     * hand-roll {@code authorization().put(READ_ONLY_ACCESS_MESSAGE)}.
     */
    public static CairoException readOnlyAccess() {
        return authorization().setReadOnlyAccessRefusal().put(READ_ONLY_ACCESS_MESSAGE);
    }

    /**
     * A non-critical error raised BECAUSE the wire value's type, shape or
     * precision is fundamentally incompatible with the target column (e.g.
     * an unsupported type coercion or a geohash precision mismatch). The
     * refusal is DETERMINISTIC under byte-identical replay -- the same bytes
     * produce the same rejection every time -- so protocol layers that must
     * tell a permanent data error apart from a transient write refusal (the
     * QWP NACK classification) key on {@link #isSchemaMismatch()} to reject
     * it terminally instead of retrying. Every such site uses this factory so
     * the marker is correct by construction; callers must not re-derive the
     * distinction from message text.
     */
    public static CairoException schemaMismatch() {
        return nonCritical().setSchemaMismatch();
    }

    public static CairoException sequencerMetadataOpenFailed(TableToken tableToken, int causeErrno, CharSequence causeMessage) {
        return critical(SEQUENCER_METADATA_OPEN_FAILED)
                .put("could not open sequencer metadata [table=").put(tableToken)
                .put(", errno=").put(causeErrno)
                .put(", error=").put(causeMessage)
                .put(']');
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

    public static CairoException tableSuspended(TableToken tableToken) {
        return critical(TABLE_SUSPENDED)
                .put("table is suspended [dirName=").put(tableToken.getDirName())
                .put(", tableName=").put(tableToken.getTableName())
                .put(']');
    }

    public static CairoException txnApplyBlockError(TableToken tableToken) {
        return critical(TXN_BLOCK_APPLY_FAILED)
                .put("sorting transaction block failed, need to be re-run in 1 by 1 apply mode [dirName=").put(tableToken.getDirName())
                .put(", tableName=").put(tableToken.getTableName()).put(']');
    }

    public static CairoException viewDoesNotExist(CharSequence viewName) {
        return critical(VIEW_DOES_NOT_EXIST).put("view does not exist [view=").put(viewName).put(']');
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
        return (flags & FLAG_AUTHORIZATION_ERROR) != 0;
    }

    public boolean isBlockApplyError() {
        return errno == TXN_BLOCK_APPLY_FAILED;
    }

    public boolean isCacheable() {
        return (flags & FLAG_CACHEABLE) != 0;
    }

    public boolean isCancellation() {
        return (flags & FLAG_CANCELLATION) != 0;
    }

    public boolean isCritical() {
        return errno != NON_CRITICAL
                && errno != PARTITION_MANIPULATION_RECOVERABLE
                && errno != METADATA_VALIDATION_RECOVERABLE
                && errno != TABLE_DROPPED
                && errno != TABLE_SUSPENDED
                && errno != MAT_VIEW_DOES_NOT_EXIST
                && errno != VIEW_DOES_NOT_EXIST
                && errno != TABLE_DOES_NOT_EXIST;
    }

    public boolean isFileCannotRead() {
        return Files.isErrnoFileCannotRead(errno);
    }

    public boolean isFileTooSmall() {
        return errno == FILE_TOO_SMALL;
    }

    public boolean isHousekeeping() {
        return (flags & FLAG_HOUSEKEEPING) != 0;
    }

    public boolean isInterruption() {
        return (flags & FLAG_INTERRUPTION) != 0;
    }

    public boolean isMetadataValidation() {
        return errno == METADATA_VALIDATION
                || errno == METADATA_VALIDATION_RECOVERABLE
                || errno == METADATA_VERSION_MISMATCH;
    }

    public boolean isMetadataVersionMismatch() {
        return errno == METADATA_VERSION_MISMATCH;
    }

    public boolean isOutOfMemory() {
        return (flags & FLAG_OUT_OF_MEMORY) != 0;
    }

    public boolean isPreferencesOutOfDateError() {
        return (flags & FLAG_PREFERENCES_OUT_OF_DATE_ERROR) != 0;
    }

    /**
     * Whether this refusal was raised BECAUSE the node is read-only (set only by
     * {@link #readOnlyAccess()}). Implies {@link #isAuthorizationError()}. False
     * for genuine ACL denials.
     */
    public boolean isReadOnlyAccessRefusal() {
        return (flags & FLAG_READ_ONLY_ACCESS_REFUSAL) != 0;
    }

    /**
     * Whether this refusal is a deterministic wire-value/column-type mismatch
     * (set only by {@link #schemaMismatch()}). Implies the error is
     * non-critical and permanent -- replay of the same bytes cannot succeed.
     */
    public boolean isSchemaMismatch() {
        return (flags & FLAG_SCHEMA_MISMATCH) != 0;
    }

    public boolean isSequencerMetadataOpenFailed() {
        return errno == SEQUENCER_METADATA_OPEN_FAILED;
    }

    public boolean isTableDoesNotExist() {
        return errno == TABLE_DOES_NOT_EXIST;
    }

    public boolean isTableDropped() {
        return errno == TABLE_DROPPED;
    }

    public boolean isTableSuspended() {
        return errno == TABLE_SUSPENDED;
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
        setFlag(FLAG_CACHEABLE, cacheable);
        return this;
    }

    public CairoException setCancellation(boolean cancellation) {
        setFlag(FLAG_CANCELLATION, cancellation);
        return this;
    }

    public void setHousekeeping(boolean housekeeping) {
        setFlag(FLAG_HOUSEKEEPING, housekeeping);
    }

    public CairoException setInterruption(boolean interruption) {
        setFlag(FLAG_INTERRUPTION, interruption);
        return this;
    }

    public CairoException setOutOfMemory(boolean outOfMemory) {
        setFlag(FLAG_OUT_OF_MEMORY, outOfMemory);
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
        // With continuations there is a possibility that multiple
        // threads use the same instance with thread local / carrier local.
        // Abolish ThreadLocal exception idea
        CairoException ex = new CairoException();
        ex.clear(errno);
        return ex;
    }

    // N.B.: Change the API with care! This method is called from native code via JNI.
    // See `struct CairoException` in the `qdb-core` Rust crate.
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
        this.flags |= FLAG_AUTHORIZATION_ERROR;
        return this;
    }

    private void setFlag(int flag, boolean value) {
        if (value) {
            this.flags |= flag;
        } else {
            this.flags &= ~flag;
        }
    }

    private CairoException setPreferencesOutOfDateError() {
        this.flags |= FLAG_PREFERENCES_OUT_OF_DATE_ERROR;
        return this;
    }

    private CairoException setReadOnlyAccessRefusal() {
        this.flags |= FLAG_READ_ONLY_ACCESS_REFUSAL;
        return this;
    }

    private CairoException setSchemaMismatch() {
        this.flags |= FLAG_SCHEMA_MISMATCH;
        return this;
    }

    protected void clear(int errno) {
        message.clear();
        nativeBacktrace.clear();
        this.errno = errno;
        // clear() fully resets state so the instance starts from a clean slate. The base
        // CairoException.instance() allocates a fresh object every call, so for it these resets
        // are belt-and-suspenders. They are load-bearing for subclasses that still recycle a pooled
        // flyweight through this method (e.g. LineProtocolException via ThreadLocal): without a full
        // reset a stale flag would leak onto the next exception built on the same flyweight.
        flags = 0;
        messagePosition = 0;
    }
}
