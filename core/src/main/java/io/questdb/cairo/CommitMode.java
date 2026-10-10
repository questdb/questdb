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

import io.questdb.std.Chars;

public final class CommitMode {
    /**
     * Returned by {@link #fromString(CharSequence)} for a token that is not a recognized mode name, so the
     * caller can reject it with a precise error. Never a valid mode.
     */
    public static final int UNKNOWN = -2;
    /**
     * "No mode threaded in yet" on the writer-scoped {@code setCommitMode} seams ({@code MemoryMA}), whose holders
     * then read the instance-global mode. Not a mode anything runs under.
     */
    public static final int UNSET = -1;
    public static final int ASYNC = 0;
    public static final int SYNC = 1;
    public static final int NOSYNC = 2;
    /**
     * A durable WAL and a lazily applied table.
     * <p>
     * A WAL commit fdatasyncs its segment files (column data, then the {@code _event} file) before the
     * transaction is sequenced. The sequencer record is fdatasynced at once when the group-commit window
     * ({@code cairo.adaptive.commit.group.window}) is 0, and otherwise within the window, batched with other
     * commits, so a crash loses at most the acknowledged commits younger than the window. The durable frontier
     * advances only after the sequencer flush.
     * <p>
     * The apply path is lazy ({@link #appliesColumnSync} excludes ADAPTIVE). The durable epoch and the recovery
     * roll-forward from the WAL make the applied state crash-safe.
     */
    public static final int ADAPTIVE = 3;

    /**
     * The commit mode that applies when nothing is configured, for both the server and the embedded API.
     * {@code PropServerConfiguration} and {@code DefaultCairoConfiguration} both read this constant, so a server
     * and an embedded process cannot default to different durability. The test suite chooses its own mode
     * through {@code questdb.test.commit.mode} (see {@code Overrides}).
     */
    public static final int DEFAULT = NOSYNC;

    /**
     * Returns {@code true} iff the mode flushes the table's applied files on every commit: partition columns,
     * {@code _txn}, {@code _cv} and index files. SYNC blocks until they are durable, ASYNC issues
     * {@code msync(MS_ASYNC)} and leaves writeback to the kernel, NOSYNC never flushes.
     * <p>
     * ADAPTIVE returns {@code false}: its WAL is durable, and the durable epoch
     * ({@code TableWriter.fsyncMaterializedState()}, which flushes in every mode) plus the recovery replay of
     * {@code (epoch.seqTxn, frontier]} rebuild the applied files after a crash. {@code _txn} and {@code _cv}
     * stay lazy with the columns, so a crash rolls the pointers and the data back together.
     * <p>
     * Use this gate only where replaying the WAL from the epoch re-derives the content. Structural writes (the
     * {@code _meta} swap, parquet {@code _pm}) and one-shot writers outside a table writer
     * ({@link #structuralCommitMode(int)}) take other gates, and {@code fsyncMaterializedState()} always
     * flushes. Only SYNC fsyncs the directory of a partition that {@code TableWriter.openPartition} opens;
     * under ADAPTIVE the epoch makes it durable and recovery re-creates later ones. A non-WAL table under
     * ADAPTIVE gets NOSYNC apply durability.
     */
    public static boolean appliesColumnSync(int commitMode) {
        return commitMode == SYNC || commitMode == ASYNC;
    }

    /**
     * Maps a commit mode onto the grade of a one-shot structural write outside a table writer: conversion between
     * WAL and non-WAL, WAL staging-directory creation, checkpoint restore. Those writes have no durable epoch and
     * no WAL to replay them from, so ADAPTIVE maps to SYNC. Every other mode maps to itself.
     */
    public static int structuralCommitMode(int commitMode) {
        return commitMode == ADAPTIVE ? SYNC : commitMode;
    }

    /**
     * Parses a {@code cairo.commit.mode} token, ignoring case. Returns {@link #UNKNOWN} for null or an
     * unrecognized token, so the caller can reject a typo such as {@code syncc} instead of running under a mode
     * the operator did not ask for. Callers test for {@link #UNKNOWN} before using the result.
     */
    public static int fromString(CharSequence mode) {
        if (mode == null) {
            return UNKNOWN;
        }
        if (Chars.equalsIgnoreCase(mode, "nosync")) {
            return NOSYNC;
        }
        if (Chars.equalsIgnoreCase(mode, "sync")) {
            return SYNC;
        }
        if (Chars.equalsIgnoreCase(mode, "async")) {
            return ASYNC;
        }
        if (Chars.equalsIgnoreCase(mode, "adaptive")) {
            return ADAPTIVE;
        }
        return UNKNOWN;
    }

    /**
     * Returns {@code true} iff the mode promises that a WAL commit survives a power loss: SYNC once the commit
     * returns, ADAPTIVE once it is acknowledged. NOSYNC and ASYNC make no such promise.
     * <p>
     * Only under these modes does the WAL writer fsync the directory that holds a new WAL directory, segment
     * directory or segment file before it sequences a commit that names it. ASYNC fsyncs only the directory of a
     * newly opened segment.
     */
    public static boolean isPowerLossDurable(int commitMode) {
        return commitMode == SYNC || commitMode == ADAPTIVE;
    }

    /**
     * Returns the lower-case canonical name of a commit mode, or {@code "unset"} for {@link #UNSET}. Used
     * by the startup durability log lines and the configuration error messages.
     */
    public static String toString(int commitMode) {
        return switch (commitMode) {
            case SYNC -> "sync";
            case ASYNC -> "async";
            case NOSYNC -> "nosync";
            case ADAPTIVE -> "adaptive";
            case UNSET -> "unset";
            default -> "unknown";
        };
    }
}
