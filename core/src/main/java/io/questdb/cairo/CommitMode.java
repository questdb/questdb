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
     * A durable WAL and a lazily applied table. A WAL commit fdatasyncs its segment files before the
     * transaction is sequenced; the sequencer record is fdatasynced at once when the group-commit window is 0,
     * otherwise batched within it, so a crash loses at most the acknowledged commits younger than the window,
     * and the durable frontier advances only after that flush.
     * <p>The apply path is lazy ({@link #appliesColumnSync} excludes ADAPTIVE); the durable epoch and recovery
     * roll-forward from the WAL make the applied state crash-safe.
     */
    public static final int ADAPTIVE = 3;

    /**
     * The commit mode that applies when nothing is configured, for both the server and the embedded API.
     * {@code PropServerConfiguration} and {@code DefaultCairoConfiguration} both read this constant.
     */
    public static final int DEFAULT = NOSYNC;

    /**
     * Returns {@code true} iff the mode flushes the table's applied files on every commit: partition columns,
     * {@code _txn}, {@code _cv} and index files. SYNC blocks until durable, ASYNC issues
     * {@code msync(MS_ASYNC)}, NOSYNC never flushes.
     * <p>ADAPTIVE returns {@code false}: its WAL is durable, and the durable epoch plus the recovery replay of
     * {@code (epoch.seqTxn, frontier]} rebuild the applied files after a crash.
     * <p>Structural writes and one-shot writers outside a table writer ({@link #structuralCommitMode(int)})
     * take other gates. A non-WAL table under ADAPTIVE gets NOSYNC apply durability.
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
     * unrecognized token, so the caller can reject a typo such as {@code syncc} instead of silently falling back.
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
     * <p>Only under these modes does the WAL writer fsync a new WAL/segment directory before sequencing a
     * commit that names it; ASYNC fsyncs only a newly opened segment's directory.
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
