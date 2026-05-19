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

package io.questdb.cairo.sql;

import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.str.DirectUtf8StringList;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.NotNull;

/**
 * SPI for resolving remote-storage URIs in {@code read_parquet()} arguments to
 * local files the existing parquet cursors can mmap. Discovered via
 * {@link java.util.ServiceLoader}; the OSS distribution ships no provider, so
 * remote URIs return "no provider" until an Enterprise (or third-party) module
 * registers one. With no provider, {@code read_parquet('s3://...')} falls through
 * to the local-file path and fails the sandbox check, the same as before.
 * <p>
 * <b>OSS/Enterprise boundary:</b> all object-store networking, credential
 * handling, retry logic, and download caching is the provider's responsibility
 * and MUST live in the Enterprise (or third-party) module. The OSS distribution
 * carries only this interface, the {@link java.util.ServiceLoader} dispatch in
 * {@code ReadParquetFunctionFactory}, the
 * {@code SecurityContext#authorizeReadRemoteParquet} hook (default no-op), and
 * the existing local-file cursors. No S3, GCS, Azure, HTTP, or OpenDAL code
 * may be added to the OSS Rust or OSS Java tree - if you find yourself
 * reaching for an object-store library, you're working in the wrong module.
 * <p>
 * Providers MUST:
 * <ul>
 *   <li>Implement {@link #canHandle(CharSequence)} cheaply (scheme/prefix check
 *       only). The caller may consult several providers before falling back.</li>
 *   <li>Perform any authorisation check inside {@link #resolveLocal} /
 *       {@link #expandAndResolveGlob}, throwing {@link SqlException} on denial.
 *       The OSS dispatcher does NOT enforce permissions on its own.</li>
 *   <li>Return absolute local paths under a stable cache directory so the
 *       caller can re-open the same URI via mmap without re-downloading on
 *       every call. Caching, eviction, and ETag/Last-Modified invalidation
 *       are the provider's responsibility.</li>
 * </ul>
 * Glob expansion ({@link #expandAndResolveGlob}) downloads every matched file;
 * callers feed the result directly into the existing hive cursor, which
 * already understands hive {@code key=value} segments. Providers SHOULD
 * preserve the source URI's directory shape under the cache root so partition
 * parsing on the returned paths produces the same column set the user would
 * see locally.
 */
public interface RemoteParquetFsProvider {

    /**
     * Cheap prefix / scheme test - the caller asks this on every parquet read,
     * so it must not perform I/O. Returning {@code true} commits the caller to
     * dispatch the resolution to this provider.
     */
    boolean canHandle(@NotNull CharSequence uri);

    /**
     * Resolve a glob URI to a list of concrete local paths after downloading
     * every matching remote file into the provider's cache. The returned
     * {@link ResolvedGlob} owns its matched file list - the caller must free it.
     * Each entry is an absolute local path safe to mmap, with {@code key=value}
     * directory segments preserved exactly as in the source URI so the hive
     * partition parser sees the same column set as a local read.
     *
     * @throws SqlException on auth failure, listing failure, or download
     *                      failure. Position should point at {@code argPos}.
     */
    @NotNull
    ResolvedGlob expandAndResolveGlob(
            @NotNull CharSequence globUri,
            int argPos,
            @NotNull SqlExecutionContext ctx
    ) throws SqlException;

    /**
     * Resolve a single (non-glob) URI to an absolute local path. The provider
     * downloads the remote object if absent from its cache; on a cache hit it
     * returns the cached path without remote I/O (subject to its freshness
     * policy).
     *
     * @throws SqlException on auth failure or download failure.
     */
    @NotNull
    Utf8String resolveLocal(
            @NotNull CharSequence uri,
            int argPos,
            @NotNull SqlExecutionContext ctx
    ) throws SqlException;

    /**
     * Result of {@link #expandAndResolveGlob}. Holds the resolved local file list
     * plus the byte offset at which hive partition {@code key=value} segments
     * begin in every entry. The hive cursor uses {@code nonGlobRootByteLen} when
     * parsing partition values from a matched path. The {@code matchedFiles} list
     * transfers ownership to the caller; the caller must {@code free()} it.
     */
    final class ResolvedGlob {
        private final DirectUtf8StringList matchedFiles;
        private final int nonGlobRootByteLen;

        public ResolvedGlob(@NotNull DirectUtf8StringList matchedFiles, int nonGlobRootByteLen) {
            this.matchedFiles = matchedFiles;
            this.nonGlobRootByteLen = nonGlobRootByteLen;
        }

        @NotNull
        public DirectUtf8StringList getMatchedFiles() {
            return matchedFiles;
        }

        public int getNonGlobRootByteLen() {
            return nonGlobRootByteLen;
        }
    }
}
