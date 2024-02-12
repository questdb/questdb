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

package io.questdb.cutlass.http;

import io.questdb.std.str.Utf8String;

public final class HttpConstants {
    public static final String CONTENT_TYPE_CSV = "text/csv; charset=utf-8";
    public static final String CONTENT_TYPE_HTML = "text/html; charset=utf-8";
    public static final String CONTENT_TYPE_JSON = "application/json; charset=utf-8";
    public static final String CONTENT_TYPE_TEXT = "text/plain; charset=utf-8";

    public static final char COOKIE_VALUE_SEPARATOR = '=';
    public static final Utf8String HEADER_CONNECTION = new Utf8String("Connection", true);
    public static final Utf8String HEADER_CONTENT_ACCEPT_ENCODING = new Utf8String("Accept-Encoding", true);
    public static final Utf8String HEADER_CONTENT_DISPOSITION = new Utf8String("Content-Disposition", true);
    public static final Utf8String HEADER_CONTENT_LENGTH = new Utf8String("Content-Length", true);
    public static final Utf8String HEADER_CONTENT_TYPE = new Utf8String("Content-Type", true);
    public static final Utf8String HEADER_COOKIE = new Utf8String("Cookie", true);
    public static final Utf8String HEADER_IF_NONE_MATCH = new Utf8String("If-None-Match", true);
    public static final Utf8String HEADER_RANGE = new Utf8String("Range", true);
    public static final Utf8String HEADER_SET_COOKIE = new Utf8String("Set-Cookie", true);
    public static final Utf8String HEADER_STATEMENT_TIMEOUT = new Utf8String("Statement-Timeout", true);
    public static final Utf8String HEADER_TRANSFER_ENCODING = new Utf8String("Transfer-Encoding", true);
    public static final Utf8String URL_PARAM_ATOMICITY = new Utf8String("atomicity", true);
    public static final Utf8String URL_PARAM_ATTACHMENT = new Utf8String("attachment", true);
    public static final Utf8String URL_PARAM_COLS = new Utf8String("cols", true);
    public static final Utf8String URL_PARAM_COUNT = new Utf8String("count", true);
    public static final Utf8String URL_PARAM_CREATE = new Utf8String("create", true);
    public static final Utf8String URL_PARAM_DELIMITER = new Utf8String("delimiter", true);
    public static final Utf8String URL_PARAM_EXPLAIN = new Utf8String("explain", true);
    public static final Utf8String URL_PARAM_FILENAME = new Utf8String("filename", true);
    public static final Utf8String URL_PARAM_FMT = new Utf8String("fmt", true);
    public static final Utf8String URL_PARAM_FORCE_HEADER = new Utf8String("forceHeader", true);
    public static final Utf8String URL_PARAM_LIMIT = new Utf8String("limit", true);
    public static final Utf8String URL_PARAM_MAX_UNCOMMITTED_ROWS = new Utf8String("maxUncommittedRows", true);
    public static final Utf8String URL_PARAM_NAME = new Utf8String("name", true);
    public static final Utf8String URL_PARAM_NM = new Utf8String("nm", true);
    public static final Utf8String URL_PARAM_O3_MAX_LAG = new Utf8String("o3MaxLag", true);
    public static final Utf8String URL_PARAM_OVERWRITE = new Utf8String("overwrite", true);
    public static final Utf8String URL_PARAM_PARTITION_BY = new Utf8String("partitionBy", true);
    public static final Utf8String URL_PARAM_QUERY = new Utf8String("query", true);
    public static final Utf8String URL_PARAM_QUOTE_LARGE_NUM = new Utf8String("quoteLargeNum", true);
    public static final Utf8String URL_PARAM_SKIP_LEV = new Utf8String("skipLev", true);
    public static final Utf8String URL_PARAM_SRC = new Utf8String("src", true);
    public static final Utf8String URL_PARAM_STATUS_FORMAT = new Utf8String("f", true);
    public static final Utf8String URL_PARAM_STATUS_TABLE_NAME = new Utf8String("j", true);
    public static final Utf8String URL_PARAM_TIMESTAMP = new Utf8String("timestamp", true);
    public static final Utf8String URL_PARAM_TIMINGS = new Utf8String("timings", true);

    private HttpConstants() {
    }
}
