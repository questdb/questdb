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

package io.questdb.cutlass.http;

import io.questdb.std.str.Utf8String;

public final class HttpConstants {
    public static final String CONTENT_TYPE_CSV = "text/csv; charset=utf-8";
    public static final String CONTENT_TYPE_JSON = "application/json; charset=utf-8";
    @SuppressWarnings("unused")
    public static final String CONTENT_TYPE_MULTIPART_FORM_DATA = "multipart/form-data";
    @SuppressWarnings("unused")
    public static final String CONTENT_TYPE_MULTIPART_MIXED = "multipart/mixed";
    public static final String CONTENT_TYPE_TEXT = "text/plain; charset=utf-8";
    public static final char COOKIE_VALUE_SEPARATOR = '=';
    public static final Utf8String HEADER_AUTHORIZATION = new Utf8String("Authorization");
    public static final Utf8String HEADER_CONNECTION = new Utf8String("Connection");
    public static final Utf8String HEADER_CONTENT_ACCEPT_ENCODING = new Utf8String("Accept-Encoding");
    public static final Utf8String HEADER_CONTENT_DISPOSITION = new Utf8String("Content-Disposition");
    public static final Utf8String HEADER_CONTENT_LENGTH = new Utf8String("Content-Length");
    public static final Utf8String HEADER_CONTENT_TYPE = new Utf8String("Content-Type");
    public static final Utf8String HEADER_COOKIE = new Utf8String("Cookie");
    public static final Utf8String HEADER_IF_NONE_MATCH = new Utf8String("If-None-Match");
    public static final Utf8String HEADER_RANGE = new Utf8String("Range");
    public static final Utf8String HEADER_SET_COOKIE = new Utf8String("Set-Cookie");
    public static final Utf8String HEADER_STATEMENT_TIMEOUT = new Utf8String("Statement-Timeout");
    public static final Utf8String HEADER_TRANSFER_ENCODING = new Utf8String("Transfer-Encoding");
    @SuppressWarnings("unused")
    public static final String HEADER_TRANSFER_ENCODING_CHUNKED = "chunked";
    @SuppressWarnings("unused")
    public static final String METHOD_GET = "GET";
    @SuppressWarnings("unused")
    public static final String METHOD_POST = "POST";
    @SuppressWarnings("unused")
    public static final String METHOD_PUT = "PUT";
    public static final Utf8String URL_PARAM_ATOMICITY = new Utf8String("atomicity");
    public static final Utf8String URL_PARAM_ATTACHMENT = new Utf8String("attachment");
    public static final Utf8String URL_PARAM_COLS = new Utf8String("cols");
    public static final Utf8String URL_PARAM_COUNT = new Utf8String("count");
    public static final Utf8String URL_PARAM_CREATE = new Utf8String("create");
    public static final Utf8String URL_PARAM_DELIMITER = new Utf8String("delimiter");
    public static final Utf8String URL_PARAM_EXPLAIN = new Utf8String("explain");
    public static final Utf8String URL_PARAM_FILENAME = new Utf8String("filename");
    public static final Utf8String URL_PARAM_FMT = new Utf8String("fmt");
    public static final Utf8String URL_PARAM_FORCE_HEADER = new Utf8String("forceHeader");
    public static final Utf8String URL_PARAM_LIMIT = new Utf8String("limit");
    public static final Utf8String URL_PARAM_MAX_UNCOMMITTED_ROWS = new Utf8String("maxUncommittedRows");
    public static final Utf8String URL_PARAM_NAME = new Utf8String("name");
    public static final Utf8String URL_PARAM_NM = new Utf8String("nm");
    public static final Utf8String URL_PARAM_O3_MAX_LAG = new Utf8String("o3MaxLag");
    public static final Utf8String URL_PARAM_OVERWRITE = new Utf8String("overwrite");
    public static final Utf8String URL_PARAM_PARTITION_BY = new Utf8String("partitionBy");
    public static final Utf8String URL_PARAM_QUERY = new Utf8String("query");
    public static final Utf8String URL_PARAM_QUOTE_LARGE_NUM = new Utf8String("quoteLargeNum");
    public static final Utf8String URL_PARAM_SKIP_LEV = new Utf8String("skipLev");
    public static final Utf8String URL_PARAM_SRC = new Utf8String("src");
    public static final Utf8String URL_PARAM_STATUS_FORMAT = new Utf8String("f");
    public static final Utf8String URL_PARAM_STATUS_TABLE_NAME = new Utf8String("j");
    public static final Utf8String URL_PARAM_TIMESTAMP = new Utf8String("timestamp");
    public static final Utf8String URL_PARAM_TIMINGS = new Utf8String("timings");
    public static final Utf8String URL_PARAM_VERSION = new Utf8String("version");

    private HttpConstants() {
    }
}
