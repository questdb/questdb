/*******************************************************************************
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

package io.questdb.cutlass.http;

import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

public interface HttpRequestHeader {
    DirectUtf8Sequence getBoundary();

    DirectUtf8Sequence getCharset();

    DirectUtf8Sequence getContentDisposition();

    DirectUtf8Sequence getContentDispositionFilename();

    DirectUtf8Sequence getContentDispositionName();

    long getContentLength();

    DirectUtf8Sequence getContentType();

    DirectUtf8Sequence getHeader(Utf8Sequence name);

    ObjList<? extends Utf8Sequence> getHeaderNames();

    DirectUtf8Sequence getMethod();

    DirectUtf8Sequence getMethodLine();

    @Nullable
    DirectUtf8String getQuery();

    long getStatementTimeout();

    DirectUtf8String getUrl();

    DirectUtf8Sequence getUrlParam(Utf8Sequence name);

    boolean isGetRequest();

    boolean isPostRequest();

    boolean isPutRequest();
}
