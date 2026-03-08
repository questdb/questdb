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

import io.questdb.std.Mutable;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

public class HttpCookie implements Mutable, Sinkable {
    public DirectUtf8String cookieName;
    public DirectUtf8String domain;
    public long expires = -1L;
    public boolean httpOnly;
    public long maxAge;
    public boolean partitioned;
    public DirectUtf8String path;
    public DirectUtf8String sameSite;
    public boolean secure;
    public DirectUtf8String value;

    @Override
    public void clear() {
        this.domain = null;
        this.expires = -1L;
        this.httpOnly = false;
        this.maxAge = 0L;
        this.partitioned = false;
        this.path = null;
        this.sameSite = null;
        this.secure = false;
        this.value = null;
        this.cookieName = null;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put('{');

        sink.put("cookieName=").putQuoted(cookieName);
        sink.put(", value=").putQuoted(value);
        if (domain != null) {
            sink.put(", domain=").putQuoted(domain);
        }
        if (path != null) {
            sink.put(", path=").putQuoted(path);
        }
        sink.put(", secure=").put(secure);
        sink.put(", httpOnly=").put(httpOnly);
        sink.put(", partitioned=").put(partitioned);
        sink.put(", expires=").put(expires);
        sink.put(", maxAge=").put(maxAge);
        if (sameSite != null) {
            sink.put(", sameSite=").putQuoted(sameSite);
        }
        sink.put('}');
    }

    public boolean isDeleted() {
        return expires == 0L;
    }
}
