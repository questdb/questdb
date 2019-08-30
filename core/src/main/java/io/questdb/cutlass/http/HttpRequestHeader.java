/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.cutlass.http;

import io.questdb.std.ObjList;
import io.questdb.std.str.DirectByteCharSequence;

public interface HttpRequestHeader {
    DirectByteCharSequence getBoundary();

    DirectByteCharSequence getCharset();

    CharSequence getContentDisposition();

    CharSequence getContentDispositionFilename();

    CharSequence getContentDispositionName();

    CharSequence getContentType();

    DirectByteCharSequence getHeader(CharSequence name);

    ObjList<CharSequence> getHeaderNames();

    CharSequence getMethod();

    CharSequence getMethodLine();

    CharSequence getUrl();

    CharSequence getUrlParam(CharSequence name);
}
