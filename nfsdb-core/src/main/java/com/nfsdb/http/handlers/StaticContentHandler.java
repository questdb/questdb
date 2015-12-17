/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.http.handlers;

import com.nfsdb.http.ContextHandler;
import com.nfsdb.http.FileSender;
import com.nfsdb.http.IOContext;
import com.nfsdb.misc.Chars;

import java.io.File;
import java.io.IOException;

public class StaticContentHandler implements ContextHandler {

    private final File publicDir;
    private final FileSender sender;

    public StaticContentHandler(File publicDir, FileSender sender) {
        this.publicDir = publicDir;
        this.sender = sender;
    }

    public void _continue(IOContext context) throws IOException {
        sender._continue(context);
    }

    @Override
    public void handle(IOContext context) throws IOException {
        //todo: implement non-allocating version of file existence check
        CharSequence url = context.request.getUrl();
        if (Chars.containts(url, "..")) {
            context.response.simple(404);
        } else {
            File file = new File(publicDir, url.toString());
            if (file.exists()) {
                sender.send(context, file, false);
            } else {
                context.response.simple(404);
            }
        }
    }
}
