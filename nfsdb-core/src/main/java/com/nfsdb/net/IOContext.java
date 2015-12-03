/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.net;

import com.nfsdb.collections.Mutable;
import com.nfsdb.io.parser.TextParser;
import com.nfsdb.io.parser.listener.JournalImportListener;
import com.nfsdb.misc.Misc;
import com.nfsdb.net.http.Request;
import com.nfsdb.net.http.Response;
import com.nfsdb.storage.PlainFile;

import java.io.Closeable;

public class IOContext implements Closeable, Mutable {
    // todo: extract config
    public final Request request = new Request(128 * 1024, 16 * 1024 * 1024, 1024);
    public final Response response = new Response(1024, 1024 * 1024);
    public IOWorkerContext threadContext;

    // multipart generic
    public boolean chunky = false;

    // file upload fields
    public PlainFile mf;
    public long wptr = 0;

    // import handler fields
    public boolean analysed = false;
    public boolean dataFormatValid = false;
    public TextParser textParser;
    public JournalImportListener importer;

    public IOContext() {
    }

    @Override
    public void clear() {
        request.clear();
        response.clear();
        this.chunky = false;
        freeResources();
    }

    @Override
    public void close() {
        request.close();
        response.close();
        freeResources();
    }

    private void freeResources() {
        mf = Misc.free(mf);
        textParser = Misc.free(textParser);
        importer = Misc.free(importer);
    }
}
