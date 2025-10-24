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

package io.questdb.cutlass.http.processors.v1;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpKeywords;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sequence;

public class ExportsRouter implements HttpRequestHandler {
    private final HttpRequestProcessor deleteProcessor;
    private final HttpRequestProcessor getProcessor;

    public ExportsRouter(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration) {
        getProcessor = new FileGetProcessor(cairoEngine, configuration, FilesRootDir.EXPORTS);
        deleteProcessor = new FileDeleteProcessor(cairoEngine, configuration, FilesRootDir.EXPORTS);
    }

    public static ObjList<String> getRoutes(ObjList<String> parentRoutes) {
        ObjList<String> out = new ObjList<>(parentRoutes.size());
        for (int i = 0; i < parentRoutes.size(); i++) {
            out.extendAndSet(i, parentRoutes.get(i) + "/exports");
        }
        return out;
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        DirectUtf8Sequence method = requestHeader.getMethod();
        if (HttpKeywords.isGET(method)) {
            return getProcessor;
        } else if (HttpKeywords.isDELETE(method)) {
            return deleteProcessor;
        }
        return null;
    }
}
