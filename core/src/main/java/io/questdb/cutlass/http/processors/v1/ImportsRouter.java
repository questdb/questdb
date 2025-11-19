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
import io.questdb.cutlass.http.HttpMultipartContentProcessor;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.DirectUtf8Sequence;

public class ImportsRouter implements HttpRequestHandler {
    private final HttpRequestProcessor deleteProcessor;
    private final HttpRequestProcessor getProcessor;
    private final HttpMultipartContentProcessor postProcessor;

    public ImportsRouter(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration) {
        postProcessor = new FileUploadProcessor(cairoEngine, configuration, FilesRootDir.IMPORTS);
        getProcessor = new FileGetProcessor(cairoEngine, configuration, FilesRootDir.IMPORTS);
        deleteProcessor = new FileDeleteProcessor(cairoEngine, configuration, FilesRootDir.IMPORTS);
    }

    public static ObjHashSet<String> getRoutes(ObjHashSet<String> parentRoutes) {
        ObjHashSet<String> out = new ObjHashSet<>(parentRoutes.size());
        for (int i = 0; i < parentRoutes.size(); i++) {
            out.add(parentRoutes.get(i) + "/imports");
        }
        return out;
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        DirectUtf8Sequence method = requestHeader.getMethod();
        if (HttpKeywords.isPOST(method)) {
            return postProcessor;
        } else if (HttpKeywords.isGET(method) || HttpKeywords.isHEAD(method)) {
            return getProcessor;
        } else if (HttpKeywords.isDELETE(method)) {
            return deleteProcessor;
        }
        return null;
    }
}
