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
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.DirectUtf8Sequence;

public class ExportsRouter implements HttpRequestHandler {
    private final HttpRequestProcessor fileDeleteProcessor;
    private final HttpRequestProcessor fileDownloadProcessor;
    private final HttpRequestProcessor fileListProcessor;
    private final HttpRequestProcessor fileMetadataProcessor;

    public ExportsRouter(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration) {
        fileListProcessor = new FileListProcessor(cairoEngine, configuration, FilesRootDir.EXPORTS);
        fileDownloadProcessor = new FileDownloadProcessor(cairoEngine, configuration, FilesRootDir.EXPORTS);
        fileMetadataProcessor = new FileMetadataProcessor(cairoEngine, configuration, FilesRootDir.EXPORTS);
        fileDeleteProcessor = new FileDeleteProcessor(cairoEngine, configuration, FilesRootDir.EXPORTS);
    }

    public static ObjHashSet<String> getRoutes(ObjHashSet<String> parentRoutes) {
        ObjHashSet<String> out = new ObjHashSet<>(parentRoutes.size());
        for (int i = 0; i < parentRoutes.size(); i++) {
            out.add(parentRoutes.get(i) + "/exports");
        }
        return out;
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        DirectUtf8Sequence method = requestHeader.getMethod();
        if (HttpKeywords.isGET(method)) {
            // Check if a file is specified in the URL
            DirectUtf8Sequence file = FileGetProcessorHelper.extractFilePathFromUrl(requestHeader, FilesRootDir.EXPORTS);
            if (file == null || file.size() == 0) {
                // No file specified - list files
                return fileListProcessor;
            } else {
                // File specified - download file
                return fileDownloadProcessor;
            }
        } else if (HttpKeywords.isHEAD(method)) {
            // HEAD requests always target a specific file and return metadata
            return fileMetadataProcessor;
        } else if (HttpKeywords.isDELETE(method)) {
            return fileDeleteProcessor;
        }
        return null;
    }
}
