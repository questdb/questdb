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

import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.std.ObjHashSet;

/**
 * Routes OpenAPI specification requests to the OpenApiProcessor.
 * Serves the OpenAPI 3.0 specification at /api/v1/ or /api/v1
 */
public class OpenApiRouter implements HttpRequestHandler {
    private final HttpRequestProcessor processor;

    public OpenApiRouter() {
        processor = new OpenApiProcessor();
    }

    public static ObjHashSet<String> getRoutes(ObjHashSet<String> parentRoutes) {
        ObjHashSet<String> out = new ObjHashSet<>(parentRoutes.size());
        for (int i = 0; i < parentRoutes.size(); i++) {
            String parentRoute = parentRoutes.get(i);
            out.add(parentRoute);
        }
        return out;
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return processor;
    }
}
