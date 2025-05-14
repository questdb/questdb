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

import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import io.questdb.std.ConcurrentCacheConfiguration;
import io.questdb.std.ObjList;

public interface HttpFullFatServerConfiguration extends HttpServerConfiguration {
    String DEFAULT_PROCESSOR_URL = "*";

    ConcurrentCacheConfiguration getConcurrentCacheConfiguration();

    default ObjList<String> getContextPathDefault() {
        return new ObjList<>(DEFAULT_PROCESSOR_URL);
    }

    default ObjList<String> getContextPathExec() {
        return new ObjList<>("/exec");
    }

    default ObjList<String> getContextPathExport() {
        return new ObjList<>("/exp");
    }

    default ObjList<String> getContextPathILP() {
        return new ObjList<>("/write", "/api/v2/write");
    }

    default ObjList<String> getContextPathILPPing() {
        return new ObjList<>("/ping");
    }

    default ObjList<String> getContextPathImport() {
        return new ObjList<>("/imp");
    }

    default ObjList<String> getContextPathSettings() {
        return new ObjList<>("/settings");
    }

    default ObjList<String> getContextPathTableStatus() {
        return new ObjList<>("/chk");
    }

    default ObjList<String> getContextPathWarnings() {
        return new ObjList<>("/warnings");
    }

    default String getContextPathWebConsole() {
        return "";
    }

    JsonQueryProcessorConfiguration getJsonQueryProcessorConfiguration();

    LineHttpProcessorConfiguration getLineHttpProcessorConfiguration();

    String getPassword();

    StaticContentProcessorConfiguration getStaticContentProcessorConfiguration();

    String getUsername();

    boolean isQueryCacheEnabled();
}
