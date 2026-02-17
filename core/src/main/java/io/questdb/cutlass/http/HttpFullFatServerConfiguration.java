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

import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import io.questdb.std.ConcurrentCacheConfiguration;
import io.questdb.std.ObjHashSet;

public interface HttpFullFatServerConfiguration extends HttpServerConfiguration {
    ObjHashSet<String> CONTEXT_PATH_EXEC = new ObjHashSet<>() {{
        add("/exec");
        add("/api/v1/sql/execute");
    }};
    ObjHashSet<String> CONTEXT_PATH_EXPORT = new ObjHashSet<>() {{
        add("/exp");
    }};
    ObjHashSet<String> CONTEXT_PATH_ILP = new ObjHashSet<>() {{
        add("/write");
        add("/api/v2/write");
    }};
    ObjHashSet<String> CONTEXT_PATH_IMPORT = new ObjHashSet<>() {{
        add("/imp");
    }};
    ObjHashSet<String> CONTEXT_PATH_SETTINGS = new ObjHashSet<>() {{
        add("/settings");
    }};
    ObjHashSet<String> CONTEXT_PATH_TABLE_STATUS = new ObjHashSet<>() {{
        add("/chk");
    }};
    ObjHashSet<String> CONTEXT_PATH_VALIDATE = new ObjHashSet<>() {{
        add("/api/v1/sql/validate");
    }};
    ObjHashSet<String> CONTEXT_PATH_WARNINGS = new ObjHashSet<>() {{
        add("/warnings");
    }};
    String DEFAULT_PROCESSOR_URL = "*";

    ConcurrentCacheConfiguration getConcurrentCacheConfiguration();

    default ObjHashSet<String> getContextPathDefault() {
        return new ObjHashSet<>() {{
            add(DEFAULT_PROCESSOR_URL);
        }};
    }

    default ObjHashSet<String> getContextPathExec() {
        return CONTEXT_PATH_EXEC;
    }

    default ObjHashSet<String> getContextPathExport() {
        return CONTEXT_PATH_EXPORT;
    }

    default ObjHashSet<String> getContextPathILP() {
        return CONTEXT_PATH_ILP;
    }

    default ObjHashSet<String> getContextPathILPPing() {
        return new ObjHashSet<>() {{
            add("/ping");
        }};
    }

    default ObjHashSet<String> getContextPathImport() {
        return CONTEXT_PATH_IMPORT;
    }

    default ObjHashSet<String> getContextPathSettings() {
        return CONTEXT_PATH_SETTINGS;
    }

    default ObjHashSet<String> getContextPathSqlValidation() {
        return CONTEXT_PATH_VALIDATE;
    }

    default ObjHashSet<String> getContextPathTableStatus() {
        return CONTEXT_PATH_TABLE_STATUS;
    }

    default ObjHashSet<String> getContextPathWarnings() {
        return CONTEXT_PATH_WARNINGS;
    }

    default String getContextPathWebConsole() {
        return "";
    }

    JsonQueryProcessorConfiguration getJsonQueryProcessorConfiguration();

    LineHttpProcessorConfiguration getLineHttpProcessorConfiguration();

    String getPassword();

    StaticContentProcessorConfiguration getStaticContentProcessorConfiguration();

    String getUsername();

    boolean isAcceptingWrites();

    boolean isQueryCacheEnabled();

    boolean isSettingsReadOnly();
}
