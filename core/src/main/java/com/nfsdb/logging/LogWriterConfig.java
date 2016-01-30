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

package com.nfsdb.logging;

public class LogWriterConfig {
    private final String scope;
    private final int level;
    private final LogWriterFactory factory;

    public LogWriterConfig(int level, LogWriterFactory factory) {
        this("", level, factory);
    }

    public LogWriterConfig(String scope, int level, LogWriterFactory factory) {
        this.scope = scope == null ? "" : scope;
        this.level = level;
        this.factory = factory;
    }

    public LogWriterFactory getFactory() {
        return factory;
    }

    public int getLevel() {
        return level;
    }

    public String getScope() {
        return scope;
    }
}
