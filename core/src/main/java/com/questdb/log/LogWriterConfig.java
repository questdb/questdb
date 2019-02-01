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

package com.questdb.log;

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
