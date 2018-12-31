/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.tuck.http;

import com.questdb.std.CharSequenceObjHashMap;

public class SimpleUrlMatcher extends CharSequenceObjHashMap<ContextHandler> implements UrlMatcher {
    private ContextHandler defaultHandler;

    @Override
    public ContextHandler get(CharSequence key) {
        ContextHandler res = super.get(key);
        return res == null ? defaultHandler : res;
    }

    public void setDefaultHandler(ContextHandler defaultHandler) {
        this.defaultHandler = defaultHandler;
    }

    @Override
    public void setupHandlers() {
        for (int i = 0, n = size(); i < n; i++) {
            valueQuick(i).setupThread();
        }
        if (defaultHandler != null) {
            defaultHandler.setupThread();
        }
    }
}
