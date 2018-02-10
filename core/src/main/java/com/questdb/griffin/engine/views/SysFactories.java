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

package com.questdb.griffin.engine.views;

import com.questdb.std.CharSequenceObjHashMap;

public final class SysFactories {

    private final static CharSequenceObjHashMap<SystemViewFactory> sysViewFactories = new CharSequenceObjHashMap<>();

    private SysFactories() {
    }

    public static SystemViewFactory getFactory(CharSequence name) {
        return sysViewFactories.get(name);
    }

    static {
//        sysViewFactories.put("$tabs", $TabsFactory.INSTANCE);
//        sysViewFactories.put("$cols", $ColsFactory.INSTANCE);
//        sysViewFactories.put("$locales", $LocalesFactory.INSTANCE);
//        sysViewFactories.put("dual", DualFactory.INSTANCE);
    }
}
