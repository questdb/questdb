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

package com.questdb.griffin.engine;

import com.questdb.common.ColumnType;
import com.questdb.griffin.Function;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.FunctionFactoryService;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.ObjList;

import java.util.ServiceLoader;

public class FunctionRepository {
    private final static Log LOG = LogFactory.getLog(FunctionRepository.class);
    private final static CharSequenceObjHashMap<ObjList<FunctionFactory>> factories = new CharSequenceObjHashMap<>();

    public static FunctionFactory find(CharSequence name, ObjList<Function> args) {
        ObjList<FunctionFactory> overload = factories.get(name);
        if (overload == null) {
            // no such name
            return null;
        }

        final int argCount = args.size();
        FunctionFactory candidate = null;
        int matchCount = 0;
        for (int i = 0, n = overload.size(); i < n; i++) {
            final FunctionFactory factory = overload.getQuick(i);
            final String signature = factory.getSignature();

            if (argCount == 0 && signature.length() == 0) {
                // this is no-arg function, match right away
                return factory;
            }

            // otherwise, is number of arguments the same?
            if (signature.length() == argCount * 2) {
                int match = 2; // match

                for (int k = 0; k < argCount; k++) {
                    final Function arg = args.getQuick(k);
                    final boolean sigArgConst = signature.charAt(k * 2) == '!';
                    final int sigArgType;

                    switch (signature.charAt(k * 2 + 1)) {
                        case 'D':
                            sigArgType = ColumnType.DOUBLE;
                            break;
                        case 'B':
                            sigArgType = ColumnType.BYTE;
                            break;
                        case 'F':
                            sigArgType = ColumnType.FLOAT;
                            break;
                        case 'I':
                            sigArgType = ColumnType.INT;
                            break;
                        case 'L':
                            sigArgType = ColumnType.LONG;
                            break;
                        case 'S':
                            sigArgType = ColumnType.STRING;
                            break;
                        case 'T':
                            sigArgType = ColumnType.BOOLEAN;
                            break;
                        case 'K':
                            sigArgType = ColumnType.SYMBOL;
                            break;
                        case 'M':
                            sigArgType = ColumnType.DATE;
                            break;
                        case 'N':
                            sigArgType = ColumnType.TIMESTAMP;
                            break;
                        default:
                            sigArgType = -1;
                            break;
                    }

                    if (sigArgConst && !arg.isConstant()) {
                        match = 0; // no match
                        break;
                    }

                    if (sigArgType == arg.getType()) {
                        continue;
                    }

                    // can we use overload mechanism?
                    if (arg.getType() >= ColumnType.BYTE
                            && arg.getType() <= ColumnType.DOUBLE
                            && sigArgType >= ColumnType.BYTE
                            && sigArgType <= ColumnType.DOUBLE
                            && arg.getType() < sigArgType) {
                        match = 1; // fuzzy match
                    } else {
                        // types mismatch
                        match = 0;
                        break;
                    }
                }

                if (match == 2) {
                    // exact match?
                    return factory;
                } else if (match == 1) {
                    // fuzzy match
                    if (candidate == null) {
                        candidate = factory;
                    }
                    matchCount++;
                }
            }
        }
        if (matchCount > 1) {
            // ambiguous invocation target
            return null;
        }

        if (matchCount == 0) {
            // no signature match
            return null;
        }

        return candidate;
    }

    public static void load() {
        ObjList<FunctionFactory> importedFactories = new ObjList<>();
        ServiceLoader<FunctionFactoryService> services = ServiceLoader.load(FunctionFactoryService.class);

        for (FunctionFactoryService service : services) {

            importedFactories.clear();
            service.export(importedFactories);

            for (int i = 0, n = importedFactories.size(); i < n; i++) {
                FunctionFactory factory = importedFactories.getQuick(i);
                final int index = factories.keyIndex(factory.getName());
                final ObjList<FunctionFactory> overload;
                if (index < 0) {
                    overload = factories.valueAt(index);
                } else {
                    overload = new ObjList<>(4);
                    factories.putAt(index, factory.getName(), overload);
                }
                overload.add(factory);
            }
        }
    }
}
