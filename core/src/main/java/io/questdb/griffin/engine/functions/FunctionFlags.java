/*+*****************************************************************************
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


package io.questdb.griffin.engine.functions;

import io.questdb.cairo.sql.Function;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * The properties a function that holds other functions carries up from them, for the classes
 * that do not get them from {@link UnaryFunction}, {@link BinaryFunction}, {@link TernaryFunction},
 * {@link QuaternaryFunction} or {@link MultiArgFunction}: the planner asks them of the outermost
 * function only (see SqlCodeGenerator.isCallOrderFree() and isRowPure()), so a wrapper that does
 * not forward them hides an argument that counts its calls, draws from a generator, or reads the
 * clock. FunctionFlagsForwardingTest checks that every function holding functions forwards them.
 * Null arguments are skipped.
 */
public final class FunctionFlags {
    private FunctionFlags() {
    }

    public static boolean hasPerCallState(@Nullable Function a) {
        return a != null && a.hasPerCallState();
    }

    public static boolean hasPerCallState(@Nullable Function a, @Nullable Function b) {
        return hasPerCallState(a) || hasPerCallState(b);
    }

    public static boolean hasPerCallState(@Nullable Function a, @Nullable Function b, @Nullable Function c) {
        return hasPerCallState(a) || hasPerCallState(b) || hasPerCallState(c);
    }

    public static boolean hasPerCallState(@Nullable Function a, @Nullable Function b, @Nullable Function c, @Nullable Function d) {
        return hasPerCallState(a, b) || hasPerCallState(c, d);
    }

    public static boolean hasPerCallState(@Nullable ObjList<? extends Function> functions) {
        if (functions != null) {
            for (int i = 0, n = functions.size(); i < n; i++) {
                if (hasPerCallState(functions.getQuick(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isNonDeterministic(@Nullable Function a) {
        return a != null && a.isNonDeterministic();
    }

    public static boolean isNonDeterministic(@Nullable Function a, @Nullable Function b) {
        return isNonDeterministic(a) || isNonDeterministic(b);
    }

    public static boolean isNonDeterministic(@Nullable Function a, @Nullable Function b, @Nullable Function c) {
        return isNonDeterministic(a) || isNonDeterministic(b) || isNonDeterministic(c);
    }

    public static boolean isNonDeterministic(@Nullable Function a, @Nullable Function b, @Nullable Function c, @Nullable Function d) {
        return isNonDeterministic(a, b) || isNonDeterministic(c, d);
    }

    public static boolean isNonDeterministic(@Nullable ObjList<? extends Function> functions) {
        if (functions != null) {
            for (int i = 0, n = functions.size(); i < n; i++) {
                if (isNonDeterministic(functions.getQuick(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isRandom(@Nullable Function a) {
        return a != null && a.isRandom();
    }

    public static boolean isRandom(@Nullable Function a, @Nullable Function b) {
        return isRandom(a) || isRandom(b);
    }

    public static boolean isRandom(@Nullable Function a, @Nullable Function b, @Nullable Function c) {
        return isRandom(a) || isRandom(b) || isRandom(c);
    }

    public static boolean isRandom(@Nullable Function a, @Nullable Function b, @Nullable Function c, @Nullable Function d) {
        return isRandom(a, b) || isRandom(c, d);
    }

    public static boolean isRandom(@Nullable ObjList<? extends Function> functions) {
        if (functions != null) {
            for (int i = 0, n = functions.size(); i < n; i++) {
                if (isRandom(functions.getQuick(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isStableWithinExecution(@Nullable Function a) {
        return a == null || a.isStableWithinExecution();
    }

    public static boolean isStableWithinExecution(@Nullable Function a, @Nullable Function b) {
        return isStableWithinExecution(a) && isStableWithinExecution(b);
    }

    public static boolean isStableWithinExecution(@Nullable Function a, @Nullable Function b, @Nullable Function c) {
        return isStableWithinExecution(a) && isStableWithinExecution(b) && isStableWithinExecution(c);
    }

    public static boolean isStableWithinExecution(@Nullable Function a, @Nullable Function b, @Nullable Function c, @Nullable Function d) {
        return isStableWithinExecution(a, b) && isStableWithinExecution(c, d);
    }

    public static boolean isStableWithinExecution(@Nullable ObjList<? extends Function> functions) {
        if (functions != null) {
            for (int i = 0, n = functions.size(); i < n; i++) {
                if (!isStableWithinExecution(functions.getQuick(i))) {
                    return false;
                }
            }
        }
        return true;
    }
}
