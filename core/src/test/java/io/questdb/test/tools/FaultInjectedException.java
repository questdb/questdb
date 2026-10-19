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

package io.questdb.test.tools;

/**
 * The exception a test's injected fault throws. Tests catch it to prove the fault fired and that
 * nothing else swallowed it on the way out.
 * <p>
 * The no-argument constructor reports only that a fault fired. It skips the stack trace, which the
 * test never reads. The one-argument constructor also records which fault point fired, so a test
 * that arms one of several fault points can assert that the fault it asked for is the fault it got.
 * {@link #faultPoint} is therefore null whenever the no-argument constructor built the exception:
 * it means "the thrower did not name a fault point", not "no fault point fired".
 */
public class FaultInjectedException extends RuntimeException {
    public final Enum<?> faultPoint;

    public FaultInjectedException() {
        super("injected", null, false, false);
        this.faultPoint = null;
    }

    public FaultInjectedException(Enum<?> faultPoint) {
        super("injected failure at " + faultPoint);
        this.faultPoint = faultPoint;
    }
}
