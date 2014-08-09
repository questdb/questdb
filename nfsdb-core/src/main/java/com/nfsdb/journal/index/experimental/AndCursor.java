/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.journal.index.experimental;

import com.nfsdb.journal.index.Cursor;

public final class AndCursor extends AbstractCursor {

    private final Cursor a;
    private final Cursor b;

    public AndCursor(Cursor a, Cursor b) {
        this.a = a;
        this.b = b;
    }

    @Override
    protected final long getNext() {
        long nextA = -1;
        long nextB = -1;

        while (true) {
            if (nextA == -1 && a.hasNext()) {
                nextA = a.next();
            }

            if (nextB == -1 && b.hasNext()) {
                nextB = b.next();
            }

            if (nextA == -1 || nextB == -1) {
                return -2;
            }

            if (nextA > nextB) {
                nextA = -1;
            } else if (nextA < nextB) {
                nextB = -1;
            } else {
                return nextA;
            }
        }
    }
}
