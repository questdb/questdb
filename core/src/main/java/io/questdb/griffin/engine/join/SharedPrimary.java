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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * A factory that several places of one factory tree read. The place that holds the factory, its home, reads the
 * cursor of the factory itself, see {@link io.questdb.cairo.sql.RecordCursorFactory#getCursor}, and each other place
 * reads a cursor of its own through a {@link SharedRecordCursorFactory}, see
 * {@link io.questdb.cairo.sql.RecordCursorFactory#getSharedCursor}. LateralJoinRewriter shares a sub-query this way,
 * and the code generator builds the factory once, when it reaches the first place that reads it.
 * <p>
 * Which place the code generator reaches first, and whether the finished tree still holds the home, depends on the
 * order in which the join planner orders the places and on the sub-trees that the code generator drops, such as
 * those of a constant false join. This object therefore owns the factory while the code generator builds the tree:
 * a place that drops the factory, or a reader that closes, reports it here, see {@link #releaseHome()} and
 * {@link #releaseReader}, and the factory closes once no place reads it. When the tree is built, {@link #handOver()}
 * gives the factory to the tree: to its home, or, when the tree holds no home, to one of the readers, which then reads
 * the cursor of the factory itself.
 */
public final class SharedPrimary implements QuietCloseable {
    private final AbstractRecordCursorFactory factory;
    private final ObjList<SharedRecordCursorFactory> readers = new ObjList<>();
    private int homeCount;
    private boolean isClosed;
    private boolean isHandedOver;

    public SharedPrimary(AbstractRecordCursorFactory factory) {
        this.factory = factory;
        factory.setSharedPrimary(this);
    }

    /**
     * Records a place of the tree that holds the factory and reads its cursor, and returns the factory for it.
     */
    public AbstractRecordCursorFactory addHome() {
        assert !isClosed && !isHandedOver;
        homeCount++;
        return factory;
    }

    /**
     * Closes the factory, unless {@link #handOver()} gave it to the tree.
     */
    @Override
    public void close() {
        if (!isClosed && !isHandedOver) {
            isClosed = true;
            factory.setSharedPrimary(null);
            factory.close();
        }
    }

    public AbstractRecordCursorFactory getFactory() {
        return factory;
    }

    /**
     * Gives the factory to the finished tree: to its home, when the tree still holds one, else to the first reader
     * that the tree still holds, which reads the cursor of the factory from then on.
     */
    public void handOver() {
        if (isClosed) {
            // the code generator dropped every place that read the factory
            return;
        }
        assert !isHandedOver && (homeCount > 0 || readers.size() > 0);
        isHandedOver = true;
        factory.setSharedPrimary(null);
        if (homeCount == 0) {
            readers.getQuick(0).promoteToHome();
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Called by {@link AbstractRecordCursorFactory#close()} while this object owns the factory: a place of the tree
     * that held the factory dropped it.
     */
    public void releaseHome() {
        // the owners of a factory close it more than once on error paths, see AbstractRecordCursorFactory
        homeCount = Math.max(homeCount - 1, 0);
        closeIfUnread();
    }

    void addReader(SharedRecordCursorFactory reader) {
        assert !isClosed && !isHandedOver;
        readers.add(reader);
    }

    void releaseReader(SharedRecordCursorFactory reader) {
        final int index = readers.indexOf(reader);
        // AbstractRecordCursorFactory closes a reader once
        assert index > -1;
        readers.remove(index);
        closeIfUnread();
    }

    private void closeIfUnread() {
        if (homeCount == 0 && readers.size() == 0) {
            close();
        }
    }
}
