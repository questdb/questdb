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

package io.questdb.griffin.plan.logical;

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;

/**
 * A sub-query of the statement, shared by every {@link CursorExpression} that reads it: its plan, the first column id
 * the plan does not use, the nesting depth it bound at, whether every evaluation within one execution yields the same
 * rows and, while the statement binds, its bound output as a factory that opens no cursor, which binding builds the
 * sub-query's consumers over.
 */
public final class Subquery implements Mutable {
    public static final ObjectFactory<Subquery> FACTORY = Subquery::new;
    private final OutputMetadataFactory outputMetadata = new OutputMetadataFactory();
    private int depth;
    private boolean isStable;
    private LogicalPlan root;

    @Override
    public void clear() {
        depth = 0;
        isStable = false;
        root = null;
        outputMetadata.metadata.clear();
    }

    public int getDepth() {
        return depth;
    }

    /**
     * The bound output of the sub-query; the factory opens no cursor.
     */
    public RecordCursorFactory getOutputMetadata() {
        return outputMetadata;
    }

    public LogicalPlan getRoot() {
        return root;
    }

    public boolean isStable() {
        return isStable;
    }

    public Subquery of(LogicalPlan root, int depth, boolean isStable) {
        this.root = root;
        this.depth = depth;
        this.isStable = isStable;
        outputMetadata.of(root.getOutput());
        return this;
    }

    public void setRoot(LogicalPlan root) {
        this.root = root;
    }

    private static final class OutputMetadataFactory implements RecordCursorFactory {
        private final GenericRecordMetadata metadata = new GenericRecordMetadata();

        @Override
        public RecordMetadata getMetadata() {
            return metadata;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        // A consumer that validates this capability while binding is rebuilt over the generated factory, which decides.
        @Override
        public boolean supportsPageFrameCursor() {
            return true;
        }

        private void of(OutputSchema output) {
            metadata.clear();
            for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                metadata.add(new TableColumnMetadata(Chars.toString(output.getColumnName(i)), output.getColumnType(i),
                        IndexType.NONE, 0, output.isSymbolTableStatic(i), null));
            }
            metadata.setTimestampIndex(output.getTimestampIndex());
        }
    }
}
