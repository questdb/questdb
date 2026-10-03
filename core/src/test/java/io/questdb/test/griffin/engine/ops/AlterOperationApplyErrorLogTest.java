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

package io.questdb.test.griffin.engine.ops;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.seq.MetadataServiceStub;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

/**
 * The error branch of {@link AlterOperation#apply} logs the metadata service's table token.
 * Replay-only metadata services (the sequencer metadata change log consumers built on
 * {@link MetadataServiceStub}) leave {@code getTableToken()} unimplemented, so the call is
 * throw-capable. It must be resolved before the log record reserves its ring-buffer slot:
 * an exception thrown inside the chain unwinds past the {@code I$()} terminator, leaks the
 * slot forever, and replaces the real CairoException with an UnsupportedOperationException.
 */
public class AlterOperationApplyErrorLogTest {

    @Test
    public void testApplyReportsCairoExceptionWhenTableTokenUnsupported() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TableToken tableToken = new TableToken("t", "t~1", null, 1, true, false, false);
            final AlterOperationBuilder builder = new AlterOperationBuilder();
            final AlterOperation op = builder.ofSetColumnNotNull(0, tableToken, 1, "x").build();
            final CairoException e = Assert.assertThrows(
                    CairoException.class,
                    () -> op.apply(new TokenlessMetadataService(), true)
            );
            TestUtils.assertContains(e.getFlyweightMessage(), "not-null is not supported here");
        });
    }

    /**
     * Models a replay-only metadata service: it rejects the operation and cannot produce a
     * table token, exactly like the deferred-rename tracker replaying a metadata change log.
     */
    private static class TokenlessMetadataService implements MetadataServiceStub {

        @Override
        public void changeColumnType(CharSequence columnName, int newType, int symbolCapacity, boolean symbolCacheFlag, byte indexType, int indexValueBlockCapacity, boolean isSequential, SecurityContext securityContext) {
        }

        @Override
        public TableRecordMetadata getMetadata() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableToken getTableToken() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getTimestampType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeColumn(@NotNull CharSequence columnName, SecurityContext securityContext) {
        }

        @Override
        public void renameColumn(@NotNull CharSequence columnName, @NotNull CharSequence newName, SecurityContext securityContext) {
        }

        @Override
        public void renameTable(@NotNull CharSequence fromNameTable, @NotNull CharSequence toTableName) {
        }

        @Override
        public void setColumnNotNull(CharSequence columnName, boolean isNotNull) {
            throw CairoException.critical(0).put("not-null is not supported here");
        }
    }
}
