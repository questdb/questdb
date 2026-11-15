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

package io.questdb.griffin;

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Mutable;
import io.questdb.std.str.StringSink;

/**
 * Validates the SQL of a materialized view for the statements that change the view against its SQL, under the
 * caller's context: view ADD and DROP COLUMN, and later RESUME and ALTER MATERIALIZED VIEW ... AS. The parse tells
 * whether the SQL names its columns, see {@link SqlExecutionContext#hasViewSqlGeneratedColumnNames()}, and the
 * compile, which reads the SQL as the view's refresh does, gives the columns the query produces. A SQL that does not
 * parse, or does not compile, leaves its error here. The caller decides what that means: view ADD COLUMN skips its
 * cast check when the SQL does not compile, so that a view whose SQL a base table change broke can still take new
 * columns.
 * <p>
 * An authorization error is no compile failure: the caller may not read a table the SQL reads, and the validation
 * fails with that error. A cancelled or timed-out compile fails it too.
 * <p>
 * The validation borrows a compiler of its own, so a statement can validate the view's SQL in the middle of its own
 * compile.
 */
public final class MatViewSqlValidator implements Mutable {
    private final LowerCaseCharSequenceHashSet bareSelectNames = new LowerCaseCharSequenceHashSet();
    private final StringSink error = new StringSink();
    private final GenericRecordMetadata queryMetadata = new GenericRecordMetadata();
    private boolean hasGeneratedColumnNames;
    private boolean isCompiled;
    private boolean isParsed;

    @Override
    public void clear() {
        bareSelectNames.clear();
        error.clear();
        queryMetadata.clear();
        hasGeneratedColumnNames = false;
        isCompiled = false;
        isParsed = false;
    }

    /**
     * Returns the bare select-list names of the view's SQL, as the SQL writes them, once {@link #isParsed()}, see
     * {@link SqlCompiler#collectMatViewBareSelectNames}: for a validation that reads bare names as columns, as for
     * RESUME, of a view that is not passthrough; empty otherwise. Names compare without regard to case.
     */
    public LowerCaseCharSequenceHashSet getBareSelectNames() {
        assert isParsed;
        return bareSelectNames;
    }

    /**
     * Returns the error of the parse or the compile that failed.
     */
    public CharSequence getError() {
        return error;
    }

    /**
     * Returns the columns the view's query produces, once {@link #isCompiled()}.
     */
    public RecordMetadata getQueryMetadata() {
        assert isCompiled;
        return queryMetadata;
    }

    /**
     * Returns true when the view's SQL relies on generated column names, once {@link #isParsed()}. Refresh copies
     * such a view's query columns into its columns by position.
     */
    public boolean hasGeneratedColumnNames() {
        assert isParsed;
        return hasGeneratedColumnNames;
    }

    public boolean isCompiled() {
        return isCompiled;
    }

    public boolean isParsed() {
        return isParsed;
    }

    /**
     * Validates the SQL of the view, compiling it only when {@code isCompiling} is set; otherwise only parses it.
     *
     * @throws io.questdb.cairo.CairoException an authorization error, a cancellation or a timeout
     */
    public void validate(MatViewDefinition viewDefinition, boolean isCompiling, SqlExecutionContext executionContext) throws SqlException {
        validate(viewDefinition, isCompiling, false, executionContext);
    }

    /**
     * Validates the SQL of the view, as {@link #validate(MatViewDefinition, boolean, SqlExecutionContext)} does. With
     * {@code isReadingBareNamesAsColumns} set, as for RESUME, the compile reads every bare name as a column, as
     * CREATE does, and a bare name of a zero-argument function fails it with the parentheses the call needs.
     *
     * @throws io.questdb.cairo.CairoException an authorization error, a cancellation or a timeout
     */
    public void validate(
            MatViewDefinition viewDefinition,
            boolean isCompiling,
            boolean isReadingBareNamesAsColumns,
            SqlExecutionContext executionContext
    ) throws SqlException {
        clear();
        try (SqlCompiler compiler = executionContext.getCairoEngine().getSqlCompiler()) {
            compiler.validateMatViewSql(viewDefinition, isCompiling, isReadingBareNamesAsColumns, executionContext, this);
        }
    }

    // Collects the bare select-list names of the parsed view SQL, before the optimiser rewrites the model.
    void ofBareSelectNames(IQueryModel model) {
        MatViewBareSelectNames.collect(model, bareSelectNames);
    }

    void ofCompiled(RecordMetadata metadata) {
        assert isParsed;
        queryMetadata.clear();
        GenericRecordMetadata.copyColumns(metadata, queryMetadata);
        queryMetadata.setTimestampIndex(metadata.getTimestampIndex());
        isCompiled = true;
    }

    void ofError(CharSequence error) {
        this.error.clear();
        this.error.put(error);
    }

    void ofParsed(boolean hasGeneratedColumnNames) {
        this.hasGeneratedColumnNames = hasGeneratedColumnNames;
        isParsed = true;
    }
}
