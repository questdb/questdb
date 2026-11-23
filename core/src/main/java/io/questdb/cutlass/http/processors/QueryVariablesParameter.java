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

package io.questdb.cutlass.http.processors;

import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlParser;
import io.questdb.log.LogRecord;
import io.questdb.std.Mutable;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_VARIABLES;

/**
 * The {@code variables} parameter of {@code /exec}, {@code /exp} and
 * {@code /api/v1/sql/validate}: the assignments of a DECLARE block, which the query reads as a
 * DECLARE block that opens it, see
 * {@link io.questdb.griffin.SqlExecutionContext#getQueryVariables()}.
 * <p>
 * The parser reads the variables at {@link SqlParser#QUERY_VARIABLES_POSITION}, which no other
 * text the query reads reaches, so the position of an error tells whether the error lies in them.
 * {@link #locateError(int)} tells such an error apart and reports it at the position in the
 * parameter, with {@code "errorSource":"variables"}.
 */
final class QueryVariablesParameter implements Mutable {
    private final StringSink text = new StringSink();
    private boolean isErrorInVariables;

    @Override
    public void clear() {
        text.clear();
        isErrorInVariables = false;
    }

    /**
     * Returns the decoded parameter, empty without it.
     */
    CharSequence getText() {
        return text;
    }

    /**
     * Returns the position to report for an error at a position in what the query was compiled
     * from, and remembers whether the error lies in the variables. A position at or past
     * {@link SqlParser#QUERY_VARIABLES_POSITION} lies in them, and is reported as the position in
     * the parameter. Any other position is left as it is: one in the query's text, one in the
     * body of a view the query reads, the fixed one some runtime errors carry, or none.
     */
    int locateError(int position) {
        isErrorInVariables = text.length() > 0 && position >= SqlParser.QUERY_VARIABLES_POSITION;
        return isErrorInVariables ? position - SqlParser.QUERY_VARIABLES_POSITION : position;
    }

    /**
     * Records that the error lies in the parameter itself, such as an encoding error.
     */
    void markError() {
        isErrorInVariables = true;
    }

    /**
     * Decodes the parameter of the request. Without it, the query has no variables.
     */
    void of(HttpRequestHeader request) throws SqlException {
        clear();
        final DirectUtf8Sequence variables = request.getUrlParam(URL_PARAM_VARIABLES);
        if (variables != null && !Utf8s.utf8ToUtf16(variables.lo(), variables.hi(), text)) {
            text.clear();
            throw SqlException.$(0, "variables are not valid UTF-8");
        }
    }

    /**
     * Adds {@code "errorSource":"variables"} to an error object when the error lies in the
     * variables, see {@link #locateError(int)}.
     */
    void putErrorSource(Utf8Sink response) {
        if (isErrorInVariables) {
            response.putAscii(',').putAsciiQuoted("errorSource").putAscii(':').putAsciiQuoted("variables");
        }
    }

    /**
     * Adds {@code , vars=`...`} to a log record that shows the query's text, when the client
     * sent variables: the text alone does not tell what ran.
     */
    void putLog(LogRecord log) {
        if (text.length() > 0) {
            log.$(", vars=`").$safe(text).$('`');
        }
    }
}
