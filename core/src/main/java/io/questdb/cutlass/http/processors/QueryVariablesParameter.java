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
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
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
 * The parser reads the variables after the query's text and a newline, so the position of an
 * error in them lies past the query's text. {@link #locateError(int, int)} tells such an error
 * apart and reports it at the position in the parameter, with {@code "errorSource":"variables"}.
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
     * from, and remembers whether the error lies in the variables. A position past the query's
     * text and within the variables is reported as the position in the parameter. A position
     * past the variables is no position in either text, such as the fixed one some runtime
     * errors carry, and is left as it is.
     */
    int locateError(int position, int queryLength) {
        isErrorInVariables = false;
        final int offset = position - queryLength - 1;
        if (position == Numbers.INT_NULL || text.length() == 0 || offset < 0 || offset > text.length()) {
            return position;
        }
        isErrorInVariables = true;
        return offset;
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
     * variables, see {@link #locateError(int, int)}.
     */
    void putErrorSource(Utf8Sink response) {
        if (isErrorInVariables) {
            response.putAscii(',').putAsciiQuoted("errorSource").putAscii(':').putAsciiQuoted("variables");
        }
    }
}
