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

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

/**
 * Writes explicit column names into the SQL of a materialized view that relies on generated column names, or calls
 * a zero-argument function without parentheses, as the SQL of views created before CREATE MATERIALIZED VIEW
 * required explicit names can. SHOW CREATE prints the result, so that its output replays under those rules and
 * reproduces the view's columns. {@link SqlCompilerImpl#addExplicitColumnNames} compiles the SQL; this class reads
 * the parsed query, records the columns that may need a name, and edits the text.
 * <p>
 * A column of a select model of the SQL's own text needs a name when its name is generated: a computed column, a
 * plain column reference that another column renamed, or one whose column name is longer than the default
 * generated name size, see {@link SqlParser#isNamedAfterReferencedColumn}. So does a bare name that the optimiser
 * reads as a call to a zero-argument function, which also gets its parentheses. A column of the top-level select
 * list takes the name of the view column it feeds by position. A column of a model below takes the name of the
 * view column of the same output name, which a wildcard of the top-level select list may bring it to, and otherwise
 * keeps its generated name, which the select models above it reference. When a guess is wrong, the compile of the
 * result, see {@link SqlCompilerImpl#addExplicitColumnNames}, finds that it does not reproduce the view's columns.
 * A materialized view's SQL has no top-level UNION: CREATE requires the designated timestamp, which a UNION does not
 * produce.
 */
final class MatViewExplicitColumnNames implements Mutable {
    // Of each column that may need a name: the position of its expression's AST, where the optimiser reports a
    // bare call, where its expression ends, the name the parser generated, whether that name depends on the
    // server version or settings, and whether the column is one of the top-level select list.
    private final IntList astPositions = new IntList();
    // The edits, as (lo, hi, text index) triples over the SQL text: each replaces [lo, hi) with its text.
    private final IntList edits = new IntList();
    private final ObjList<String> editTexts = new ObjList<>();
    private final IntList exprEnds = new IntList();
    private final ObjList<String> generatedNames = new ObjList<>();
    private final IntList isNameGenerated = new IntList();
    private final IntList isTopLevel = new IntList();
    private final StringSink sink = new StringSink();
    private boolean hasTopLevelWildcard;

    @Override
    public void clear() {
        astPositions.clear();
        edits.clear();
        editTexts.clear();
        exprEnds.clear();
        generatedNames.clear();
        isNameGenerated.clear();
        isTopLevel.clear();
        sink.clear();
        hasTopLevelWildcard = false;
    }

    // Writes name as an identifier SQL reads back as the same name: as it is when it is a plain identifier and not
    // a keyword, and double-quoted otherwise. No column name has a double quote, and a generated name with one,
    // such as that of a string constant, makes a quoted identifier that the compile of the result rejects.
    static void putIdentifier(StringSink sink, CharSequence name) {
        if (isPlainIdentifier(name) && !SqlKeywords.isKeyword(name)) {
            sink.put(name);
        } else {
            sink.putAscii('"').put(name).putAscii('"');
        }
    }

    /**
     * Records the columns of a parsed query that may need an explicit name. The parse has to record where the
     * columns' expressions end, see {@link SqlParser#setRecordingColumnExprEnds(boolean)}.
     */
    void collect(IQueryModel query) {
        clear();
        collect(query, true);
    }

    /**
     * Returns the SQL with the explicit names, or null when it needs none, or when the compiled query produces
     * another number of columns than the view has, so that the columns cannot be told apart by position.
     *
     * @param sql             the SQL that {@link #collect(IQueryModel)} read the parse of
     * @param output          the metadata of the compiled SQL
     * @param viewColumnNames the names of the view's columns, in the order the view stores them
     * @param callNames       the bare names the optimiser read as calls to zero-argument functions
     * @param callPositions   where those bare names are in the SQL
     */
    @Nullable String edit(
            CharSequence sql,
            RecordMetadata output,
            ObjList<CharSequence> viewColumnNames,
            ObjList<String> callNames,
            IntList callPositions
    ) {
        if (output.getColumnCount() != viewColumnNames.size()) {
            return null;
        }
        edits.clear();
        editTexts.clear();
        for (int i = 0, n = callPositions.size(); i < n; i++) {
            // The optimiser reports where the parse found the bare name, which is the name's token, plain or
            // double-quoted. The call replaces the token: a quoted name loses its quotes, as a function name.
            final int pos = callPositions.getQuick(i);
            final String name = callNames.getQuick(i);
            addEdit(pos, pos + name.length() + (sql.charAt(pos) == '"' ? 2 : 0), name + "()");
        }
        for (int i = 0, n = exprEnds.size(); i < n; i++) {
            if (isNameGenerated.getQuick(i) == 0 && !callPositions.contains(astPositions.getQuick(i))) {
                continue;
            }
            sink.clear();
            sink.putAscii(" AS ");
            putIdentifier(sink, chooseName(i, output, viewColumnNames));
            addEdit(exprEnds.getQuick(i), exprEnds.getQuick(i), sink.toString());
        }
        if (edits.size() == 0) {
            return null;
        }
        edits.sortGroups(3);

        sink.clear();
        int copied = 0;
        for (int i = 0, n = edits.size(); i < n; i += 3) {
            final int lo = edits.getQuick(i);
            final int hi = edits.getQuick(i + 1);
            if (i > 0 && lo == edits.getQuick(i - 3)) {
                // The parse read the same text twice, as for a declared sub-query that the query reads twice: a
                // call and a name never start at the same position, so the edits are the same.
                continue;
            }
            // A call replaces one name token, and a name goes where a column's expression ends, so no edit
            // starts inside another: a column's expression never ends inside a token.
            assert lo >= copied;
            sink.put(sql, copied, lo);
            sink.put(editTexts.getQuick(edits.getQuick(i + 2)));
            copied = hi;
        }
        sink.put(sql, copied, sql.length());
        return sink.toString();
    }

    private static boolean isPlainIdentifier(CharSequence name) {
        for (int i = 0, n = name.length(); i < n; i++) {
            final char c = name.charAt(i);
            final boolean isLetter = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
            if (!isLetter && (i == 0 || c < '0' || c > '9')) {
                return false;
            }
        }
        // a column name is never empty
        return true;
    }

    private void addEdit(int lo, int hi, String text) {
        edits.add(lo);
        edits.add(hi);
        edits.add(editTexts.size());
        editTexts.add(text);
    }

    // Returns the name column i takes, see the class comment.
    private CharSequence chooseName(int i, RecordMetadata output, ObjList<CharSequence> viewColumnNames) {
        final String generatedName = generatedNames.getQuick(i);
        if (isTopLevel.getQuick(i) == 1) {
            // The output names its columns after the top-level select list. A wildcard that brings up a column
            // of the same name takes the name in the column's stead, never away from both.
            final int index = SqlUtil.getColumnIndexQuiet(output, generatedName);
            assert index > -1;
            return viewColumnNames.getQuick(index);
        }
        if (hasTopLevelWildcard) {
            final int index = SqlUtil.getColumnIndexQuiet(output, generatedName);
            if (index > -1) {
                return viewColumnNames.getQuick(index);
            }
        }
        return generatedName;
    }

    private void collect(@Nullable IQueryModel model, boolean isTopLevelModel) {
        for (IQueryModel m = model; m != null; m = m.getNestedModel()) {
            if (m.getViewNameExpr() != null) {
                // a regular view's text, which the SQL cannot change
                return;
            }
            final ObjList<QueryColumn> columns = m.getBottomUpColumns();
            for (int i = 0, n = columns.size(); i < n; i++) {
                final QueryColumn column = columns.getQuick(i);
                final ExpressionNode ast = column.getAst();
                if (ast.isWildcard()) {
                    hasTopLevelWildcard |= isTopLevelModel;
                    continue;
                }
                if (column.getAliasPosition() != QueryColumn.SYNTHESIZED_ALIAS_POSITION) {
                    continue;
                }
                final CharSequence alias = column.getAlias();
                astPositions.add(ast.position);
                exprEnds.add(column.getExprEndPosition());
                generatedNames.add(Chars.toString(alias));
                isNameGenerated.add(ast.type != ExpressionNode.LITERAL || !SqlParser.isNamedAfterReferencedColumn(alias, ast.token) ? 1 : 0);
                isTopLevel.add(isTopLevelModel ? 1 : 0);
            }

            final ObjList<IQueryModel> joinModels = m.getJoinModels();
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                final IQueryModel joinModel = joinModels.getQuick(i);
                if (joinModel != m) {
                    collect(joinModel, false);
                }
            }
            collect(m.getUnionModel(), false);
            isTopLevelModel = false;
        }
    }
}
