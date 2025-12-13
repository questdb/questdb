/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.fuzz.sql;

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

/**
 * Runtime context for SQL generation.
 * Tracks state during recursive query generation including depth, scope (CTEs, table aliases),
 * and provides helper methods for building SQL strings.
 * <p>
 * This class is mutable and designed to be reused across generation iterations by calling {@link #reset()}.
 * For recursive generation, use {@link #childContext()} to create a scoped context that can be discarded.
 */
public final class GeneratorContext {

    private final Rnd rnd;
    private final GeneratorConfig config;
    private final TokenizedQuery tokens;

    // Depth tracking
    private int queryDepth;
    private int expressionDepth;

    // Scope tracking - lists of names visible at current scope
    private final ObjList<String> cteNames;
    private final ObjList<String> tableAliases;
    private final ObjList<String> columnNames;

    // Flags for current context
    private boolean inAggregateContext;
    private boolean hasTimestampColumn;
    private boolean inWindowFunction;

    // Counters for synthetic name generation (per-query uniqueness)
    private int tableCounter;
    private int cteCounter;
    private int columnCounter;
    private int aliasCounter;

    /**
     * Creates a new context with the given random source and configuration.
     */
    public GeneratorContext(Rnd rnd, GeneratorConfig config) {
        this.rnd = rnd;
        this.config = config;
        this.tokens = new TokenizedQuery();
        this.cteNames = new ObjList<>();
        this.tableAliases = new ObjList<>();
        this.columnNames = new ObjList<>();
        reset();
    }

    /**
     * Creates a child context that shares the same Rnd and config but has its own scope.
     * Child context starts with a copy of current scope that can be modified independently.
     * Query depth is inherited and incremented.
     */
    public GeneratorContext childContext() {
        GeneratorContext child = new GeneratorContext(rnd, config);
        child.queryDepth = this.queryDepth + 1;
        child.expressionDepth = 0;
        // Copy current scope to child (child can see parent's definitions)
        child.cteNames.addAll(this.cteNames);
        child.tableAliases.addAll(this.tableAliases);
        child.columnNames.addAll(this.columnNames);
        child.hasTimestampColumn = this.hasTimestampColumn;
        // Counters continue from parent to ensure global uniqueness
        child.tableCounter = this.tableCounter;
        child.cteCounter = this.cteCounter;
        child.columnCounter = this.columnCounter;
        child.aliasCounter = this.aliasCounter;
        return child;
    }

    /**
     * Resets context for a new generation iteration.
     * Clears all state except for Rnd and config.
     */
    public void reset() {
        tokens.clear();
        queryDepth = 0;
        expressionDepth = 0;
        cteNames.clear();
        tableAliases.clear();
        columnNames.clear();
        inAggregateContext = false;
        hasTimestampColumn = false;
        inWindowFunction = false;
        tableCounter = 0;
        cteCounter = 0;
        columnCounter = 0;
        aliasCounter = 0;
    }

    // --- Accessors ---

    public Rnd rnd() {
        return rnd;
    }

    public GeneratorConfig config() {
        return config;
    }

    public TokenizedQuery tokens() {
        return tokens;
    }

    public int queryDepth() {
        return queryDepth;
    }

    public int expressionDepth() {
        return expressionDepth;
    }

    public ObjList<String> cteNames() {
        return cteNames;
    }

    public ObjList<String> tableAliases() {
        return tableAliases;
    }

    public ObjList<String> columnNames() {
        return columnNames;
    }

    public boolean inAggregateContext() {
        return inAggregateContext;
    }

    public boolean hasTimestampColumn() {
        return hasTimestampColumn;
    }

    public boolean inWindowFunction() {
        return inWindowFunction;
    }

    // --- Mutators ---

    public void setInAggregateContext(boolean value) {
        this.inAggregateContext = value;
    }

    public void setHasTimestampColumn(boolean value) {
        this.hasTimestampColumn = value;
    }

    public void setInWindowFunction(boolean value) {
        this.inWindowFunction = value;
    }

    public void incrementExpressionDepth() {
        expressionDepth++;
    }

    public void decrementExpressionDepth() {
        expressionDepth--;
    }

    // --- Scope management ---

    /**
     * Registers a CTE name in the current scope.
     */
    public void registerCte(String name) {
        cteNames.add(name);
    }

    /**
     * Registers a table alias in the current scope.
     */
    public void registerTableAlias(String alias) {
        tableAliases.add(alias);
    }

    /**
     * Registers a column name in the current scope.
     */
    public void registerColumn(String name) {
        columnNames.add(name);
    }

    /**
     * Clears table aliases (for new FROM clause).
     */
    public void clearTableAliases() {
        tableAliases.clear();
    }

    /**
     * Clears column names (for new SELECT).
     */
    public void clearColumns() {
        columnNames.clear();
    }

    // --- Name generation ---

    /**
     * Generates a new unique table name (t1, t2, ...).
     */
    public String newTableName() {
        return "t" + (++tableCounter);
    }

    /**
     * Generates a new unique CTE name (cte1, cte2, ...).
     */
    public String newCteName() {
        return "cte" + (++cteCounter);
    }

    /**
     * Generates a new unique column name (col1, col2, ...).
     */
    public String newColumnName() {
        return "col" + (++columnCounter);
    }

    /**
     * Generates a new unique alias (a1, a2, ...).
     */
    public String newAlias() {
        return "a" + (++aliasCounter);
    }

    /**
     * Returns a random table name from a small fixed set.
     * Used when we don't need uniqueness.
     */
    public String randomTableName() {
        return "t" + (1 + rnd.nextInt(5));
    }

    /**
     * Returns a random column name from a small fixed set,
     * occasionally returning QuestDB-specific column names (ts, sym).
     */
    public String randomColumnName() {
        if (rnd.nextDouble() < 0.2) {
            return rnd.nextBoolean() ? "ts" : "sym";
        }
        return "col" + (1 + rnd.nextInt(10));
    }

    /**
     * Returns a random column reference, optionally qualified with a table alias.
     */
    public String randomColumnReference() {
        String colName = randomColumnName();
        if (tableAliases.size() > 0 && rnd.nextDouble() < 0.5) {
            String alias = tableAliases.get(rnd.nextInt(tableAliases.size()));
            return alias + "." + colName;
        }
        return colName;
    }

    /**
     * Returns a random function name (func1, func2, ...).
     * Synthetic names are used initially; real function names can be added later.
     */
    public String randomFunctionName() {
        return "func" + (1 + rnd.nextInt(20));
    }

    /**
     * Returns a random CTE name from those registered, or null if none registered.
     */
    public String randomCteName() {
        if (cteNames.size() == 0) {
            return null;
        }
        return cteNames.get(rnd.nextInt(cteNames.size()));
    }

    /**
     * Returns a random table alias from those registered, or null if none registered.
     */
    public String randomTableAlias() {
        if (tableAliases.size() == 0) {
            return null;
        }
        return tableAliases.get(rnd.nextInt(tableAliases.size()));
    }

    // --- Recursion control ---

    /**
     * Returns true if we should recurse (add more nesting).
     * Probability decreases with depth.
     */
    public boolean shouldRecurseQuery() {
        if (queryDepth >= config.maxDepth()) {
            return false;
        }
        // Probability decreases exponentially with depth
        double prob = Math.pow(0.5, queryDepth);
        return rnd.nextDouble() < prob;
    }

    /**
     * Returns true if we should recurse in expressions.
     */
    public boolean shouldRecurseExpression() {
        if (expressionDepth >= config.maxExpressionDepth()) {
            return false;
        }
        double prob = Math.pow(0.5, expressionDepth);
        return rnd.nextDouble() < prob;
    }

    /**
     * Returns true if we can add more CTEs.
     */
    public boolean canAddCte() {
        return cteNames.size() < config.maxCteCount();
    }

    /**
     * Returns true if we can add more joins.
     */
    public boolean canAddJoin() {
        return tableAliases.size() <= config.maxJoins();
    }

    // --- Probability checks ---

    /**
     * Returns true with given probability.
     */
    public boolean withProbability(double prob) {
        return rnd.nextDouble() < prob;
    }

    /**
     * Returns a random integer in range [0, bound).
     */
    public int nextInt(int bound) {
        return rnd.nextInt(bound);
    }

    /**
     * Returns a random boolean.
     */
    public boolean nextBoolean() {
        return rnd.nextBoolean();
    }

    // --- Token building helpers ---

    /**
     * Appends a keyword token.
     */
    public GeneratorContext keyword(String kw) {
        tokens.addKeyword(kw);
        return this;
    }

    /**
     * Appends an identifier token.
     */
    public GeneratorContext identifier(String id) {
        tokens.addIdentifier(id);
        return this;
    }

    /**
     * Appends a literal token (number, string, etc.).
     */
    public GeneratorContext literal(String lit) {
        tokens.addLiteral(lit);
        return this;
    }

    /**
     * Appends an operator token.
     */
    public GeneratorContext operator(String op) {
        tokens.addOperator(op);
        return this;
    }

    /**
     * Appends a punctuation token.
     */
    public GeneratorContext punctuation(String p) {
        tokens.addPunctuation(p);
        return this;
    }

    /**
     * Appends an open parenthesis.
     */
    public GeneratorContext openParen() {
        return punctuation("(");
    }

    /**
     * Appends a close parenthesis.
     */
    public GeneratorContext closeParen() {
        return punctuation(")");
    }

    /**
     * Appends a comma.
     */
    public GeneratorContext comma() {
        return punctuation(",");
    }

    /**
     * Appends a semicolon.
     */
    public GeneratorContext semicolon() {
        return punctuation(";");
    }

    /**
     * Appends a dot.
     */
    public GeneratorContext dot() {
        return punctuation(".");
    }

    /**
     * Returns the generated SQL string.
     */
    public String toSql() {
        return tokens.serialize();
    }
}
