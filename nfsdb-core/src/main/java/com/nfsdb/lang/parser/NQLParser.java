/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.lang.parser;

import com.nfsdb.PartitionType;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.factory.configuration.GenericIntBuilder;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StdoutSink;
import com.nfsdb.lang.ast.QueryModel;
import com.nfsdb.lang.ast.Statement;
import com.nfsdb.lang.ast.StatementType;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordSource;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Numbers;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class NQLParser {

    private final TokenStream tokenStream = new TokenStream() {{
        defineSymbol(" ");
        defineSymbol("(");
        defineSymbol(")");
        defineSymbol(",");
        defineSymbol("+");
    }};

    public static void main(String[] args) throws ParserException, JournalException {
        NQLParser parser = new NQLParser();
        QueryExecutor executor = new QueryExecutor(new JournalFactory("/Users/vlad/dev/data"));
//        executor.execute(parser.parse("create journal xyz (a INT index buckets 250, b DOUBLE)  partition by MONTH"));
        RecordSourcePrinter p = new RecordSourcePrinter(new StdoutSink());
        parser.setContent("select vendorId, medallion, pickupLongitude from trip_data_1.csv");
        RecordSource<? extends Record> src = executor.execute(parser.parse());
        if (src != null) {
            p.print(src);
        }

        System.out.println("ok");
    }

    public Statement parse() throws ParserException {
        CharSequence tok = tok();
        if (Chars.equals(tok, "create")) {
            return parseCreateStatement();
        }

        if (Chars.equals(tok, "select")) {
            return parseQuery();
        }

        throw err("create | select expected");
    }

    public void setContent(CharSequence cs) {
        tokenStream.setContent(cs);
    }

    private ParserException err(String msg) {
        return new ParserException(tokenStream.position(), msg);
    }

    private void expectTok(CharSequence tok, CharSequence expected) throws ParserException {
        if (!Chars.equals(tok, expected)) {
            throw err("\"" + expected + "\" expected");
        }

    }

    private boolean isFieldTerm(CharSequence tok) {
        return Chars.equals(tok, ')') || Chars.equals(tok, ',');
    }

    private String notTermTok() throws ParserException {
        CharSequence tok = tok();
        if (isFieldTerm(tok)) {
            throw err("Invalid column definition");
        }
        return tok.toString();
    }

    private CharSequence optionTok() {
        while (tokenStream.hasNext()) {
            CharSequence cs = tokenStream.next();
            if (!Chars.equals(cs, ' ')) {
                return cs;
            }
        }
        return null;
    }

    private Statement parseCreateJournal() throws ParserException {
        JournalStructure structure = new JournalStructure(tok().toString());
        parseJournalFields(structure);
        CharSequence tok = optionTok();
        if (tok != null) {
            expectTok(tok, "partition");
            expectTok(tok(), "by");
            structure.partitionBy(PartitionType.valueOf(tok().toString()));
        }
        return new Statement(StatementType.CREATE_JOURNAL, structure);
    }

    private Statement parseCreateStatement() throws ParserException {
        CharSequence tok = tok();
        if (Chars.equals(tok, "journal")) {
            return parseCreateJournal();
        }

        throw err("journal expected");
    }

    @SuppressFBWarnings({"LEST_LOST_EXCEPTION_STACK_TRACE"})
    private CharSequence parseIntDefinition(GenericIntBuilder genericIntBuilder) throws ParserException {
        CharSequence tok = tok();

        if (isFieldTerm(tok)) {
            return tok;
        }

        expectTok(tok, "index");
        genericIntBuilder.index();

        if (isFieldTerm(tok = tok())) {
            return tok;
        }

        expectTok(tok, "buckets");

        try {
            genericIntBuilder.buckets(Numbers.parseInt(tok()));
        } catch (NumberFormatException e) {
            throw err("expected number of buckets (int)");
        }

        return null;
    }

    private void parseJournalFields(JournalStructure struct) throws ParserException {
        if (!Chars.equals(tok(), '(')) {
            throw err("( expected");
        }

        while (true) {
            String name = notTermTok();
            CharSequence tok = null;
            switch (ColumnType.valueOf(notTermTok())) {
                case INT:
                    tok = parseIntDefinition(struct.$int(name));
                    break;
                case DOUBLE:
                    struct.$double(name);
                    break;
                case BOOLEAN:
                    struct.$bool(name);
                    break;
                case FLOAT:
                    struct.$float(name);
                    break;
                case LONG:
                    struct.$long(name);
                    break;
                case SHORT:
                    struct.$short(name);
                    break;
                case STRING:
                    struct.$str(name);
                    break;
                case SYMBOL:
                    struct.$sym(name);
                    break;
                case BINARY:
                    struct.$bin(name);
                    break;
                case DATE:
                    struct.$date(name);
                    break;
                default:
                    throw new ParserException(tokenStream.position(), "Unsupported type");
            }

            if (tok == null) {
                tok = tok();
            }

            if (Chars.equals(tok, ')')) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err(", or ) expected");
            }
        }
    }

    private Statement parseQuery() throws ParserException {

        QueryModel model = new QueryModel();
        while (true) {
            model.addColumn(tok().toString());
            CharSequence tok = tok();
            if (Chars.equals(tok, "from")) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err(", expected");
            }
        }

        model.setJournalName(tok().toString());

        return new Statement(StatementType.QUERY_JOURNAL, model);

    }

    private CharSequence tok() throws ParserException {
        CharSequence tok = tokenStream.optionTok();
        if (tok == null) {
            throw err("Unexpected end of input");
        }
        return tok;
    }
}
