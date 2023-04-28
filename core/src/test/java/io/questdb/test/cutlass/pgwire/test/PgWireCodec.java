/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cutlass.pgwire.test;

import io.questdb.std.str.StringSink;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;

public class PgWireCodec implements Closeable {
    private static final byte[] USER = "user".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DB = "database".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CLIENT_ENCODING = "client_encoding".getBytes(StandardCharsets.UTF_8);
    private static final int PROT_VERSION = 196608;

    private final OutboundChannel out = new OutboundChannel();
    private final InboundChannel in = new InboundChannel();
    private final RowDescription rowDescription = new RowDescription();
    private final RowData rowData = new RowData();
    private final ParameterDescription parameterDescription = new ParameterDescription();
    private final CustomStartup customStartup = new CustomStartup();
    private final PgWireRecorder rec = new PgWireRecorder(1024 * 1024); // 1MB
    private final Bind bind = new Bind();

    public void clear() {
        rec.clear();
    }

    @Override
    public void close() {
        rec.close();
    }

    public OutboundChannel out() {
        rec.controlOut();
        return out;
    }

    public void dumpAndClear(StringSink sink) {
        rec.appendHexAndClear(sink);
        rec.clear();
    }

    public class Bind {
        public class ResultFormats {
            private short count;
            private long countOffset;

            private ResultFormats begin() {
                this.count = 0;
                this.countOffset = rec.offsetAndAdvanceShort();
                return this;
            }

            public ResultFormats add(short format) {
                rec.appendShortBE(format);
                count++;
                return this;
            }

            public ResultFormats addBin() {
                return add((short) 1);
            }

            public ResultFormats addText() {
                return add((short) 0);
            }

            public Bind doneResultFormats() {
                rec.setShortBE(count, countOffset);
                return Bind.this;
            }
        }

        public class ParameterValues {
            private short count;
            private long countOffset;

            private ParameterValues begin() {
                this.count = 0;
                this.countOffset = rec.offsetAndAdvanceShort();
                return this;
            }

            public ParameterValues addNull() {
                rec.appendIntBE(-1);
                count++;
                return this;
            }

            public ParameterValues addInt4bin(int value) {
                rec.appendIntBE(4);
                rec.appendIntBE(value);
                count++;
                return this;
            }

            public Bind doneParamValues() {
                rec.setShortBE(count, countOffset);
                return Bind.this;
            }
        }

        public class ParameterFormat {
            private short count;
            private long countOffset;

            private ParameterFormat begin() {
                this.count = 0;
                this.countOffset = rec.offsetAndAdvanceShort();
                return this;
            }

            public ParameterFormat add(short format) {
                rec.appendShortBE(format);
                count++;
                return this;
            }

            public ParameterFormat addBin() {
                return add((short) 1);
            }

            public ParameterFormat addText() {
                return add((short) 0);
            }

            public Bind doneParamFormats() {
                rec.setShortBE(count, countOffset);
                return Bind.this;
            }
        }

        private long startOffset;
        private final ParameterFormat paramFormatCodes = new ParameterFormat();
        private final ParameterValues paramValues = new ParameterValues();

        private Bind begin(String portalName, String statementName) {
            assert portalName != null;
            assert statementName != null;
            rec.appendByte((byte) 'B');
            startOffset = rec.offsetAndAdvanceInt();
            rec.appendAsciiZ(portalName);
            rec.appendAsciiZ(statementName);
            return this;
        }

        public ParameterFormat paramFormats() {
            return paramFormatCodes.begin();
        }

        public ParameterValues paramValues() {
            return paramValues.begin();
        }

        public ResultFormats resultFormats() {
            return new ResultFormats().begin();
        }

        public OutboundChannel bindDone() {
            rec.setMsgSize(startOffset);
            return out;
        }
    }

    public class CustomStartup {
        private long startOffset;

        public CustomStartup begin() {
            startOffset = rec.offsetAndAdvanceInt();
            rec.appendIntBE(PROT_VERSION);
            return this;
        }

        public CustomStartup addParam(String key, String value) {
            rec.appendAsciiZ(key);
            rec.appendAsciiZ(value);
            return this;
        }

        public OutboundChannel startupDone() {
            rec.appendByte((byte) 0);
            rec.setMsgSize(startOffset);
            return out;
        }
    }

    public class ParameterDescription {
        private long startOffset;
        private long countOffset;
        private short count;

        public ParameterDescription begin() {
            rec.appendByte((byte) 't');
            startOffset = rec.offsetAndAdvanceInt();
            countOffset = rec.offsetAndAdvanceShort();
            count = 0;
            return this;
        }

        public ParameterDescription int8Column() {
            rec.appendIntBE(20); // int8
            count++;
            return this;
        }

        public ParameterDescription int4Column() {
            rec.appendIntBE(23); // int4
            count++;
            return this;
        }

        public ParameterDescription varcharColumn() {
            rec.appendIntBE(1043); // varchar
            count++;
            return this;
        }

        public ParameterDescription customColumn(int oid) {
            rec.appendIntBE(oid);
            count++;
            return this;
        }

        public InboundChannel parameterDescriptionDone() {
            rec.setMsgSize(startOffset);
            rec.setShortBE(count, countOffset);
            return in;
        }
    }

    public class RowData {
        private short columnCount;
        private long startOffset;
        private long columnCountOffset;

        private RowData begin() {
            columnCount = 0;
            rec.appendByte((byte) 'D');
            startOffset = rec.offsetAndAdvanceInt();
            columnCountOffset = rec.offsetAndAdvanceShort();
            return this;
        }

        public RowData int8ColumnText(long value) {
            String valueString = String.valueOf(value);
            return varcharColumn(valueString);
        }

        public RowData int4ColumnText(int value) {
            String valueString = String.valueOf(value);
            return varcharColumn(valueString);
        }

        public RowData boolColumnBin(boolean value) {
            rec.appendIntBE(1);
            rec.appendByte((byte) (value ? 1 : 0));
            columnCount++;
            return this;
        }

        public RowData varcharColumn(String value) {
            if (value == null) {
                rec.appendIntBE(-1);
                columnCount++;
                return this;
            }
            rec.appendAsciiIntSizePrefix(value);
            columnCount++;
            return this;
        }

        public InboundChannel rowDataDone() {
            rec.setMsgSize(startOffset);
            rec.setShortBE(columnCount, columnCountOffset);
            return in;
        }
    }

    public class RowDescription {
        private short columnCount;
        private long startOffset;
        private long columnCountOffset;

        private RowDescription begin() {
            rec.appendByte((byte) 'T');
            columnCount = 0;
            startOffset = rec.offsetAndAdvanceInt();
            columnCountOffset = rec.offsetAndAdvanceShort();
            return this;
        }

        public RowDescription int8ColumnText(String name) {
            return columnText(name, 20, (short) 8);
        }

        public RowDescription int8ColumnBin(String name) {
            return columnBin(name, 20, (short) 8);
        }

        public RowDescription int4ColumnText(String name) {
            return columnText(name, 23, (short) 4);
        }

        public RowDescription int4ColumnBin(String name) {
            return columnBin(name, 23, (short) 4);
        }

        public RowDescription boolColumnText(String name) {
            return columnText(name, 16, (short) 1);
        }

        public RowDescription boolColumnBin(String name) {
            return columnBin(name, 16, (short) 1);
        }

        public RowDescription varcharColumnText(String name) {
            return columnText(name, 1043, (short) -1);
        }

        public RowDescription columnText(String name, int oid, short size) {
            return column(name, oid, size, 0, -1, (short) 0);
        }

        public RowDescription columnBin(String name, int oid, short size) {
            return column(name, oid, size, 0, -1, (short) 1);
        }

        public RowDescription column(String name, int oid, short size, int tableId, int typeModifier, short formatCode) {
            columnCount++;
            rec.appendAsciiZ(name);
            rec.appendIntBE(tableId);
            rec.appendShortBE(columnCount);
            rec.appendIntBE(oid);
            rec.appendShortBE(size);
            rec.appendIntBE(typeModifier);
            rec.appendShortBE(formatCode);
            return this;
        }

        public InboundChannel rowDescriptionDone() {
            rec.setMsgSize(startOffset);
            rec.setShortBE(columnCount, columnCountOffset);
            return in;
        }
    }

    public class InboundChannel {

        public InboundChannel sslDeclined() {
            rec.appendByte((byte) 'N');
            return this;
        }

        public InboundChannel closeComplete() {
            rec.appendByte((byte) '3');
            rec.appendIntBE(4);
            return this;
        }

        public InboundChannel commandCompletion(String tag) {
            rec.appendByte((byte) 'C');
            long startOffset = rec.offsetAndAdvanceInt();
            rec.appendAsciiZ(tag);
            rec.setMsgSize(startOffset);
            return this;
        }

        public InboundChannel authenticationCleartextPassword() {
            rec.appendByte((byte) 'R');
            rec.appendIntBE(8);
            rec.appendIntBE(3);
            return this;
        }

        public InboundChannel authenticationOk() {
            rec.appendByte((byte) 'R');
            rec.appendIntBE(8);
            rec.appendIntBE(0);
            return this;
        }

        public InboundChannel backendKeyData(int processId, int secretKey) {
            rec.appendByte((byte) 'K');
            rec.appendIntBE(12);
            rec.appendIntBE(processId);
            rec.appendIntBE(secretKey);
            return this;
        }

        public InboundChannel parameterStatus(String name, String value) {
            rec.appendByte((byte) 'S');
            long startOffset = rec.offsetAndAdvanceInt();
            rec.appendAsciiZ(name);
            rec.appendAsciiZ(value);
            rec.setMsgSize(startOffset);
            return this;
        }

        public InboundChannel readyForQuery_idle() {
            rec.appendByte((byte) 'Z');
            rec.appendIntBE(5);
            rec.appendByte((byte) 'I');
            return this;
        }

        public RowDescription rowDescription() {
            return rowDescription.begin();
        }

        public RowData rowData() {
            return rowData.begin();
        }

        public InboundChannel parseComplete() {
            rec.appendByte((byte) '1');
            rec.appendIntBE(4);
            return in;
        }

        public InboundChannel bindComplete() {
            rec.appendByte((byte) '2');
            rec.appendIntBE(4);
            return in;
        }

        public ParameterDescription parameterDescription() {
            return parameterDescription.begin();
        }

        public OutboundChannel out() {
            rec.controlOut();
            return out;
        }

        public InboundChannel newLine() {
            rec.controlIn();
            return in;
        }

        public InboundChannel error(int position, String message) {
            rec.appendByte((byte) 'E');
            long startOffset = rec.offsetAndAdvanceInt();
            rec.appendByte((byte) 'C'); // SQLSTATE. QuestDB always returns 00000
            rec.appendAsciiZ("00000");
            rec.appendByte((byte) 'M'); // Message
            rec.appendAsciiZ(message);
            rec.appendByte((byte) 'S'); // Severity
            rec.appendAsciiZ("ERROR");
            if (position > 0) {
                rec.appendByte((byte) 'P'); // Position
                rec.appendAsciiZ(Integer.toString(position));
            }
            rec.appendByte((byte) 0); // no more fields
            rec.setMsgSize(startOffset);
            return this;
        }

        public void dumpAndClear(StringSink stringSink) {
            rec.appendHexAndClear(stringSink);
            rec.clear();
        }
    }

    public class OutboundChannel {

        public void dumpAndClear(StringSink sink) {
            rec.appendHexAndClear(sink);
            rec.clear();
        }

        public OutboundChannel password(String password) {
            rec.appendByte((byte) 'p');
            long startOffset = rec.offsetAndAdvanceInt();
            rec.appendAsciiZ(password);
            rec.setMsgSize(startOffset);
            return this;
        }

        public OutboundChannel parse(String statementName, String query, int... paramTypes) {
            rec.appendByte((byte) 'P');
            long startOffset = rec.offsetAndAdvanceInt();
            rec.appendAsciiZ(statementName);
            rec.appendAsciiZ(query);
            rec.appendShortBE((short) paramTypes.length);
            for (int paramType : paramTypes) {
                rec.appendIntBE(paramType);
            }
            rec.setMsgSize(startOffset);
            return this;
        }

        public OutboundChannel sync() {
            rec.appendByte((byte) 'S');
            rec.appendIntBE(4);
            return this;
        }

        public OutboundChannel sslRequest() {
            rec.appendIntBE(8);
            rec.appendIntBE(80877103);
            return out;
        }

        public OutboundChannel execute(String portalName, int maxRows) {
            rec.appendByte((byte) 'E');
            long startOffset = rec.offsetAndAdvanceInt();
            rec.appendAsciiZ(portalName);
            rec.appendIntBE(maxRows);
            rec.setMsgSize(startOffset);
            return this;
        }

        public OutboundChannel execute() {
            return execute("", 0);
        }

        public OutboundChannel closePortal(String portalName) {
            rec.appendByte((byte) 'C');
            long startOffset = rec.offsetAndAdvanceInt();
            rec.appendByte((byte) 'P');
            rec.appendAsciiZ(portalName);
            rec.setMsgSize(startOffset);
            return this;
        }

        public OutboundChannel terminate() {
            rec.appendByte((byte) 'X');
            rec.appendIntBE(4);
            return this;
        }

        public OutboundChannel closePortal() {
            return closePortal("");
        }

        public OutboundChannel closeStatement(String statementName) {
            rec.appendByte((byte) 'C');
            long startOffset = rec.offsetAndAdvanceInt();
            rec.appendByte((byte) 'S');
            rec.appendAsciiZ(statementName);
            rec.setMsgSize(startOffset);
            return this;
        }

        public OutboundChannel closeStatement() {
            return closeStatement("");
        }

        public Bind bind(String portalName, String statementName) {
            return bind.begin(portalName, statementName);
        }

        public OutboundChannel describeStatement(String statementName) {
            rec.appendByte((byte) 'D');
            long startOffset = rec.offsetAndAdvanceInt();
            rec.appendByte((byte) 'S');
            rec.appendAsciiZ(statementName);
            rec.setMsgSize(startOffset);
            return this;
        }

        public OutboundChannel describePortal(String portalName) {
            rec.appendByte((byte) 'D');
            long startOffset = rec.offsetAndAdvanceInt();
            rec.appendByte((byte) 'P');
            rec.appendAsciiZ(portalName);
            rec.setMsgSize(startOffset);
            return this;
        }

        public OutboundChannel flush() {
            rec.appendByte((byte) 'H');
            rec.appendIntBE(4);
            return this;
        }

        public OutboundChannel newLine() {
            rec.controlOut();
            return this;
        }

        public OutboundChannel startup(String username, String database) {
            long startOffset = rec.offsetAndAdvanceInt();
            rec.appendIntBE(PROT_VERSION);
            rec.appendBytesZ(USER);
            rec.appendAsciiZ(username);
            rec.appendBytesZ(DB);
            rec.appendAsciiZ(database);
            rec.setMsgSize(startOffset);
            return this;
        }

        public CustomStartup startup() {
            return customStartup.begin();
        }

        public OutboundChannel startup(String username, String database, String clientEncoding) {
            long startOffset = rec.offsetAndAdvanceInt();
            rec.appendIntBE(PROT_VERSION);
            rec.appendBytesZ(CLIENT_ENCODING);
            rec.appendAsciiZ(clientEncoding);
            rec.appendBytesZ(USER);
            rec.appendAsciiZ(username);
            rec.appendBytesZ(DB);
            rec.appendAsciiZ(database);
            rec.appendByte((byte) 0);
            rec.setMsgSize(startOffset);
            return this;
        }

        public InboundChannel in() {
            rec.controlIn();
            return in;
        }

        public OutboundChannel simpleQuery(String query) {
            rec.appendByte((byte) 'Q');
            long startOffset = rec.offsetAndAdvanceInt();
            rec.appendAsciiZ(query);
            rec.setMsgSize(startOffset);
            return this;
        }
    }
}
