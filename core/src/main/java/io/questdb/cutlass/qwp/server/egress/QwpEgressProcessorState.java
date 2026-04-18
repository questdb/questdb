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

package io.questdb.cutlass.qwp.server.egress;

import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.ConnectionAware;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * Per-connection state for QWP egress (query results) processing.
 * <p>
 * Holds the reusable scratch objects for the per-connection processing loop:
 * inbound decoder, outbound result-batch accumulator, schema registry, bind
 * variable service, and column-definition pool. Each is allocated once and
 * reused across queries on the same connection.
 */
public class QwpEgressProcessorState implements QuietCloseable, ConnectionAware {

    private final QwpResultBatchBuffer batchBuffer = new QwpResultBatchBuffer();
    private final BindVariableServiceImpl bindVariableService;
    private final ObjList<QwpEgressColumnDef> columnDefsPool = new ObjList<>();
    private final QwpEgressRequestDecoder decoder = new QwpEgressRequestDecoder();
    private long fd = -1;
    private byte negotiatedVersion = QwpConstants.VERSION_1;
    private int nextSchemaId;
    private int recvBufferLen;
    private SecurityContext securityContext;
    private boolean wsHandshakeSent;

    public QwpEgressProcessorState(io.questdb.cairo.CairoConfiguration cairoConfiguration) {
        this.bindVariableService = new BindVariableServiceImpl(cairoConfiguration);
    }

    /**
     * Returns the next unused schema id on this connection and advances the counter.
     */
    public int allocateSchemaId() {
        return nextSchemaId++;
    }

    /**
     * Returns (or extends+returns) a column-definition slot. The pool grows to at
     * least {@code requiredSize}; caller re-populates each slot via {@link QwpEgressColumnDef#of}.
     */
    public ObjList<QwpEgressColumnDef> borrowColumnDefs(int requiredSize) {
        int currentPos = columnDefsPool.size();
        if (requiredSize > currentPos) {
            columnDefsPool.setPos(requiredSize);
            for (int i = currentPos; i < requiredSize; i++) {
                if (columnDefsPool.getQuick(i) == null) {
                    columnDefsPool.setQuick(i, new QwpEgressColumnDef());
                }
            }
        } else {
            columnDefsPool.setPos(requiredSize);
        }
        return columnDefsPool;
    }

    public void clear() {
        recvBufferLen = 0;
        wsHandshakeSent = false;
        fd = -1;
        securityContext = null;
        negotiatedVersion = QwpConstants.VERSION_1;
        nextSchemaId = 0;
        batchBuffer.reset();
        bindVariableService.clear();
    }

    @Override
    public void close() {
        clear();
        Misc.free(batchBuffer);
    }

    public QwpResultBatchBuffer getBatchBuffer() {
        return batchBuffer;
    }

    public BindVariableServiceImpl getBindVariableService() {
        return bindVariableService;
    }

    public QwpEgressRequestDecoder getDecoder() {
        return decoder;
    }

    public long getFd() {
        return fd;
    }

    public byte getNegotiatedVersion() {
        return negotiatedVersion;
    }

    public int getRecvBufferLen() {
        return recvBufferLen;
    }

    public SecurityContext getSecurityContext() {
        return securityContext;
    }

    public boolean isWsHandshakeSent() {
        return wsHandshakeSent;
    }

    public void of(long fd, SecurityContext securityContext) {
        this.fd = fd;
        this.securityContext = securityContext;
    }

    public void onDisconnected() {
        clear();
    }

    public void setNegotiatedVersion(byte negotiatedVersion) {
        this.negotiatedVersion = negotiatedVersion;
    }

    public void setRecvBufferLen(int recvBufferLen) {
        this.recvBufferLen = recvBufferLen;
    }

    public void setWsHandshakeSent(boolean wsHandshakeSent) {
        this.wsHandshakeSent = wsHandshakeSent;
    }
}
