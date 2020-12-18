package io.questdb.cairo.replication;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.OnePageMemory;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectCharSequence;
import io.questdb.std.str.Path;

public class MasterReplicationService {
    private static final Log LOG = LogFactory.getLog(MasterReplicationService.class);
    private final NetworkFacade nf;
    private final FilesFacade ff;
    private final CairoEngine engine;
    private final CharSequence root;

    public MasterReplicationService(NetworkFacade nf, FilesFacade ff, CharSequence root, CairoEngine engine, MasterReplicationConfiguration masterReplicationConf, WorkerPool workerPool) {
        this.nf = nf;
        this.ff = ff;
        this.engine = engine;
        this.root = root;

        int sz = masterReplicationConf.masterIps.size();
        LongList listenFds = new LongList(sz);
        for (int n = 0; n < sz; n++) {
            long listenFd = nf.socketTcp(true);
            CharSequence ipv4Address = masterReplicationConf.masterIps.get(n);
            int port = masterReplicationConf.masterPorts.get(n);
            if (!nf.bindTcp(listenFd, ipv4Address, port)) {
                LOG.error().$("replication master failed to bind to ").$(ipv4Address).$(':').$(port).$();
                nf.close(listenFd);
                continue;
            }
            nf.listen(listenFd, masterReplicationConf.backlog);
            listenFds.add(listenFd);
            nf.configureNonBlocking(listenFd);
            LOG.info().$("replication master listening on ").$(ipv4Address).$(':').$(port).$(" [fd=").$(listenFd).$(']').$();
        }
        ReplicationMasterControllerJob job = new ReplicationMasterControllerJob(listenFds);
        workerPool.assign(job);
        workerPool.assign(0, job::close);
    }

    public static class MasterReplicationConfiguration {
        private ObjList<CharSequence> masterIps;
        private IntList masterPorts;
        private int backlog;

        public MasterReplicationConfiguration(ObjList<CharSequence> masterIps, IntList masterPorts, int backlog) {
            super();
            this.masterIps = masterIps;
            this.masterPorts = masterPorts;
            this.backlog = backlog;
        }
    }

    private class ReplicationMasterControllerJob extends SynchronizedJob {
        private final Path path = new Path();
        private final DirectCharSequence charSeq = new DirectCharSequence();
        private final OnePageMemory tempMem = new OnePageMemory();
        private LongList listenFds;
        private final IntObjHashMap<CharSequence> tableNameById;
        private ObjList<InitialMasterConnection> idleConnections;
        private ObjList<InitialMasterConnection> connectionCache;
        private boolean busy;

        private ReplicationMasterControllerJob(LongList listenFds) {
            this.listenFds = listenFds;
            tableNameById = new IntObjHashMap<>();
            idleConnections = new ObjList<>();
            connectionCache = new ObjList<>();
        }

        @Override
        protected boolean runSerially() {
            busy = false;
            if (null != listenFds) {
                acceptConnections();
                handleConnections();
            }
            return busy;
        }

        private void acceptConnections() {
            for (int n = 0, sz = listenFds.size(); n < sz; n++) {
                long listenFd = listenFds.get(n);
                long clientFd = nf.accept(listenFd);
                if (clientFd < 0) {
                    if (nf.errno() != Net.EWOULDBLOCK) {
                        LOG.error().$("could not accept [ret=").$(clientFd).$(", errno=").$(nf.errno()).$(']').$();
                    }
                    continue;
                }

                busy = true;
                InitialMasterConnection connection;
                if (connectionCache.size() > 0) {
                    int i = connectionCache.size() - 1;
                    connection = connectionCache.get(i);
                    connectionCache.remove(i);
                } else {
                    connection = new InitialMasterConnection(nf);
                }
                nf.configureNonBlocking(clientFd);
                if (nf.setTcpNoDelay(clientFd, true) != 0) {
                    LOG.info().$("failed to set TCP no delay [fd=").$(clientFd).$(']').$();
                }
                connection.of(clientFd);
                idleConnections.add(connection);
                LOG.info().$("peer connected [ip=").$ip(nf.getPeerIP(clientFd)).$(", fd=").$(clientFd).$(']').$();
            }
        }

        private void handleConnections() {
            int n = 0;
            int sz = idleConnections.size();
            while (n < sz) {
                InitialMasterConnection connection = idleConnections.get(n);
                if (connection.handleIO()) {
                    if (connection.isDisconnected()) {
                        idleConnections.remove(n);
                        sz--;
                        continue;
                    }
                }
                n++;
            }
        }

        public void close() {
            if (null != listenFds) {
                Misc.freeObjList(idleConnections);
                idleConnections.clear();
                Misc.freeObjList(connectionCache);
                connectionCache.clear();
                for (int n = 0, sz = listenFds.size(); n < sz; n++) {
                    nf.close(listenFds.get(n), LOG);
                }
                path.close();
                tempMem.close();
                listenFds = null;
            }
        }

        private class InitialMasterConnection extends AbstractFramedConnection {
            private int tableId;

            protected InitialMasterConnection(NetworkFacade nf) {
                super(nf);
            }

            private InitialMasterConnection of(long fd) {
                if (!init(fd)) {
                    LOG.error().$("failed to allocate buffer [fd=").$(fd).$(']').$();
                    close();
                    return this;
                }
                tableId = 0;
                resetReading();
                return this;
            }

            @Override
            protected boolean handleFrame(byte frameType) {
                switch (frameType) {
                    case TableReplicationStreamHeaderSupport.FRAME_TYPE_REQUEST_TABLE_INFO:
                        handleRequestTableInfo();
                        return true;
                    case TableReplicationStreamHeaderSupport.FRAME_TYPE_REQUEST_REPLICATION_STREAM:
                        handleRequestReplicationStream();
                        return true;
                }
                return false;
            }

            private void handleRequestTableInfo() {
                long tableNameLo = bufferAddress + TableReplicationStreamHeaderSupport.RTI_HEADER_SIZE;
                long tableNameHi = bufferAddress + bufferLen;
                CharSequence tableName = charSeq.of(tableNameLo, tableNameHi);
                LOG.info().$("received request for table info [fd=").$(fd).$(", tableName=").$(tableName).$(']').$();
                int protocolVersion = Unsafe.getUnsafe().getInt(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_RTI_PROTOCOL_VERSION);
                if (protocolVersion != TableReplicationStreamHeaderSupport.PROTOCOL_VERSION) {
                    // TODO handle protocol mismatch
                    throw new RuntimeException("Not implemented");
                }
                int masterTableId;
                long rowCount;
                long tableStructureVersion;
                if (engine.getStatus(AllowAllCairoSecurityContext.INSTANCE, path, tableName) == TableUtils.TABLE_EXISTS) {
                    TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName);
                    rowCount = reader.size();
                    tableStructureVersion = reader.getVersion();
                    masterTableId = reader.getMetadata().getId();
                    reader.close();

                    if (null == tableNameById.get(masterTableId)) {
                        tableNameById.put(masterTableId, tableName.toString());
                    }
                } else {
                    masterTableId = 0;
                    rowCount = Long.MIN_VALUE;
                    tableStructureVersion = Long.MIN_VALUE;
                }
                tableId = masterTableId;

                int frameLen = TableReplicationStreamHeaderSupport.TI_HEADER_SIZE + tableName.length() * 2;
                resetWriting(TableReplicationStreamHeaderSupport.FRAME_TYPE_TABLE_INFO, frameLen);
                Unsafe.getUnsafe().copyMemory(tableNameLo, bufferAddress + TableReplicationStreamHeaderSupport.TI_HEADER_SIZE, tableNameHi - tableNameLo);
                Unsafe.getUnsafe().putInt(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID, masterTableId);
                Unsafe.getUnsafe().putLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_TI_TABLE_ROW_COUNT, rowCount);
                Unsafe.getUnsafe().putLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_TI_TABLE_STRUCTURE_VERSION, tableStructureVersion);
                resetWriting(TableReplicationStreamHeaderSupport.FRAME_TYPE_TABLE_INFO, frameLen);
            }

            private void handleRequestReplicationStream() {
                int masterTableId = Unsafe.getUnsafe().getInt(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID);
                long requestedTableStructureVersion = Unsafe.getUnsafe().getLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_RRS_TABLE_STRUCTURE_VERSION);
                long startingRowCount = Unsafe.getUnsafe().getLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_RRS_INITIAL_ROW_COUNT);
                CharSequence tableName = tableNameById.get(masterTableId);
                if (null != tableName && engine.getStatus(AllowAllCairoSecurityContext.INSTANCE, path, tableName) == TableUtils.TABLE_EXISTS) {
                    TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName);
                    try {
                        long tableStructureVersion = reader.getVersion();
                        if (tableStructureVersion != requestedTableStructureVersion) {
                            // TODO Support structure mismatch
                            throw new RuntimeException("Not implemented");
                        }

                        path.of(root).concat(tableName).concat(TableUtils.META_FILE_NAME).$();
                        int metaSize = (int) ff.length(path);
                        tempMem.of(ff, path, ff.getPageSize(), metaSize);

                        int frameLen = TableReplicationStreamHeaderSupport.SORS_HEADER_SIZE + metaSize;
                        resetWriting(TableReplicationStreamHeaderSupport.FRAME_TYPE_START_OF_REPLICATION_STREAM, frameLen);
                        Unsafe.getUnsafe().putInt(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID, masterTableId);
                        Unsafe.getUnsafe().putLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_SORS_TABLE_STRUCTURE_VERSION,
                                tableStructureVersion);
                        Unsafe.getUnsafe().putLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_SORS_INITIAL_ROW_COUNT,
                                startingRowCount);
                        Unsafe.getUnsafe().copyMemory(tempMem.addressOf(0), bufferAddress + TableReplicationStreamHeaderSupport.SORS_HEADER_SIZE,
                                metaSize);
                    } finally {
                        reader.close();
                    }
                } else {
                    // TODO invalid request
                    throw new RuntimeException("Not implemented");
                }
            }
        }
    }
}
