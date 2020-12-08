package io.questdb.cairo.replication;

import java.io.Closeable;

import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionWorkerEvent;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionWorkerJob;
import io.questdb.cairo.replication.ReplicationPeerDetails.SequencedQueue;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

abstract class AbstractMultipleConnectionManager implements Closeable {
    private static final Log LOG = LogFactory.getLog(ReplicationMasterConnectionDemultiplexer.class);
    protected final FilesFacade ff;
    private LongObjHashMap<ReplicationPeerDetails> peerById = new LongObjHashMap<>();
    private ObjList<ReplicationPeerDetails> peers = new ObjList<>();
    protected int nWorkers;
    protected final ConnectionWorkerJob[] connectionWorkerJobs;

    public AbstractMultipleConnectionManager(
            FilesFacade ff,
            WorkerPool connectionWorkerPool,
            int connectionCallbackQueueLen,
            int newConnectionQueueLen
    ) {
        super();
        this.ff = ff;

        nWorkers = connectionWorkerPool.getWorkerCount();
        connectionWorkerJobs = new ConnectionWorkerJob[nWorkers];
        for (int n = 0; n < nWorkers; n++) {
            final SequencedQueue<ConnectionWorkerEvent> consumerQueue = SequencedQueue.createSingleProducerSingleConsumerQueue(newConnectionQueueLen, ConnectionWorkerEvent::new);
            ConnectionWorkerJob sendJob = new ConnectionWorkerJob(consumerQueue);
            connectionWorkerJobs[n] = sendJob;
            connectionWorkerPool.assign(n, sendJob);
        }
    }

    final boolean tryAddConnection(long peerId, long fd) {
        LOG.info().$("peer connected [peerId=").$(peerId).$(", fd=").$(fd).$(']').$();
        ReplicationPeerDetails peerDetails = getPeerDetails(peerId);
        return peerDetails.tryAddConnection(fd);
    }

    abstract boolean handleTasks();

    abstract ReplicationPeerDetails createNewReplicationPeerDetails(long peerId);

    @SuppressWarnings("unchecked")
    final protected <T extends ReplicationPeerDetails> T getPeerDetails(long peerId) {
        ReplicationPeerDetails peerDetails = peerById.get(peerId);
        if (null == peerDetails) {
            peerDetails = createNewReplicationPeerDetails(peerId);
            peers.add(peerDetails);
            peerById.put(peerId, peerDetails);
        }
        return (T) peerDetails;
    }

    @Override
    public void close() {
        if (null != peerById) {
            Misc.freeObjList(peers);
            peers = null;
            peerById.clear();
            peerById = null;
        }
    }
}
