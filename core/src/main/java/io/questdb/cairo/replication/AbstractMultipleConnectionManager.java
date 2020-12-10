package io.questdb.cairo.replication;

import java.io.Closeable;

import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionCallbackEvent;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionWorkerEvent;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionWorkerJob;
import io.questdb.cairo.replication.ReplicationPeerDetails.FanOutSequencedQueue;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.network.NetworkFacade;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

abstract class AbstractMultipleConnectionManager<WKEV extends ConnectionWorkerEvent, CBEV extends ConnectionCallbackEvent> implements Closeable {
    private static final Log LOG = LogFactory.getLog(ReplicationMasterConnectionDemultiplexer.class);
    protected final NetworkFacade nf;
    protected final FanOutSequencedQueue<WKEV> connectionWorkerQueue;
    protected final SequencedQueue<CBEV> connectionCallbackQueue;
    private LongObjHashMap<ReplicationPeerDetails> peerById = new LongObjHashMap<>();
    private ObjList<ReplicationPeerDetails> peers = new ObjList<>();
    protected int nWorkers;
    protected final ConnectionWorkerJob<WKEV, CBEV>[] connectionWorkerJobs;

    @SuppressWarnings("unchecked")
    public AbstractMultipleConnectionManager(
            NetworkFacade nf,
            WorkerPool connectionWorkerPool,
            int connectionCallbackQueueLen,
            int connectionWorkerQueueLen,
            ObjectFactory<WKEV> workerEventFactory,
            ObjectFactory<CBEV> callbackEventFactory
    ) {
        super();
        this.nf = nf;

        nWorkers = connectionWorkerPool.getWorkerCount();
        connectionWorkerQueue = FanOutSequencedQueue.createSingleProducerFanOutConsumerQueue(connectionWorkerQueueLen, workerEventFactory,
                nWorkers);
        connectionCallbackQueue = SequencedQueue.createMultipleProducerSingleConsumerQueue(connectionCallbackQueueLen, callbackEventFactory);
        connectionWorkerJobs = new ConnectionWorkerJob[nWorkers];
        for (int n = 0; n < nWorkers; n++) {
            ConnectionWorkerJob<WKEV, CBEV> sendJob = createConnectionWorkerJob(n, connectionWorkerQueue);
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

    abstract ConnectionWorkerJob<WKEV, CBEV> createConnectionWorkerJob(int nWorker, FanOutSequencedQueue<WKEV> connectionWorkerQueue);

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
