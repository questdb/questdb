package io.questdb.cutlass.line.tcp;

import io.questdb.cutlass.line.tcp.LineTcpMeasurementScheduler.NetworkIOJob;

public class AggressiveRecvLineTcpConnectionContext extends LineTcpConnectionContext {
    AggressiveRecvLineTcpConnectionContext(LineTcpReceiverConfiguration configuration, LineTcpMeasurementScheduler scheduler) {
        super(configuration, scheduler);
    }

    @Override
    IOContextResult handleIO(NetworkIOJob netIoJob) {
        IOContextResult rc;
        read();
        do {
            rc = parseMeasurements(netIoJob);
        } while (rc == IOContextResult.NEEDS_READ && read());
        return rc;
    }
}
