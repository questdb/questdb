package io.questdb.cutlass.line.tcp;

public class AggressiveRecvLineTcpConnectionContext extends LineTcpConnectionContext {
    AggressiveRecvLineTcpConnectionContext(LineTcpReceiverConfiguration configuration, LineTcpMeasurementScheduler scheduler) {
        super(configuration, scheduler);
    }

    @Override
    IOContextResult handleIO(int workerId) {
        IOContextResult rc;
        read();
        do {
            rc = parseMeasurements(workerId);
        } while (rc == IOContextResult.NEEDS_READ && read());
        return rc;
    }
}
