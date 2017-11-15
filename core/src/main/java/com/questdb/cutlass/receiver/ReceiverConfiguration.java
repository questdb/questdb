package com.questdb.cutlass.receiver;

import com.questdb.std.NetFacade;

public interface ReceiverConfiguration {

    int getCommitRate();

    CharSequence getBindIPv4Address();

    CharSequence getGroupIPv4Address();

    int getMsgBufferSize();

    int getMsgCount();

    int getPort();

    int getReceiveBufferSize();

    NetFacade getNetFacade();
}
