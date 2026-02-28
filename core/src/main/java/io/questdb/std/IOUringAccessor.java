/*******************************************************************************
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

package io.questdb.std;

public class IOUringAccessor {

    static final short CQE_RES_OFFSET;
    static final short CQE_USER_DATA_OFFSET;
    static final short CQ_CQES_OFFSET;
    static final short CQ_KHEAD_OFFSET;
    static final short CQ_KRING_ENTRIES_OFFSET;
    static final short CQ_KRING_MASK_OFFSET;
    static final short CQ_KTAIL_OFFSET;
    static final byte IORING_OP_FSYNC = 3;
    static final byte IORING_OP_NOP = 0;
    static final byte IORING_OP_READ = 22;
    static final byte IORING_OP_WRITE = 23;
    static final byte IORING_OP_WRITE_FIXED = 5;
    static final short RING_FD_OFFSET;
    static final short SIZEOF_CQE;
    static final short SIZEOF_SQE;
    static final short SQE_ADDR_OFFSET;
    static final short SQE_BUF_INDEX_OFFSET;
    static final short SQE_FD_OFFSET;
    static final short SQE_FLAGS_OFFSET;
    static final short SQE_LEN_OFFSET;
    static final short SQE_OFF_OFFSET;
    static final short SQE_OPCODE_OFFSET;
    static final short SQE_USER_DATA_OFFSET;
    static final short SQ_KHEAD_OFFSET;
    static final short SQ_KRING_ENTRIES_OFFSET;
    static final short SQ_KRING_MASK_OFFSET;
    static final short SQ_KTAIL_OFFSET;
    static final short SQ_SQES_OFFSET;
    static final short SQ_SQE_HEAD_OFFSET;
    static final short SQ_SQE_TAIL_OFFSET;

    static native void close(long ptr);

    static native long create(int capacity, int flags);

    static native short getCqCqesOffset();

    static native short getCqKheadOffset();

    static native short getCqKringEntriesOffset();

    static native short getCqKringMaskOffset();

    static native short getCqKtailOffset();

    static native short getCqOffset();

    static native short getCqeResOffset();

    static native short getCqeSize();

    static native short getCqeUserDataOffset();

    static native short getRingFdOffset();

    static native short getSqKheadOffset();

    static native short getSqKringEntriesOffset();

    static native short getSqKringMaskOffset();

    static native short getSqKtailOffset();

    static native short getSqOffset();

    static native short getSqSqeHeadOffset();

    static native short getSqSqeTailOffset();

    static native short getSqSqesOffset();

    static native short getSqeAddrOffset();

    static native short getSqeBufIndexOffset();

    static native short getSqeFDOffset();

    static native short getSqeFlagsOffset();

    static native short getSqeLenOffset();

    static native short getSqeOffOffset();

    static native short getSqeOpcodeOffset();

    static native short getSqeSize();

    static native short getSqeUserDataOffset();

    static native String kernelVersion();

    static native int registerBuffers(long ptr, long iovecs, int count);

    static native int registerFilesSparse(long ptr, int count);

    static native int submit(long ptr);

    static native int submitAndWait(long ptr, int waitNr);

    static native int unregisterBuffers(long ptr);

    static native int unregisterFiles(long ptr);

    static native int updateRegisteredFiles(long ptr, int offset, long fdsAddr, int count);

    static {
        RING_FD_OFFSET = getRingFdOffset();

        final short sqOffset = getSqOffset();
        SQ_KHEAD_OFFSET = (short) (sqOffset + getSqKheadOffset());
        SQ_KTAIL_OFFSET = (short) (sqOffset + getSqKtailOffset());
        SQ_KRING_MASK_OFFSET = (short) (sqOffset + getSqKringMaskOffset());
        SQ_KRING_ENTRIES_OFFSET = (short) (sqOffset + getSqKringEntriesOffset());
        SQ_SQES_OFFSET = (short) (sqOffset + getSqSqesOffset());
        SQ_SQE_HEAD_OFFSET = (short) (sqOffset + getSqSqeHeadOffset());
        SQ_SQE_TAIL_OFFSET = (short) (sqOffset + getSqSqeTailOffset());

        SIZEOF_SQE = getSqeSize();
        SQE_OPCODE_OFFSET = getSqeOpcodeOffset();
        SQE_FD_OFFSET = getSqeFDOffset();
        SQE_FLAGS_OFFSET = getSqeFlagsOffset();
        SQE_OFF_OFFSET = getSqeOffOffset();
        SQE_ADDR_OFFSET = getSqeAddrOffset();
        SQE_BUF_INDEX_OFFSET = getSqeBufIndexOffset();
        SQE_LEN_OFFSET = getSqeLenOffset();
        SQE_USER_DATA_OFFSET = getSqeUserDataOffset();

        final short cqOffset = getCqOffset();
        CQ_KHEAD_OFFSET = (short) (cqOffset + getCqKheadOffset());
        CQ_KTAIL_OFFSET = (short) (cqOffset + getCqKtailOffset());
        CQ_KRING_MASK_OFFSET = (short) (cqOffset + getCqKringMaskOffset());
        CQ_KRING_ENTRIES_OFFSET = (short) (sqOffset + getCqKringEntriesOffset());
        CQ_CQES_OFFSET = (short) (cqOffset + getCqCqesOffset());

        SIZEOF_CQE = getCqeSize();
        CQE_USER_DATA_OFFSET = getCqeUserDataOffset();
        CQE_RES_OFFSET = getCqeResOffset();
    }
}
