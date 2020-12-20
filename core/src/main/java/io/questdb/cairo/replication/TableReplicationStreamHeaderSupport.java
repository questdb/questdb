package io.questdb.cairo.replication;

public class TableReplicationStreamHeaderSupport {
    public static final int PROTOCOL_VERSION = 0;
    public static final byte FRAME_TYPE_DATA_FRAME = 0; // DF contains the raw column data (MASTER -> SLAVE)
    public static final byte FRAME_TYPE_SYMBOL_STRINGS_FRAME = 1; // SSF contains the raw symbol strings data (MASTER -> SLAVE)
    public static final byte FRAME_TYPE_END_OF_BLOCK = 2; // EOB sent to indicate a block update has been sent (MASTER -> SLAVE)
    public static final byte FRAME_TYPE_SLAVE_COMMIT_READY = 3; // SCR sent to indicate that the slave is ready to commit (SLAVE -> MASTER)
    public static final byte FRAME_TYPE_COMMIT_BLOCK = 4; // CB sent to commit a block (MASTER -> SLAVE)
    public static final byte FRAME_TYPE_REQUEST_TABLE_INFO = 5; // RTI sent to discover table information (SLAVE -> MASTEE)
    public static final byte FRAME_TYPE_TABLE_INFO = 6; // TS sent with table information (MASTER -> SLAVE)
    public static final byte FRAME_TYPE_REQUEST_REPLICATION_STREAM = 7; // RRS sent to request the start of a replication stream (SLAVE ->
                                                                        // MASTER)
    public static final byte FRAME_TYPE_START_OF_REPLICATION_STREAM = 8; // SORS sent just before replication starts (MASTER -> SLAVE)
    public static final byte FRAME_TYPE_MIN_ID = FRAME_TYPE_DATA_FRAME;
    public static final byte FRAME_TYPE_MAX_ID = FRAME_TYPE_START_OF_REPLICATION_STREAM;
    public static final byte FRAME_TYPE_UNKNOWN = (byte) 0xff;

    public static final int OFFSET_FRAME_SIZE = 0;
    public static final int OFFSET_FRAME_TYPE = OFFSET_FRAME_SIZE + Integer.BYTES;
    public static final int OFFSET_MASTER_TABLE_ID = OFFSET_FRAME_TYPE + Byte.BYTES;

    public static final int OFFSET_DF_FIRST_TIMESTAMP = OFFSET_MASTER_TABLE_ID + Integer.BYTES;
    public static final int OFFSET_DF_COLUMN_INDEX = OFFSET_DF_FIRST_TIMESTAMP + Long.BYTES;
    public static final int OFFSET_DF_DATA_OFFSET = OFFSET_DF_COLUMN_INDEX + Integer.BYTES;
    public static final int DF_HEADER_SIZE = OFFSET_DF_DATA_OFFSET + Long.BYTES;

    public static final int OFFSET_SFF_COLUMN_INDEX = OFFSET_MASTER_TABLE_ID + Integer.BYTES;
    public static final int OFFSET_SFF_DATA_OFFSET = OFFSET_SFF_COLUMN_INDEX + Integer.BYTES;
    public static final int SFF_HEADER_SIZE = OFFSET_SFF_DATA_OFFSET + Long.BYTES;

    public static final int SCR_HEADER_SIZE = OFFSET_MASTER_TABLE_ID + Integer.BYTES;

    public static final int CB_HEADER_SIZE = OFFSET_MASTER_TABLE_ID + Integer.BYTES;

    public static final int OFFSET_EOB_N_FRAMES_SENT = OFFSET_MASTER_TABLE_ID + Integer.BYTES;
    public static final int EOB_HEADER_SIZE = OFFSET_EOB_N_FRAMES_SENT + Integer.BYTES;

    public static final int OFFSET_RTI_PROTOCOL_VERSION = OFFSET_MASTER_TABLE_ID + Integer.BYTES;
    public static final int OFFSET_RTI_UUID_1 = OFFSET_RTI_PROTOCOL_VERSION + Integer.BYTES;
    public static final int OFFSET_RTI_UUID_2 = OFFSET_RTI_UUID_1 + Long.BYTES;
    public static final int OFFSET_RTI_UUID_3 = OFFSET_RTI_UUID_2 + Long.BYTES;
    public static final int RTI_HEADER_SIZE = OFFSET_RTI_UUID_3 + Long.BYTES;

    public static final int OFFSET_TI_TABLE_STRUCTURE_VERSION = OFFSET_MASTER_TABLE_ID + Integer.BYTES;
    public static final int OFFSET_TI_TABLE_ROW_COUNT = OFFSET_TI_TABLE_STRUCTURE_VERSION + Long.BYTES;
    public static final int TI_HEADER_SIZE = OFFSET_TI_TABLE_ROW_COUNT + Long.BYTES;

    public static final int OFFSET_RRS_TABLE_STRUCTURE_VERSION = OFFSET_MASTER_TABLE_ID + Integer.BYTES;
    public static final int OFFSET_RRS_INITIAL_ROW_COUNT = OFFSET_RRS_TABLE_STRUCTURE_VERSION + Long.BYTES;
    public static final int RRS_HEADER_SIZE = OFFSET_RRS_INITIAL_ROW_COUNT + Long.BYTES;

    public static final int OFFSET_SORS_TABLE_STRUCTURE_VERSION = OFFSET_MASTER_TABLE_ID + Integer.BYTES;
    public static final int OFFSET_SORS_INITIAL_ROW_COUNT = OFFSET_SORS_TABLE_STRUCTURE_VERSION + Long.BYTES;
    public static final int SORS_HEADER_SIZE = OFFSET_SORS_INITIAL_ROW_COUNT + Long.BYTES;

    public static final int MIN_HEADER_SIZE;
    public static final int MAX_HEADER_SIZE;

    private static int[] HEADER_SIZE_BY_FRAME_TYPE;
    static {
        HEADER_SIZE_BY_FRAME_TYPE = new int[FRAME_TYPE_MAX_ID - FRAME_TYPE_MIN_ID + 1];
        HEADER_SIZE_BY_FRAME_TYPE[FRAME_TYPE_DATA_FRAME - FRAME_TYPE_MIN_ID] = DF_HEADER_SIZE;
        HEADER_SIZE_BY_FRAME_TYPE[FRAME_TYPE_SYMBOL_STRINGS_FRAME - FRAME_TYPE_MIN_ID] = SFF_HEADER_SIZE;
        HEADER_SIZE_BY_FRAME_TYPE[FRAME_TYPE_END_OF_BLOCK - FRAME_TYPE_MIN_ID] = EOB_HEADER_SIZE;
        HEADER_SIZE_BY_FRAME_TYPE[FRAME_TYPE_SLAVE_COMMIT_READY - FRAME_TYPE_MIN_ID] = SCR_HEADER_SIZE;
        HEADER_SIZE_BY_FRAME_TYPE[FRAME_TYPE_COMMIT_BLOCK - FRAME_TYPE_MIN_ID] = CB_HEADER_SIZE;
        HEADER_SIZE_BY_FRAME_TYPE[FRAME_TYPE_REQUEST_TABLE_INFO - FRAME_TYPE_MIN_ID] = RTI_HEADER_SIZE;
        HEADER_SIZE_BY_FRAME_TYPE[FRAME_TYPE_TABLE_INFO - FRAME_TYPE_MIN_ID] = TI_HEADER_SIZE;
        HEADER_SIZE_BY_FRAME_TYPE[FRAME_TYPE_REQUEST_REPLICATION_STREAM - FRAME_TYPE_MIN_ID] = RRS_HEADER_SIZE;
        HEADER_SIZE_BY_FRAME_TYPE[FRAME_TYPE_START_OF_REPLICATION_STREAM - FRAME_TYPE_MIN_ID] = SORS_HEADER_SIZE;

        int minHeaderSize = Integer.MAX_VALUE;
        int maxHeaderSize = Integer.MIN_VALUE;
        for (int sz : HEADER_SIZE_BY_FRAME_TYPE) {
            if (sz < minHeaderSize) {
                minHeaderSize = sz;
            }
            if (sz > maxHeaderSize) {
                maxHeaderSize = sz;
            }
        }
        MIN_HEADER_SIZE = minHeaderSize;
        MAX_HEADER_SIZE = maxHeaderSize;
    }

    public static final int getFrameHeaderSize(byte frameType) {
        return HEADER_SIZE_BY_FRAME_TYPE[frameType - FRAME_TYPE_MIN_ID];
    }
}
