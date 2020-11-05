package io.questdb.cairo.replication;

public class TableReplicationStreamHeaderSupport {
    public static final byte FRAME_TYPE_BLOCK_META_FRAME = 0; // BMF
    public static final byte FRAME_TYPE_DATA_FRAME = 1; // DF
    public static final byte FRAME_TYPE_MIN_ID = FRAME_TYPE_BLOCK_META_FRAME;
    public static final byte FRAME_TYPE_MAX_ID = FRAME_TYPE_DATA_FRAME;
    public static final byte FRAME_TYPE_UNKNOWN = (byte) 0xff;

    public static final long OFFSET_FRAME_SIZE = 0;
    public static final long OFFSET_FRAME_TYPE = OFFSET_FRAME_SIZE + Integer.BYTES;
    public static final long OFFSET_MASTER_TABLE_ID = OFFSET_FRAME_TYPE + Byte.BYTES;
    public static final long OFFSET_FRAME_SEQUENCE_ID = OFFSET_FRAME_TYPE + Integer.BYTES;

    public static final long OFFSET_BMF_BLOCK_FIRST_TIMESTAMP = OFFSET_FRAME_SEQUENCE_ID + Integer.BYTES;

    public static final long OFFSET_DF_COLUMN_INDEX = OFFSET_FRAME_SEQUENCE_ID + Integer.BYTES;

    public static final long BMF_HEADER_SIZE = OFFSET_BMF_BLOCK_FIRST_TIMESTAMP + Long.BYTES;
    public static final long DF_HEADER_SIZE = OFFSET_DF_COLUMN_INDEX + Integer.BYTES;
    public static final long MIN_HEADER_SIZE = DF_HEADER_SIZE;
    public static final long MAX_HEADER_SIZE = DF_HEADER_SIZE;

    private static long[] HEADER_SIZE_BY_FRAME_TYPE;
    static {
        HEADER_SIZE_BY_FRAME_TYPE = new long[FRAME_TYPE_MAX_ID - FRAME_TYPE_MIN_ID + 1];
        HEADER_SIZE_BY_FRAME_TYPE[FRAME_TYPE_BLOCK_META_FRAME] = BMF_HEADER_SIZE;
        HEADER_SIZE_BY_FRAME_TYPE[FRAME_TYPE_DATA_FRAME] = DF_HEADER_SIZE;
    }

    public static final long getFrameHeaderSize(byte frameType) {
        return HEADER_SIZE_BY_FRAME_TYPE[frameType - FRAME_TYPE_MIN_ID];
    }
}
