package io.questdb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.questdb.cairo.TableToken;

public class BinarySerializer {
    private static final byte TYPE_NULL = 0;
    private static final byte TYPE_INTEGER = 1;
    private static final byte TYPE_LONG = 2;
    private static final byte TYPE_DOUBLE = 3;
    private static final byte TYPE_STRING = 4;
    private static final byte TYPE_BOOLEAN = 5;
    private static final byte TYPE_TABLE_TOKEN = 6;
    private static final byte TYPE_TIMESTAMP_KEY = 7;

    public static void serializeToBinary(Map<TableTokenTimestampKey, Object> data, String outputPath) throws IOException {
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(outputPath)))) {
            
            // Write number of entries
            out.writeInt(data.size());
            
            // Write each entry
            for (Map.Entry<TableTokenTimestampKey, Object> entry : data.entrySet()) {
                // Serialize TableTokenTimestampKey
                writeTableTokenTimestampKey(out, entry.getKey());
                
                // Write value
                writeValue(out, entry.getValue());
            }
        }
    }

    private static void writeTableTokenTimestampKey(DataOutputStream out, TableTokenTimestampKey key) throws IOException {
        out.writeByte(TYPE_TIMESTAMP_KEY);
        writeTableToken(out, key.getTableToken());
        out.writeLong(key.getTimestampMacro());
    }

    private static void writeTableToken(DataOutputStream out, TableToken token) throws IOException {
        out.writeByte(TYPE_TABLE_TOKEN);
        
        // Write tableName
        writeString(out, token.getTableName());
        
        // Write dirName
        writeString(out, token.getDirName());
        
        // Write tableId
        out.writeInt(token.getTableId());
        
        // Write boolean flags
        out.writeBoolean(token.isWal());
        out.writeBoolean(token.isSystem());
        out.writeBoolean(token.isProtected());
        out.writeBoolean(token.isPublic());
    }

    private static void writeString(DataOutputStream out, String str) throws IOException {
        byte[] bytes = str.getBytes("UTF-8");
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void writeValue(DataOutputStream out, Object value) throws IOException {
        if (value == null) {
            out.writeByte(TYPE_NULL);
        } else if (value instanceof Integer) {
            out.writeByte(TYPE_INTEGER);
            out.writeInt((Integer) value);
        } else if (value instanceof Long) {
            out.writeByte(TYPE_LONG);
            out.writeLong((Long) value);
        } else if (value instanceof Double) {
            out.writeByte(TYPE_DOUBLE);
            out.writeDouble((Double) value);
        } else if (value instanceof String) {
            out.writeByte(TYPE_STRING);
            writeString(out, (String) value);
        } else if (value instanceof Boolean) {
            out.writeByte(TYPE_BOOLEAN);
            out.writeBoolean((Boolean) value);
        } else if (value instanceof TableToken) {
            writeTableToken(out, (TableToken) value);
        } else if (value instanceof TableTokenTimestampKey) {
            writeTableTokenTimestampKey(out, (TableTokenTimestampKey) value);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass());
        }
    }

    public static Map<TableTokenTimestampKey, Object> deserializeFromBinary(String inputPath) throws IOException {
        Map<TableTokenTimestampKey, Object> result = new HashMap<>();
        
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(
                new FileInputStream(inputPath)))) {
            
            // Read number of entries
            int numEntries = in.readInt();
            
            // Read each entry
            for (int i = 0; i < numEntries; i++) {
                TableTokenTimestampKey key = readTableTokenTimestampKey(in);
                Object value = readValue(in);
                result.put(key, value);
            }
            
            return result;
        }
    }

    private static TableTokenTimestampKey readTableTokenTimestampKey(DataInputStream in) throws IOException {
        byte type = in.readByte();
        if (type != TYPE_TIMESTAMP_KEY) {
            throw new IOException("Expected TIMESTAMP_KEY type but got: " + type);
        }
        
        TableToken tableToken = readTableToken(in);
        long timestamp = in.readLong();
        
        return new TableTokenTimestampKey(tableToken, timestamp);
    }

    private static TableToken readTableToken(DataInputStream in) throws IOException {
        byte type = in.readByte();
        if (type != TYPE_TABLE_TOKEN) {
            throw new IOException("Expected TABLE_TOKEN type but got: " + type);
        }
        
        String tableName = readString(in);
        String dirName = readString(in);
        int tableId = in.readInt();
        boolean isWal = in.readBoolean();
        boolean isSystem = in.readBoolean();
        boolean isProtected = in.readBoolean();
        boolean isPublic = in.readBoolean();
        
        return new TableToken(tableName, dirName, tableId, isWal, isSystem, isProtected, isPublic);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, "UTF-8");
    }

    private static Object readValue(DataInputStream in) throws IOException {
        byte type = in.readByte();
        switch (type) {
            case TYPE_NULL:
                return null;
            case TYPE_INTEGER:
                return in.readInt();
            case TYPE_LONG:
                return in.readLong();
            case TYPE_DOUBLE:
                return in.readDouble();
            case TYPE_STRING:
                return readString(in);
            case TYPE_BOOLEAN:
                return in.readBoolean();
            case TYPE_TABLE_TOKEN:
                return readTableToken(in);
            case TYPE_TIMESTAMP_KEY:
                return readTableTokenTimestampKey(in);
            default:
                throw new IOException("Unknown type: " + type);
        }
    }
}