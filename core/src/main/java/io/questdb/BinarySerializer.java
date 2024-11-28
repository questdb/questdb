package io.questdb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BinarySerializer {
    private static final byte TYPE_NULL = 0;
    private static final byte TYPE_INTEGER = 1;
    private static final byte TYPE_LONG = 2;
    private static final byte TYPE_DOUBLE = 3;
    private static final byte TYPE_STRING = 4;
    private static final byte TYPE_BOOLEAN = 5;

    public static void serializeToBinary(List<Object[]> data, String outputPath) throws IOException {
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(outputPath)))) {
            
            // Write number of rows
            out.writeInt(data.size());
            
            // Write number of columns (using first row)
            if (!data.isEmpty()) {
                out.writeInt(data.get(0).length);
            } else {
                out.writeInt(0);
                return;
            }

            // Write each row
            for (Object[] row : data) {
                for (Object value : row) {
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
                        byte[] bytes = ((String) value).getBytes("UTF-8");
                        out.writeInt(bytes.length);
                        out.write(bytes);
                    } else if (value instanceof Boolean) {
                        out.writeByte(TYPE_BOOLEAN);
                        out.writeBoolean((Boolean) value);
                    } else {
                        throw new IllegalArgumentException("Unsupported type: " + value.getClass());
                    }
                }
            }
        }
    }

    public static List<Object[]> deserializeFromBinary(String inputPath) throws IOException {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(
                new FileInputStream(inputPath)))) {
            
            // Read number of rows and columns
            int numRows = in.readInt();
            int numColumns = in.readInt();
            
            List<Object[]> result = new ArrayList<>(numRows);

            // Read each row
            for (int i = 0; i < numRows; i++) {
                Object[] row = new Object[numColumns];
                for (int j = 0; j < numColumns; j++) {
                    byte type = in.readByte();
                    switch (type) {
                        case TYPE_NULL:
                            row[j] = null;
                            break;
                        case TYPE_INTEGER:
                            row[j] = in.readInt();
                            break;
                        case TYPE_LONG:
                            row[j] = in.readLong();
                            break;
                        case TYPE_DOUBLE:
                            row[j] = in.readDouble();
                            break;
                        case TYPE_STRING:
                            int length = in.readInt();
                            byte[] bytes = new byte[length];
                            in.readFully(bytes);
                            row[j] = new String(bytes, "UTF-8");
                            break;
                        case TYPE_BOOLEAN:
                            row[j] = in.readBoolean();
                            break;
                        default:
                            throw new IOException("Unknown type: " + type);
                    }
                }
                result.add(row);
            }
            return result;
        }
    }

}