package org.questdb;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.io.IOException;

public class JsonSerializer {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public static String serialize(ObjList<Object[]> data, String[] columnNames) throws IOException {
        StringBuilder sb = new StringBuilder();
        JsonGenerator jg = JSON_FACTORY.createGenerator(Unsafe.stringWriter(sb));

        jg.writeStartArray();

        for (Object[] row : data) {
            jg.writeStartObject();

            for (int i = 0; i < columnNames.length; i++) {
                jg.writeFieldName(columnNames[i]);
                jg.writeObject(row[i]);
            }

            jg.writeEndObject();
        }

        jg.writeEndArray();

        jg.close();

        sb.append(LINE_SEPARATOR);

        return sb.toString();
    }
}
