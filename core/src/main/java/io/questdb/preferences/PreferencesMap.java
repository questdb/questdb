package io.questdb.preferences;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.vm.Vm;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.StringSink;

public class PreferencesMap {
    private static final int PREFERENCES_FORMAT_MSG_TYPE = 0;
    private final ObjectPool<StringSink> csPool;
    private final CharSequenceObjHashMap<StringSink> map = new CharSequenceObjHashMap<>();

    PreferencesMap(CairoConfiguration configuration) {
        csPool = new ObjectPool<>(StringSink::new, configuration.getPreferencesStringPoolCapacity());
    }

    public CharSequence get(CharSequence key) {
        return map.get(key);
    }

    void clear() {
        map.clear();
        csPool.clear();
    }

    ObjList<CharSequence> keys() {
        return map.keys();
    }

    void put(CharSequence key, CharSequence value) {
        final ObjList<CharSequence> keys = map.keys();
        final int keyIndex = keys.indexOf(key);
        final StringSink keySink;
        if (keyIndex > -1) {
            keySink = (StringSink) keys.getQuick(keyIndex);
        } else {
            keySink = csPool.next();
            keySink.clear();
            keySink.put(key);
        }

        final StringSink valueSink = map.contains(key) ? map.get(key) : csPool.next();
        valueSink.clear();
        valueSink.put(value);

        map.put(keySink, valueSink);
    }

    void putAll(CharSequenceObjHashMap<CharSequence> map) {
        final ObjList<CharSequence> keys = map.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final CharSequence key = keys.getQuick(i);
            final CharSequence value = map.get(key);
            put(key, value);
        }
    }

    long readFromBlock(BlockFileReader.BlockCursor cursor) {
        long version = 0L;

        long offset = 0;
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() != PREFERENCES_FORMAT_MSG_TYPE) {
                // ignore unknown block
                continue;
            }

            version = block.getLong(offset);
            offset += Long.BYTES;

            final int size = block.getInt(offset);
            offset += Integer.BYTES;
            for (int i = 0; i < size; i++) {
                final CharSequence key = block.getStr(offset);
                offset += Vm.getStorageLength(key);
                final StringSink keySink = csPool.next();
                keySink.clear();
                keySink.put(key);

                final CharSequence value = block.getStr(offset);
                offset += Vm.getStorageLength(value);
                final StringSink valueSink = csPool.next();
                valueSink.clear();
                valueSink.put(value);

                map.put(keySink, valueSink);
            }
        }
        return version;
    }

    void writeToBlock(AppendableBlock block, long version) {
        block.putLong(version);
        block.putInt(map.size());
        final ObjList<CharSequence> keys = map.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final CharSequence key = keys.getQuick(i);
            final CharSequence value = map.get(key);
            block.putStr(key);
            block.putStr(value);
        }
        block.commit(PREFERENCES_FORMAT_MSG_TYPE);
    }
}
