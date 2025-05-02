package io.questdb.config;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.vm.Vm;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.StringSink;

class ConfigMap {
    private static final int CONFIG_FORMAT_MSG_TYPE = 0;
    private final CharSequenceObjHashMap<StringSink> configMap = new CharSequenceObjHashMap<>();
    private final ObjectPool<StringSink> csPool;

    ConfigMap(CairoConfiguration configuration) {
        // TODO: move initial capacity to config
        csPool = new ObjectPool<>(StringSink::new, 64);
    }

    void clear() {
        configMap.clear();
        csPool.clear();
    }

    CharSequence get(CharSequence key) {
        return configMap.get(key);
    }

    ObjList<CharSequence> keys() {
        return configMap.keys();
    }

    void put(CharSequence key, CharSequence value) {
        final ObjList<CharSequence> keys = configMap.keys();
        final int keyIndex = keys.indexOf(key);
        final StringSink keySink;
        if (keyIndex > -1) {
            keySink = (StringSink) keys.getQuick(keyIndex);
        } else {
            keySink = csPool.next();
            keySink.clear();
            keySink.put(key);
        }

        final StringSink valueSink = configMap.contains(key) ? configMap.get(key) : csPool.next();
        valueSink.clear();
        valueSink.put(value);

        configMap.put(keySink, valueSink);
    }

    void putAll(CharSequenceObjHashMap<CharSequence> map) {
        final ObjList<CharSequence> keys = map.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final CharSequence key = keys.getQuick(i);
            final CharSequence value = map.get(key);
            put(key, value);
        }
    }

    void readFromBlock(BlockFileReader.BlockCursor cursor) {
        long offset = 0;
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() != CONFIG_FORMAT_MSG_TYPE) {
                // ignore unknown block
                continue;
            }

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

                configMap.put(keySink, valueSink);
            }
        }
    }

    void writeToBlock(AppendableBlock block) {
        block.putInt(configMap.size());
        final ObjList<CharSequence> keys = configMap.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final CharSequence key = keys.getQuick(i);
            final CharSequence value = configMap.get(key);
            block.putStr(key);
            block.putStr(value);
        }
        block.commit(CONFIG_FORMAT_MSG_TYPE);
    }
}
