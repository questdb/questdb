package io.questdb.cairo.lv;

import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

import java.util.Map;

/**
 * Thread-safe registry of live view instances.
 */
public class LiveViewRegistry implements QuietCloseable {
    private final ConcurrentHashMap<LiveViewInstance> viewsByName = new ConcurrentHashMap<>();

    @Override
    public void close() {
        clear();
    }

    public void clear() {
        for (Map.Entry<CharSequence, LiveViewInstance> entry : viewsByName.entrySet()) {
            Misc.free(entry.getValue());
        }
        viewsByName.clear();
    }

    public LiveViewInstance getViewInstance(CharSequence name) {
        return viewsByName.get(name);
    }

    /**
     * Collects all live view instances that depend on the given base table.
     * Linear scan — acceptable for the V1 draft.
     */
    public void getViewsForBaseTable(CharSequence baseTableName, ObjList<LiveViewInstance> sink) {
        sink.clear();
        for (LiveViewInstance instance : viewsByName.values()) {
            if (instance.getDefinition().getBaseTableName().contentEquals(baseTableName)) {
                sink.add(instance);
            }
        }
    }

    public boolean hasView(CharSequence name) {
        return viewsByName.get(name) != null;
    }

    public boolean hasViewsForBaseTable(CharSequence baseTableName) {
        for (LiveViewInstance instance : viewsByName.values()) {
            if (instance.getDefinition().getBaseTableName().contentEquals(baseTableName)) {
                return true;
            }
        }
        return false;
    }

    public void registerView(LiveViewInstance instance) {
        viewsByName.put(instance.getDefinition().getViewName(), instance);
    }

    public LiveViewInstance removeView(CharSequence name) {
        return viewsByName.remove(name);
    }
}
