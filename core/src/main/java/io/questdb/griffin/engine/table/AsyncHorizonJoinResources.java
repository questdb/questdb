package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Owns async horizon resources while construction transfers them from the SQL generator to an atom: the filter of
 * the master's page frames, the per-worker aggregate functions, the key functions and their per-worker clones, and
 * the filter of the slave.
 */
public final class AsyncHorizonJoinResources extends AsyncFilterResources {
    private @Nullable ObjList<Function> keyFunctions;
    private @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private @Nullable ObjList<ObjList<Function>> perWorkerKeyFunctions;
    private @Nullable ObjList<Function> perWorkerSlaveFilters;
    private @Nullable Function slaveFilter;

    /**
     * Takes every resource of {@code filter}, the filter of the master's page frames, and the filter function and its
     * per-worker clones of {@code slaveFilter}, the filter of the slave's rows, if any.
     */
    public AsyncHorizonJoinResources(
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @Nullable ObjList<Function> keyFunctions,
            @Nullable ObjList<ObjList<Function>> perWorkerKeyFunctions,
            AsyncFilterResources filter,
            @Nullable AsyncFilterResources slaveFilter
    ) {
        super(filter);
        this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;
        this.keyFunctions = keyFunctions;
        this.perWorkerKeyFunctions = perWorkerKeyFunctions;
        if (slaveFilter != null) {
            this.slaveFilter = slaveFilter.takeFilter();
            this.perWorkerSlaveFilters = slaveFilter.takePerWorkerFilters();
        }
    }

    @Override
    public void close() {
        Throwable cleanupFailure = null;
        cleanupFailure = PerWorkerFunctionList.closePerWorker(cleanupFailure, perWorkerGroupByFunctions);
        perWorkerGroupByFunctions = null;
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, keyFunctions);
        keyFunctions = null;
        cleanupFailure = PerWorkerFunctionList.closePerWorker(cleanupFailure, perWorkerKeyFunctions);
        perWorkerKeyFunctions = null;
        cleanupFailure = freeBestEffort(cleanupFailure);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, slaveFilter);
        slaveFilter = null;
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerSlaveFilters);
        perWorkerSlaveFilters = null;
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    @Nullable ObjList<Function> takeKeyFunctions() {
        final ObjList<Function> value = keyFunctions;
        keyFunctions = null;
        return value;
    }

    @Nullable ObjList<ObjList<GroupByFunction>> takePerWorkerGroupByFunctions() {
        final ObjList<ObjList<GroupByFunction>> value = perWorkerGroupByFunctions;
        perWorkerGroupByFunctions = null;
        return value;
    }

    @Nullable ObjList<ObjList<Function>> takePerWorkerKeyFunctions() {
        final ObjList<ObjList<Function>> value = perWorkerKeyFunctions;
        perWorkerKeyFunctions = null;
        return value;
    }

    @Nullable ObjList<Function> takePerWorkerSlaveFilters() {
        final ObjList<Function> value = perWorkerSlaveFilters;
        perWorkerSlaveFilters = null;
        return value;
    }

    @Nullable Function takeSlaveFilter() {
        final Function value = slaveFilter;
        slaveFilter = null;
        return value;
    }
}
