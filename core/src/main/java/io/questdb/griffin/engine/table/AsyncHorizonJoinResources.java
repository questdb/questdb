package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Owns async horizon resources while construction transfers them from the SQL generator to an atom.
 */
public final class AsyncHorizonJoinResources implements QuietCloseable {
    private @Nullable ObjList<Function> bindVarFunctions;
    private @Nullable MemoryCARW bindVarMemory;
    private @Nullable CompiledFilter compiledFilter;
    private @Nullable Function filter;
    private final @Nullable IntHashSet filterUsedColumnIndexes;
    private @Nullable ObjList<Function> perWorkerFilters;
    private @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private @Nullable ObjList<ObjList<Function>> perWorkerKeyFunctions;

    public AsyncHorizonJoinResources(
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @Nullable ObjList<ObjList<Function>> perWorkerKeyFunctions,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters
    ) {
        this.bindVarFunctions = bindVarFunctions;
        this.bindVarMemory = bindVarMemory;
        this.compiledFilter = compiledFilter;
        this.filter = filter;
        this.filterUsedColumnIndexes = filterUsedColumnIndexes;
        this.perWorkerFilters = perWorkerFilters;
        this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;
        this.perWorkerKeyFunctions = perWorkerKeyFunctions;
    }

    @Override
    public void close() {
        Throwable cleanupFailure = null;
        cleanupFailure = closeWorkerFunctions(cleanupFailure, perWorkerGroupByFunctions);
        perWorkerGroupByFunctions = null;
        cleanupFailure = closeWorkerFunctions(cleanupFailure, perWorkerKeyFunctions);
        perWorkerKeyFunctions = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, compiledFilter);
        compiledFilter = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, bindVarMemory);
        bindVarMemory = null;
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, bindVarFunctions);
        bindVarFunctions = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, filter);
        filter = null;
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerFilters);
        perWorkerFilters = null;
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    @Nullable ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    @Nullable MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    @Nullable CompiledFilter getCompiledFilter() {
        return compiledFilter;
    }

    @Nullable Function getFilter() {
        return filter;
    }

    @Nullable IntHashSet getFilterUsedColumnIndexes() {
        return filterUsedColumnIndexes;
    }

    @Nullable ObjList<Function> getPerWorkerFilters() {
        return perWorkerFilters;
    }

    @Nullable ObjList<Function> takeBindVarFunctions() {
        final ObjList<Function> value = bindVarFunctions;
        bindVarFunctions = null;
        return value;
    }

    @Nullable MemoryCARW takeBindVarMemory() {
        final MemoryCARW value = bindVarMemory;
        bindVarMemory = null;
        return value;
    }

    @Nullable CompiledFilter takeCompiledFilter() {
        final CompiledFilter value = compiledFilter;
        compiledFilter = null;
        return value;
    }

    @Nullable Function takeFilter() {
        final Function value = filter;
        filter = null;
        return value;
    }

    @Nullable ObjList<Function> takePerWorkerFilters() {
        final ObjList<Function> value = perWorkerFilters;
        perWorkerFilters = null;
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

    private static Throwable closeWorkerFunctions(Throwable cleanupFailure, ObjList<? extends ObjList<? extends Function>> workers) {
        if (workers != null) {
            for (int i = 0, n = workers.size(); i < n; i++) {
                final ObjList<? extends Function> functions = workers.getQuick(i);
                workers.setQuick(i, null);
                try {
                    PerWorkerFunctionList.close(functions);
                } catch (Throwable th) {
                    if (cleanupFailure == null) {
                        cleanupFailure = th;
                    } else if (cleanupFailure != th) {
                        cleanupFailure.addSuppressed(th);
                    }
                }
            }
        }
        return cleanupFailure;
    }
}
