/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.sql;

import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.StrConstant;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Utility class for SQL argument {@link Function}s.
 *
 * @param <T> Argument type
 */
public abstract class Argument<T> {
    /**
     * Creates a SQL argument representation from a constant, runtime constant or dynamic SQL arg function.
     *
     * @param func                  The SQL arg function.
     * @param name                  The argument name.
     * @param position              The argument position.
     * @param valueSupplierFunction The {@link Function} method to call to get the argument value, e.g. {@link Function#getStr(Record)}.
     * @param <T>                   Argument type
     * @return A SQL argument representation.
     */
    public static <T> Argument<T> fromDynamicFunction(Function func, String name, int position,
                                                      ValueSupplierFunction<T> valueSupplierFunction) {
        Argument<T> argument;
        if (func.isConstant()) {
            ValueSupplier<T> valueSupplier = valueSupplierFunction.getArgSupplier(func);
            T value = valueSupplier.getValue(null);
            argument = new ConstantArgument<>(func, name, position, value);
        } else if (func.isRuntimeConstant()) {
            ValueSupplier<T> valueSupplier = valueSupplierFunction.getArgSupplier(func);
            argument = new RuntimeConstantArgument<>(func, name, position, () -> valueSupplier.getValue(null));
        } else {
            ValueSupplier<T> valueSupplier = valueSupplierFunction.getArgSupplier(func);
            argument = new DynamicArgument<>(func, name, position, valueSupplier);
        }
        return argument;
    }

    /**
     * Creates a SQL argument representation from a constant or runtime constant SQL arg function.
     *
     * @param func                  The SQL arg function.
     * @param name                  The argument name.
     * @param position              The argument position.
     * @param valueSupplierFunction The {@link Function} method to call to get the argument value, e.g. {@link Function#getStr(Record)}.
     * @param <T>                   Argument type
     * @return A SQL argument representation.
     * @throws SqlException If the given SQL arg function is a dynamic function.
     */
    public static <T> Argument<T> fromRuntimeConstantFunction(Function func, String name, int position,
                                                              ValueSupplierFunction<T> valueSupplierFunction) throws SqlException {
        if (func.isConstant() || func.isRuntimeConstant()) {
            return fromDynamicFunction(func, name, position, valueSupplierFunction);
        } else {    // dynamic function
            throw SqlException.$(position, String.format("%s must be a constant or runtime-constant", name));
        }
    }

    /**
     * Creates a SQL argument representation from a constant SQL arg function.
     *
     * @param func                  The SQL arg function.
     * @param name                  The argument name.
     * @param position              The argument position.
     * @param valueSupplierFunction The {@link Function} method to call to get the value, e.g. {@link Function#getStr(Record)}.
     * @param <T>                   Argument type
     * @return A SQL argument representation.
     * @throws SqlException If the given SQL arg function is a dynamic or runtime constant function.
     */
    public static <T> Argument<T> fromConstantFunction(Function func, String name, int position,
                                                       ValueSupplierFunction<T> valueSupplierFunction) throws SqlException {
        if (func.isConstant()) {
            return fromDynamicFunction(func, name, position, valueSupplierFunction);
        } else {    // dynamic or runtime constant function
            throw SqlException.$(position, String.format("%s must be a constant", name));
        }
    }

    /**
     * If this argument is constant, validates it with the given validator immediately, else do nothing.
     *
     * @param validator The constant validator.
     * @return A fallback SQL function if the validation fails, otherwise returns {@code null}.
     * @throws SqlException If the constant validation fails.
     */
    public Function validateConstant(ConstValidator<T> validator) throws SqlException {
        if (this instanceof Argument.ConstantArgument) {
            ConstantArgument<T> constantArg = (ConstantArgument<T>) this;
            return validator.validate(constantArg.getValue(), constantArg.getName(), constantArg.getPosition());
        }
        return null;
    }

    /**
     * If this argument is runtime constant, registers the given validator.
     *
     * <p>Call {@link #init(SymbolTableSource, SqlExecutionContext)} to validate this argument with it.
     *
     * @param validator The runtime constant validator.
     */
    public void registerRuntimeConstantValidator(RuntimeConstValidator<T> validator) {
        if (this instanceof Argument.RuntimeConstantArgument) {
            RuntimeConstantArgument<T> runtimeConstantArg = (RuntimeConstantArgument<T>) this;
            runtimeConstantArg.addValidator(validator);
        }
    }

    /**
     * If this argument is dynamic or runtime constant, initializes the SQL function.
     *
     * <p>If this argument is runtime constant, validates the argument value with the runtime constant validator
     * registered with {@link #registerRuntimeConstantValidator(RuntimeConstValidator)}.
     *
     * @throws SqlException If a runtime constant validation fails.
     * @see Function#init(SymbolTableSource, SqlExecutionContext)
     */
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (!(this instanceof Argument.ConstantArgument)) {
            getFunc().init(symbolTableSource, executionContext);
        }
        if (this instanceof Argument.RuntimeConstantArgument) {
            ((RuntimeConstantArgument<?>) this).validate();
        }
    }

    private final Function func;
    private final String name;
    private final int position;

    private Argument(Function func, String name, int position) {
        this.func = func;
        this.name = name;
        this.position = position;
    }

    public abstract T getValue(Record rec);

    public Function getFunc() {
        return func;
    }

    public String getName() {
        return name;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "Argument{" +
                "name='" + name + '\'' +
                ", position=" + position +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Argument<?> argument = (Argument<?>) o;
        return Objects.equals(func, argument.func);
    }

    @Override
    public int hashCode() {
        return Objects.hash(func);
    }

    protected static class ConstantArgument<T> extends Argument<T> {
        private final T value;

        public ConstantArgument(Function func, String name, int position, T value) {
            super(func, name, position);
            this.value = value;
        }

        @Override
        public T getValue(Record rec) {
            return getValue();
        }

        public T getValue() {
            return value;
        }
    }

    protected static class RuntimeConstantArgument<T> extends Argument<T> {
        private final List<RuntimeConstValidator<T>> validators = new ArrayList<>();
        private final Supplier<T> argSupplier;

        public RuntimeConstantArgument(Function func, String name, int position, Supplier<T> argSupplier) {
            super(func, name, position);
            this.argSupplier = argSupplier;
        }

        @Override
        public T getValue(Record rec) {
            return getValue();
        }

        public T getValue() {
            return argSupplier.get();
        }

        public void addValidator(RuntimeConstValidator<T> validator) {
            validators.add(validator);
        }

        public void validate() throws SqlException {
            T value = getValue();
            String name = getName();
            int position = getPosition();
            for (RuntimeConstValidator<T> validator : validators) {
                validator.validate(value, name, position);
            }
        }
    }

    protected static class DynamicArgument<T> extends Argument<T> {
        private final ValueSupplier<T> valueSupplier;

        public DynamicArgument(Function func, String name, int position, ValueSupplier<T> valueSupplier) {
            super(func, name, position);
            this.valueSupplier = valueSupplier;
        }

        @Override
        public T getValue(Record rec) {
            return valueSupplier.getValue(rec);
        }
    }

    @FunctionalInterface
    public interface ValueSupplierFunction<T> {
        ValueSupplier<T> getArgSupplier(Function func);
    }

    @FunctionalInterface
    public interface ValueSupplier<T> {
        T getValue(Record rec);
    }

    @FunctionalInterface
    public interface ConstValidator<T> {
        /**
         * Validate the argument value.
         *
         * @param value       The argument value.
         * @param argName     The argument name.
         * @param argPosition The argument position.
         * @return {@code null} to indicate a passed validation, otherwise return a fallback
         * {@link Function} (e.g. {@link StrConstant#NULL} to fail validation with a fallback.
         * @throws SqlException To fail validation without a fallback.
         */
        Function validate(T value, String argName, int argPosition) throws SqlException;
    }

    @FunctionalInterface
    public interface RuntimeConstValidator<T> {
        /**
         * Validate the argument value.
         *
         * @param value       The argument value.
         * @param argName     The argument name.
         * @param argPosition The argument position.
         * @throws SqlException To fail validation.
         */
        void validate(T value, String argName, int argPosition) throws SqlException;
    }
}
