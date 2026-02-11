/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

#include "compiler.h"

#ifdef __aarch64__
#include "aarch64.h"
#else
#include "x86.h"
#include "avx2.h"
#endif

using namespace asmjit;

struct JitErrorHandler : public ErrorHandler
{
    JitErrorHandler()
        : error(kErrorOk) {}

    void handle_error(Error err, const char *msg, BaseEmitter * /*origin*/)
    {
        error = err;
        message.assign(msg);
    }

    asmjit::Error error;
    asmjit::String message;
};

struct JitGlobalContext
{
    // rt allocator is thread-safe
    JitRuntime rt;
};

static JitGlobalContext gGlobalContext;

using CompiledFn = int64_t (*)(int64_t *cols, int64_t cols_count,
                               int64_t *varsize_indexes,
                               int64_t *vars, int64_t vars_count,
                               int64_t *filtered_rows, int64_t rows_count);

// unlike Function, only returns the total number of filtered rows without writing row ids to a long list
using CompiledCountOnlyFn = int64_t (*)(int64_t *cols, int64_t cols_count,
                                        int64_t *varsize_indexes,
                                        int64_t *vars, int64_t vars_count,
                                        int64_t rows_count);

#ifdef __aarch64__

struct Function
{
    explicit Function(a64::Compiler &cc)
        : c(cc), arena(4094) {
          };

    void preload_columns_and_constants(const instruction_t *istream, size_t size)
    {
        questdb::aarch64::preload_column_addresses(c, istream, size, data_ptr, addr_cache);
        questdb::aarch64::preload_constants(c, istream, size, const_cache);
    }

    void compile(const instruction_t *istream, size_t size, uint32_t options)
    {
        bool null_check = (options >> 6) & 1;
        int unroll_factor = 1;
        // ARM64: always scalar (no SIMD yet)
        scalar_loop(istream, size, null_check, unroll_factor);
    };

    void scalar_tail(const instruction_t *istream, size_t size, bool null_check, const a64::Gp &stop, int unroll_factor = 1)
    {
        Label l_loop = c.new_label();
        Label l_exit = c.new_label();
        Label l_next_row = c.new_label();
        Label l_store_row = c.new_label();

        questdb::aarch64::LabelArray labels;
        labels.set(0, l_next_row);
        labels.set(1, l_store_row);

        c.cmp(input_index, stop);
        c.b_ge(l_exit);

        c.bind(l_loop);

        for (int i = 0; i < unroll_factor; ++i)
        {
            value_cache.clear();
            questdb::aarch64::emit_code(c, arena, istream, size, values, null_check, data_ptr, varsize_aux_ptr, vars_ptr,
                                        input_index, labels, addr_cache, const_cache, value_cache);

            if (!values.is_empty())
            {
                auto mask = values.pop();
                c.cbz(mask.gp().w(), l_next_row);
            }

            c.bind(l_store_row);

            // rows_ptr[output_index * 8] = input_index
            a64::Gp store_addr = c.new_gp64("store_addr");
            c.add(store_addr, rows_ptr, output_index, asmjit::a64::lsl(3));
            c.str(input_index, a64::ptr(store_addr));
            c.add(output_index, output_index, imm(1));
        }

        c.bind(l_next_row);
        c.add(input_index, input_index, imm(unroll_factor));

        c.cmp(input_index, stop);
        c.b_lt(l_loop);
        c.bind(l_exit);
    }

    void scalar_loop(const instruction_t *istream, size_t size, bool null_check, int unroll_factor = 1)
    {
        preload_columns_and_constants(istream, size);

        if (unroll_factor > 1)
        {
            a64::Gp stop = c.new_gp64("stop");
            c.mov(stop, rows_size);
            c.sub(stop, stop, imm(unroll_factor - 1));
            scalar_tail(istream, size, null_check, stop, unroll_factor);
            scalar_tail(istream, size, null_check, rows_size, 1);
        }
        else
        {
            scalar_tail(istream, size, null_check, rows_size, 1);
        }
        c.ret(output_index);
    }

    void begin_fn()
    {
        auto *fn = c.add_func(FuncSignature::build<int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t>(
            CallConvId::kCDecl));
        data_ptr = c.new_gp_ptr("data_ptr");
        data_size = c.new_gp64("data_size");

        fn->set_arg(0, data_ptr);
        fn->set_arg(1, data_size);

        varsize_aux_ptr = c.new_gp_ptr("varsize_aux_ptr");

        fn->set_arg(2, varsize_aux_ptr);

        vars_ptr = c.new_gp_ptr("vars_ptr");
        vars_size = c.new_gp64("vars_size");

        fn->set_arg(3, vars_ptr);
        fn->set_arg(4, vars_size);

        rows_ptr = c.new_gp_ptr("rows_ptr");
        rows_size = c.new_gp64("rows_size");

        fn->set_arg(5, rows_ptr);
        fn->set_arg(6, rows_size);

        input_index = c.new_gp64("input_index");
        c.mov(input_index, 0);

        output_index = c.new_gp64("output_index");
        c.mov(output_index, 0);
    }

    void end_fn()
    {
        c.end_func();
    }

    a64::Compiler &c;

    Arena arena;
    ArenaVector<jit_value_t> values;

    a64::Gp data_ptr;
    a64::Gp data_size;
    a64::Gp varsize_aux_ptr;
    a64::Gp vars_ptr;
    a64::Gp vars_size;
    a64::Gp rows_ptr;
    a64::Gp rows_size;
    a64::Gp input_index;
    a64::Gp output_index;
    ColumnAddressCache addr_cache;
    ConstantCache const_cache;
    ColumnValueCache value_cache;
};

struct CountOnlyFunction
{
    explicit CountOnlyFunction(a64::Compiler &cc)
        : c(cc), arena(4094) {
          };

    void preload_columns_and_constants(const instruction_t *istream, size_t size)
    {
        questdb::aarch64::preload_column_addresses(c, istream, size, data_ptr, addr_cache);
        questdb::aarch64::preload_constants(c, istream, size, const_cache);
    }

    void compile(const instruction_t *istream, size_t size, uint32_t options)
    {
        bool null_check = (options >> 6) & 1;
        int unroll_factor = 1;
        scalar_loop(istream, size, null_check, unroll_factor);
    };

    void scalar_tail(const instruction_t *istream, size_t size, bool null_check, const a64::Gp &stop, int unroll_factor = 1)
    {
        Label l_loop = c.new_label();
        Label l_exit = c.new_label();
        Label l_next_row = c.new_label();
        Label l_inc_count = c.new_label();

        questdb::aarch64::LabelArray labels;
        labels.set(0, l_next_row);
        labels.set(1, l_inc_count);

        c.cmp(input_index, stop);
        c.b_ge(l_exit);

        c.bind(l_loop);

        for (int i = 0; i < unroll_factor; ++i)
        {
            value_cache.clear();
            questdb::aarch64::emit_code(c, arena, istream, size, values, null_check, data_ptr, varsize_aux_ptr, vars_ptr,
                                        input_index, labels, addr_cache, const_cache, value_cache);

            if (!values.is_empty())
            {
                auto mask = values.pop();
                c.cbz(mask.gp().w(), l_next_row);
            }

            c.bind(l_inc_count);

            c.add(output_index, output_index, imm(1));
        }

        c.bind(l_next_row);
        c.add(input_index, input_index, imm(unroll_factor));

        c.cmp(input_index, stop);
        c.b_lt(l_loop);
        c.bind(l_exit);
    }

    void scalar_loop(const instruction_t *istream, size_t size, bool null_check, int unroll_factor = 1)
    {
        preload_columns_and_constants(istream, size);

        if (unroll_factor > 1)
        {
            a64::Gp stop = c.new_gp64("stop");
            c.mov(stop, rows_size);
            c.sub(stop, stop, imm(unroll_factor - 1));
            scalar_tail(istream, size, null_check, stop, unroll_factor);
            scalar_tail(istream, size, null_check, rows_size, 1);
        }
        else
        {
            scalar_tail(istream, size, null_check, rows_size, 1);
        }
        c.ret(output_index);
    }

    void begin_fn()
    {
        auto *fn = c.add_func(FuncSignature::build<int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t>(
            CallConvId::kCDecl));
        data_ptr = c.new_gp_ptr("data_ptr");
        data_size = c.new_gp64("data_size");

        fn->set_arg(0, data_ptr);
        fn->set_arg(1, data_size);

        varsize_aux_ptr = c.new_gp_ptr("varsize_aux_ptr");

        fn->set_arg(2, varsize_aux_ptr);

        vars_ptr = c.new_gp_ptr("vars_ptr");
        vars_size = c.new_gp64("vars_size");

        fn->set_arg(3, vars_ptr);
        fn->set_arg(4, vars_size);

        rows_size = c.new_gp64("rows_size");

        fn->set_arg(5, rows_size);

        input_index = c.new_gp64("input_index");
        c.mov(input_index, 0);

        output_index = c.new_gp64("output_index");
        c.mov(output_index, 0);
    }

    void end_fn()
    {
        c.end_func();
    }

    a64::Compiler &c;

    Arena arena;
    ArenaVector<jit_value_t> values;

    a64::Gp data_ptr;
    a64::Gp data_size;
    a64::Gp varsize_aux_ptr;
    a64::Gp vars_ptr;
    a64::Gp vars_size;
    a64::Gp rows_size;
    a64::Gp input_index;
    a64::Gp output_index;
    ColumnAddressCache addr_cache;
    ConstantCache const_cache;
    ColumnValueCache value_cache;
};

#else // x86

struct Function
{
    explicit Function(x86::Compiler &cc)
        : c(cc), arena(4094) {
          };

    // Preload column addresses and constants before entering the loop
    void preload_columns_and_constants(const instruction_t *istream, size_t size)
    {
        questdb::x86::preload_column_addresses(c, istream, size, data_ptr, addr_cache);
        questdb::x86::preload_constants(c, istream, size, const_cache);
    }

    void compile(const instruction_t *istream, size_t size, uint32_t options)
    {
        auto &features = CpuInfo::host().features().x86();
        enum type_size : uint32_t
        {
            scalar = 0,
            single_size = 1,
            mixed_size = 3,
        };

        uint32_t type_size = (options >> 1) & 7; // 0 - 1B, 1 - 2B, 2 - 4B, 3 - 8B, 4 - 16B
        uint32_t exec_hint = (options >> 4) & 3; // 0 - scalar, 1 - single size type, 2 - mixed size types, ...
        bool null_check = (options >> 6) & 1;    // 1 - with null check
        int unroll_factor = 1;
        if (exec_hint == single_size && features.has_avx2())
        {
            auto step = 256 / ((1 << type_size) * 8);
            c.func()->frame().set_avx_enabled();
            avx2_loop(istream, size, step, null_check, unroll_factor);
        }
        else
        {
            scalar_loop(istream, size, null_check, unroll_factor);
        }
    };

    void scalar_tail(const instruction_t *istream, size_t size, bool null_check, const x86::Gp &stop, int unroll_factor = 1)
    {
        Label l_loop = c.new_label();
        Label l_exit = c.new_label();
        Label l_next_row = c.new_label();  // Label 0: skip row (AND failure)
        Label l_store_row = c.new_label(); // Label 1: store row (OR success)

        // Initialize label array:
        // - Index 0: l_next_row (skip row storage - used by AND_SC on false)
        // - Index 1: l_store_row (store row - used by OR_SC on true)
        questdb::x86::LabelArray labels;
        labels.set(0, l_next_row);
        labels.set(1, l_store_row);

        c.cmp(input_index, stop);
        c.jge(l_exit);

        c.bind(l_loop);

        for (int i = 0; i < unroll_factor; ++i)
        {
            // Clear value cache at the start of each row iteration
            value_cache.clear();
            // Pass the label array and caches to emit_code
            questdb::x86::emit_code(c, arena, istream, size, values, null_check, data_ptr, varsize_aux_ptr, vars_ptr,
                                    input_index, labels, addr_cache, const_cache, value_cache);

            // If stack is empty, all predicates were resolved via short-circuit jumps
            // No final test needed.
            if (!values.is_empty())
            {
                auto mask = values.pop();

                // Skip row storage if the last predicate failed
                c.test(mask.gp().r32(), mask.gp().r32());
                c.jz(l_next_row);
            }

            // OR_SC success jumps here to store the row
            c.bind(l_store_row);

            c.mov(qword_ptr(rows_ptr, output_index, 3), input_index);
            c.add(output_index, 1);
        }

        // AND_SC failure jumps here, skipping the row storage above
        c.bind(l_next_row);
        c.add(input_index, unroll_factor);

        c.cmp(input_index, stop);
        c.jl(l_loop); // input_index < stop
        c.bind(l_exit);
    }

    void scalar_loop(const instruction_t *istream, size_t size, bool null_check, int unroll_factor = 1)
    {
        // Preload column addresses and constants before the loop
        preload_columns_and_constants(istream, size);

        if (unroll_factor > 1)
        {
            x86::Gp stop = c.new_gp64("stop");
            c.mov(stop, rows_size);
            c.sub(stop, unroll_factor - 1);
            scalar_tail(istream, size, null_check, stop, unroll_factor);
            scalar_tail(istream, size, null_check, rows_size, 1);
        }
        else
        {
            scalar_tail(istream, size, null_check, rows_size, 1);
        }
        c.ret(output_index);
    }

    void avx2_loop(const instruction_t *istream, size_t size, uint32_t step, bool null_check, int unroll_factor = 1)
    {
        using namespace asmjit::x86;

        // Preload column addresses and constants before the loop (for scalar tail)
        preload_columns_and_constants(istream, size);

        // Preload constants into YMM registers for the SIMD loop
        questdb::avx2::preload_constants_ymm(c, istream, size, const_cache_ymm);

        Label l_loop = c.new_label();
        Label l_exit = c.new_label();

        c.xor_(input_index, input_index); // input_index = 0

        Gp stop = c.new_gp64();
        c.mov(stop, rows_size);
        c.sub(stop, unroll_factor * step - 1); // stop = rows_size - unroll_factor * step + 1

        c.cmp(input_index, stop);
        c.jge(l_exit);

        bool is_slow_zen = CpuInfo::host().family_id() == 23; // AMD Zen1, Zen1+ and Zen2
        Vec row_ids_reg = c.new_ymm("rows_ids");
        Vec row_ids_step = c.new_ymm("rows_ids_step");

        // mask compress optimization for longs
        // init row_ids_reg out of loop
        if (step == 4 && !is_slow_zen)
        {
            int64_t rows_id_mem[4] = {0, 1, 2, 3};
            Mem mem = c.new_const(ConstPoolScope::kLocal, &rows_id_mem, 32);

            c.vmovdqu(row_ids_reg, mem);

            int64_t step_data[4] = {step, step, step, step};
            Mem stem_mem = c.new_const(ConstPoolScope::kLocal, &step_data, 32);
            c.vmovdqu(row_ids_step, stem_mem);
        }

        c.bind(l_loop);

        for (int i = 0; i < unroll_factor; ++i)
        {
            questdb::avx2::emit_code(c, arena, istream, size, values, null_check, data_ptr, varsize_aux_ptr, vars_ptr, input_index,
                                     addr_cache, const_cache_ymm);

            auto mask = values.pop();

            if (step == 4 && !is_slow_zen)
            {
                // mask compress optimization for longs
                Gp bits = questdb::avx2::to_bits4(c, mask.vec());

                // short-circuit: skip scatter if no matches
                Label l_scatter_done = c.new_label();
                c.test(bits, bits);
                c.jz(l_scatter_done);

                Vec compacted = questdb::avx2::compress_register(c, row_ids_reg, mask.vec());
                c.vmovdqu(ymmword_ptr(rows_ptr, output_index, 3), compacted);
                c.popcnt(bits, bits);
                c.add(output_index, bits.r64());

                c.bind(l_scatter_done);
                c.vpaddq(row_ids_reg, row_ids_reg, row_ids_step);
            }
            else
            {
                Gp bits = questdb::avx2::to_bits(c, mask.vec(), step);
                // short-circuit: skip scatter if no matches
                Label l_scatter_done = c.new_label();
                c.test(bits, bits);
                c.jz(l_scatter_done);
                questdb::avx2::unrolled_loop2(c, bits.r64(), rows_ptr, input_index, output_index, step);
                c.bind(l_scatter_done);
            }
            c.add(input_index, step); // index += step
        }

        c.cmp(input_index, stop);
        c.jl(l_loop); // index < stop
        c.bind(l_exit);

        scalar_tail(istream, size, null_check, rows_size);
        c.ret(output_index);
    }

    void begin_fn()
    {
        auto *fn = c.add_func(FuncSignature::build<int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t>(
            CallConvId::kCDecl));
        data_ptr = c.new_gp_ptr("data_ptr");
        data_size = c.new_gp64("data_size");

        fn->set_arg(0, data_ptr);
        fn->set_arg(1, data_size);

        varsize_aux_ptr = c.new_gp_ptr("varsize_aux_ptr");

        fn->set_arg(2, varsize_aux_ptr);

        vars_ptr = c.new_gp_ptr("vars_ptr");
        vars_size = c.new_gp64("vars_size");

        fn->set_arg(3, vars_ptr);
        fn->set_arg(4, vars_size);

        rows_ptr = c.new_gp_ptr("rows_ptr");
        rows_size = c.new_gp64("rows_size");

        fn->set_arg(5, rows_ptr);
        fn->set_arg(6, rows_size);

        input_index = c.new_gp64("input_index");
        c.mov(input_index, 0);

        output_index = c.new_gp64("output_index");
        c.mov(output_index, 0);
    }

    void end_fn()
    {
        c.end_func();
    }

    x86::Compiler &c;

    Arena arena;
    ArenaVector<jit_value_t> values;

    x86::Gp data_ptr;
    x86::Gp data_size;
    x86::Gp varsize_aux_ptr;
    x86::Gp vars_ptr;
    x86::Gp vars_size;
    x86::Gp rows_ptr;
    x86::Gp rows_size;
    x86::Gp input_index;
    x86::Gp output_index;
    ColumnAddressCache addr_cache;
    ConstantCache const_cache;
    ConstantCacheYmm const_cache_ymm;
    ColumnValueCache value_cache;
};

struct CountOnlyFunction
{
    explicit CountOnlyFunction(x86::Compiler &cc)
        : c(cc), arena(4094) {
          };

    // Preload column addresses and constants before entering the loop
    void preload_columns_and_constants(const instruction_t *istream, size_t size)
    {
        questdb::x86::preload_column_addresses(c, istream, size, data_ptr, addr_cache);
        questdb::x86::preload_constants(c, istream, size, const_cache);
    }

    void compile(const instruction_t *istream, size_t size, uint32_t options)
    {
        auto &features = CpuInfo::host().features().x86();
        enum type_size : uint32_t
        {
            scalar = 0,
            single_size = 1,
            mixed_size = 3,
        };

        uint32_t type_size = (options >> 1) & 7; // 0 - 1B, 1 - 2B, 2 - 4B, 3 - 8B, 4 - 16B
        uint32_t exec_hint = (options >> 4) & 3; // 0 - scalar, 1 - single size type, 2 - mixed size types, ...
        bool null_check = (options >> 6) & 1;    // 1 - with null check
        int unroll_factor = 1;
        if (exec_hint == single_size && features.has_avx2())
        {
            auto step = 256 / ((1 << type_size) * 8);
            c.func()->frame().set_avx_enabled();
            avx2_loop(istream, size, step, null_check, unroll_factor);
        }
        else
        {
            scalar_loop(istream, size, null_check, unroll_factor);
        }
    };

    void scalar_tail(const instruction_t *istream, size_t size, bool null_check, const x86::Gp &stop, int unroll_factor = 1)
    {
        Label l_loop = c.new_label();
        Label l_exit = c.new_label();
        Label l_next_row = c.new_label();  // Label 0: skip row (AND failure)
        Label l_inc_count = c.new_label(); // Label 1: store row / increment counter (OR success)

        // Initialize label array:
        // - Index 0: l_next_row (skip row storage - used by AND_SC on false)
        // - Index 1: l_inc_count (store row / increment counter - used by OR_SC on true)
        questdb::x86::LabelArray labels;
        labels.set(0, l_next_row);
        labels.set(1, l_inc_count);

        c.cmp(input_index, stop);
        c.jge(l_exit);

        c.bind(l_loop);

        for (int i = 0; i < unroll_factor; ++i)
        {
            // Clear value cache at the start of each row iteration
            value_cache.clear();
            // Pass the label array and caches to emit_code
            questdb::x86::emit_code(c, arena, istream, size, values, null_check, data_ptr, varsize_aux_ptr, vars_ptr,
                                    input_index, labels, addr_cache, const_cache, value_cache);

            // If stack is empty, all predicates were resolved via short-circuit jumps
            // No final test needed.
            if (!values.is_empty())
            {
                auto mask = values.pop();

                // Skip row storage if the last predicate failed
                c.test(mask.gp().r32(), mask.gp().r32());
                c.jz(l_next_row);
            }

            // OR_SC success jumps here to increment the counter
            c.bind(l_inc_count);

            c.add(output_index, 1);
        }

        // AND_SC failure jumps here, skipping the above count increment
        c.bind(l_next_row);
        c.add(input_index, unroll_factor);

        c.cmp(input_index, stop);
        c.jl(l_loop); // input_index < stop
        c.bind(l_exit);
    }

    void scalar_loop(const instruction_t *istream, size_t size, bool null_check, int unroll_factor = 1)
    {
        // Preload column addresses and constants before the loop
        preload_columns_and_constants(istream, size);

        if (unroll_factor > 1)
        {
            x86::Gp stop = c.new_gp64("stop");
            c.mov(stop, rows_size);
            c.sub(stop, unroll_factor - 1);
            scalar_tail(istream, size, null_check, stop, unroll_factor);
            scalar_tail(istream, size, null_check, rows_size, 1);
        }
        else
        {
            scalar_tail(istream, size, null_check, rows_size, 1);
        }
        c.ret(output_index);
    }

    void avx2_loop(const instruction_t *istream, size_t size, uint32_t step, bool null_check, int unroll_factor = 1)
    {
        using namespace asmjit::x86;

        // Preload column addresses and constants before the loop (for scalar tail)
        preload_columns_and_constants(istream, size);

        // Preload constants into YMM registers for the SIMD loop
        questdb::avx2::preload_constants_ymm(c, istream, size, const_cache_ymm);

        Label l_loop = c.new_label();
        Label l_tail = c.new_label();

        c.xor_(input_index, input_index); // input_index = 0

        Gp stop = c.new_gp64();
        c.mov(stop, rows_size);
        c.sub(stop, unroll_factor * step - 1); // stop = rows_size - unroll_factor * step + 1

        c.cmp(input_index, stop);
        c.jge(l_tail);

        Vec acc;
        Vec ones;
        if (step == 4 || step == 8 || step == 16)
        {
            acc = c.new_ymm("acc");
            c.vpxor(acc, acc, acc); // acc = 0
            if (step == 16)
            {
                ones = c.new_ymm("ones");
                c.vpcmpeqd(ones, ones, ones);
                c.vpsrlw(ones, ones, 15);
            }
        }

        c.bind(l_loop);

        for (int i = 0; i < unroll_factor; ++i)
        {
            questdb::avx2::emit_code(c, arena, istream, size, values, null_check, data_ptr, varsize_aux_ptr, vars_ptr, input_index,
                                     addr_cache, const_cache_ymm);

            auto mask = values.pop();

            if (step == 4)
            {
                c.vpsubq(acc, acc, mask.vec());
            }
            else if (step == 8)
            {
                c.vpsubd(acc, acc, mask.vec());
            }
            else if (step == 16)
            {
                Vec pairs = c.new_ymm("pairs");
                c.vpmaddwd(pairs, mask.vec(), ones);
                c.vpsubd(acc, acc, pairs);
            }
            else
            {
                Gp bits = questdb::avx2::to_bits(c, mask.vec(), step);
                c.popcnt(bits, bits);
                c.add(output_index, bits.r64());
            }

            c.add(input_index, step);
        }

        c.cmp(input_index, stop);
        c.jl(l_loop);

        if (step == 4)
        {
            Vec xmm_acc = acc.xmm();
            Vec xmm_tmp = c.new_xmm("hsum_tmp");
            c.vextracti128(xmm_tmp, acc, 1);
            c.vpaddq(xmm_acc, xmm_acc, xmm_tmp);
            c.vpshufd(xmm_tmp, xmm_acc, 0x4E);
            c.vpaddq(xmm_acc, xmm_acc, xmm_tmp);
            c.vmovq(output_index, xmm_acc);
        }
        else if (step == 8 || step == 16)
        {
            Vec xmm_acc = acc.xmm();
            Vec xmm_tmp = c.new_xmm("hsum_tmp");
            c.vextracti128(xmm_tmp, acc, 1);
            c.vpaddd(xmm_acc, xmm_acc, xmm_tmp);
            c.vpshufd(xmm_tmp, xmm_acc, 0x4E);
            c.vpaddd(xmm_acc, xmm_acc, xmm_tmp);
            c.vpshufd(xmm_tmp, xmm_acc, 0xB1);
            c.vpaddd(xmm_acc, xmm_acc, xmm_tmp);
            Gp hsum = c.new_gp32("hsum");
            c.vmovd(hsum, xmm_acc);
            c.mov(output_index, hsum.r64());
        }

        c.bind(l_tail);

        scalar_tail(istream, size, null_check, rows_size);
        c.ret(output_index);
    }

    void begin_fn()
    {
        auto *fn = c.add_func(FuncSignature::build<int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t>(
            CallConvId::kCDecl));
        data_ptr = c.new_gp_ptr("data_ptr");
        data_size = c.new_gp64("data_size");

        fn->set_arg(0, data_ptr);
        fn->set_arg(1, data_size);

        varsize_aux_ptr = c.new_gp_ptr("varsize_aux_ptr");

        fn->set_arg(2, varsize_aux_ptr);

        vars_ptr = c.new_gp_ptr("vars_ptr");
        vars_size = c.new_gp64("vars_size");

        fn->set_arg(3, vars_ptr);
        fn->set_arg(4, vars_size);

        rows_size = c.new_gp64("rows_size");

        fn->set_arg(5, rows_size);

        input_index = c.new_gp64("input_index");
        c.mov(input_index, 0);

        output_index = c.new_gp64("output_index");
        c.mov(output_index, 0);
    }

    void end_fn()
    {
        c.end_func();
    }

    x86::Compiler &c;

    Arena arena;
    ArenaVector<jit_value_t> values;

    x86::Gp data_ptr;
    x86::Gp data_size;
    x86::Gp varsize_aux_ptr;
    x86::Gp vars_ptr;
    x86::Gp vars_size;
    x86::Gp rows_size;
    x86::Gp input_index;
    x86::Gp output_index;
    ColumnAddressCache addr_cache;
    ConstantCache const_cache;
    ConstantCacheYmm const_cache_ymm;
    ColumnValueCache value_cache;
};

#endif // __aarch64__

void fillJitErrorObject(JNIEnv *e, jobject error, Error code, const char *msg)
{
    if (!msg)
    {
        return;
    }

    jclass errorClass = e->GetObjectClass(error);
    if (errorClass)
    {
        jfieldID fieldError = e->GetFieldID(errorClass, "errorCode", "I");
        if (fieldError)
        {
            e->SetIntField(error, fieldError, static_cast<jint>(code));
        }
        jmethodID methodPut = e->GetMethodID(errorClass, "put", "(B)V");
        if (methodPut)
        {
            for (const char *c = msg; *c; ++c)
            {
                e->CallVoidMethod(error, methodPut, *c);
            }
        }
    }
}

JNIEXPORT jlong JNICALL
Java_io_questdb_jit_FiltersCompiler_compileFunction(JNIEnv *e,
                                                    jclass cl,
                                                    jlong filterAddress,
                                                    jlong filterSize,
                                                    jint options,
                                                    jobject error)
{
    auto size = static_cast<size_t>(filterSize) / sizeof(instruction_t);
    if (filterAddress <= 0 || size <= 0)
    {
        fillJitErrorObject(e, error, Error::kInvalidArgument, "Invalid argument passed");
        return 0;
    }

    CodeHolder code;
    code.init(gGlobalContext.rt.environment());
    FileLogger logger(stdout);
    bool debug = options & 1;
    if (debug)
    {
        logger.add_flags(FormatFlags::kRegCasts |
                         FormatFlags::kExplainImms);
        code.set_logger(&logger);
    }

    JitErrorHandler errorHandler;
    code.set_error_handler(&errorHandler);

#ifdef __aarch64__
    a64::Compiler c(&code);
#else
    x86::Compiler c(&code);
#endif
    Function function(c);

    CompiledFn fn;

    function.begin_fn();
    function.compile(reinterpret_cast<const instruction_t *>(filterAddress), size, options);
    function.end_fn();

    Error err = errorHandler.error;

    if (err == Error::kOk)
    {
        err = c.finalize();
    }

    if (err == Error::kOk)
    {
        err = gGlobalContext.rt.add(&fn, &code);
    }

    fflush(logger.file());

    if (err != Error::kOk)
    {
        fillJitErrorObject(e, error, err, errorHandler.message.data());
        return 0;
    }

    return reinterpret_cast<jlong>(fn);
}

JNIEXPORT jlong JNICALL
Java_io_questdb_jit_FiltersCompiler_compileCountOnlyFunction(JNIEnv *e,
                                                             jclass cl,
                                                             jlong filterAddress,
                                                             jlong filterSize,
                                                             jint options,
                                                             jobject error)
{
    auto size = static_cast<size_t>(filterSize) / sizeof(instruction_t);
    if (filterAddress <= 0 || size <= 0)
    {
        fillJitErrorObject(e, error, Error::kInvalidArgument, "Invalid argument passed");
        return 0;
    }

    CodeHolder code;
    code.init(gGlobalContext.rt.environment());
    FileLogger logger(stdout);
    bool debug = options & 1;
    if (debug)
    {
        logger.add_flags(FormatFlags::kRegCasts |
                         FormatFlags::kExplainImms);
        code.set_logger(&logger);
    }

    JitErrorHandler errorHandler;
    code.set_error_handler(&errorHandler);

#ifdef __aarch64__
    a64::Compiler c(&code);
#else
    x86::Compiler c(&code);
#endif
    CountOnlyFunction function(c);

    CompiledCountOnlyFn fn;

    function.begin_fn();
    function.compile(reinterpret_cast<const instruction_t *>(filterAddress), size, options);
    function.end_fn();

    Error err = errorHandler.error;

    if (err == Error::kOk)
    {
        err = c.finalize();
    }

    if (err == Error::kOk)
    {
        err = gGlobalContext.rt.add(&fn, &code);
    }

    fflush(logger.file());

    if (err != Error::kOk)
    {
        fillJitErrorObject(e, error, err, errorHandler.message.data());
        return 0;
    }

    return reinterpret_cast<jlong>(fn);
}

JNIEXPORT void JNICALL
Java_io_questdb_jit_FiltersCompiler_freeFunction(JNIEnv *e, jclass cl, jlong fnAddress)
{
    auto fn = reinterpret_cast<void *>(fnAddress);
    gGlobalContext.rt.release(fn);
}

JNIEXPORT jlong JNICALL Java_io_questdb_jit_FiltersCompiler_callFunction(JNIEnv *e,
                                                                         jclass cl,
                                                                         jlong fnAddress,
                                                                         jlong colsAddress,
                                                                         jlong colsSize,
                                                                         jlong varSizeIndexesAddress,
                                                                         jlong varsAddress,
                                                                         jlong varsSize,
                                                                         jlong filteredRowsAddress,
                                                                         jlong rowsCount)
{
    auto fn = reinterpret_cast<CompiledFn>(fnAddress);
    return fn(reinterpret_cast<int64_t *>(colsAddress),
              colsSize,
              reinterpret_cast<int64_t *>(varSizeIndexesAddress),
              reinterpret_cast<int64_t *>(varsAddress),
              varsSize,
              reinterpret_cast<int64_t *>(filteredRowsAddress),
              rowsCount);
}

JNIEXPORT jlong JNICALL Java_io_questdb_jit_FiltersCompiler_callCountOnlyFunction(JNIEnv *e,
                                                                                  jclass cl,
                                                                                  jlong fnAddress,
                                                                                  jlong colsAddress,
                                                                                  jlong colsSize,
                                                                                  jlong varSizeIndexesAddress,
                                                                                  jlong varsAddress,
                                                                                  jlong varsSize,
                                                                                  jlong rowsCount)
{
    auto fn = reinterpret_cast<CompiledCountOnlyFn>(fnAddress);
    return fn(reinterpret_cast<int64_t *>(colsAddress),
              colsSize,
              reinterpret_cast<int64_t *>(varSizeIndexesAddress),
              reinterpret_cast<int64_t *>(varsAddress),
              varsSize,
              rowsCount);
}
