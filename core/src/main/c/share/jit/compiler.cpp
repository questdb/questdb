/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
#include "x86.h"
#include "avx2.h"

using namespace asmjit;

struct JitErrorHandler : public ErrorHandler {
    JitErrorHandler()
            : error(ErrorCode::kErrorOk) {}

    void handleError(Error err, const char *msg, BaseEmitter * /*origin*/) override {
        error = err;
        message.assign(msg);
    }

    asmjit::Error error;
    asmjit::String message;
};

struct JitGlobalContext {
    //rt allocator is thread-safe
    JitRuntime rt;
};

#ifndef __aarch64__
static JitGlobalContext gGlobalContext;
#endif

using CompiledFn = int64_t (*)(int64_t *cols, int64_t cols_count, int64_t *vars, int64_t vars_count, int64_t *rows,
                               int64_t rows_count,
                               int64_t rows_start_offset);

struct Function {
    explicit Function(x86::Compiler &cc)
            : c(cc), zone(4094 - Zone::kBlockOverhead), allocator(&zone) {
        values.init(&allocator);
    };

    void compile(const instruction_t *istream, size_t size, uint32_t options) {
        auto features = CpuInfo::host().features().as<x86::Features>();
        enum type_size : uint32_t {
            scalar = 0,
            single_size = 1,
            mixed_size = 3,
        };

        uint32_t type_size = (options >> 1) & 7; // 0 - 1B, 1 - 2B, 2 - 4B, 3 - 8B, 4 - 16B
        uint32_t exec_hint = (options >> 4) & 3; // 0 - scalar, 1 - single size type, 2 - mixed size types, ...
        bool null_check = (options >> 6) & 1; // 1 - with null check
        int unroll_factor = 1;
        if (exec_hint == single_size && features.hasAVX2()) {
            auto step = 256 / ((1 << type_size) * 8);
            c.func()->frame().setAvxEnabled();
            avx2_loop(istream, size, step, null_check, unroll_factor);
        } else {
            scalar_loop(istream, size, null_check, unroll_factor);
        }
    };

    void scalar_tail(const instruction_t *istream, size_t size, bool null_check, const x86::Gp &stop, int unroll_factor = 1) {

        Label l_loop = c.newLabel();
        Label l_exit = c.newLabel();

        c.cmp(input_index, stop);
        c.jge(l_exit);

        c.bind(l_loop);

        for (int i = 0; i < unroll_factor; ++i) {
            questdb::x86::emit_code(c, istream, size, values, null_check, cols_ptr, vars_ptr, input_index);

            auto mask = values.pop();

            x86::Gp adjusted_id = c.newInt64("input_index_+_rows_id_start_offset");
            c.lea(adjusted_id, ptr(input_index, rows_id_start_offset)); // input_index + rows_id_start_offset
            c.mov(qword_ptr(rows_ptr, output_index, 3), adjusted_id);

            c.and_(mask.gp(), 1);
            c.add(output_index, mask.gp().r64());
        }
        c.add(input_index, unroll_factor);

        c.cmp(input_index, stop);
        c.jl(l_loop); // input_index < stop
        c.bind(l_exit);
    }

    void scalar_loop(const instruction_t *istream, size_t size, bool null_check, int unroll_factor = 1) {
        if(unroll_factor > 1) {
            x86::Gp stop = c.newInt64("stop");
            c.mov(stop, rows_size);
            c.sub(stop, unroll_factor - 1);
            scalar_tail(istream, size, null_check, stop, unroll_factor);
            scalar_tail(istream, size, null_check, rows_size, 1);
        } else {
            scalar_tail(istream, size, null_check, rows_size, 1);
        }
        c.ret(output_index);
    }

    void avx2_loop(const instruction_t *istream, size_t size, uint32_t step, bool null_check, int unroll_factor = 1) {
        using namespace asmjit::x86;

        Label l_loop = c.newLabel();
        Label l_exit = c.newLabel();

        c.xor_(input_index, input_index); //input_index = 0

        Gp stop = c.newGpq();
        c.mov(stop, rows_size);
        c.sub(stop, unroll_factor * step - 1); // stop = rows_size - unroll_factor * step + 1

        c.cmp(input_index, stop);
        c.jge(l_exit);


        Ymm row_ids_reg = c.newYmm("rows_ids");
        Ymm row_ids_step = c.newYmm("rows_ids_step");

        //mask compress optimization for longs
        //init row_ids_reg out of loop
        if (step == 4) {
            int64_t rows_id_mem[4] = {0, 1, 2, 3};
            Mem mem = c.newConst(ConstPool::kScopeLocal, &rows_id_mem, 32);

            c.vmovq(row_ids_reg.xmm(), rows_id_start_offset);
            c.vpbroadcastq(row_ids_reg, row_ids_reg.xmm());
            c.vpaddq(row_ids_reg, row_ids_reg, mem);

            int64_t step_data[4] = {step, step, step, step};
            Mem stem_mem = c.newConst(ConstPool::kScopeLocal, &step_data, 32);
            c.vmovdqu(row_ids_step, stem_mem);
        }

        c.bind(l_loop);

        for (int i = 0; i < unroll_factor; ++i) {
            questdb::avx2::emit_code(c, istream, size, values, null_check, cols_ptr, vars_ptr, input_index);

            auto mask = values.pop();

            //mask compress optimization for longs
            bool is_slow_zen = CpuInfo::host().familyId() == 23; // AMD Zen1, Zen1+ and Zen2
            if (step == 4 && !is_slow_zen) {
                Ymm compacted = questdb::avx2::compress_register(c, row_ids_reg, mask.ymm());
                c.vmovdqu(ymmword_ptr(rows_ptr, output_index, 3), compacted);
                Gp bits = questdb::avx2::to_bits4(c, mask.ymm());
                c.popcnt(bits, bits);
                c.add(output_index, bits.r64());
                c.vpaddq(row_ids_reg, row_ids_reg, row_ids_step);
            } else {
                Gp bits = questdb::avx2::to_bits(c, mask.ymm(), step);
                questdb::avx2::unrolled_loop2(c, bits.r64(), rows_ptr, input_index, output_index, rows_id_start_offset,
                                              step);
            }
            c.add(input_index, step); // index += step
        }

        c.cmp(input_index, stop);
        c.jl(l_loop); // index < stop
        c.bind(l_exit);

        scalar_tail(istream, size, null_check, rows_size);
        c.ret(output_index);
    }

    void begin_fn() {
        c.addFunc(FuncSignatureT<int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t *, int64_t, int64_t>(
                CallConv::kIdHost));
        cols_ptr = c.newIntPtr("cols_ptr");
        cols_size = c.newInt64("cols_size");

        c.setArg(0, cols_ptr);
        c.setArg(1, cols_size);

        vars_ptr = c.newIntPtr("vars_ptr");
        vars_size = c.newInt64("vars_size");

        c.setArg(2, vars_ptr);
        c.setArg(3, vars_size);

        rows_ptr = c.newIntPtr("rows_ptr");
        rows_size = c.newInt64("rows_size");

        c.setArg(4, rows_ptr);
        c.setArg(5, rows_size);

        rows_id_start_offset = c.newInt64("rows_id_start_offset");
        c.setArg(6, rows_id_start_offset);

        input_index = c.newInt64("input_index");
        c.mov(input_index, 0);

        output_index = c.newInt64("output_index");
        c.mov(output_index, 0);
    }

    void end_fn() {
        c.endFunc();
    }

    x86::Compiler &c;

    Zone zone;
    ZoneAllocator allocator;
    ZoneStack<jit_value_t> values;

    x86::Gp cols_ptr;
    x86::Gp cols_size;
    x86::Gp vars_ptr;
    x86::Gp vars_size;
    x86::Gp rows_ptr;
    x86::Gp rows_size;
    x86::Gp input_index;
    x86::Gp output_index;
    x86::Gp rows_id_start_offset;
};

void fillJitErrorObject(JNIEnv *e, jobject error, uint32_t code, const char *msg) {

    if (!msg) {
        return;
    }

    jclass errorClass = e->GetObjectClass(error);
    if (errorClass) {
        jfieldID fieldError = e->GetFieldID(errorClass, "errorCode", "I");
        if (fieldError) {
            e->SetIntField(error, fieldError, static_cast<jint>(code));
        }
        jmethodID methodPut = e->GetMethodID(errorClass, "put", "(B)V");
        if (methodPut) {
            for (const char *c = msg; *c; ++c) {
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
                                                    jobject error) {
#ifndef __aarch64__

    auto size = static_cast<size_t>(filterSize) / sizeof(instruction_t);
    if (filterAddress <= 0 || size <= 0) {
        fillJitErrorObject(e, error, ErrorCode::kErrorInvalidArgument, "Invalid argument passed");
        return 0;
    }

    CodeHolder code;
    code.init(gGlobalContext.rt.environment());
    FileLogger logger(stdout);
    bool debug = options & 1;
    if (debug) {
        logger.addFlags(FormatOptions::kFlagRegCasts |
                        FormatOptions::kFlagExplainImms |
                        FormatOptions::kFlagAnnotations);
        code.setLogger(&logger);
    }

    JitErrorHandler errorHandler;
    code.setErrorHandler(&errorHandler);

    x86::Compiler c(&code);
    Function function(c);

    CompiledFn fn;

    function.begin_fn();
    function.compile(reinterpret_cast<const instruction_t *>(filterAddress), size, options);
    function.end_fn();

    Error err = errorHandler.error;

    if(err == ErrorCode::kErrorOk) {
        err = c.finalize();
    }

    if(err == ErrorCode::kErrorOk) {
        err = gGlobalContext.rt.add(&fn, &code);
    }

    fflush(logger.file());

    if(err != ErrorCode::kErrorOk) {
        fillJitErrorObject(e, error, err, errorHandler.message.data());
        return 0;
    }

    return reinterpret_cast<jlong>(fn);
#else
    return 0;
#endif

}

JNIEXPORT void JNICALL
Java_io_questdb_jit_FiltersCompiler_freeFunction(JNIEnv *e, jclass cl, jlong fnAddress) {
#ifndef __aarch64__
    auto fn = reinterpret_cast<void *>(fnAddress);
    gGlobalContext.rt.release(fn);
#endif
}

JNIEXPORT jlong JNICALL Java_io_questdb_jit_FiltersCompiler_callFunction(JNIEnv *e,
                                                                         jclass cl,
                                                                         jlong fnAddress,
                                                                         jlong colsAddress,
                                                                         jlong colsSize,
                                                                         jlong varsAddress,
                                                                         jlong varsSize,
                                                                         jlong rowsAddress,
                                                                         jlong rowsSize,
                                                                         jlong rowsStartOffset) {
#ifndef __aarch64__
    auto fn = reinterpret_cast<CompiledFn>(fnAddress);
    return fn(reinterpret_cast<int64_t *>(colsAddress),
              colsSize,
              reinterpret_cast<int64_t *>(varsAddress),
              varsSize,
              reinterpret_cast<int64_t *>(rowsAddress),
              rowsSize,
              rowsStartOffset);
#else
    return 0;
#endif
}
