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

#ifndef QUESTDB_COMPILER_H
#define QUESTDB_COMPILER_H

#include <jni.h>
#include <asmjit/asmjit.h>
#include <limits>
#include <cassert>
#include <utility>
#include <variant>

enum class data_type_t : uint8_t {
    i8,
    i16,
    i32,
    i64,
    f32,
    f64,
};

enum class data_kind_t : uint8_t {
    kMemory,
    kConst,
};

enum class instruction_t : uint8_t {
    MEM_I1 = 0,
    MEM_I2 = 1,
    MEM_I4 = 2,
    MEM_I8 = 3,
    MEM_F4 = 4,
    MEM_F8 = 5,

    IMM_I1 = 6,
    IMM_I2 = 7,
    IMM_I4 = 8,
    IMM_I8 = 9,
    IMM_F4 = 10,
    IMM_F8 = 11,

    NEG = 12,               // -a
    NOT = 13,               // !a
    AND = 14,               // a && b
    OR = 15,                // a || b
    EQ = 16,                // a == b
    NE = 17,                // a != b
    LT = 18,                // a <  b
    LE = 19,                // a <= b
    GT = 20,                // a >  b
    GE = 21,                // a >= b
    ADD = 22,               // a + b
    SUB = 23,               // a - b
    MUL = 24,               // a * b
    DIV = 25,               // a / b
    MOD = 26,               // a % b
    JZ = 27,                // if a == 0 jp b
    JNZ = 28,               // if a != 0 jp b
    JP = 29,                // jp a
    RET = 30,               // ret a
    IMM_NULL = 31,          // generic null const

    //todo: change serialisation format IMM/MEM/VAR + type info
    VAR_I1 = 32,
    VAR_I2 = 33,
    VAR_I4 = 34,
    VAR_I8 = 35,
    VAR_F4 = 36,
    VAR_F8 = 37,
};

static const int64_t LONG_NULL = std::numeric_limits<int64_t>::min();
static const int32_t INT_NULL = std::numeric_limits<int32_t>::min();

//std::numeric_limits<double>::epsilon();
static const double DOUBLE_EPSILON = 0.0000000001;
static const float FLOAT_EPSILON = 0.0000000001;

template<typename T>
T read_at(const uint8_t *buf, size_t size, uint32_t pos) {
    if (pos + sizeof(T) <= size)
        return *((T *) &buf[pos]);
    return 0;
}

template<typename T>
T read(const uint8_t *buf, size_t size, uint32_t &pos) {
    T data = read_at<T>(buf, size, pos);
    pos += sizeof(T);
    return data;
}

struct jit_value_t {

    inline jit_value_t() noexcept
        : op_(), type_(), kind_() {}

    inline jit_value_t(asmjit::Operand op, data_type_t type, data_kind_t kind) noexcept
            : op_(op), type_(type), kind_(kind) {}

    inline jit_value_t(const jit_value_t &other) noexcept = default;

    inline jit_value_t &operator=(const jit_value_t &other) noexcept = default;

    inline const asmjit::x86::Ymm &ymm() const noexcept { return op_.as<asmjit::x86::Ymm>(); }

    inline const asmjit::x86::Xmm &xmm() const noexcept { return op_.as<asmjit::x86::Xmm>(); }

    inline const asmjit::x86::Gpq &gp() const noexcept { return op_.as<asmjit::x86::Gpq>(); }

    inline data_type_t dtype() const noexcept { return type_; }

    inline data_kind_t dkind() const noexcept { return kind_; }

    inline const asmjit::Operand& op() const noexcept { return op_; }

private:
    asmjit::Operand op_;
    data_type_t type_;
    data_kind_t kind_;
};

extern "C" {

JNIEXPORT jlong JNICALL Java_io_questdb_jit_FiltersCompiler_compileFunction(JNIEnv *e,
                                                                           jclass cl,
                                                                           jlong filterAddress,
                                                                           jlong filterSize,
                                                                           jint options,
                                                                           jobject error);

JNIEXPORT void JNICALL Java_io_questdb_jit_FiltersCompiler_freeFunction(JNIEnv *e, jclass cl, jlong fnAddress);

JNIEXPORT jlong JNICALL Java_io_questdb_jit_FiltersCompiler_callFunction(JNIEnv *e,
                                                                        jclass cl,
                                                                        jlong fnAddress,
                                                                        jlong colsAddress,
                                                                        jlong colsSize,
                                                                        jlong varsAddress,
                                                                        jlong varsSize,
                                                                        jlong rowsAddress,
                                                                        jlong rowsSize,
                                                                        jlong rowsStartOffset);

JNIEXPORT void JNICALL Java_io_questdb_jit_FiltersCompiler_runTests(JNIEnv *e, jclass cl);

}
namespace questdb {
    using namespace asmjit;
    using namespace asmjit::x86;

    enum class ConditionFlag {
        e,   // ==
        ne,  // !=
        l,   // <
        le,  // <=
        b,   // unsigned <
        be,  // unsigned <=
        g,   // >
        ge,  // >=
        a,   // unsigned >
        ae,  // unsigned >=
    };

    ConditionFlag operator!(ConditionFlag cond) {
        switch(cond){
            case ConditionFlag::e : return ConditionFlag::ne;
            case ConditionFlag::ne: return ConditionFlag::e ;
            case ConditionFlag::l : return ConditionFlag::ge;
            case ConditionFlag::le: return ConditionFlag::g ;
            case ConditionFlag::g : return ConditionFlag::le;
            case ConditionFlag::ge: return ConditionFlag::l ;
            case ConditionFlag::b : return ConditionFlag::ae;
            case ConditionFlag::be: return ConditionFlag::a ;
            case ConditionFlag::a : return ConditionFlag::be;
            case ConditionFlag::ae: return ConditionFlag::b ;
            default:
                __builtin_unreachable();
        }
    }

    struct Condition {
        using operand_t = std::variant<Gp, int, Mem>;

        Compiler &c;
        Gp reg;
        ConditionFlag cond;
        operand_t operand;

        Condition(Compiler &c, const Gp& reg, ConditionFlag cond, operand_t operand)
            : c(c), reg(reg), cond(cond), operand(std::move(operand)) {}

        Condition operator!() const {
            return {c, reg, !cond, operand};
        }

        void compare() const {
            switch(operand.index()){
                case 0: c.cmp(reg, std::get<Gp>(operand)); break;
                case 1: c.cmp(reg, imm(std::get<int>(operand))); break;
                case 2: c.cmp(reg, std::get<Mem>(operand)); break;
                default:
                    __builtin_unreachable();
            }
        }

        void set_byte(const Gp &dst) const {
            switch(cond){
                case ConditionFlag::e : c.sete (dst); break;
                case ConditionFlag::ne: c.setne(dst); break;
                case ConditionFlag::l : c.setl (dst); break;
                case ConditionFlag::le: c.setle(dst); break;
                case ConditionFlag::g : c.setg (dst); break;
                case ConditionFlag::ge: c.setge(dst); break;
                case ConditionFlag::b : c.setb (dst); break;
                case ConditionFlag::be: c.setbe(dst); break;
                case ConditionFlag::a : c.seta (dst); break;
                case ConditionFlag::ae: c.setae(dst); break;
                default:
                    __builtin_unreachable();
            }
        }

        void jump_to(const Label& label) const {
            switch(cond){
                case ConditionFlag::e : c.je (label); break;
                case ConditionFlag::ne: c.jne(label); break;
                case ConditionFlag::l : c.jl (label); break;
                case ConditionFlag::le: c.jle(label); break;
                case ConditionFlag::g : c.jg (label); break;
                case ConditionFlag::ge: c.jge(label); break;
                case ConditionFlag::b : c.jb (label); break;
                case ConditionFlag::be: c.jbe(label); break;
                case ConditionFlag::a : c.ja (label); break;
                case ConditionFlag::ae: c.jae(label); break;
                default:
                    __builtin_unreachable();
            }
        }
    };

    inline void jump(const Condition &cond, const Label& label) {
        cond.compare();
        cond.jump_to(label);
    }

    template<typename Fn>
    void while_do(Condition cond, Fn &&body, uint8_t unroll_factor = 1) {
        auto &c = cond.c;

        Label l_loop = c.newLabel();
        Label l_exit = c.newLabel();

        jump(!cond, l_exit);

        c.bind(l_loop);

        for (uint8_t i = 0; i < unroll_factor; ++i) {
            body();
        }

        jump(cond, l_loop);
        c.bind(l_exit);
    }

    void scalar_loop(Compiler &c, const Gp &rows_count, uint8_t unroll_factor = 1) {
        assert(unroll_factor > 0);

        Gp idx = c.newInt64("idx");
        c.xor_(idx, idx);

        Gp stop = c.newInt64("stop");
        c.mov(stop, rows_count);

        if(unroll_factor > 1) {
            c.sub(stop, unroll_factor - 1);
        }

        Condition cond(c, idx, ConditionFlag::l, stop); // idx < stop
        while_do(cond, [&] {
            for (uint8_t i = 0; i < unroll_factor; ++i) {
                //unrolled body
            }
            c.add(idx, unroll_factor);
        }, unroll_factor);

        if(unroll_factor > 1) {
            Condition tail_cond(c, idx, ConditionFlag::l, rows_count); // idx < rows_count
            while_do(tail_cond, [&] {
                //tail body

                c.add(idx, 1);
            });
        }
    }

    void simd_loop(Compiler &c, const Gp &rows_count, uint8_t simd_size, uint8_t unroll_factor = 1) {
        assert(unroll_factor > 0);

        Gp idx = c.newInt64("idx");
        c.xor_(idx, idx);

        Gp stop = c.newInt64("stop");
        c.mov(stop, rows_count);
        c.sub(stop, unroll_factor * simd_size - 1);

        Condition cond(c, idx, ConditionFlag::l, stop); // idx < stop
        while_do(cond, [&] {
            for (uint8_t i = 0; i < unroll_factor; ++i) {
                //simd body
                c.add(idx, simd_size);
            }
        }, unroll_factor);

        Condition tail_cond(c, idx, ConditionFlag::l, rows_count); // idx < rows_count
        while_do(tail_cond, [&] {
            //tail body

            c.add(idx, 1);
        });
    }
}

namespace questdb::x86 {
    using namespace asmjit;
    using namespace asmjit::x86;

    inline Gpd int32_not(Compiler &c, const Gpd &b) {
        c.not_(b);
        return b;
    }

    inline Gpd int32_and(Compiler &c, const Gpd &b1, const Gpd &b2) {
        c.and_(b1, b2);
        return b1;
    }

    inline Gpd int32_or(Compiler &c, const Gpd &b1, const Gpd &b2) {
        c.or_(b1, b2);
        return b1;
    }

    inline Gpq int32_to_int64(Compiler &c, const Gpd &rhs, bool check_null) {
        c.comment("int32_to_int64");
        Gp r = c.newInt64();
        if (!check_null) {
            c.movsxd(r, rhs);
            return r.as<Gpq>();
        }
        Gp t = c.newInt64();
        c.cmp(rhs, INT_NULL);
        c.movsxd(t, rhs);
        c.movabs(r, LONG_NULL);
        c.cmovne(r, t);
        return r.as<Gpq>();
    }

    inline Xmm int32_to_float(Compiler &c, const Gpd &rhs, bool check_null) {
        c.comment("int32_to_float");
        Xmm r = c.newXmmSs();
        if (!check_null) {
            c.vcvtsi2ss(r, r, rhs);
            return r;
        }
        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();
        Mem NaN = c.newInt32Const(asmjit::ConstPool::kScopeLocal, 0x7fc00000); // float NaN

        c.cmp(rhs, INT_NULL);
        c.je(l_null);
        c.vcvtsi2ss(r, r, rhs);
        c.jmp(l_exit);
        c.bind(l_null);
        c.vmovss(r, NaN);
        c.bind(l_exit);
        return r;
    }

    inline Xmm int32_to_double(Compiler &c, const Gpd &rhs, bool check_null) {
        c.comment("int32_to_double");
        Xmm r = c.newXmmSd();
        c.vxorps(r, r, r);
        if (!check_null) {
            c.vcvtsi2sd(r, r, rhs);
            return r;
        }
        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();
        Mem NaN = c.newInt64Const(asmjit::ConstPool::kScopeLocal, 0x7ff8000000000000LL); // double NaN

        c.cmp(rhs, INT_NULL);
        c.je(l_null);
        c.vcvtsi2sd(r, r, rhs);
        c.jmp(l_exit);
        c.bind(l_null);
        c.vmovsd(r, NaN);
        c.bind(l_exit);
        return r;
    }

    //coverage: we don't have int64 to float conversion for now
    inline Xmm int64_to_float(Compiler &c, const Gpq &rhs, bool check_null) {
        c.comment("int64_to_float");
        Xmm r = c.newXmmSs();
        if (!check_null) {
            c.vcvtsi2ss(r, r, rhs);
            return r;
        }
        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();
        Mem NaN = c.newInt32Const(asmjit::ConstPool::kScopeLocal, 0x7fc00000); // float NaN

        Gp n = c.newGpq();
        c.movabs(n, LONG_NULL);
        c.cmp(rhs, n);
        c.je(l_null);
        c.vcvtsi2ss(r, r, rhs);
        c.jmp(l_exit);
        c.bind(l_null);
        c.vmovss(r, NaN);
        c.bind(l_exit);
        return r;
    }

    inline Xmm int64_to_double(Compiler &c, const Gpq &rhs, bool check_null) {
        c.comment("int64_to_double");
        Xmm r = c.newXmmSd();
        c.vxorps(r, r, r);
        if (!check_null) {
            c.vcvtsi2sd(r, r, rhs);
            return r;
        }
        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();
        Mem NaN = c.newInt64Const(asmjit::ConstPool::kScopeLocal, 0x7ff8000000000000LL); // double NaN

        Gp n = c.newGpq();
        c.movabs(n, LONG_NULL);
        c.cmp(rhs, n);
        c.je(l_null);
        c.vcvtsi2sd(r, r, rhs);
        c.jmp(l_exit);
        c.bind(l_null);
        c.vmovsd(r, NaN);
        c.bind(l_exit);
        return r;
    }

    inline Xmm float_to_double(Compiler &c, const Xmm &rhs) {
        c.comment("float_to_double");
        Xmm r = c.newXmmSd();
        c.vxorps(r, r, r);
        c.vcvtss2sd(r, r, rhs);
        return r;
    }

    inline void check_int32_null(Compiler &c, const Gp &dst, const Gpd &lhs, const Gpd &rhs) {
        c.cmp(lhs, INT_NULL);
        c.cmove(dst, lhs);
        c.cmp(rhs, INT_NULL);
        c.cmove(dst, rhs);
    }

    inline Gpd int32_neg(Compiler &c, const Gpd &rhs, bool check_null) {
        c.comment("int32_neg");

        Gp r = c.newInt32();
        c.mov(r, rhs);
        c.neg(r);
        if (check_null) {
            Gp t = c.newInt32();
            c.mov(t, INT_NULL);
            c.cmp(rhs, t);
            c.cmove(r, t);
        }
        return r.as<Gpd>();
    }

    inline Gpq int64_neg(Compiler &c, const Gpq &rhs, bool check_null) {
        c.comment("int64_neg");

        Gp r = c.newInt64();
        c.mov(r, rhs);
        c.neg(r);
        if (check_null) {
            Gp t = c.newInt64();
            c.movabs(t, LONG_NULL);
            c.cmp(rhs, t);
            c.cmove(r, rhs);
        }
        return r.as<Gpq>();
    }

    inline Gpd int32_add(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        c.comment("int32_add");

        Gp r = c.newInt64();
        c.lea(r, ptr(lhs, rhs));
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r.as<Gpd>();
    }

    inline Gpd int32_sub(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        c.comment("int32_sub");

        Gp r = c.newInt32();
        c.mov(r, lhs);
        c.sub(r, rhs);
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r.as<Gpd>();
    }

    inline Gpd int32_mul(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        c.comment("int32_mul");

        Gp r = c.newInt32();
        c.mov(r, lhs);
        c.imul(r, rhs);
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r.as<Gpd>();
    }

    inline Gpd int32_div(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        c.comment("int32_div");

        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();

        Gp r = c.newInt32();
        Gp t = c.newInt32();

        if (!check_null) {
            c.mov(r, lhs);
            c.test(rhs, rhs);
            c.je(l_null);
            c.cdq(t, r);
            c.idiv(t, r, rhs);
            c.jmp(l_exit);
            c.bind(l_null);
            c.mov(r, INT_NULL);
            c.bind(l_exit);
            return r.as<Gpd>();
        }
        c.mov(r, INT_NULL);
        c.test(rhs, 2147483647); //INT_NULL - 1
        c.je(l_null);
        c.cmp(lhs, INT_NULL);
        c.je(l_null);
        c.mov(r, lhs);
        c.cdq(t, r);
        c.idiv(t, r, rhs);
        c.bind(l_null);
        return r.as<Gpd>();
    }

    inline void check_int64_null(Compiler &c, const Gp &dst, const Gp &lhs, const Gp &rhs) {
        c.comment("check_int64_null");
        Gp n = c.newGpq();
        c.movabs(n, LONG_NULL);
        c.cmp(lhs, n);
        c.cmove(dst, lhs);
        c.cmp(rhs, n);
        c.cmove(dst, rhs);
    }

    inline Gpq int64_add(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        c.comment("int64_add");

        Gp r = c.newInt64();
        c.lea(r, ptr(lhs, rhs));
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r.as<Gpq>();
    }

    inline Gpq int64_sub(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        c.comment("int64_sub");
        Gp r = c.newInt64();
        c.mov(r, lhs);
        c.sub(r, rhs);
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r.as<Gpq>();
    }

    inline Gpq int64_mul(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        c.comment("int64_mul");
        Gp r = c.newInt64();
        c.mov(r, lhs);
        c.imul(r, rhs);
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r.as<Gpq>();
    }

    inline Gpq int64_div(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        c.comment("int64_div");

        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();

        Gp r = c.newInt64();
        Gp t = c.newInt64();
        if (!check_null) {
            c.mov(r, lhs);
            c.test(rhs, rhs);
            c.je(l_null);
            c.cqo(t, r);
            c.idiv(t, r, rhs);
            c.jmp(l_exit);
            c.bind(l_null);
            c.movabs(r, LONG_NULL);
            c.bind(l_exit);
            return r.as<Gpq>();
        }
        c.mov(t, rhs);
        c.mov(r, lhs);
        c.btr(t, 63);
        c.test(t, t);
        c.je(l_null);
        c.movabs(t, LONG_NULL);
        c.cmp(lhs, t);
        c.je(l_null);
        c.cqo(t, r);
        c.idiv(t, r, rhs);
        c.jmp(l_exit);

        c.bind(l_null);
        c.movabs(r, LONG_NULL);
        c.bind(l_exit);
        return r.as<Gpq>();
    }

    inline Xmm float_neg(Compiler &c, const Xmm &rhs) {
        int32_t array[4] = {INT_NULL, 0, 0, 0};
        Mem mem = c.newConst(ConstPool::kScopeLocal, &array, 32);
        c.xorps(rhs, mem);
        return rhs;
    }

    inline Xmm double_neg(Compiler &c, const Xmm &rhs) {
        int32_t array[4] = {0, INT_NULL, 0, 0};
        Mem mem = c.newConst(ConstPool::kScopeLocal, &array, 32);
        c.xorpd(rhs, mem);
        return rhs;
    }

    inline Xmm float_add(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Xmm r = c.newXmmSs();
        c.vaddss(r, lhs, rhs);
        return r;
    }

    inline Xmm float_sub(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Xmm r = c.newXmmSs();
        c.vsubss(r, lhs, rhs);
        return r;
    }

    inline Xmm float_mul(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Xmm r = c.newXmmSs();
        c.vmulss(r, lhs, rhs);
        return r;
    }

    inline Xmm float_div(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Xmm r = c.newXmmSs();
        c.vdivss(r, lhs, rhs);
        return r;
    }

    inline Xmm double_add(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Xmm r = c.newXmmSd();
        c.vxorps(r, r, r);
        c.vaddsd(r, lhs, rhs);
        return r;
    }

    inline Xmm double_sub(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Xmm r = c.newXmmSd();
        c.vxorps(r, r, r);
        c.vsubsd(r, lhs, rhs);
        return r;
    }

    inline Xmm double_mul(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Xmm r = c.newXmmSd();
        c.vxorps(r, r, r);
        c.vmulsd(r, lhs, rhs);
        return r;
    }

    inline Xmm double_div(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Xmm r = c.newXmmSd();
        c.vxorps(r, r, r);
        c.vdivsd(r, lhs, rhs);
        return r;
    }

    inline Gpd int32_eq(Compiler &c, const Gpd &lhs, const Gpd &rhs) {
        Gp r = c.newInt32();
        c.xor_(r, r);
        c.cmp(lhs, rhs);
        c.sete(r.r8Lo());
        return r.as<Gpd>();
    }

    inline Gpd int32_ne(Compiler &c, const Gpd &lhs, const Gpd &rhs) {
        Gp r = c.newInt32();
        c.xor_(r, r);
        c.cmp(lhs, rhs);
        c.setne(r.r8Lo());
        return r.as<Gpd>();
    }

    inline Gpd int32_lt(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        Gp r = c.newInt32();
        c.xor_(r, r);
        if (!check_null) {
            c.cmp(lhs, rhs);
            c.setl(r.r8Lo());
            return r.as<Gpd>();
        } else {
            Gp t = c.newInt32();
            c.xor_(t, t);
            c.cmp(lhs, INT_NULL);
            c.setne(r.r8Lo());
            c.cmp(lhs, rhs);
            c.setl(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpd>();
        }
    }

    inline Gpd int32_le(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        Gp r = c.newInt32();
        c.xor_(r, r);
        if (!check_null) {
            c.cmp(lhs, rhs);
            c.setle(r.r8Lo());
            return r.as<Gpd>();
        } else {
            Gp t = c.newInt32();
            c.xor_(t, t);
            c.cmp(lhs, INT_NULL);
            c.setne(r.r8Lo());
            c.cmp(lhs, rhs);
            c.setle(t.r8Lo());
            c.and_(r, t);
            c.cmp(rhs, INT_NULL);
            c.setne(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpd>();
        }
    }

    inline Gpd int32_gt(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        Gp r = c.newInt32();
        c.xor_(r, r);
        if (!check_null) {
            c.cmp(lhs, rhs);
            c.setg(r.r8Lo());
            return r.as<Gpd>();
        } else {
            Gp t = c.newInt32();
            c.xor_(t, t);
            c.cmp(rhs, INT_NULL);
            c.setne(r.r8Lo());
            c.cmp(lhs, rhs);
            c.setg(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpd>();
        }
    }

    inline Gpd int32_ge(Compiler &c, const Gpd &lhs, const Gpd &rhs, bool check_null) {
        Gp r = c.newInt32();
        if (!check_null) {
            c.xor_(r, r);
            c.cmp(lhs, rhs);
            c.setge(r.r8Lo());
            return r.as<Gpd>();
        } else {
            Gp t = c.newInt32();
            c.xor_(r, r);
            c.cmp(lhs, INT_NULL);
            c.setne(r.r8Lo());
            c.xor_(t, t);
            c.cmp(lhs, rhs);
            c.setge(t.r8Lo());
            c.and_(r, t);
            c.cmp(rhs, INT_NULL);
            c.setne(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpd>();
        }
    }

    inline Gpq int64_eq(Compiler &c, const Gpq &lhs, const Gpq &rhs) {
        Gp r = c.newInt64();
        c.xor_(r, r);
        c.cmp(lhs, rhs);
        c.sete(r.r8Lo());
        return r.as<Gpq>();
    }

    inline Gpq int64_ne(Compiler &c, const Gpq &lhs, const Gpq &rhs) {
        Gp r = c.newInt64();
        c.xor_(r, r);
        c.cmp(lhs, rhs);
        c.setne(r.r8Lo());
        return r.as<Gpq>();
    }

    inline Gpq int64_lt(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        Gp r = c.newInt64();
        c.xor_(r, r);
        if (!check_null) {
            c.cmp(lhs, rhs);
            c.setl(r.r8Lo());
            return r.as<Gpq>();
        } else {
            Gp t = c.newInt64();
            c.xor_(t, t);
            c.movabs(r, LONG_NULL);
            c.cmp(lhs, r);
            c.setne(r.r8Lo());
            c.cmp(lhs, rhs);
            c.setl(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpq>();
        }
    }

    inline Gpq int64_le(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        Gp r = c.newInt64();
        if (!check_null) {
            c.xor_(r, r);
            c.cmp(lhs, rhs);
            c.setle(r.r8Lo());
            return r.as<Gpq>();
        } else {
            Gp t = c.newInt64();
            c.movabs(t, LONG_NULL);
            c.xor_(r, r);
            c.cmp(lhs, t);
            c.setne(r.r8Lo());
            Gp t2 = c.newInt64();
            c.xor_(t2, t2);
            c.cmp(lhs, rhs);
            c.setle(t2.r8Lo());
            c.and_(r, t2);
            c.cmp(rhs, t);
            c.setne(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpq>();
        }
    }

    inline Gpq int64_gt(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        Gp r = c.newInt64("int64_gt_r");
        c.xor_(r, r);
        if (!check_null) {
            c.cmp(lhs, rhs);
            c.setg(r.r8Lo());
            return r.as<Gpq>();
        } else {
            Gp t = c.newInt64("int64_gt_t");
            c.xor_(t, t);
            c.movabs(r, LONG_NULL);
            c.cmp(rhs, r);
            c.setne(r.r8Lo());
            c.cmp(lhs, rhs);
            c.setg(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpq>();
        }
    }

    inline Gpq int64_ge(Compiler &c, const Gpq &lhs, const Gpq &rhs, bool check_null) {
        Gp r = c.newInt64("int64_ge_r");
        c.xor_(r, r);
        if (!check_null) {
            c.cmp(lhs, rhs);
            c.setge(r.r8Lo());
            return r.as<Gpq>();
        } else {
            Gp t = c.newInt64("int64_t_ge_t");
            c.xor_(t, t);
            Gp t2 = c.newInt64("int64_t_ge_t2");
            c.xor_(t2, t2);
            c.movabs(t, LONG_NULL);
            c.cmp(lhs, t);
            c.setne(r.r8Lo());
            c.cmp(lhs, rhs);
            c.setge(t2.r8Lo());
            c.and_(r, t2);
            c.cmp(rhs, t);
            c.setne(t.r8Lo());
            c.and_(r, t);
            return r.as<Gpq>();
        }
    }

//coverage: double_cmp_epsilon used instead
//    inline Gpd double_eq(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
//        Gp r = c.newInt32();
//        c.cmpsd(lhs, rhs, Predicate::kCmpEQ);
//        c.vmovd(r, lhs);
//        c.neg(r);
//        return r.as<Gpd>();
//    }
//
//    inline Gpd double_ne(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
//        Gp r = c.newInt32();
//        c.cmpsd(lhs, rhs, Predicate::kCmpNEQ);
//        c.vmovd(r, lhs);
//        c.neg(r);
//        return r.as<Gpd>();
//    }

    inline Gpd double_lt(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpsd(lhs, rhs, Predicate::kCmpLT);
        c.vmovd(r, lhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    inline Gpd double_le(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpsd(lhs, rhs, Predicate::kCmpLE);
        c.vmovd(r, lhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    inline Gpd double_gt(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpsd(rhs, lhs, Predicate::kCmpLT);
        c.vmovd(r, rhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    inline Gpd double_ge(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpsd(rhs, lhs, Predicate::kCmpLE);
        c.vmovd(r, rhs);
        c.neg(r);
        return r.as<Gpd>();
    }

//coverage: float_cmp_epsilon used instead
//    inline Gpd float_eq(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
//        Gp r = c.newInt32();
//        c.cmpss(lhs, rhs, Predicate::kCmpEQ);
//        c.vmovd(r, lhs);
//        c.neg(r);
//        return r.as<Gpd>();
//    }
//
//    inline Gpd float_ne(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
//        Gp r = c.newInt32();
//        c.cmpss(lhs, rhs, Predicate::kCmpNEQ);
//        c.vmovd(r, lhs);
//        c.neg(r);
//        return r.as<Gpd>();
//    }

    inline Gpd float_lt(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpss(lhs, rhs, Predicate::kCmpLT);
        c.vmovd(r, lhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    inline Gpd float_le(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpss(lhs, rhs, Predicate::kCmpLE);
        c.vmovd(r, lhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    inline Gpd float_gt(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpss(rhs, lhs, Predicate::kCmpLT);
        c.vmovd(r, rhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    inline Gpd float_ge(Compiler &c, const Xmm &lhs, const Xmm &rhs) {
        Gp r = c.newInt32();
        c.cmpss(rhs, lhs, Predicate::kCmpLE);
        c.vmovd(r, rhs);
        c.neg(r);
        return r.as<Gpd>();
    }

    // (isnan(lhs) && isnan(rhs) || fabs(l - r) < 0.0000000001);
    inline Gpd double_cmp_epsilon(Compiler &c, const Xmm &xmm0, const Xmm &xmm1, double epsilon, bool eq) {
        int64_t nans[] = {0x7fffffffffffffff, 0x7fffffffffffffff}; // double NaN
        Mem nans_memory = c.newConst(ConstPool::kScopeLocal, &nans, 32);
        Mem d = c.newDoubleConst(ConstPool::kScopeLocal, epsilon);
        Label l_nan = c.newLabel();
        Label l_exit = c.newLabel();
        Gp r = c.newInt32();
        c.ucomisd(xmm0, xmm0);
        c.jnp(l_nan);
        if (eq) {
            c.mov(r.r8Lo(), 1);
        } else {
            c.xor_(r, r);
        }
        c.ucomisd(xmm1, xmm1);
        c.jnp(l_nan);
        c.jmp(l_exit);

        c.bind(l_nan);
        c.subsd(xmm0, xmm1);
        c.andpd(xmm0, nans_memory);
        c.movsd(xmm1, d);
        c.ucomisd(xmm1, xmm0);
        if (eq) {
            c.seta(r.r8Lo());
        } else {
            c.setbe(r.r8Lo());
        }
        c.bind(l_exit);
        return r.as<Gpd>();
    }

    inline Gpd double_eq_epsilon(Compiler &c, const Xmm &xmm0, const Xmm &xmm1, double epsilon) {
        return double_cmp_epsilon(c, xmm0, xmm1, epsilon, true);
    }

    inline Gpd double_ne_epsilon(Compiler &c, const Xmm &xmm0, const Xmm &xmm1, double epsilon) {
        return double_cmp_epsilon(c, xmm0, xmm1, epsilon, false);
    }

    inline Gpd float_cmp_epsilon(Compiler &c, const Xmm &xmm0, const Xmm &xmm1, float epsilon, bool eq) {
        int32_t nans[] = {0x7fffffff, 0x7fffffff, 0x7fffffff, 0x7fffffff}; // float NaN
        Mem nans_memory = c.newConst(ConstPool::kScopeLocal, &nans, 16);
        Mem d = c.newFloatConst(ConstPool::kScopeLocal, epsilon);
        Label l_nan = c.newLabel();
        Label l_exit = c.newLabel();
        Gp r = c.newInt32();
        c.ucomiss(xmm0, xmm0);
        c.jnp(l_nan);
        if (eq) {
            c.mov(r.r8Lo(), 1);
        } else {
            c.xor_(r, r);
        }
        c.ucomiss(xmm1, xmm1);
        c.jnp(l_nan);
        c.jmp(l_exit);

        c.bind(l_nan);
        c.subss(xmm0, xmm1);
        c.andps(xmm0, nans_memory);
        c.movss(xmm1, d);
        c.ucomiss(xmm1, xmm0);
        if (eq) {
            c.seta(r.r8Lo());
        } else {
            c.setbe(r.r8Lo());
        }
        c.bind(l_exit);
        return r.as<Gpd>();
    }

    inline Gpd float_eq_epsilon(Compiler &c, const Xmm &xmm0, const Xmm &xmm1, float epsilon) {
        return float_cmp_epsilon(c, xmm0, xmm1, epsilon, true);
    }

    inline Gpd float_ne_epsilon(Compiler &c, const Xmm &xmm0, const Xmm &xmm1, float epsilon) {
        return float_cmp_epsilon(c, xmm0, xmm1, epsilon, false);
    }
}

namespace questdb::avx2 {
    using namespace asmjit;
    using namespace asmjit::x86;

    inline Xmm get_low(Compiler &c, const Ymm &x) {
        return x.half();
    }

    inline Xmm get_high(Compiler &c, const Ymm &x) {
        Xmm y = c.newXmm();
        c.vextracti128(y, x, 1);
        return y;
    }

    inline Gpd to_bits32(Compiler &c, const Ymm &x) {
        //   return (uint32_t)_mm256_movemask_epi8(x);
        Gp r = c.newInt32();
        c.vpmovmskb(r, x);
        return r.as<Gpd>();
    }

    inline Gpd to_bits16(Compiler &c, const Ymm &x) {
        //    __m128i a = _mm_packs_epi16(x.get_low(), x.get_high());  // 16-bit words to bytes
        //    return (uint16_t)_mm_movemask_epi8(a);
        Gp r = c.newInt32();
        Xmm l = get_low(c, x);
        Xmm h = get_high(c, x);
        c.packsswb(l, h); // 16-bit words to bytes
        c.pmovmskb(r, l);
        c.and_(r, 0xffff);
        return r.as<Gpd>();
    }

    inline Gpd to_bits8(Compiler &c, const Ymm &x) {
        Gp r = c.newInt32();
        c.vmovmskps(r, x);
        return r.as<Gpd>();
    }

    inline Gpd to_bits4(Compiler &c, const Ymm &mask) {
        Gp r = c.newInt32();
        c.vmovmskpd(r, mask);
        return r.as<Gpd>();
    }

    inline Gpd to_bits(Compiler &c, const Ymm &mask, uint32_t step) {
        switch (step) {
            case 32:
                return to_bits32(c, mask);
            case 16:
                return to_bits16(c, mask);
            case 8:
                return to_bits8(c, mask);
            default:
                return to_bits4(c, mask);
        }
    }

    //https://stackoverflow.com/questions/36932240/avx2-what-is-the-most-efficient-way-to-pack-left-based-on-a-mask
    inline Ymm compress_register(Compiler &c, const Ymm &ymm0, const Ymm &mask) {
        c.comment("compress_register");
        x86::Gp bits = to_bits32(c, mask);
        Gp identity_indices = c.newUInt64("identity_indices");
        c.mov(identity_indices.r32(), 0x76543210);
        c.pext(identity_indices.r32(), identity_indices.r32(), bits.r32());
        Gp expanded_indices = c.newUInt64("expanded_indices");
        c.movabs(expanded_indices, 0x0F0F0F0F0F0F0F0F);
        c.pdep(identity_indices, identity_indices, expanded_indices);
        Ymm ymm1 = c.newYmm();
        c.vmovq(ymm1.xmm(), identity_indices);
        c.vpmovzxbd(ymm1, ymm1.xmm());
        c.vpermps(ymm1, ymm1, ymm0);
        return ymm1;
    }

    inline Mem vec_long_null(Compiler &c) {
        int64_t nulls[4] = {LONG_NULL, LONG_NULL, LONG_NULL, LONG_NULL};
        return c.newConst(ConstPool::kScopeLocal, &nulls, 32);
    }

    inline Mem vec_int_null(Compiler &c) {
        int32_t nulls[8] = {INT_NULL, INT_NULL, INT_NULL, INT_NULL, INT_NULL, INT_NULL, INT_NULL, INT_NULL};
        return c.newConst(ConstPool::kScopeLocal, &nulls, 32);
    }

    inline Mem vec_float_null(Compiler &c) {
        int32_t nulls[8] = {0x7fc00000, 0x7fc00000, 0x7fc00000, 0x7fc00000, 0x7fc00000, 0x7fc00000, 0x7fc00000, 0x7fc00000};
        return c.newConst(ConstPool::kScopeLocal, &nulls, 32);
    }

    inline Mem vec_double_null(Compiler &c) {
        int64_t nulls[4] = {0x7ff8000000000000LL, 0x7ff8000000000000LL, 0x7ff8000000000000LL, 0x7ff8000000000000LL};
        return c.newConst(ConstPool::kScopeLocal, &nulls, 32);
    }

    inline Mem vec_sign_mask(Compiler &c, data_type_t type) {
        switch (type) {
            case data_type_t::i8: {
                uint8_t mask[32] = {};
                memset(mask, 0x7fu, 32);
                return c.newConst(ConstPool::kScopeLocal, &mask, 32);
            }
                break;
            case data_type_t::i16: {
                uint16_t mask[16] = {0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu, 0x7fffu };
                return c.newConst(ConstPool::kScopeLocal, &mask, 32);
            }
                break;
            case data_type_t::i32:
            case data_type_t::f32: {
                uint32_t mask[] = {0x7fffffffu, 0x7fffffffu, 0x7fffffffu, 0x7fffffffu, 0x7fffffffu, 0x7fffffffu, 0x7fffffffu, 0x7fffffffu};
                return c.newConst(ConstPool::kScopeLocal, &mask, 32);
            }
                break;
            case data_type_t::i64:
            case data_type_t::f64: {
                uint64_t mask[] = {0x7fffffffffffffffu, 0x7fffffffffffffffu, 0x7fffffffffffffffu, 0x7fffffffffffffffu};
                return c.newConst(ConstPool::kScopeLocal, &mask, 32);
            }
                break;
            default:
                __builtin_unreachable();
        }
    }

    inline bool is_check_for_null(data_type_t t, bool null_check) {
        return null_check && (t == data_type_t::i32 || t == data_type_t::i64);
    }

    inline Ymm is_nan(Compiler &c, data_type_t type, const Ymm &x) {
        Ymm dst = c.newYmm();
        switch (type) {
            case data_type_t::f32:
                c.vcmpps(dst, x, x, Predicate::kCmpUNORD);
                break;
            default:
                c.vcmppd(dst, x, x, Predicate::kCmpUNORD);
                break;
        }
        return dst;
    }

    inline Ymm cmp_eq_null(Compiler &c, data_type_t type, const Ymm &x) {
        Ymm dst = c.newYmm();
        switch (type) {
            case data_type_t::i8:
            case data_type_t::i16:
                c.vpxor(dst, dst, dst);
                break;
            case data_type_t::i32:
                c.vpcmpeqd(dst, x, vec_int_null(c));
                break;
            case data_type_t::i64:
                c.vpcmpeqq(dst, x, vec_long_null(c));
                break;
            case data_type_t::f32:
            case data_type_t::f64:
                return is_nan(c, type, x);
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Ymm select_bytes(Compiler &c, const Ymm &mask, const Ymm &a, const Ymm &b) {
        Ymm dst = c.newYmm();
        c.vpblendvb(dst, a, b, mask);
        return dst;
    }

    inline Ymm select_bytes(Compiler &c, const Ymm &mask, const Ymm &a, const Mem &b) {
        Ymm dst = c.newYmm();
        c.vpblendvb(dst, a, b, mask);
        return dst;
    }

    inline Ymm mask_not(Compiler &c, const Ymm &rhs) {
        Ymm dst = c.newYmm();
        Ymm mask = c.newYmm();
        c.vpcmpeqd(mask, mask, mask);
        c.vpxor(dst, rhs, mask);
        return dst;
    }

    inline Ymm mask_and(Compiler &c, const Ymm &lhs, const Ymm &rhs) {
        Ymm dst = c.newYmm();
        c.vpand(dst, lhs, rhs);
        return dst;
    }

    inline Ymm mask_and(Compiler &c, const Ymm &lhs, const Mem &rhs) {
        Ymm dst = c.newYmm();
        c.vpand(dst, lhs, rhs);
        return dst;
    }

    inline Ymm mask_or(Compiler &c, const Ymm &lhs, const Ymm &rhs) {
        Ymm dst = c.newYmm();
        c.vpor(dst, lhs, rhs);
        return dst;
    }

    inline Ymm nulls_mask(Compiler &c, data_type_t &type, const Ymm &lhs, const Ymm &rhs) {
        Ymm lhs_nulls = cmp_eq_null(c, type, lhs);
        Ymm rhs_nulls = cmp_eq_null(c, type, rhs);
        return mask_or(c, lhs_nulls, rhs_nulls);
    }

    inline Ymm not_nulls_mask(Compiler &c, data_type_t &type, const Ymm &lhs, const Ymm &rhs) {
        return mask_not(c, nulls_mask(c, type, lhs, rhs));
    }

    inline Ymm cmp_eq(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs) {
        Ymm dst = c.newYmm();
        switch (type) {
            case data_type_t::i8: {
                c.vpcmpeqb(dst, lhs, rhs);
            }
                break;
            case data_type_t::i16: {
                c.vpcmpeqw(dst, lhs, rhs);
            }
                break;
            case data_type_t::i32: {
                c.vpcmpeqd(dst, lhs, rhs);
            }
                break;
            case data_type_t::i64: {
                c.vpcmpeqq(dst, lhs, rhs);
            }
                break;
            case data_type_t::f32: {
                Ymm nans = mask_and(c, is_nan(c, data_type_t::f32, lhs), is_nan(c, data_type_t::f32, rhs));
//                c.vcmpps(dst, lhs, rhs, Predicate::kCmpEQ);
//                c.vpor(dst, dst, nans);

                Mem sign_mask = vec_sign_mask(c, type);
                c.vsubps(lhs, lhs, rhs); //(lhs - rhs)
                c.vpand(lhs, lhs, sign_mask); // abs(lhs - rhs)
                float eps[8] = {FLOAT_EPSILON,FLOAT_EPSILON,FLOAT_EPSILON,FLOAT_EPSILON,FLOAT_EPSILON,FLOAT_EPSILON,FLOAT_EPSILON,FLOAT_EPSILON};
                Mem epsilon = c.newConst(ConstPool::kScopeLocal, &eps, 32);
                c.vcmpps(dst, lhs, epsilon, Predicate::kCmpLT);
                c.vpor(dst, dst, nans);
            }
                break;
            case data_type_t::f64: {
                Ymm nans = mask_and(c, is_nan(c, data_type_t::f64, lhs), is_nan(c, data_type_t::f64, rhs));
//                c.vcmppd(dst, lhs, rhs, Predicate::kCmpEQ);
//                c.vpor(dst, dst, nans);
                Mem sign_mask = vec_sign_mask(c, type);
                c.vsubpd(lhs, lhs, rhs); //(lhs - rhs)
                c.vpand(lhs, lhs, sign_mask); // abs(lhs - rhs)
                double eps[4] = {DOUBLE_EPSILON, DOUBLE_EPSILON, DOUBLE_EPSILON, DOUBLE_EPSILON};
                Mem epsilon = c.newConst(ConstPool::kScopeLocal, &eps, 32);
                c.vcmppd(dst, lhs, epsilon, Predicate::kCmpLT);
                c.vpor(dst, dst, nans);
            }
                break;
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Ymm cmp_ne(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs) {
        Ymm dst = c.newYmm();
        switch (type) {
            case data_type_t::f32: {
                Ymm nans = mask_and(c, is_nan(c, data_type_t::f32, lhs), is_nan(c, data_type_t::f32, rhs));
                c.vcmpps(dst, lhs, rhs, Predicate::kCmpNEQ);
                c.vpand(dst, dst, mask_not(c,nans));
            }
                break;
            case data_type_t::f64: {
                Ymm nans = mask_and(c, is_nan(c, data_type_t::f64, lhs), is_nan(c, data_type_t::f64, rhs));
                c.vcmppd(dst, lhs, rhs, Predicate::kCmpNEQ);
                c.vpand(dst, dst, mask_not(c,nans));
            }
                break;
            default:
                return mask_not(c, cmp_eq(c, type, lhs, rhs));
        }
        return dst;
    }

    inline Ymm cmp_lt(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs) {
        Ymm dst = c.newYmm();
        switch (type) {
            case data_type_t::i8:
                c.vpcmpgtb(dst, rhs, lhs);
                break;
            case data_type_t::i16:
                c.vpcmpgtw(dst, rhs, lhs);
                break;
            case data_type_t::i32:
                c.vpcmpgtd(dst, rhs, lhs);
                break;
            case data_type_t::i64:
                c.vpcmpgtq(dst, rhs, lhs);
                break;
            case data_type_t::f32:
                c.vcmpps(dst, lhs, rhs, Predicate::kCmpLT);
                break;
            case data_type_t::f64:
                c.vcmppd(dst, lhs, rhs, Predicate::kCmpLT);
                break;
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Ymm cmp_gt(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs) {
        return cmp_lt(c, type, rhs, lhs);
    }

    inline Ymm cmp_gt(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return cmp_gt(c, type, lhs, rhs);
        } else {
            Ymm r = cmp_gt(c, type, lhs, rhs);
            Ymm not_nulls = not_nulls_mask(c, type, lhs, rhs);
            return mask_and(c, r, not_nulls);
        }
    }

    inline Ymm cmp_lt(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return cmp_lt(c, type, lhs, rhs);
        } else {
            Ymm r = cmp_lt(c, type, lhs, rhs);
            Ymm not_nulls = not_nulls_mask(c, type, lhs, rhs);
            return mask_and(c, r, not_nulls);
        }
    }

    inline Ymm cmp_le(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs, bool null_check);

    inline Ymm cmp_ge(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs, bool null_check) {
        switch (type) {
            case data_type_t::f32:
            case data_type_t::f64:
                return cmp_le(c, type, rhs, lhs, null_check);
            default: {
                Ymm mask = mask_not(c, cmp_lt(c, type, lhs, rhs));
                Ymm not_nulls = not_nulls_mask(c, type, lhs, rhs);
                return mask_and(c, mask, not_nulls);
            }
        }
    }


    inline Ymm cmp_le(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs, bool null_check) {
        switch (type) {
            case data_type_t::f32: {
                Ymm dst = c.newYmm();
                c.vcmpps(dst.ymm(), lhs.ymm(), rhs.ymm(), Predicate::kCmpLE);
                return dst;
            }
            case data_type_t::f64: {
                Ymm dst = c.newYmm();
                c.vcmppd(dst.ymm(), lhs.ymm(), rhs.ymm(), Predicate::kCmpLE);
                return dst;
            }
            default:
                return cmp_ge(c, type, rhs, lhs, null_check);
        }
    }

    inline Ymm add(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs) {
        Ymm dst = c.newYmm();
        switch (type) {
            case data_type_t::i8:
                c.vpaddb(dst, lhs, rhs);
                break;
            case data_type_t::i16:
                c.vpaddw(dst, lhs, rhs);
                break;
            case data_type_t::i32:
                c.vpaddd(dst, lhs, rhs);
                break;
            case data_type_t::i64:
                c.vpaddq(dst, lhs, rhs);
                break;
            case data_type_t::f32:
                c.vaddps(dst, lhs, rhs);
                break;
            case data_type_t::f64:
                c.vaddpd(dst, lhs, rhs);
                break;
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Ymm blend_with_nulls(Compiler &c, data_type_t &type, const Ymm &t, const Ymm &lhs, const Ymm &rhs) {
        Ymm nulls_msk = nulls_mask(c, type, lhs, rhs);
        Mem nulls_const =  (type == data_type_t::i32) ? vec_int_null(c) : vec_long_null(c);
        return select_bytes(c, nulls_msk, t, nulls_const);
    }

    inline Ymm add(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return add(c, type, lhs, rhs);
        } else {
            Ymm t = add(c, type, lhs, rhs);
            return blend_with_nulls(c, type, t, lhs, rhs);
        }
    }

    inline Ymm sub(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs) {
        Ymm dst = c.newYmm();
        switch (type) {
            case data_type_t::i8:
                c.vpsubb(dst, lhs, rhs);
                break;
            case data_type_t::i16:
                c.vpsubw(dst, lhs, rhs);
                break;
            case data_type_t::i32:
                c.vpsubd(dst, lhs, rhs);
                break;
            case data_type_t::i64:
                c.vpsubq(dst, lhs, rhs);
                break;
            case data_type_t::f32:
                c.vsubps(dst, lhs, rhs);
                break;
            case data_type_t::f64:
                c.vsubpd(dst, lhs, rhs);
                break;
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Ymm sub(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return sub(c, type, lhs, rhs);
        } else {
            Ymm t = sub(c, type, lhs, rhs);
            return blend_with_nulls(c, type, t, lhs, rhs);
        }
    }

    inline Ymm mul(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs) {
        Ymm dst = c.newYmm();
        switch (type) {
            case data_type_t::i8:
                // There is no 8-bit multiply in AVX2. Split into two 16-bit multiplications
                //            __m256i aodd    = _mm256_srli_epdata_type_t::i16(a,8);              // odd numbered elements of a
                //            __m256i bodd    = _mm256_srli_epdata_type_t::i16(b,8);              // odd numbered elements of b
                //            __m256i muleven = _mm256_mullo_epdata_type_t::i16(a,b);             // product of even numbered elements
                //            __m256i mulodd  = _mm256_mullo_epdata_type_t::i16(aodd,bodd);       // product of odd  numbered elements
                //            mulodd  = _mm256_slli_epdata_type_t::i16(mulodd,8);         // put odd numbered elements back in place
                //            __m256i mask    = _mm256_set1_epdata_type_t::i32(0x00FF00FF);       // mask for even positions
                //            __m256i product = _mm256_blendv_epdata_type_t::i8(mask,muleven,mulodd);        // interleave even and odd
                //            return product
            {
                Ymm aodd = c.newYmm();
                c.vpsrlw(aodd, lhs, 8);
                Ymm bodd = c.newYmm();
                c.vpsrlw(bodd, rhs, 8);
                c.vpmullw(lhs, lhs, rhs); // muleven
                c.vpmullw(aodd, aodd, bodd); // mulodd
                c.vpsllw(aodd, aodd, 8); // mulodd
                uint8_t array[] = {255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255,
                                   0,
                                   255, 0, 255, 0, 255, 0, 255, 0, 255, 0};
                Mem c0 = c.newConst(asmjit::ConstPool::kScopeLocal, &array, 32);
                Ymm mask = c.newYmm();
                c.vmovdqa(mask, c0);
                c.vpblendvb(dst, aodd, lhs, mask);
            }
                break;
            case data_type_t::i16:
                c.vpmullw(dst, lhs, rhs);
                break;
            case data_type_t::i32:
                c.vpmulld(dst, lhs, rhs);
                break;
            case data_type_t::i64:
                //            __m256i bswap   = _mm256_shuffle_epdata_type_t::i32(b,0xB1);        // swap H<->L
                //            __m256i prodlh  = _mm256_mullo_epdata_type_t::i32(a,bswap);         // 32 bit L*H products
                //            __m256i zero    = _mm256_setzero_si256();              // 0
                //            __m256i prodlh2 = _mm256_hadd_epdata_type_t::i32(prodlh,zero);      // a0Lb0H+a0Hb0L,a1Lb1H+a1Hb1L,0,0
                //            __m256i prodlh3 = _mm256_shuffle_epdata_type_t::i32(prodlh2,0x73);  // 0, a0Lb0H+a0Hb0L, 0, a1Lb1H+a1Hb1L
                //            __m256i prodll  = _mm256_mul_epu32(a,b);               // a0Lb0L,a1Lb1L, 64 bit unsigned products
                //            __m256i prod    = _mm256_add_epdata_type_t::i64(prodll,prodlh3);    // a0Lb0L+(a0Lb0H+a0Hb0L)<<32, a1Lb1L+(a1Lb1H+a1Hb1L)<<32
                //            return  prod;
            {
                Ymm t = c.newYmm();
                c.vpshufd(t, rhs, 0xB1);
                c.vpmulld(t, t, lhs);
                Ymm z = c.newYmm();
                c.vpxor(z, z, z);
                c.vphaddd(t, t, z);
                c.vpshufd(t, t, 0x73);
                c.vpmuludq(lhs, lhs, rhs);
                c.vpaddq(dst, t, lhs);
            }
                break;
            case data_type_t::f32:
                c.vmulps(dst, lhs, rhs);
                break;
            case data_type_t::f64:
                c.vmulpd(dst, lhs, rhs);
                break;
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Ymm mul(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return mul(c, type, lhs, rhs);
        } else {
            Ymm t = mul(c, type, lhs, rhs);
            return blend_with_nulls(c, type, t, lhs, rhs);
        }
    }

    inline Ymm div_unrolled(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs) {
        Ymm dst = c.newYmm();
        switch (type) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
            case data_type_t::i64: {
                Mem lhs_m = c.newStack(32, 32);
                Mem rhs_m = c.newStack(32, 32);

                lhs_m.setSize(32);
                rhs_m.setSize(32);

                c.vmovdqu(lhs_m, lhs);
                c.vmovdqu(rhs_m, rhs);

                switch (type) {
                    case data_type_t::i8: {
                        auto size = 1;
                        auto step = 32;

                        Gp a = c.newGpd();
                        Gp b = c.newGpd();

                        lhs_m.setSize(size);
                        rhs_m.setSize(size);
                        for (int32_t i = 0; i < step; ++i) {
                            lhs_m.setOffset(i * size);
                            c.movsx(a.r32(), lhs_m);
                            rhs_m.setOffset(i * size);
                            c.movsx(b.r32(), rhs_m);
                            Gp r = x86::int32_div(c, a.r32(), b.r32(), true);
                            c.mov(lhs_m, r.r8());
                        }

                    }
                        break;
                    case data_type_t::i16: {
                        auto size = 2;
                        auto step = 16;

                        Gp a = c.newGpd();
                        Gp b = c.newGpd();

                        lhs_m.setSize(size);
                        rhs_m.setSize(size);

                        for (int32_t i = 0; i < step; ++i) {
                            lhs_m.setOffset(i * size);
                            c.movsx(a.r32(), lhs_m);
                            rhs_m.setOffset(i * size);
                            c.movsx(b.r32(), rhs_m);

                            Gp r = x86::int32_div(c, a.r32(), b.r32(), true);
                            c.mov(lhs_m, r.r16());
                        }
                    }
                        break;
                    case data_type_t::i32: {
                        auto size = 4;
                        auto step = 8;

                        Gp a = c.newGpd();
                        Gp b = c.newGpd();

                        lhs_m.setSize(size);
                        rhs_m.setSize(size);

                        for (int32_t i = 0; i < step; ++i) {
                            lhs_m.setOffset(i * size);
                            c.mov(a.r32(), lhs_m);
                            rhs_m.setOffset(i * size);
                            c.mov(b.r32(), rhs_m);
                            Gp r = x86::int32_div(c, a.r32(), b.r32(), true);
                            c.mov(lhs_m, r.r32());
                        }
                    }
                        break;
                    default: {
                        auto size = 8;
                        auto step = 4;

                        Gp a = c.newGpq();
                        Gp b = c.newGpq();

                        lhs_m.setSize(size);
                        rhs_m.setSize(size);

                        for (int32_t i = 0; i < step; ++i) {
                            lhs_m.setOffset(i * size);
                            rhs_m.setOffset(i * size);
                            c.mov(a, lhs_m);
                            c.mov(b, rhs_m);
                            Gp r = x86::int64_div(c, a.r64(), b.r64(), true);
                            c.mov(lhs_m, r);
                        }
                    }
                        break;
                }

                lhs_m.resetOffset();
                lhs_m.setSize(32);
                c.vmovdqu(dst, lhs_m);
            }
                break;
            case data_type_t::f32:
                c.vdivps(dst, lhs, rhs);
                break;
            case data_type_t::f64:
                c.vdivpd(dst, lhs, rhs);
                break;
            default:
                __builtin_unreachable();
        }
        return dst;
    }

    inline Ymm div(Compiler &c, data_type_t type, const Ymm &lhs, const Ymm &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return div_unrolled(c, type, lhs, rhs);
        } else {
            Ymm t = div_unrolled(c, type, lhs, rhs);
            return blend_with_nulls(c, type, t, lhs, rhs);
        }
    }

    inline Ymm neg(Compiler &c, data_type_t type, const Ymm &rhs) {
        Ymm zero = c.newYmm();
        c.vxorps(zero, zero, zero);
        return sub(c, type, zero, rhs);
    }

    inline Ymm neg(Compiler &c, data_type_t type, const Ymm &rhs, bool null_check) {
        if(!is_check_for_null(type, null_check)) {
            return neg(c, type, rhs);
        } else {
            Ymm r = neg(c, type, rhs);
            Ymm nulls = cmp_eq_null(c, type, rhs);
            return select_bytes(c, nulls, r, rhs);
        }
    }

    inline Ymm abs(Compiler &c, data_type_t type, const Ymm &rhs) {
        return mask_and(c, rhs, vec_sign_mask(c, type));
    }

    inline Ymm cvt_itof(Compiler &c, const Ymm &rhs, bool null_check) {
        Ymm dst = c.newYmm();
        c.vcvtdq2ps(dst, rhs);
        if(null_check) {
            Ymm int_nulls_mask = cmp_eq_null(c, data_type_t::i32, rhs);
            Ymm nans = c.newYmm();
            c.vmovups(nans, vec_float_null(c));
            return select_bytes(c, int_nulls_mask, dst, nans);
        }
        return dst;
    }

    inline Ymm cvt_ltod(Compiler &c, const Ymm &rhs, bool null_check) {
        Ymm dst = c.newYmm();

        Xmm xmm1 = c.newXmm();
        Xmm xmm2 = c.newXmm();
        Xmm xmm3 = c.newXmm();
        Xmm xmm4 = c.newXmm();
        Xmm xmm5 = c.newXmm();
        Xmm xmm6 = c.newXmm();


        c.vxorpd( xmm1, xmm1, xmm1);
        c.vxorpd( xmm2, xmm2, xmm2);
        c.vxorpd( xmm3, xmm3, xmm3);
        c.vxorpd( xmm4, xmm4, xmm4);

        Mem mem = c.newStack(32, 32);
        c.vmovdqu( mem, rhs);
        mem.setSize(8);
        c.vcvtsi2sd( xmm1, xmm1, mem);
        mem.addOffset(8);
        c.vcvtsi2sd( xmm2, xmm2, mem);
        mem.addOffset(8);
        c.vcvtsi2sd( xmm3, xmm3, mem);
        mem.addOffset(8);
        c.vcvtsi2sd( xmm4, xmm4, mem);

        c.vunpcklpd( xmm5, xmm1, xmm2);
        c.vunpcklpd( xmm6, xmm3, xmm4);
        c.vinsertf128( dst, xmm5.ymm(), xmm6, 1);
        //c.vzeroupper();
        if(null_check) {
            Ymm int_nulls_mask = cmp_eq_null(c, data_type_t::i64, rhs);
            Ymm nans = c.newYmm();
            c.vmovups(nans, vec_double_null(c));
            return select_bytes(c, int_nulls_mask, dst, nans);
        }
        return dst;
    }
}

#endif //QUESTDB_COMPILER_H
