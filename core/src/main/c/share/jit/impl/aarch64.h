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

#ifndef QUESTDB_JIT_IMPL_AARCH64_H
#define QUESTDB_JIT_IMPL_AARCH64_H

#include "consts.h"

namespace questdb::aarch64 {
    using namespace asmjit;
    using namespace asmjit::a64;

    // ARM64 cmp only supports 12-bit immediates. For larger values, load into a register first.
    inline void cmp_imm32(Compiler &c, const Gp &reg, int32_t value) {
        Gp tmp = c.new_gp32();
        c.mov(tmp, value);
        c.cmp(reg, tmp);
    }

    inline void cmp_imm64(Compiler &c, const Gp &reg, int64_t value) {
        Gp tmp = c.new_gp64();
        c.mov(tmp, value);
        c.cmp(reg, tmp);
    }

    inline Gp int32_not(Compiler &c, const Gp &b) {
        Gp r = c.new_gp32();
        c.eor(r, b, imm(1));
        return r;
    }

    inline Gp int32_and(Compiler &c, const Gp &b1, const Gp &b2) {
        Gp r = c.new_gp32();
        c.and_(r, b1, b2);
        return r;
    }

    inline Gp int32_or(Compiler &c, const Gp &b1, const Gp &b2) {
        Gp r = c.new_gp32();
        c.orr(r, b1, b2);
        return r;
    }

    inline Gp int32_to_int64(Compiler &c, const Gp &rhs, bool check_null) {
        Gp r = c.new_gp64();
        if (!check_null) {
            c.sxtw(r, rhs);
            return r;
        }
        Gp t = c.new_gp64();
        c.sxtw(t, rhs);
        c.mov(r, LONG_NULL);
        cmp_imm32(c, rhs, INT_NULL);
        c.csel(r, r, t, CondCode::kEQ);
        return r;
    }

    inline Vec int32_to_float(Compiler &c, const Gp &rhs, bool check_null) {
        Vec r = c.new_vec_s();
        if (!check_null) {
            c.scvtf(r, rhs);
            return r;
        }
        Label l_null = c.new_label();
        Label l_exit = c.new_label();

        cmp_imm32(c, rhs, INT_NULL);
        c.b_eq(l_null);
        c.scvtf(r, rhs);
        c.b(l_exit);
        c.bind(l_null);
        // float NaN = 0x7fc00000
        Gp nan_bits = c.new_gp32();
        c.mov(nan_bits, 0x7fc00000);
        c.fmov(r, nan_bits);
        c.bind(l_exit);
        return r;
    }

    inline Vec int32_to_double(Compiler &c, const Gp &rhs, bool check_null) {
        Vec r = c.new_vec_d();
        if (!check_null) {
            c.scvtf(r, rhs);
            return r;
        }
        Label l_null = c.new_label();
        Label l_exit = c.new_label();

        cmp_imm32(c, rhs, INT_NULL);
        c.b_eq(l_null);
        c.scvtf(r, rhs);
        c.b(l_exit);
        c.bind(l_null);
        // double NaN = 0x7ff8000000000000
        Gp nan_bits = c.new_gp64();
        c.mov(nan_bits, int64_t(0x7ff8000000000000LL));
        c.fmov(r, nan_bits);
        c.bind(l_exit);
        return r;
    }

    inline Vec int64_to_float(Compiler &c, const Gp &rhs, bool check_null) {
        Vec r = c.new_vec_s();
        if (!check_null) {
            c.scvtf(r, rhs);
            return r;
        }
        Label l_null = c.new_label();
        Label l_exit = c.new_label();

        Gp n = c.new_gp64();
        c.mov(n, LONG_NULL);
        c.cmp(rhs, n);
        c.b_eq(l_null);
        c.scvtf(r, rhs);
        c.b(l_exit);
        c.bind(l_null);
        Gp nan_bits = c.new_gp32();
        c.mov(nan_bits, 0x7fc00000);
        c.fmov(r, nan_bits);
        c.bind(l_exit);
        return r;
    }

    inline Vec int64_to_double(Compiler &c, const Gp &rhs, bool check_null) {
        Vec r = c.new_vec_d();
        if (!check_null) {
            c.scvtf(r, rhs);
            return r;
        }
        Label l_null = c.new_label();
        Label l_exit = c.new_label();

        Gp n = c.new_gp64();
        c.mov(n, LONG_NULL);
        c.cmp(rhs, n);
        c.b_eq(l_null);
        c.scvtf(r, rhs);
        c.b(l_exit);
        c.bind(l_null);
        Gp nan_bits = c.new_gp64();
        c.mov(nan_bits, int64_t(0x7ff8000000000000LL));
        c.fmov(r, nan_bits);
        c.bind(l_exit);
        return r;
    }

    inline Vec float_to_double(Compiler &c, const Vec &rhs) {
        Vec r = c.new_vec_d();
        c.fcvt(r, rhs);
        return r;
    }

    inline void check_int32_null(Compiler &c, const Gp &dst, const Gp &lhs, const Gp &rhs) {
        Gp null_val = c.new_gp32();
        c.mov(null_val, INT_NULL);
        c.cmp(lhs, null_val);
        c.csel(dst, null_val, dst, CondCode::kEQ);
        c.cmp(rhs, null_val);
        c.csel(dst, null_val, dst, CondCode::kEQ);
    }

    inline Gp int32_neg(Compiler &c, const Gp &rhs, bool check_null) {
        Gp r = c.new_gp32();
        c.neg(r, rhs);
        if (check_null) {
            Gp t = c.new_gp32();
            c.mov(t, INT_NULL);
            c.cmp(rhs, t);
            c.csel(r, t, r, CondCode::kEQ);
        }
        return r;
    }

    inline Gp int64_neg(Compiler &c, const Gp &rhs, bool check_null) {
        Gp r = c.new_gp64();
        c.neg(r, rhs);
        if (check_null) {
            Gp t = c.new_gp64();
            c.mov(t, LONG_NULL);
            c.cmp(rhs, t);
            c.csel(r, rhs, r, CondCode::kEQ);
        }
        return r;
    }

    inline Gp int32_add(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        Gp r = c.new_gp32();
        c.add(r, lhs, rhs);
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r;
    }

    inline Gp int32_sub(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        Gp r = c.new_gp32();
        c.sub(r, lhs, rhs);
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r;
    }

    inline Gp int32_mul(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        Gp r = c.new_gp32();
        c.mul(r, lhs, rhs);
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r;
    }

    inline Gp int32_div(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        Label l_null = c.new_label();
        Label l_exit = c.new_label();

        Gp r = c.new_gp32();

        if (!check_null) {
            c.cbz(rhs, l_null);
            c.sdiv(r, lhs, rhs);
            c.b(l_exit);
            c.bind(l_null);
            c.mov(r, INT_NULL);
            c.bind(l_exit);
            return r;
        }
        Gp t = c.new_gp32();
        c.mov(r, INT_NULL);
        c.and_(t, rhs, imm(2147483647)); // INT_NULL - 1
        c.cbz(t, l_null);
        cmp_imm32(c, lhs, INT_NULL);
        c.b_eq(l_null);
        c.sdiv(r, lhs, rhs);
        c.bind(l_null);
        return r;
    }

    inline void check_int64_null(Compiler &c, const Gp &dst, const Gp &lhs, const Gp &rhs) {
        Gp n = c.new_gp64();
        c.mov(n, LONG_NULL);
        c.cmp(lhs, n);
        c.csel(dst, n, dst, CondCode::kEQ);
        c.cmp(rhs, n);
        c.csel(dst, n, dst, CondCode::kEQ);
    }

    inline Gp int64_add(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        Gp r = c.new_gp64();
        c.add(r, lhs, rhs);
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r;
    }

    inline Gp int64_sub(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        Gp r = c.new_gp64();
        c.sub(r, lhs, rhs);
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r;
    }

    inline Gp int64_mul(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        Gp r = c.new_gp64();
        c.mul(r, lhs, rhs);
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r;
    }

    inline Gp int64_div(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        Label l_null = c.new_label();
        Label l_exit = c.new_label();

        Gp r = c.new_gp64();
        if (!check_null) {
            c.cbz(rhs, l_null);
            c.sdiv(r, lhs, rhs);
            c.b(l_exit);
            c.bind(l_null);
            c.mov(r, LONG_NULL);
            c.bind(l_exit);
            return r;
        }
        // check if rhs is LONG_NULL or 0
        Gp t = c.new_gp64();
        c.mov(r, LONG_NULL);
        // Clear bit 63 and test remaining bits
        Gp tmp = c.new_gp64();
        c.mov(tmp, rhs);
        c.and_(tmp, tmp, imm(int64_t(0x7FFFFFFFFFFFFFFFLL)));
        c.cbz(tmp, l_null);
        // Check if lhs is LONG_NULL
        c.mov(t, LONG_NULL);
        c.cmp(lhs, t);
        c.b_eq(l_null);
        c.sdiv(r, lhs, rhs);
        c.b(l_exit);

        c.bind(l_null);
        c.mov(r, LONG_NULL);
        c.bind(l_exit);
        return r;
    }

    inline Vec float_neg(Compiler &c, const Vec &rhs) {
        Vec r = c.new_vec_s();
        c.fneg(r, rhs);
        return r;
    }

    inline Vec double_neg(Compiler &c, const Vec &rhs) {
        Vec r = c.new_vec_d();
        c.fneg(r, rhs);
        return r;
    }

    inline Vec float_add(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r = c.new_vec_s();
        c.fadd(r, lhs, rhs);
        return r;
    }

    inline Vec float_sub(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r = c.new_vec_s();
        c.fsub(r, lhs, rhs);
        return r;
    }

    inline Vec float_mul(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r = c.new_vec_s();
        c.fmul(r, lhs, rhs);
        return r;
    }

    inline Vec float_div(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r = c.new_vec_s();
        c.fdiv(r, lhs, rhs);
        return r;
    }

    inline Vec double_add(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r = c.new_vec_d();
        c.fadd(r, lhs, rhs);
        return r;
    }

    inline Vec double_sub(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r = c.new_vec_d();
        c.fsub(r, lhs, rhs);
        return r;
    }

    inline Vec double_mul(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r = c.new_vec_d();
        c.fmul(r, lhs, rhs);
        return r;
    }

    inline Vec double_div(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Vec r = c.new_vec_d();
        c.fdiv(r, lhs, rhs);
        return r;
    }

    inline Gp int32_eq(Compiler &c, const Gp &lhs, const Gp &rhs) {
        Gp r = c.new_gp32();
        c.cmp(lhs, rhs);
        c.cset(r, CondCode::kEQ);
        return r;
    }

    inline Gp int32_ne(Compiler &c, const Gp &lhs, const Gp &rhs) {
        Gp r = c.new_gp32();
        c.cmp(lhs, rhs);
        c.cset(r, CondCode::kNE);
        return r;
    }

    inline Gp int32_eq_zero(Compiler &c, const Gp &lhs) {
        Gp r = c.new_gp32();
        c.tst(lhs, lhs);
        c.cset(r, CondCode::kEQ);
        return r;
    }

    inline Gp int32_ne_zero(Compiler &c, const Gp &lhs) {
        Gp r = c.new_gp32();
        c.tst(lhs, lhs);
        c.cset(r, CondCode::kNE);
        return r;
    }

    inline Gp int32_lt_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool gt, bool check_null) {
        if (!check_null) {
            Gp r = c.new_gp32();
            c.cmp(lhs, rhs);
            c.cset(r, gt ? CondCode::kGT : CondCode::kLT);
            return r;
        }
        Gp v = c.new_gp32();
        Gp l = c.new_gp32();
        Gp r = c.new_gp32();

        cmp_imm32(c, lhs, INT_NULL);
        c.cset(l, CondCode::kNE);
        cmp_imm32(c, rhs, INT_NULL);
        c.cset(r, CondCode::kNE);
        c.and_(r, r, l);
        c.cmp(lhs, rhs);
        c.cset(v, gt ? CondCode::kGT : CondCode::kLT);
        c.and_(v, v, r);
        return v;
    }

    inline Gp int32_le_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool ge, bool check_null) {
        if (!check_null) {
            Gp r = c.new_gp32();
            c.cmp(lhs, rhs);
            c.cset(r, ge ? CondCode::kGE : CondCode::kLE);
            return r;
        }
        Gp v = c.new_gp32();
        Gp l = c.new_gp32();
        Gp r = c.new_gp32();

        cmp_imm32(c, lhs, INT_NULL);
        c.cset(l, CondCode::kEQ);
        cmp_imm32(c, rhs, INT_NULL);
        c.cset(r, CondCode::kNE);
        c.eor(r, r, l);
        c.cmp(lhs, rhs);
        c.cset(v, ge ? CondCode::kGE : CondCode::kLE);
        c.and_(v, v, r);
        return v;
    }

    inline Gp int32_lt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int32_lt_gt(c, lhs, rhs, false, check_null);
    }

    inline Gp int32_le(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int32_le_ge(c, lhs, rhs, false, check_null);
    }

    inline Gp int32_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int32_lt_gt(c, lhs, rhs, true, check_null);
    }

    inline Gp int32_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int32_le_ge(c, lhs, rhs, true, check_null);
    }

    inline Gp int64_eq(Compiler &c, const Gp &lhs, const Gp &rhs) {
        Gp r = c.new_gp32();
        c.cmp(lhs, rhs);
        c.cset(r, CondCode::kEQ);
        return r;
    }

    inline Gp int64_ne(Compiler &c, const Gp &lhs, const Gp &rhs) {
        Gp r = c.new_gp32();
        c.cmp(lhs, rhs);
        c.cset(r, CondCode::kNE);
        return r;
    }

    inline Gp int64_eq_zero(Compiler &c, const Gp &lhs) {
        Gp r = c.new_gp32();
        c.tst(lhs, lhs);
        c.cset(r, CondCode::kEQ);
        return r;
    }

    inline Gp int64_ne_zero(Compiler &c, const Gp &lhs) {
        Gp r = c.new_gp32();
        c.tst(lhs, lhs);
        c.cset(r, CondCode::kNE);
        return r;
    }

    inline void int128_cmp(Compiler &c, const Vec &lhs, const Vec &rhs) {
        // Extract both 64-bit halves and compare using cmp + ccmp
        Gp lo_l = c.new_gp64();
        Gp hi_l = c.new_gp64();
        Gp lo_r = c.new_gp64();
        Gp hi_r = c.new_gp64();
        // fmov only works Gp<->D, not Gp<->Q. Use D view for low half.
        c.fmov(lo_l, lhs.d());
        c.fmov(lo_r, rhs.d());
        // Use umov to extract the upper 64-bit element (index 1)
        c.umov(hi_l, lhs.d(1));
        c.umov(hi_r, rhs.d(1));
        c.cmp(lo_l, lo_r);
        // ccmp: if lo equal, also compare hi; otherwise set flags to NE
        c.ccmp(hi_l, hi_r, imm(0), CondCode::kEQ);
    }

    inline Gp int128_eq(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        int128_cmp(c, lhs, rhs);
        c.cset(r, CondCode::kEQ);
        return r;
    }

    inline Gp int128_ne(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        int128_cmp(c, lhs, rhs);
        c.cset(r, CondCode::kNE);
        return r;
    }

    inline Gp int64_lt_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool gt, bool check_null) {
        if (!check_null) {
            Gp r = c.new_gp32();
            c.cmp(lhs, rhs);
            c.cset(r, gt ? CondCode::kGT : CondCode::kLT);
            return r;
        }
        Gp v = c.new_gp32();
        Gp l = c.new_gp32();
        Gp r = c.new_gp32();
        Gp n = c.new_gp64();

        c.mov(n, LONG_NULL);
        c.cmp(lhs, n);
        c.cset(l, CondCode::kNE);
        c.cmp(rhs, n);
        c.cset(r, CondCode::kNE);
        c.and_(r, r, l);
        c.cmp(lhs, rhs);
        c.cset(v, gt ? CondCode::kGT : CondCode::kLT);
        c.and_(v, v, r);
        return v;
    }

    inline Gp int64_le_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool ge, bool check_null) {
        if (!check_null) {
            Gp r = c.new_gp32();
            c.cmp(lhs, rhs);
            c.cset(r, ge ? CondCode::kGE : CondCode::kLE);
            return r;
        }
        Gp v = c.new_gp32();
        Gp l = c.new_gp32();
        Gp r = c.new_gp32();
        Gp n = c.new_gp64();

        c.mov(n, LONG_NULL);
        c.cmp(lhs, n);
        c.cset(l, CondCode::kEQ);
        c.cmp(rhs, n);
        c.cset(r, CondCode::kNE);
        c.eor(r, r, l);
        c.cmp(lhs, rhs);
        c.cset(v, ge ? CondCode::kGE : CondCode::kLE);
        c.and_(v, v, r);
        return v;
    }

    inline Gp int64_lt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int64_lt_gt(c, lhs, rhs, false, check_null);
    }

    inline Gp int64_le(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int64_le_ge(c, lhs, rhs, false, check_null);
    }

    inline Gp int64_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int64_lt_gt(c, lhs, rhs, true, check_null);
    }

    inline Gp int64_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int64_le_ge(c, lhs, rhs, true, check_null);
    }

    inline Gp float_lt(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.fcmp(lhs, rhs);
        c.cset(r, CondCode::kMI);  // ordered less than (false for NaN)
        return r;
    }

    inline Gp float_le(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.fcmp(lhs, rhs);
        c.cset(r, CondCode::kLS);  // ordered less or equal (false for NaN)
        return r;
    }

    inline Gp float_gt(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.fcmp(lhs, rhs);
        c.cset(r, CondCode::kGT);
        return r;
    }

    inline Gp float_ge(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.fcmp(lhs, rhs);
        c.cset(r, CondCode::kGE);
        return r;
    }

    inline Gp double_lt(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.fcmp(lhs, rhs);
        c.cset(r, CondCode::kMI);  // ordered less than (false for NaN)
        return r;
    }

    inline Gp double_le(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.fcmp(lhs, rhs);
        c.cset(r, CondCode::kLS);  // ordered less or equal (false for NaN)
        return r;
    }

    inline Gp double_gt(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.fcmp(lhs, rhs);
        c.cset(r, CondCode::kGT);
        return r;
    }

    inline Gp double_ge(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.new_gp32();
        c.fcmp(lhs, rhs);
        c.cset(r, CondCode::kGE);
        return r;
    }

    // Copy a Vec to a target Vec, converting if register types differ (e.g. S->D or D->S)
    inline void vec_copy(Compiler &c, const Vec &dst, const Vec &src) {
        if (src.reg_type() == dst.reg_type()) {
            c.fmov(dst, src);
        } else {
            c.fcvt(dst, src);  // e.g. S -> D or D -> S
        }
    }

    // fabs(l - r) < epsilon, with special handling for infinities
    inline Gp double_cmp_epsilon(Compiler &c, const Vec &xmm0, const Vec &xmm1, double epsilon, bool eq) {
        Label l_nan = c.new_label();
        Label l_exit = c.new_label();
        Gp r = c.new_gp32();

        // Work on copies — use fcvt if element sizes differ (e.g. when f32 const is reused for f64)
        Vec lhs = c.new_vec_d();
        Vec rhs = c.new_vec_d();
        vec_copy(c, lhs, xmm0);
        vec_copy(c, rhs, xmm1);

        // Check if lhs is infinity
        Gp int_r = c.new_gp64();
        c.fmov(int_r, lhs);
        Gp inf_bits = c.new_gp64();
        c.mov(inf_bits, int64_t(0x7FF0000000000000LL));
        c.and_(int_r, int_r, inf_bits);
        c.cmp(int_r, inf_bits);
        c.b_ne(l_nan);
        if (eq) {
            c.mov(r, 1);
        } else {
            c.mov(r, 0);
        }
        // Check if rhs is also infinity
        c.fmov(int_r, rhs);
        c.and_(int_r, int_r, inf_bits);
        c.cmp(int_r, inf_bits);
        c.b_ne(l_nan);
        c.b(l_exit);

        c.bind(l_nan);
        // fabs(lhs - rhs) < epsilon
        c.fsub(lhs, lhs, rhs);
        c.fabs(lhs, lhs);
        Mem eps_mem = c.new_double_const(ConstPoolScope::kLocal, epsilon);
        c.ldr(rhs, eps_mem);
        c.fcmp(rhs, lhs);
        if (eq) {
            c.cset(r, CondCode::kGT);   // epsilon > |diff| => GT (ordered greater, false for NaN)
        } else {
            c.cset(r, CondCode::kLE);   // !(epsilon > |diff|) => LE (ordered less or equal, true for NaN)
        }
        c.bind(l_exit);
        return r;
    }

    inline Gp double_eq_epsilon(Compiler &c, const Vec &xmm0, const Vec &xmm1, double epsilon) {
        return double_cmp_epsilon(c, xmm0, xmm1, epsilon, true);
    }

    inline Gp double_ne_epsilon(Compiler &c, const Vec &xmm0, const Vec &xmm1, double epsilon) {
        return double_cmp_epsilon(c, xmm0, xmm1, epsilon, false);
    }

    inline Gp float_cmp_epsilon(Compiler &c, const Vec &xmm0, const Vec &xmm1, float epsilon, bool eq) {
        Label l_nan = c.new_label();
        Label l_exit = c.new_label();
        Gp r = c.new_gp32();

        // Work on copies — use fcvt if element sizes differ
        Vec lhs = c.new_vec_s();
        Vec rhs = c.new_vec_s();
        vec_copy(c, lhs, xmm0);
        vec_copy(c, rhs, xmm1);

        // Check if lhs is infinity
        Gp int_r = c.new_gp32();
        c.fmov(int_r, lhs);
        c.and_(int_r, int_r, imm(0x7F800000));
        cmp_imm32(c, int_r, 0x7F800000);
        c.b_ne(l_nan);
        if (eq) {
            c.mov(r, 1);
        } else {
            c.mov(r, 0);
        }
        // Check if rhs is also infinity
        c.fmov(int_r, rhs);
        c.and_(int_r, int_r, imm(0x7F800000));
        cmp_imm32(c, int_r, 0x7F800000);
        c.b_ne(l_nan);
        c.b(l_exit);

        c.bind(l_nan);
        c.fsub(lhs, lhs, rhs);
        c.fabs(lhs, lhs);
        Mem eps_mem = c.new_float_const(ConstPoolScope::kLocal, epsilon);
        c.ldr(rhs, eps_mem);
        c.fcmp(rhs, lhs);
        if (eq) {
            c.cset(r, CondCode::kGT);   // ordered greater (false for NaN)
        } else {
            c.cset(r, CondCode::kLE);   // ordered less or equal (true for NaN)
        }
        c.bind(l_exit);
        return r;
    }

    inline Gp float_eq_epsilon(Compiler &c, const Vec &xmm0, const Vec &xmm1, float epsilon) {
        return float_cmp_epsilon(c, xmm0, xmm1, epsilon, true);
    }

    inline Gp float_ne_epsilon(Compiler &c, const Vec &xmm0, const Vec &xmm1, float epsilon) {
        return float_cmp_epsilon(c, xmm0, xmm1, epsilon, false);
    }

}
#endif //QUESTDB_JIT_IMPL_AARCH64_H
