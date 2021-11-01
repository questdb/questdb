// AsmJit - Machine code generation for C++
//
//  * Official AsmJit Home Page: https://asmjit.com
//  * Official Github Repository: https://github.com/asmjit/asmjit
//
// Copyright (c) 2008-2020 The AsmJit Authors
//
// This software is provided 'as-is', without any express or implied
// warranty. In no event will the authors be held liable for any damages
// arising from the use of this software.
//
// Permission is granted to anyone to use this software for any purpose,
// including commercial applications, and to alter it and redistribute it
// freely, subject to the following restrictions:
//
// 1. The origin of this software must not be misrepresented; you must not
//    claim that you wrote the original software. If you use this software
//    in a product, an acknowledgment in the product documentation would be
//    appreciated but is not required.
// 2. Altered source versions must be plainly marked as such, and must not be
//    misrepresented as being the original software.
// 3. This notice may not be removed or altered from any source distribution.

#ifndef ASMJIT_X86_H_INCLUDED
#define ASMJIT_X86_H_INCLUDED

//! \addtogroup asmjit_x86
//!
//! ### Namespace
//!
//!   - \ref x86 - x86 namespace provides support for X86/X64 code generation.
//!
//! ### Emitters
//!
//!   - \ref x86::Assembler - X86/X64 assembler (must read, provides examples).
//!   - \ref x86::Builder - X86/X64 builder.
//!   - \ref x86::Compiler - X86/X64 compiler.
//!   - \ref x86::Emitter - X86/X64 emitter (abstract).
//!
//! ### Supported Instructions
//!
//!   - Emitters:
//!     - \ref x86::EmitterExplicitT - Provides all instructions that use
//!       explicit operands, provides also utility functions. The member
//!       functions provided are part of all X86 emitters.
//!     - \ref x86::EmitterImplicitT - Provides all instructions that use
//!       implicit operands, these cannot be used with \ref x86::Compiler.
//!
//!   - Instruction representation:
//!     - \ref x86::Inst::Id - instruction identifiers.
//!     - \ref x86::Inst::Options - instruction options.
//!
//! ### Register Operands
//!
//!   - \ref x86::Reg - Base class for any X86 register.
//!     - \ref x86::Gp - General purpose register:
//!       - \ref x86::GpbLo - 8-bit low register.
//!       - \ref x86::GpbHi - 8-bit high register.
//!       - \ref x86::Gpw - 16-bit register.
//!       - \ref x86::Gpd - 32-bit register.
//!       - \ref x86::Gpq - 64-bit register (X64 only).
//!     - \ref x86::Vec - Vector (SIMD) register:
//!       - \ref x86::Xmm - 128-bit SIMD register (SSE+).
//!       - \ref x86::Ymm - 256-bit SIMD register (AVX+).
//!       - \ref x86::Zmm - 512-bit SIMD register (AVX512+).
//!     - \ref x86::Mm - 64-bit MMX register.
//!     - \ref x86::St - 80-bit FPU register.
//!     - \ref x86::KReg - opmask registers (AVX512+).
//!     - \ref x86::SReg - segment register.
//!     - \ref x86::CReg - control register.
//!     - \ref x86::DReg - debug register.
//!     - \ref x86::Bnd - bound register (discontinued).
//!     - \ref x86::Rip - relative instruction pointer.
//!
//! ### Memory Operands
//!
//!   - \ref x86::Mem - X86/X64 memory operand that provides support for all
//!     X86 and X64 addressing features including absolute addresses, index
//!     scales, and segment override prefixes.
//!
//! ### Other
//!
//!   - \ref x86::Features - X86/X64 CPU features on top of \ref BaseFeatures.
//!
//! ### Status and Control Words
//!
//!   - \ref asmjit::x86::FpuWord::Status - FPU status word.
//!   - \ref asmjit::x86::FpuWord::Control - FPU control word.
//!
//! ### Predicates
//!
//!   - \ref x86::Predicate - namespace that provides X86/X64 predicates.
//!     - \ref x86::Predicate::Cmp - `CMP[PD|PS|SD|SS]` predicate (SSE+).
//!     - \ref x86::Predicate::PCmpStr - `[V]PCMP[I|E]STR[I|M]` predicate (SSE4.1+).
//!     - \ref x86::Predicate::Round - `ROUND[PD|PS|SD|SS]` predicate (SSE+).
//!     - \ref x86::Predicate::VCmp - `VCMP[PD|PS|SD|SS]` predicate (AVX+).
//!     - \ref x86::Predicate::VFixupImm - `VFIXUPIMM[PD|PS|SD|SS]` predicate (AVX512+).
//!     - \ref x86::Predicate::VFPClass - `VFPCLASS[PD|PS|SD|SS]` predicate (AVX512+).
//!     - \ref x86::Predicate::VGetMant - `VGETMANT[PD|PS|SD|SS]` predicate (AVX512+).
//!     - \ref x86::Predicate::VPCmp - `VPCMP[U][B|W|D|Q]` predicate (AVX512+).
//!     - \ref x86::Predicate::VPCom - `VPCOM[U][B|W|D|Q]` predicate (XOP).
//!     - \ref x86::Predicate::VRange - `VRANGE[PD|PS|SD|SS]` predicate (AVX512+).
//!     - \ref x86::Predicate::VReduce - `REDUCE[PD|PS|SD|SS]` predicate (AVX512+).
//!   - \ref x86::TLog - namespace that provides `VPTERNLOG[D|Q]` predicate / operations.

#include "core.h"

#include "asmjit-scope-begin.h"
#include "x86/x86assembler.h"
#include "x86/x86builder.h"
#include "x86/x86compiler.h"
#include "x86/x86emitter.h"
#include "x86/x86features.h"
#include "x86/x86globals.h"
#include "x86/x86instdb.h"
#include "x86/x86operand.h"
#include "asmjit-scope-end.h"

#endif // ASMJIT_X86_H_INCLUDED
