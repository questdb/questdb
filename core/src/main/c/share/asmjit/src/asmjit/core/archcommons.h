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

#ifndef ASMJIT_CORE_ARCHCOMMONS_H_INCLUDED
#define ASMJIT_CORE_ARCHCOMMONS_H_INCLUDED

// This file provides architecture-specific classes that are required in the
// core library. For example Imm operand allows to be created from arm::Shift
// in a const-expr way, so the arm::Shift must be provided. So this header
// file provides everything architecture-specific that is used by the Core API.

#include "../core/globals.h"

// ============================================================================
// [asmjit::arm]
// ============================================================================

ASMJIT_BEGIN_SUB_NAMESPACE(arm)

//! \addtogroup asmjit_arm
//! \{

//! Represents ARM immediate shift operation type and value.
class Shift {
public:
  //! Operation predicate (ARM) describes either SHIFT or EXTEND operation.
  //!
  //! \note The constants are AsmJit specific. The first 5 values describe real
  //! constants on ARM32 and AArch64 hardware, however, the addition constants
  //! that describe extend modes are specific to AsmJit and would be translated
  //! to the AArch64 specific constants by the assembler.
  enum Op : uint32_t {
    //! Shift left logical operation (default).
    //!
    //! Available to all ARM architectures.
    kOpLSL = 0x00u,

    //! Shift right logical operation.
    //!
    //! Available to all ARM architectures.
    kOpLSR = 0x01u,

    //! Shift right arithmetic operation.
    //!
    //! Available to all ARM architectures.
    kOpASR = 0x02u,

    //! Rotate right operation.
    //!
    //! \note Not available in AArch64 mode.
    kOpROR = 0x03u,

    //! Rotate right with carry operation (encoded as `kShiftROR` with zero).
    //!
    //! \note Not available in AArch64 mode.
    kOpRRX = 0x04u,

    //! Shift left by filling low order bits with ones.
    kOpMSL = 0x05u,

    //! UXTN extend register operation (AArch64 only).
    kOpUXTB = 0x06u,
    //! UXTH extend register operation (AArch64 only).
    kOpUXTH = 0x07u,
    //! UXTW extend register operation (AArch64 only).
    kOpUXTW = 0x08u,
    //! UXTX extend register operation (AArch64 only).
    kOpUXTX = 0x09u,

    //! SXTB extend register operation (AArch64 only).
    kOpSXTB = 0x0Au,
    //! SXTH extend register operation (AArch64 only).
    kOpSXTH = 0x0Bu,
    //! SXTW extend register operation (AArch64 only).
    kOpSXTW = 0x0Cu,
    //! SXTX extend register operation (AArch64 only).
    kOpSXTX = 0x0Du

    // NOTE: 0xE and 0xF are used by memory operand to specify POST|PRE offset mode.
  };

  //! Shift operation.
  uint32_t _op;
  //! Shift Value.
  uint32_t _value;

  //! Default constructed Shift is not initialized.
  inline Shift() noexcept = default;

  //! Copy constructor (default)
  constexpr Shift(const Shift& other) noexcept = default;

  //! Constructs Shift from operation `op` and shift `value`.
  constexpr Shift(uint32_t op, uint32_t value) noexcept
    : _op(op),
      _value(value) {}

  //! Returns the shift operation.
  constexpr uint32_t op() const noexcept { return _op; }
  //! Returns the shift smount.
  constexpr uint32_t value() const noexcept { return _value; }

  //! Sets shift operation to `op`.
  inline void setOp(uint32_t op) noexcept { _op = op; }
  //! Sets shift amount to `value`.
  inline void setValue(uint32_t value) noexcept { _value = value; }
};

//! Constructs a `LSL #value` shift (logical shift left).
static constexpr Shift lsl(uint32_t value) noexcept { return Shift(Shift::kOpLSL, value); }
//! Constructs a `LSR #value` shift (logical shift right).
static constexpr Shift lsr(uint32_t value) noexcept { return Shift(Shift::kOpLSR, value); }
//! Constructs a `ASR #value` shift (arithmetic shift right).
static constexpr Shift asr(uint32_t value) noexcept { return Shift(Shift::kOpASR, value); }
//! Constructs a `ROR #value` shift (rotate right).
static constexpr Shift ror(uint32_t value) noexcept { return Shift(Shift::kOpROR, value); }
//! Constructs a `RRX` shift (rotate with carry by 1).
static constexpr Shift rrx() noexcept { return Shift(Shift::kOpRRX, 0); }
//! Constructs a `MSL #value` shift (logical shift left filling ones).
static constexpr Shift msl(uint32_t value) noexcept { return Shift(Shift::kOpMSL, value); }

//! Constructs a `UXTB #value` extend and shift (unsigned byte extend).
static constexpr Shift uxtb(uint32_t value) noexcept { return Shift(Shift::kOpUXTB, value); }
//! Constructs a `UXTH #value` extend and shift (unsigned hword extend).
static constexpr Shift uxth(uint32_t value) noexcept { return Shift(Shift::kOpUXTH, value); }
//! Constructs a `UXTW #value` extend and shift (unsigned word extend).
static constexpr Shift uxtw(uint32_t value) noexcept { return Shift(Shift::kOpUXTW, value); }
//! Constructs a `UXTX #value` extend and shift (unsigned dword extend).
static constexpr Shift uxtx(uint32_t value) noexcept { return Shift(Shift::kOpUXTX, value); }

//! Constructs a `SXTB #value` extend and shift (signed byte extend).
static constexpr Shift sxtb(uint32_t value) noexcept { return Shift(Shift::kOpSXTB, value); }
//! Constructs a `SXTH #value` extend and shift (signed hword extend).
static constexpr Shift sxth(uint32_t value) noexcept { return Shift(Shift::kOpSXTH, value); }
//! Constructs a `SXTW #value` extend and shift (signed word extend).
static constexpr Shift sxtw(uint32_t value) noexcept { return Shift(Shift::kOpSXTW, value); }
//! Constructs a `SXTX #value` extend and shift (signed dword extend).
static constexpr Shift sxtx(uint32_t value) noexcept { return Shift(Shift::kOpSXTX, value); }

//! \}

ASMJIT_END_SUB_NAMESPACE

#endif // ASMJIT_CORE_ARCHCOMMONS_H_INCLUDED
