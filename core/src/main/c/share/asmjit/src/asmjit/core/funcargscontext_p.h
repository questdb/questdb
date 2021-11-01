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

#ifndef ASMJIT_CORE_FUNCARGSCONTEXT_P_H_INCLUDED
#define ASMJIT_CORE_FUNCARGSCONTEXT_P_H_INCLUDED

#include "../core/archtraits.h"
#include "../core/environment.h"
#include "../core/func.h"
#include "../core/operand.h"
#include "../core/radefs_p.h"
#include "../core/support.h"

ASMJIT_BEGIN_NAMESPACE

//! \cond INTERNAL
//! \addtogroup asmjit_core
//! \{

// ============================================================================
// [TODO: Place somewhere else]
// ============================================================================

static inline RegInfo getSuitableRegForMemToMemMove(uint32_t arch, uint32_t dstTypeId, uint32_t srcTypeId) noexcept {
  const ArchTraits& archTraits = ArchTraits::byArch(arch);

  uint32_t dstSize = Type::sizeOf(dstTypeId);
  uint32_t srcSize = Type::sizeOf(srcTypeId);
  uint32_t maxSize = Support::max<uint32_t>(dstSize, srcSize);
  uint32_t regSize = Environment::registerSizeFromArch(arch);

  uint32_t signature = 0;
  if (maxSize <= regSize || (Type::isInt(dstTypeId) && Type::isInt(srcTypeId)))
    signature = maxSize <= 4 ? archTraits.regTypeToSignature(BaseReg::kTypeGp32)
                             : archTraits.regTypeToSignature(BaseReg::kTypeGp64);
  else if (maxSize <= 8 && archTraits.hasRegType(BaseReg::kTypeVec64))
    signature = archTraits.regTypeToSignature(BaseReg::kTypeVec64);
  else if (maxSize <= 16 && archTraits.hasRegType(BaseReg::kTypeVec128))
    signature = archTraits.regTypeToSignature(BaseReg::kTypeVec128);
  else if (maxSize <= 32 && archTraits.hasRegType(BaseReg::kTypeVec256))
    signature = archTraits.regTypeToSignature(BaseReg::kTypeVec256);
  else if (maxSize <= 64 && archTraits.hasRegType(BaseReg::kTypeVec512))
    signature = archTraits.regTypeToSignature(BaseReg::kTypeVec512);

  return RegInfo { signature };
}

// ============================================================================
// [asmjit::FuncArgsContext]
// ============================================================================

class FuncArgsContext {
public:
  enum VarId : uint32_t {
    kVarIdNone = 0xFF
  };

  //! Contains information about a single argument or SA register that may need shuffling.
  struct Var {
    FuncValue cur;
    FuncValue out;

    inline void init(const FuncValue& cur_, const FuncValue& out_) noexcept {
      cur = cur_;
      out = out_;
    }

    //! Reset the value to its unassigned state.
    inline void reset() noexcept {
      cur.reset();
      out.reset();
    }

    inline bool isDone() const noexcept { return cur.isDone(); }
    inline void markDone() noexcept { cur.addFlags(FuncValue::kFlagIsDone); }
  };

  struct WorkData {
    //! All allocable registers provided by the architecture.
    uint32_t _archRegs;
    //! All registers that can be used by the shuffler.
    uint32_t _workRegs;
    //! Registers used by the shuffler (all).
    uint32_t _usedRegs;
    //! Assigned registers.
    uint32_t _assignedRegs;
    //! Destination registers assigned to arguments or SA.
    uint32_t _dstRegs;
    //! Destination registers that require shuffling.
    uint32_t _dstShuf;
    //! Number of register swaps.
    uint8_t _numSwaps;
    //! Number of stack loads.
    uint8_t _numStackArgs;
    //! Reserved (only used as padding).
    uint8_t _reserved[6];
    //! Physical ID to variable ID mapping.
    uint8_t _physToVarId[32];

    inline void reset() noexcept {
      _archRegs = 0;
      _workRegs = 0;
      _usedRegs = 0;
      _assignedRegs = 0;
      _dstRegs = 0;
      _dstShuf = 0;
      _numSwaps = 0;
      _numStackArgs = 0;
      memset(_reserved, 0, sizeof(_reserved));
      memset(_physToVarId, kVarIdNone, 32);
    }

    inline bool isAssigned(uint32_t regId) const noexcept {
      ASMJIT_ASSERT(regId < 32);
      return Support::bitTest(_assignedRegs, regId);
    }

    inline void assign(uint32_t varId, uint32_t regId) noexcept {
      ASMJIT_ASSERT(!isAssigned(regId));
      ASMJIT_ASSERT(_physToVarId[regId] == kVarIdNone);

      _physToVarId[regId] = uint8_t(varId);
      _assignedRegs ^= Support::bitMask(regId);
    }

    inline void reassign(uint32_t varId, uint32_t newId, uint32_t oldId) noexcept {
      ASMJIT_ASSERT( isAssigned(oldId));
      ASMJIT_ASSERT(!isAssigned(newId));
      ASMJIT_ASSERT(_physToVarId[oldId] == varId);
      ASMJIT_ASSERT(_physToVarId[newId] == kVarIdNone);

      _physToVarId[oldId] = uint8_t(kVarIdNone);
      _physToVarId[newId] = uint8_t(varId);
      _assignedRegs ^= Support::bitMask(newId) ^ Support::bitMask(oldId);
    }

    inline void swap(uint32_t aVarId, uint32_t aRegId, uint32_t bVarId, uint32_t bRegId) noexcept {
      ASMJIT_ASSERT(isAssigned(aRegId));
      ASMJIT_ASSERT(isAssigned(bRegId));
      ASMJIT_ASSERT(_physToVarId[aRegId] == aVarId);
      ASMJIT_ASSERT(_physToVarId[bRegId] == bVarId);

      _physToVarId[aRegId] = uint8_t(bVarId);
      _physToVarId[bRegId] = uint8_t(aVarId);
    }

    inline void unassign(uint32_t varId, uint32_t regId) noexcept {
      ASMJIT_ASSERT(isAssigned(regId));
      ASMJIT_ASSERT(_physToVarId[regId] == varId);

      DebugUtils::unused(varId);
      _physToVarId[regId] = uint8_t(kVarIdNone);
      _assignedRegs ^= Support::bitMask(regId);
    }

    inline uint32_t archRegs() const noexcept { return _archRegs; }
    inline uint32_t workRegs() const noexcept { return _workRegs; }
    inline uint32_t usedRegs() const noexcept { return _usedRegs; }
    inline uint32_t assignedRegs() const noexcept { return _assignedRegs; }
    inline uint32_t dstRegs() const noexcept { return _dstRegs; }
    inline uint32_t availableRegs() const noexcept { return _workRegs & ~_assignedRegs; }
  };

  //! Architecture traits.
  const ArchTraits* _archTraits = nullptr;
  const RAConstraints* _constraints = nullptr;
  //! Architecture identifier.
  uint8_t _arch = 0;
  //! Has arguments passed via stack (SRC).
  bool _hasStackSrc = false;
  //! Has preserved frame-pointer (FP).
  bool _hasPreservedFP = false;
  //! Has arguments assigned to stack (DST).
  uint8_t _stackDstMask = 0;
  //! Register swap groups (bit-mask).
  uint8_t _regSwapsMask = 0;
  uint8_t _saVarId = kVarIdNone;
  uint32_t _varCount = 0;
  WorkData _workData[BaseReg::kGroupVirt];
  Var _vars[Globals::kMaxFuncArgs * Globals::kMaxValuePack + 1];

  FuncArgsContext() noexcept;

  inline const ArchTraits& archTraits() const noexcept { return *_archTraits; }
  inline uint32_t arch() const noexcept { return _arch; }

  inline uint32_t varCount() const noexcept { return _varCount; }
  inline size_t indexOf(const Var* var) const noexcept { return (size_t)(var - _vars); }

  inline Var& var(size_t varId) noexcept { return _vars[varId]; }
  inline const Var& var(size_t varId) const noexcept { return _vars[varId]; }

  Error initWorkData(const FuncFrame& frame, const FuncArgsAssignment& args, const RAConstraints* constraints) noexcept;
  Error markScratchRegs(FuncFrame& frame) noexcept;
  Error markDstRegsDirty(FuncFrame& frame) noexcept;
  Error markStackArgsReg(FuncFrame& frame) noexcept;
};

//! \}
//! \endcond

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_FUNCARGSCONTEXT_P_H_INCLUDED
