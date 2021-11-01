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

#ifndef ASMJIT_CORE_FUNC_H_INCLUDED
#define ASMJIT_CORE_FUNC_H_INCLUDED

#include "../core/archtraits.h"
#include "../core/environment.h"
#include "../core/operand.h"
#include "../core/type.h"
#include "../core/support.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_function
//! \{

// ============================================================================
// [asmjit::CallConv]
// ============================================================================

//! Function calling convention.
//!
//! Function calling convention is a scheme that defines how function parameters
//! are passed and how function returns its result. AsmJit defines a variety of
//! architecture and OS specific calling conventions and also provides a compile
//! time detection to make the code-generation easier.
struct CallConv {
  //! Calling convention id, see \ref Id.
  uint8_t _id;
  //! Architecture identifier, see \ref Environment::Arch.
  uint8_t _arch;
  //! Register assignment strategy, see \ref Strategy.
  uint8_t _strategy;

  //! Red zone size (AMD64 == 128 bytes).
  uint8_t _redZoneSize;
  //! Spill zone size (WIN-X64 == 32 bytes).
  uint8_t _spillZoneSize;
  //! Natural stack alignment as defined by OS/ABI.
  uint8_t _naturalStackAlignment;

  //! Flags.
  uint16_t _flags;

  //! Size to save/restore per register group.
  uint8_t _saveRestoreRegSize[BaseReg::kGroupVirt];
  //! Alignment of save/restore groups.
  uint8_t _saveRestoreAlignment[BaseReg::kGroupVirt];

  //! Mask of all passed registers, per group.
  uint32_t _passedRegs[BaseReg::kGroupVirt];
  //! Mask of all preserved registers, per group.
  uint32_t _preservedRegs[BaseReg::kGroupVirt];

  //! Internal limits of AsmJit's CallConv.
  enum Limits : uint32_t {
    //! Maximum number of register arguments per register group.
    //!
    //! \note This is not really AsmJit's limitatation, it's just the number
    //! that makes sense considering all common calling conventions. Usually
    //! even conventions that use registers to pass function arguments are
    //! limited to 8 and less arguments passed via registers per group.
    kMaxRegArgsPerGroup = 16
  };

  //! Passed registers' order.
  union RegOrder {
    //! Passed registers, ordered.
    uint8_t id[kMaxRegArgsPerGroup];
    //! Packed IDs in `uint32_t` array.
    uint32_t packed[(kMaxRegArgsPerGroup + 3) / 4];
  };

  //! Passed registers' order, per register group.
  RegOrder _passedOrder[BaseReg::kGroupVirt];

  //! Calling convention id.
  //!
  //! Calling conventions can be divided into the following groups:
  //!
  //!   - Universal - calling conventions are applicable to any target. They
  //!     will be converted to a target dependent calling convention at runtime
  //!     by \ref init(). The purpose of these conventions is to make using
  //!     functions less target dependent and closer to how they are declared
  //!     in C and C++.
  //!
  //!   - Target specific - calling conventions that are used by a particular
  //!     architecture and ABI. For example Windows 64-bit calling convention
  //!     and AMD64 SystemV calling convention.
  enum Id : uint32_t {
    //! None or invalid (can't be used).
    kIdNone = 0,

    // ------------------------------------------------------------------------
    // [Universal Calling Conventions]
    // ------------------------------------------------------------------------

    //! Standard function call or explicit `__cdecl` where it can be specified.
    //!
    //! This is a universal calling convention, which is used to initialize
    //! specific calling connventions based on architecture, platform, and its ABI.
    kIdCDecl = 1,

    //! `__stdcall` on targets that support this calling convention (X86).
    //!
    //! \note This calling convention is only supported on 32-bit X86. If used
    //! on environment that doesn't support this calling convention it will be
    //! replaced by \ref kIdCDecl.
    kIdStdCall = 2,

    //! `__fastcall` on targets that support this calling convention (X86).
    //!
    //! \note This calling convention is only supported on 32-bit X86. If used
    //! on environment that doesn't support this calling convention it will be
    //! replaced by \ref kIdCDecl.
    kIdFastCall = 3,

    //! `__vectorcall` on targets that support this calling convention (X86/X64).
    //!
    //! \note This calling convention is only supported on 32-bit and 64-bit
    //! X86 architecture on Windows platform. If used on environment that doesn't
    //! support this calling it will be replaced by \ref kIdCDecl.
    kIdVectorCall = 4,

    //! `__thiscall` on targets that support this calling convention (X86).
    //!
    //! \note This calling convention is only supported on 32-bit X86 Windows
    //! platform. If used on environment that doesn't support this calling
    //! convention it will be replaced by \ref kIdCDecl.
    kIdThisCall = 5,

    //! `__attribute__((regparm(1)))` convention (GCC and Clang).
    kIdRegParm1 = 6,
    //! `__attribute__((regparm(2)))` convention (GCC and Clang).
    kIdRegParm2 = 7,
    //! `__attribute__((regparm(3)))` convention (GCC and Clang).
    kIdRegParm3 = 8,

    //! Soft-float calling convention (ARM).
    //!
    //! Floating point arguments are passed via general purpose registers.
    kIdSoftFloat = 9,

    //! Hard-float calling convention (ARM).
    //!
    //! Floating point arguments are passed via SIMD registers.
    kIdHardFloat = 10,

    //! AsmJit specific calling convention designed for calling functions
    //! inside a multimedia code that don't use many registers internally,
    //! but are long enough to be called and not inlined. These functions are
    //! usually used to calculate trigonometric functions, logarithms, etc...
    kIdLightCall2 = 16,
    kIdLightCall3 = 17,
    kIdLightCall4 = 18,

    // ------------------------------------------------------------------------
    // [ABI-Specific Calling Conventions]
    // ------------------------------------------------------------------------

    //! X64 System-V calling convention.
    kIdX64SystemV = 32,
    //! X64 Windows calling convention.
    kIdX64Windows = 33,

    // ------------------------------------------------------------------------
    // [Host]
    // ------------------------------------------------------------------------

    //! Host calling convention detected at compile-time.
    kIdHost =
#if ASMJIT_ARCH_ARM == 32 && defined(__SOFTFP__)
      kIdSoftFloat
#elif ASMJIT_ARCH_ARM == 32 && !defined(__SOFTFP__)
      kIdHardFloat
#else
      kIdCDecl
#endif

#ifndef ASMJIT_NO_DEPRECATE
    , kIdHostCDecl = kIdCDecl
    , kIdHostStdCall = kIdStdCall
    , kIdHostFastCall = kIdFastCall
    , kIdHostLightCall2 = kIdLightCall2
    , kIdHostLightCall3 = kIdLightCall3
    , kIdHostLightCall4 = kIdLightCall4
#endif // !ASMJIT_NO_DEPRECATE
  };

  //! Strategy used to assign registers to function arguments.
  //!
  //! This is AsmJit specific. It basically describes how AsmJit should convert
  //! the function arguments defined by `FuncSignature` into register IDs and
  //! stack offsets. The default strategy `kStrategyDefault` assigns registers
  //! and then stack whereas `kStrategyWin64` strategy does register shadowing
  //! as defined by WIN64 calling convention - it applies to 64-bit calling
  //! conventions only.
  enum Strategy : uint32_t {
    //! Default register assignment strategy.
    kStrategyDefault = 0,
    //! Windows 64-bit ABI register assignment strategy.
    kStrategyX64Windows = 1,
    //! Windows 64-bit __vectorcall register assignment strategy.
    kStrategyX64VectorCall = 2,

    //! Number of assignment strategies.
    kStrategyCount = 3
  };

  //! Calling convention flags.
  enum Flags : uint32_t {
    //! Callee is responsible for cleaning up the stack.
    kFlagCalleePopsStack = 0x0001u,
    //! Pass vector arguments indirectly (as a pointer).
    kFlagIndirectVecArgs = 0x0002u,
    //! Pass F32 and F64 arguments via VEC128 register.
    kFlagPassFloatsByVec = 0x0004u,
    //! Pass MMX and vector arguments via stack if the function has variable arguments.
    kFlagPassVecByStackIfVA = 0x0008u,
    //! MMX registers are passed and returned via GP registers.
    kFlagPassMmxByGp = 0x0010u,
    //! MMX registers are passed and returned via XMM registers.
    kFlagPassMmxByXmm = 0x0020u,
    //! Calling convention can be used with variable arguments.
    kFlagVarArgCompatible = 0x0080u
  };

  //! \name Construction & Destruction
  //! \{

  //! Initializes this calling convention to the given `ccId` based on the
  //! `environment`.
  //!
  //! See \ref Id and \ref Environment for more details.
  ASMJIT_API Error init(uint32_t ccId, const Environment& environment) noexcept;

  //! Resets this CallConv struct into a defined state.
  //!
  //! It's recommended to reset the \ref CallConv struct in case you would
  //! like create a custom calling convention as it prevents from using an
  //! uninitialized data (CallConv doesn't have a constructor that would
  //! initialize it, it's just a struct).
  inline void reset() noexcept {
    memset(this, 0, sizeof(*this));
    memset(_passedOrder, 0xFF, sizeof(_passedOrder));
  }

  //! \}

  //! \name Accessors
  //! \{

  //! Returns the calling convention id, see `Id`.
  inline uint32_t id() const noexcept { return _id; }
  //! Sets the calling convention id, see `Id`.
  inline void setId(uint32_t id) noexcept { _id = uint8_t(id); }

  //! Returns the calling function architecture id.
  inline uint32_t arch() const noexcept { return _arch; }
  //! Sets the calling function architecture id.
  inline void setArch(uint32_t arch) noexcept { _arch = uint8_t(arch); }

  //! Returns the strategy used to assign registers to arguments, see `Strategy`.
  inline uint32_t strategy() const noexcept { return _strategy; }
  //! Sets the strategy used to assign registers to arguments, see `Strategy`.
  inline void setStrategy(uint32_t strategy) noexcept { _strategy = uint8_t(strategy); }

  //! Tests whether the calling convention has the given `flag` set.
  inline bool hasFlag(uint32_t flag) const noexcept { return (uint32_t(_flags) & flag) != 0; }
  //! Returns the calling convention flags, see `Flags`.
  inline uint32_t flags() const noexcept { return _flags; }
  //! Adds the calling convention flags, see `Flags`.
  inline void setFlags(uint32_t flag) noexcept { _flags = uint16_t(flag); };
  //! Adds the calling convention flags, see `Flags`.
  inline void addFlags(uint32_t flags) noexcept { _flags = uint16_t(_flags | flags); };

  //! Tests whether this calling convention specifies 'RedZone'.
  inline bool hasRedZone() const noexcept { return _redZoneSize != 0; }
  //! Tests whether this calling convention specifies 'SpillZone'.
  inline bool hasSpillZone() const noexcept { return _spillZoneSize != 0; }

  //! Returns size of 'RedZone'.
  inline uint32_t redZoneSize() const noexcept { return _redZoneSize; }
  //! Returns size of 'SpillZone'.
  inline uint32_t spillZoneSize() const noexcept { return _spillZoneSize; }

  //! Sets size of 'RedZone'.
  inline void setRedZoneSize(uint32_t size) noexcept { _redZoneSize = uint8_t(size); }
  //! Sets size of 'SpillZone'.
  inline void setSpillZoneSize(uint32_t size) noexcept { _spillZoneSize = uint8_t(size); }

  //! Returns a natural stack alignment.
  inline uint32_t naturalStackAlignment() const noexcept { return _naturalStackAlignment; }
  //! Sets a natural stack alignment.
  //!
  //! This function can be used to override the default stack alignment in case
  //! that you know that it's alignment is different. For example it allows to
  //! implement custom calling conventions that guarantee higher stack alignment.
  inline void setNaturalStackAlignment(uint32_t value) noexcept { _naturalStackAlignment = uint8_t(value); }

  //! Returns the size of a register (or its part) to be saved and restored of the given `group`.
  inline uint32_t saveRestoreRegSize(uint32_t group) const noexcept { return _saveRestoreRegSize[group]; }
  //! Sets the size of a vector register (or its part) to be saved and restored.
  inline void setSaveRestoreRegSize(uint32_t group, uint32_t size) noexcept { _saveRestoreRegSize[group] = uint8_t(size); }

  //! Returns the alignment of a save-restore area of the given `group`.
  inline uint32_t saveRestoreAlignment(uint32_t group) const noexcept { return _saveRestoreAlignment[group]; }
  //! Sets the alignment of a save-restore area of the given `group`.
  inline void setSaveRestoreAlignment(uint32_t group, uint32_t alignment) noexcept { _saveRestoreAlignment[group] = uint8_t(alignment); }

  //! Returns the order of passed registers of the given `group`, see \ref BaseReg::RegGroup.
  inline const uint8_t* passedOrder(uint32_t group) const noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    return _passedOrder[group].id;
  }

  //! Returns the mask of passed registers of the given `group`, see \ref BaseReg::RegGroup.
  inline uint32_t passedRegs(uint32_t group) const noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    return _passedRegs[group];
  }

  inline void _setPassedPacked(uint32_t group, uint32_t p0, uint32_t p1, uint32_t p2, uint32_t p3) noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);

    _passedOrder[group].packed[0] = p0;
    _passedOrder[group].packed[1] = p1;
    _passedOrder[group].packed[2] = p2;
    _passedOrder[group].packed[3] = p3;
  }

  //! Resets the order and mask of passed registers.
  inline void setPassedToNone(uint32_t group) noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);

    _setPassedPacked(group, 0xFFFFFFFFu, 0xFFFFFFFFu, 0xFFFFFFFFu, 0xFFFFFFFFu);
    _passedRegs[group] = 0u;
  }

  //! Sets the order and mask of passed registers.
  inline void setPassedOrder(uint32_t group, uint32_t a0, uint32_t a1 = 0xFF, uint32_t a2 = 0xFF, uint32_t a3 = 0xFF, uint32_t a4 = 0xFF, uint32_t a5 = 0xFF, uint32_t a6 = 0xFF, uint32_t a7 = 0xFF) noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);

    // NOTE: This should always be called with all arguments known at compile time,
    // so even if it looks scary it should be translated into few instructions.
    _setPassedPacked(group, Support::bytepack32_4x8(a0, a1, a2, a3),
                            Support::bytepack32_4x8(a4, a5, a6, a7),
                            0xFFFFFFFFu,
                            0xFFFFFFFFu);

    _passedRegs[group] = (a0 != 0xFF ? 1u << a0 : 0u) |
                         (a1 != 0xFF ? 1u << a1 : 0u) |
                         (a2 != 0xFF ? 1u << a2 : 0u) |
                         (a3 != 0xFF ? 1u << a3 : 0u) |
                         (a4 != 0xFF ? 1u << a4 : 0u) |
                         (a5 != 0xFF ? 1u << a5 : 0u) |
                         (a6 != 0xFF ? 1u << a6 : 0u) |
                         (a7 != 0xFF ? 1u << a7 : 0u) ;
  }

  //! Returns preserved register mask of the given `group`, see \ref BaseReg::RegGroup.
  inline uint32_t preservedRegs(uint32_t group) const noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    return _preservedRegs[group];
  }

  //! Sets preserved register mask of the given `group`, see \ref BaseReg::RegGroup.
  inline void setPreservedRegs(uint32_t group, uint32_t regs) noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    _preservedRegs[group] = regs;
  }

  //! \}
};

// ============================================================================
// [asmjit::FuncSignature]
// ============================================================================

//! Function signature.
//!
//! Contains information about function return type, count of arguments and
//! their TypeIds. Function signature is a low level structure which doesn't
//! contain platform specific or calling convention specific information.
struct FuncSignature {
  //! Calling convention id.
  uint8_t _callConv;
  //! Count of arguments.
  uint8_t _argCount;
  //! Index of a first VA or `kNoVarArgs`.
  uint8_t _vaIndex;
  //! Return value TypeId.
  uint8_t _ret;
  //! Function arguments TypeIds.
  const uint8_t* _args;

  enum : uint8_t {
    //! Doesn't have variable number of arguments (`...`).
    kNoVarArgs = 0xFF
  };

  //! \name Initializtion & Reset
  //! \{

  //! Initializes the function signature.
  inline void init(uint32_t ccId, uint32_t vaIndex, uint32_t ret, const uint8_t* args, uint32_t argCount) noexcept {
    ASMJIT_ASSERT(ccId <= 0xFF);
    ASMJIT_ASSERT(argCount <= 0xFF);

    _callConv = uint8_t(ccId);
    _argCount = uint8_t(argCount);
    _vaIndex = uint8_t(vaIndex);
    _ret = uint8_t(ret);
    _args = args;
  }

  inline void reset() noexcept { memset(this, 0, sizeof(*this)); }

  //! \}

  //! \name Accessors
  //! \{

  //! Returns the calling convention.
  inline uint32_t callConv() const noexcept { return _callConv; }
  //! Sets the calling convention to `ccId`;
  inline void setCallConv(uint32_t ccId) noexcept { _callConv = uint8_t(ccId); }

  //! Tests whether the function has variable number of arguments (...).
  inline bool hasVarArgs() const noexcept { return _vaIndex != kNoVarArgs; }
  //! Returns the variable arguments (...) index, `kNoVarArgs` if none.
  inline uint32_t vaIndex() const noexcept { return _vaIndex; }
  //! Sets the variable arguments (...) index to `index`.
  inline void setVaIndex(uint32_t index) noexcept { _vaIndex = uint8_t(index); }
  //! Resets the variable arguments index (making it a non-va function).
  inline void resetVaIndex() noexcept { _vaIndex = kNoVarArgs; }

  //! Returns the number of function arguments.
  inline uint32_t argCount() const noexcept { return _argCount; }

  inline bool hasRet() const noexcept { return _ret != Type::kIdVoid; }
  //! Returns the return value type.
  inline uint32_t ret() const noexcept { return _ret; }

  //! Returns the type of the argument at index `i`.
  inline uint32_t arg(uint32_t i) const noexcept {
    ASMJIT_ASSERT(i < _argCount);
    return _args[i];
  }
  //! Returns the array of function arguments' types.
  inline const uint8_t* args() const noexcept { return _args; }

  //! \}
};

// ============================================================================
// [asmjit::FuncSignatureT]
// ============================================================================

template<typename... RET_ARGS>
class FuncSignatureT : public FuncSignature {
public:
  inline FuncSignatureT(uint32_t ccId = CallConv::kIdHost, uint32_t vaIndex = kNoVarArgs) noexcept {
    static const uint8_t ret_args[] = { (uint8_t(Type::IdOfT<RET_ARGS>::kTypeId))... };
    init(ccId, vaIndex, ret_args[0], ret_args + 1, uint32_t(ASMJIT_ARRAY_SIZE(ret_args) - 1));
  }
};

// ============================================================================
// [asmjit::FuncSignatureBuilder]
// ============================================================================

//! Function signature builder.
class FuncSignatureBuilder : public FuncSignature {
public:
  uint8_t _builderArgList[Globals::kMaxFuncArgs];

  //! \name Initializtion & Reset
  //! \{

  inline FuncSignatureBuilder(uint32_t ccId = CallConv::kIdHost, uint32_t vaIndex = kNoVarArgs) noexcept {
    init(ccId, vaIndex, Type::kIdVoid, _builderArgList, 0);
  }

  //! \}

  //! \name Accessors
  //! \{

  //! Sets the return type to `retType`.
  inline void setRet(uint32_t retType) noexcept { _ret = uint8_t(retType); }
  //! Sets the return type based on `T`.
  template<typename T>
  inline void setRetT() noexcept { setRet(Type::IdOfT<T>::kTypeId); }

  //! Sets the argument at index `index` to `argType`.
  inline void setArg(uint32_t index, uint32_t argType) noexcept {
    ASMJIT_ASSERT(index < _argCount);
    _builderArgList[index] = uint8_t(argType);
  }
  //! Sets the argument at index `i` to the type based on `T`.
  template<typename T>
  inline void setArgT(uint32_t index) noexcept { setArg(index, Type::IdOfT<T>::kTypeId); }

  //! Appends an argument of `type` to the function prototype.
  inline void addArg(uint32_t type) noexcept {
    ASMJIT_ASSERT(_argCount < Globals::kMaxFuncArgs);
    _builderArgList[_argCount++] = uint8_t(type);
  }
  //! Appends an argument of type based on `T` to the function prototype.
  template<typename T>
  inline void addArgT() noexcept { addArg(Type::IdOfT<T>::kTypeId); }

  //! \}
};

// ============================================================================
// [asmjit::FuncValue]
// ============================================================================

//! Argument or return value (or its part) as defined by `FuncSignature`, but
//! with register or stack address (and other metadata) assigned.
struct FuncValue {
  uint32_t _data;

  enum Parts : uint32_t {
    kTypeIdShift      = 0,             //!< TypeId shift.
    kTypeIdMask       = 0x000000FFu,   //!< TypeId mask.

    kFlagIsReg        = 0x00000100u,   //!< Passed by register.
    kFlagIsStack      = 0x00000200u,   //!< Passed by stack.
    kFlagIsIndirect   = 0x00000400u,   //!< Passed indirectly by reference (internally a pointer).
    kFlagIsDone       = 0x00000800u,   //!< Used internally by arguments allocator.

    kStackOffsetShift = 12,            //!< Stack offset shift.
    kStackOffsetMask  = 0xFFFFF000u,   //!< Stack offset mask (must occupy MSB bits).

    kRegIdShift       = 16,            //!< RegId shift.
    kRegIdMask        = 0x00FF0000u,   //!< RegId mask.

    kRegTypeShift     = 24,            //!< RegType shift.
    kRegTypeMask      = 0xFF000000u    //!< RegType mask.
  };

  //! \name Initializtion & Reset
  //! \{

  // These initialize the whole `FuncValue` to either register or stack. Useful
  // when you know all of these properties and wanna just set it up.

  //! Initializes the `typeId` of this `FuncValue`.
  inline void initTypeId(uint32_t typeId) noexcept {
    _data = typeId << kTypeIdShift;
  }

  inline void initReg(uint32_t regType, uint32_t regId, uint32_t typeId, uint32_t flags = 0) noexcept {
    _data = (regType << kRegTypeShift) | (regId << kRegIdShift) | (typeId << kTypeIdShift) | kFlagIsReg | flags;
  }

  inline void initStack(int32_t offset, uint32_t typeId) noexcept {
    _data = (uint32_t(offset) << kStackOffsetShift) | (typeId << kTypeIdShift) | kFlagIsStack;
  }

  //! Resets the value to its unassigned state.
  inline void reset() noexcept { _data = 0; }

  //! \}

  //! \name Assign
  //! \{

  // These initialize only part of `FuncValue`, useful when building `FuncValue`
  // incrementally. The caller should first init the type-id by caliing `initTypeId`
  // and then continue building either register or stack.

  inline void assignRegData(uint32_t regType, uint32_t regId) noexcept {
    ASMJIT_ASSERT((_data & (kRegTypeMask | kRegIdMask)) == 0);
    _data |= (regType << kRegTypeShift) | (regId << kRegIdShift) | kFlagIsReg;
  }

  inline void assignStackOffset(int32_t offset) noexcept {
    ASMJIT_ASSERT((_data & kStackOffsetMask) == 0);
    _data |= (uint32_t(offset) << kStackOffsetShift) | kFlagIsStack;
  }

  //! \}

  //! \name Accessors
  //! \{

  inline explicit operator bool() const noexcept { return _data != 0; }

  inline void _replaceValue(uint32_t mask, uint32_t value) noexcept { _data = (_data & ~mask) | value; }

  //! Tests whether the `FuncValue` has a flag `flag` set.
  inline bool hasFlag(uint32_t flag) const noexcept { return (_data & flag) != 0; }
  //! Adds `flags` to `FuncValue`.
  inline void addFlags(uint32_t flags) noexcept { _data |= flags; }
  //! Clears `flags` of `FuncValue`.
  inline void clearFlags(uint32_t flags) noexcept { _data &= ~flags; }

  //! Tests whether the value is initialized (i.e. contains a valid data).
  inline bool isInitialized() const noexcept { return _data != 0; }
  //! Tests whether the argument is passed by register.
  inline bool isReg() const noexcept { return hasFlag(kFlagIsReg); }
  //! Tests whether the argument is passed by stack.
  inline bool isStack() const noexcept { return hasFlag(kFlagIsStack); }
  //! Tests whether the argument is passed by register.
  inline bool isAssigned() const noexcept { return hasFlag(kFlagIsReg | kFlagIsStack); }
  //! Tests whether the argument is passed through a pointer (used by WIN64 to pass XMM|YMM|ZMM).
  inline bool isIndirect() const noexcept { return hasFlag(kFlagIsIndirect); }

  //! Tests whether the argument was already processed (used internally).
  inline bool isDone() const noexcept { return hasFlag(kFlagIsDone); }

  //! Returns a register type of the register used to pass function argument or return value.
  inline uint32_t regType() const noexcept { return (_data & kRegTypeMask) >> kRegTypeShift; }
  //! Sets a register type of the register used to pass function argument or return value.
  inline void setRegType(uint32_t regType) noexcept { _replaceValue(kRegTypeMask, regType << kRegTypeShift); }

  //! Returns a physical id of the register used to pass function argument or return value.
  inline uint32_t regId() const noexcept { return (_data & kRegIdMask) >> kRegIdShift; }
  //! Sets a physical id of the register used to pass function argument or return value.
  inline void setRegId(uint32_t regId) noexcept { _replaceValue(kRegIdMask, regId << kRegIdShift); }

  //! Returns a stack offset of this argument.
  inline int32_t stackOffset() const noexcept { return int32_t(_data & kStackOffsetMask) >> kStackOffsetShift; }
  //! Sets a stack offset of this argument.
  inline void setStackOffset(int32_t offset) noexcept { _replaceValue(kStackOffsetMask, uint32_t(offset) << kStackOffsetShift); }

  //! Tests whether the argument or return value has associated `Type::Id`.
  inline bool hasTypeId() const noexcept { return (_data & kTypeIdMask) != 0; }
  //! Returns a TypeId of this argument or return value.
  inline uint32_t typeId() const noexcept { return (_data & kTypeIdMask) >> kTypeIdShift; }
  //! Sets a TypeId of this argument or return value.
  inline void setTypeId(uint32_t typeId) noexcept { _replaceValue(kTypeIdMask, typeId << kTypeIdShift); }

  //! \}
};

// ============================================================================
// [asmjit::FuncValuePack]
// ============================================================================

//! Contains multiple `FuncValue` instances in an array so functions that use
//! multiple registers for arguments or return values can represent all inputs
//! and outputs.
struct FuncValuePack {
public:
  //! Values data.
  FuncValue _values[Globals::kMaxValuePack];

  inline void reset() noexcept {
    for (size_t i = 0; i < Globals::kMaxValuePack; i++)
      _values[i].reset();
  }

  //! Calculates how many values are in the pack, checking for non-values
  //! from the end.
  inline uint32_t count() const noexcept {
    uint32_t n = Globals::kMaxValuePack;
    while (n && !_values[n - 1])
      n--;
    return n;
  }

  inline FuncValue* values() noexcept { return _values; }
  inline const FuncValue* values() const noexcept { return _values; }

  inline void resetValue(size_t index) noexcept {
    ASMJIT_ASSERT(index < Globals::kMaxValuePack);
    _values[index].reset();
  }

  inline bool hasValue(size_t index) noexcept {
    ASMJIT_ASSERT(index < Globals::kMaxValuePack);
    return _values[index].isInitialized();
  }

  inline void assignReg(size_t index, const BaseReg& reg, uint32_t typeId = Type::kIdVoid) noexcept {
    ASMJIT_ASSERT(index < Globals::kMaxValuePack);
    ASMJIT_ASSERT(reg.isPhysReg());
    _values[index].initReg(reg.type(), reg.id(), typeId);
  }

  inline void assignReg(size_t index, uint32_t regType, uint32_t regId, uint32_t typeId = Type::kIdVoid) noexcept {
    ASMJIT_ASSERT(index < Globals::kMaxValuePack);
    _values[index].initReg(regType, regId, typeId);
  }

  inline void assignStack(size_t index, int32_t offset, uint32_t typeId = Type::kIdVoid) noexcept {
    ASMJIT_ASSERT(index < Globals::kMaxValuePack);
    _values[index].initStack(offset, typeId);
  }

  inline FuncValue& operator[](size_t index) {
    ASMJIT_ASSERT(index < Globals::kMaxValuePack);
    return _values[index];
  }

  inline const FuncValue& operator[](size_t index) const {
    ASMJIT_ASSERT(index < Globals::kMaxValuePack);
    return _values[index];
  }
};

// ============================================================================
// [asmjit::FuncDetail]
// ============================================================================

//! Function detail - CallConv and expanded FuncSignature.
//!
//! Function detail is architecture and OS dependent representation of a function.
//! It contains calling convention and expanded function signature so all
//! arguments have assigned either register type & id or stack address.
class FuncDetail {
public:
  //! Calling convention.
  CallConv _callConv;
  //! Number of function arguments.
  uint8_t _argCount;
  //! Variable arguments index of `kNoVarArgs`.
  uint8_t _vaIndex;
  //! Reserved for future use.
  uint16_t _reserved;
  //! Registers that contains arguments.
  uint32_t _usedRegs[BaseReg::kGroupVirt];
  //! Size of arguments passed by stack.
  uint32_t _argStackSize;
  //! Function return value(s).
  FuncValuePack _rets;
  //! Function arguments.
  FuncValuePack _args[Globals::kMaxFuncArgs];

  enum : uint8_t {
    //! Doesn't have variable number of arguments (`...`).
    kNoVarArgs = 0xFF
  };

  //! \name Construction & Destruction
  //! \{

  inline FuncDetail() noexcept { reset(); }
  inline FuncDetail(const FuncDetail& other) noexcept = default;

  //! Initializes this `FuncDetail` to the given signature.
  ASMJIT_API Error init(const FuncSignature& signature, const Environment& environment) noexcept;
  inline void reset() noexcept { memset(this, 0, sizeof(*this)); }

  //! \}

  //! \name Accessors
  //! \{

  //! Returns the function's calling convention, see `CallConv`.
  inline const CallConv& callConv() const noexcept { return _callConv; }

  //! Returns the associated calling convention flags, see `CallConv::Flags`.
  inline uint32_t flags() const noexcept { return _callConv.flags(); }
  //! Checks whether a CallConv `flag` is set, see `CallConv::Flags`.
  inline bool hasFlag(uint32_t ccFlag) const noexcept { return _callConv.hasFlag(ccFlag); }

  //! Tests whether the function has a return value.
  inline bool hasRet() const noexcept { return bool(_rets[0]); }
  //! Returns the number of function arguments.
  inline uint32_t argCount() const noexcept { return _argCount; }

  //! Returns function return values.
  inline FuncValuePack& retPack() noexcept { return _rets; }
  //! Returns function return values.
  inline const FuncValuePack& retPack() const noexcept { return _rets; }

  //! Returns a function return value associated with the given `valueIndex`.
  inline FuncValue& ret(size_t valueIndex = 0) noexcept { return _rets[valueIndex]; }
  //! Returns a function return value associated with the given `valueIndex` (const).
  inline const FuncValue& ret(size_t valueIndex = 0) const noexcept { return _rets[valueIndex]; }

  //! Returns function argument packs array.
  inline FuncValuePack* argPacks() noexcept { return _args; }
  //! Returns function argument packs array (const).
  inline const FuncValuePack* argPacks() const noexcept { return _args; }

  //! Returns function argument pack at the given `argIndex`.
  inline FuncValuePack& argPack(size_t argIndex) noexcept {
    ASMJIT_ASSERT(argIndex < Globals::kMaxFuncArgs);
    return _args[argIndex];
  }

  //! Returns function argument pack at the given `argIndex` (const).
  inline const FuncValuePack& argPack(size_t argIndex) const noexcept {
    ASMJIT_ASSERT(argIndex < Globals::kMaxFuncArgs);
    return _args[argIndex];
  }

  //! Returns an argument at `valueIndex` from the argument pack at the given `argIndex`.
  inline FuncValue& arg(size_t argIndex, size_t valueIndex = 0) noexcept {
    ASMJIT_ASSERT(argIndex < Globals::kMaxFuncArgs);
    return _args[argIndex][valueIndex];
  }

  //! Returns an argument at `valueIndex` from the argument pack at the given `argIndex` (const).
  inline const FuncValue& arg(size_t argIndex, size_t valueIndex = 0) const noexcept {
    ASMJIT_ASSERT(argIndex < Globals::kMaxFuncArgs);
    return _args[argIndex][valueIndex];
  }

  //! Resets an argument at the given `argIndex`.
  //!
  //! If the argument is a parameter pack (has multiple values) all values are reset.
  inline void resetArg(size_t argIndex) noexcept {
    ASMJIT_ASSERT(argIndex < Globals::kMaxFuncArgs);
    _args[argIndex].reset();
  }

  //! Tests whether the function has variable arguments.
  inline bool hasVarArgs() const noexcept { return _vaIndex != kNoVarArgs; }
  //! Returns an index of a first variable argument.
  inline uint32_t vaIndex() const noexcept { return _vaIndex; }

  //! Tests whether the function passes one or more argument by stack.
  inline bool hasStackArgs() const noexcept { return _argStackSize != 0; }
  //! Returns stack size needed for function arguments passed on the stack.
  inline uint32_t argStackSize() const noexcept { return _argStackSize; }

  //! Returns red zone size.
  inline uint32_t redZoneSize() const noexcept { return _callConv.redZoneSize(); }
  //! Returns spill zone size.
  inline uint32_t spillZoneSize() const noexcept { return _callConv.spillZoneSize(); }
  //! Returns natural stack alignment.
  inline uint32_t naturalStackAlignment() const noexcept { return _callConv.naturalStackAlignment(); }

  //! Returns a mask of all passed registers of the given register `group`.
  inline uint32_t passedRegs(uint32_t group) const noexcept { return _callConv.passedRegs(group); }
  //! Returns a mask of all preserved registers of the given register `group`.
  inline uint32_t preservedRegs(uint32_t group) const noexcept { return _callConv.preservedRegs(group); }

  //! Returns a mask of all used registers of the given register `group`.
  inline uint32_t usedRegs(uint32_t group) const noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    return _usedRegs[group];
  }

  //! Adds `regs` to the mask of used registers of the given register `group`.
  inline void addUsedRegs(uint32_t group, uint32_t regs) noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    _usedRegs[group] |= regs;
  }

  //! \}
};

// ============================================================================
// [asmjit::FuncFrame]
// ============================================================================

//! Function frame.
//!
//! Function frame is used directly by prolog and epilog insertion (PEI) utils.
//! It provides information necessary to insert a proper and ABI comforming
//! prolog and epilog. Function frame calculation is based on `CallConv` and
//! other function attributes.
//!
//! SSE vs AVX vs AVX-512
//! ---------------------
//!
//! Function frame provides a way to tell prolog/epilog inserter to use AVX
//! instructions instead of SSE. Use `setAvxEnabled()` and `setAvx512Enabled()`
//! to enable AVX and/or AVX-512, respectively. Enabling AVX-512 is mostly for
//! Compiler as it would use 32 SIMD registers instead of 16 when enabled.
//!
//! Function Frame Structure
//! ------------------------
//!
//! Various properties can contribute to the size and structure of the function
//! frame. The function frame in most cases won't use all of the properties
//! illustrated (for example Spill Zone and Red Zone are never used together).
//!
//! ```
//!   +-----------------------------+
//!   | Arguments Passed by Stack   |
//!   +-----------------------------+
//!   | Spill Zone                  |
//!   +-----------------------------+ <- Stack offset (args) starts from here.
//!   | Return Address, if Pushed   |
//!   +-----------------------------+ <- Stack pointer (SP) upon entry.
//!   | Save/Restore Stack.         |
//!   +-----------------------------+-----------------------------+
//!   | Local Stack                 |                             |
//!   +-----------------------------+          Final Stack        |
//!   | Call Stack                  |                             |
//!   +-----------------------------+-----------------------------+ <- SP after prolog.
//!   | Red Zone                    |
//!   +-----------------------------+
//! ```
class FuncFrame {
public:
  enum Tag : uint32_t {
    //! Tag used to inform that some offset is invalid.
    kTagInvalidOffset = 0xFFFFFFFFu
  };

  //! Attributes are designed in a way that all are initially false, and user
  //! or FuncFrame finalizer adds them when necessary.
  enum Attributes : uint32_t {
    //! Function has variable number of arguments.
    kAttrHasVarArgs = 0x00000001u,
    //! Preserve frame pointer (don't omit FP).
    kAttrHasPreservedFP = 0x00000010u,
    //! Function calls other functions (is not leaf).
    kAttrHasFuncCalls = 0x00000020u,

    //! Function uses AVX (X86).
    //!
    //! This flag instructs prolog and epilog emitter to use AVX instead of SSE for manipulating
    //! XMM registers.
    kAttrX86AvxEnabled = 0x00010000u,
    //! Function uses AVX-512 (X86).
    //!
    //! This flag instructs Compiler register allocator to use additional 16 registers introduced
    //! by AVX-512.
    kAttrX86Avx512Enabled = 0x00020000u,

    //! This flag instructs epilog writer to emit EMMS instruction before RET (X86).
    kAttrX86MmxCleanup = 0x00040000u,

    //! This flag instructs epilog writer to emit VZEROUPPER instruction before RET (X86).
    kAttrX86AvxCleanup = 0x00080000u,

    //! Function has aligned save/restore of vector registers.
    kAttrAlignedVecSR = 0x40000000u,
    //! FuncFrame is finalized and can be used by PEI.
    kAttrIsFinalized = 0x80000000u
  };

  //! Function attributes.
  uint32_t _attributes;

  //! Architecture, see \ref Environment::Arch.
  uint8_t _arch;
  //! SP register ID (to access call stack and local stack).
  uint8_t _spRegId;
  //! SA register ID (to access stack arguments).
  uint8_t _saRegId;

  //! Red zone size (copied from CallConv).
  uint8_t _redZoneSize;
  //! Spill zone size (copied from CallConv).
  uint8_t _spillZoneSize;
  //! Natural stack alignment (copied from CallConv).
  uint8_t _naturalStackAlignment;
  //! Minimum stack alignment to turn on dynamic alignment.
  uint8_t _minDynamicAlignment;

  //! Call stack alignment.
  uint8_t _callStackAlignment;
  //! Local stack alignment.
  uint8_t _localStackAlignment;
  //! Final stack alignment.
  uint8_t _finalStackAlignment;

  //! Adjustment of the stack before returning (X86-STDCALL).
  uint16_t _calleeStackCleanup;

  //! Call stack size.
  uint32_t _callStackSize;
  //! Local stack size.
  uint32_t _localStackSize;
  //! Final stack size (sum of call stack and local stack).
  uint32_t _finalStackSize;

  //! Local stack offset (non-zero only if call stack is used).
  uint32_t _localStackOffset;
  //! Offset relative to SP that contains previous SP (before alignment).
  uint32_t _daOffset;
  //! Offset of the first stack argument relative to SP.
  uint32_t _saOffsetFromSP;
  //! Offset of the first stack argument relative to SA (_saRegId or FP).
  uint32_t _saOffsetFromSA;

  //! Local stack adjustment in prolog/epilog.
  uint32_t _stackAdjustment;

  //! Registers that are dirty.
  uint32_t _dirtyRegs[BaseReg::kGroupVirt];
  //! Registers that must be preserved (copied from CallConv).
  uint32_t _preservedRegs[BaseReg::kGroupVirt];
  //! Size to save/restore per register group.
  uint8_t _saveRestoreRegSize[BaseReg::kGroupVirt];
  //! Alignment of save/restore area per register group.
  uint8_t _saveRestoreAlignment[BaseReg::kGroupVirt];

  //! Stack size required to save registers with push/pop.
  uint16_t _pushPopSaveSize;
  //! Stack size required to save extra registers that cannot use push/pop.
  uint16_t _extraRegSaveSize;
  //! Offset where registers saved/restored via push/pop are stored
  uint32_t _pushPopSaveOffset;
  //! Offset where extra ragisters that cannot use push/pop are stored.
  uint32_t _extraRegSaveOffset;

  //! \name Construction & Destruction
  //! \{

  inline FuncFrame() noexcept { reset(); }
  inline FuncFrame(const FuncFrame& other) noexcept = default;

  ASMJIT_API Error init(const FuncDetail& func) noexcept;

  inline void reset() noexcept {
    memset(this, 0, sizeof(FuncFrame));
    _spRegId = BaseReg::kIdBad;
    _saRegId = BaseReg::kIdBad;
    _daOffset = kTagInvalidOffset;
  }

  //! \}

  //! \name Accessors
  //! \{

  //! Returns the target architecture of the function frame.
  inline uint32_t arch() const noexcept { return _arch; }

  //! Returns function frame attributes, see `Attributes`.
  inline uint32_t attributes() const noexcept { return _attributes; }
  //! Checks whether the FuncFame contains an attribute `attr`.
  inline bool hasAttribute(uint32_t attr) const noexcept { return (_attributes & attr) != 0; }
  //! Adds attributes `attrs` to the FuncFrame.
  inline void addAttributes(uint32_t attrs) noexcept { _attributes |= attrs; }
  //! Clears attributes `attrs` from the FrameFrame.
  inline void clearAttributes(uint32_t attrs) noexcept { _attributes &= ~attrs; }

  //! Tests whether the function has variable number of arguments.
  inline bool hasVarArgs() const noexcept { return hasAttribute(kAttrHasVarArgs); }
  //! Sets the variable arguments flag.
  inline void setVarArgs() noexcept { addAttributes(kAttrHasVarArgs); }
  //! Resets variable arguments flag.
  inline void resetVarArgs() noexcept { clearAttributes(kAttrHasVarArgs); }

  //! Tests whether the function preserves frame pointer (EBP|ESP on X86).
  inline bool hasPreservedFP() const noexcept { return hasAttribute(kAttrHasPreservedFP); }
  //! Enables preserved frame pointer.
  inline void setPreservedFP() noexcept { addAttributes(kAttrHasPreservedFP); }
  //! Disables preserved frame pointer.
  inline void resetPreservedFP() noexcept { clearAttributes(kAttrHasPreservedFP); }

  //! Tests whether the function calls other functions.
  inline bool hasFuncCalls() const noexcept { return hasAttribute(kAttrHasFuncCalls); }
  //! Sets `kFlagHasCalls` to true.
  inline void setFuncCalls() noexcept { addAttributes(kAttrHasFuncCalls); }
  //! Sets `kFlagHasCalls` to false.
  inline void resetFuncCalls() noexcept { clearAttributes(kAttrHasFuncCalls); }

  //! Tests whether the function has AVX enabled.
  inline bool isAvxEnabled() const noexcept { return hasAttribute(kAttrX86AvxEnabled); }
  //! Enables AVX use.
  inline void setAvxEnabled() noexcept { addAttributes(kAttrX86AvxEnabled); }
  //! Disables AVX use.
  inline void resetAvxEnabled() noexcept { clearAttributes(kAttrX86AvxEnabled); }

  //! Tests whether the function has AVX-512 enabled.
  inline bool isAvx512Enabled() const noexcept { return hasAttribute(kAttrX86Avx512Enabled); }
  //! Enables AVX-512 use.
  inline void setAvx512Enabled() noexcept { addAttributes(kAttrX86Avx512Enabled); }
  //! Disables AVX-512 use.
  inline void resetAvx512Enabled() noexcept { clearAttributes(kAttrX86Avx512Enabled); }

  //! Tests whether the function has MMX cleanup - 'emms' instruction in epilog.
  inline bool hasMmxCleanup() const noexcept { return hasAttribute(kAttrX86MmxCleanup); }
  //! Enables MMX cleanup.
  inline void setMmxCleanup() noexcept { addAttributes(kAttrX86MmxCleanup); }
  //! Disables MMX cleanup.
  inline void resetMmxCleanup() noexcept { clearAttributes(kAttrX86MmxCleanup); }

  //! Tests whether the function has AVX cleanup - 'vzeroupper' instruction in epilog.
  inline bool hasAvxCleanup() const noexcept { return hasAttribute(kAttrX86AvxCleanup); }
  //! Enables AVX cleanup.
  inline void setAvxCleanup() noexcept { addAttributes(kAttrX86AvxCleanup); }
  //! Disables AVX cleanup.
  inline void resetAvxCleanup() noexcept { clearAttributes(kAttrX86AvxCleanup); }

  //! Tests whether the function uses call stack.
  inline bool hasCallStack() const noexcept { return _callStackSize != 0; }
  //! Tests whether the function uses local stack.
  inline bool hasLocalStack() const noexcept { return _localStackSize != 0; }
  //! Tests whether vector registers can be saved and restored by using aligned reads and writes.
  inline bool hasAlignedVecSR() const noexcept { return hasAttribute(kAttrAlignedVecSR); }
  //! Tests whether the function has to align stack dynamically.
  inline bool hasDynamicAlignment() const noexcept { return _finalStackAlignment >= _minDynamicAlignment; }

  //! Tests whether the calling convention specifies 'RedZone'.
  inline bool hasRedZone() const noexcept { return _redZoneSize != 0; }
  //! Tests whether the calling convention specifies 'SpillZone'.
  inline bool hasSpillZone() const noexcept { return _spillZoneSize != 0; }

  //! Returns the size of 'RedZone'.
  inline uint32_t redZoneSize() const noexcept { return _redZoneSize; }
  //! Returns the size of 'SpillZone'.
  inline uint32_t spillZoneSize() const noexcept { return _spillZoneSize; }
  //! Returns natural stack alignment (guaranteed stack alignment upon entry).
  inline uint32_t naturalStackAlignment() const noexcept { return _naturalStackAlignment; }
  //! Returns natural stack alignment (guaranteed stack alignment upon entry).
  inline uint32_t minDynamicAlignment() const noexcept { return _minDynamicAlignment; }

  //! Tests whether the callee must adjust SP before returning (X86-STDCALL only)
  inline bool hasCalleeStackCleanup() const noexcept { return _calleeStackCleanup != 0; }
  //! Returns home many bytes of the stack the the callee must adjust before returning (X86-STDCALL only)
  inline uint32_t calleeStackCleanup() const noexcept { return _calleeStackCleanup; }

  //! Returns call stack alignment.
  inline uint32_t callStackAlignment() const noexcept { return _callStackAlignment; }
  //! Returns local stack alignment.
  inline uint32_t localStackAlignment() const noexcept { return _localStackAlignment; }
  //! Returns final stack alignment (the maximum value of call, local, and natural stack alignments).
  inline uint32_t finalStackAlignment() const noexcept { return _finalStackAlignment; }

  //! Sets call stack alignment.
  //!
  //! \note This also updates the final stack alignment.
  inline void setCallStackAlignment(uint32_t alignment) noexcept {
    _callStackAlignment = uint8_t(alignment);
    _finalStackAlignment = Support::max(_naturalStackAlignment, _callStackAlignment, _localStackAlignment);
  }

  //! Sets local stack alignment.
  //!
  //! \note This also updates the final stack alignment.
  inline void setLocalStackAlignment(uint32_t value) noexcept {
    _localStackAlignment = uint8_t(value);
    _finalStackAlignment = Support::max(_naturalStackAlignment, _callStackAlignment, _localStackAlignment);
  }

  //! Combines call stack alignment with `alignment`, updating it to the greater value.
  //!
  //! \note This also updates the final stack alignment.
  inline void updateCallStackAlignment(uint32_t alignment) noexcept {
    _callStackAlignment = uint8_t(Support::max<uint32_t>(_callStackAlignment, alignment));
    _finalStackAlignment = Support::max(_finalStackAlignment, _callStackAlignment);
  }

  //! Combines local stack alignment with `alignment`, updating it to the greater value.
  //!
  //! \note This also updates the final stack alignment.
  inline void updateLocalStackAlignment(uint32_t alignment) noexcept {
    _localStackAlignment = uint8_t(Support::max<uint32_t>(_localStackAlignment, alignment));
    _finalStackAlignment = Support::max(_finalStackAlignment, _localStackAlignment);
  }

  //! Returns call stack size.
  inline uint32_t callStackSize() const noexcept { return _callStackSize; }
  //! Returns local stack size.
  inline uint32_t localStackSize() const noexcept { return _localStackSize; }

  //! Sets call stack size.
  inline void setCallStackSize(uint32_t size) noexcept { _callStackSize = size; }
  //! Sets local stack size.
  inline void setLocalStackSize(uint32_t size) noexcept { _localStackSize = size; }

  //! Combines call stack size with `size`, updating it to the greater value.
  inline void updateCallStackSize(uint32_t size) noexcept { _callStackSize = Support::max(_callStackSize, size); }
  //! Combines local stack size with `size`, updating it to the greater value.
  inline void updateLocalStackSize(uint32_t size) noexcept { _localStackSize = Support::max(_localStackSize, size); }

  //! Returns final stack size (only valid after the FuncFrame is finalized).
  inline uint32_t finalStackSize() const noexcept { return _finalStackSize; }

  //! Returns an offset to access the local stack (non-zero only if call stack is used).
  inline uint32_t localStackOffset() const noexcept { return _localStackOffset; }

  //! Tests whether the function prolog/epilog requires a memory slot for storing unaligned SP.
  inline bool hasDAOffset() const noexcept { return _daOffset != kTagInvalidOffset; }
  //! Returns a memory offset used to store DA (dynamic alignment) slot (relative to SP).
  inline uint32_t daOffset() const noexcept { return _daOffset; }

  inline uint32_t saOffset(uint32_t regId) const noexcept {
    return regId == _spRegId ? saOffsetFromSP()
                             : saOffsetFromSA();
  }

  inline uint32_t saOffsetFromSP() const noexcept { return _saOffsetFromSP; }
  inline uint32_t saOffsetFromSA() const noexcept { return _saOffsetFromSA; }

  //! Returns mask of registers of the given register `group` that are modified
  //! by the function. The engine would then calculate which registers must be
  //! saved & restored by the function by using the data provided by the calling
  //! convention.
  inline uint32_t dirtyRegs(uint32_t group) const noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    return _dirtyRegs[group];
  }

  //! Sets which registers (as a mask) are modified by the function.
  //!
  //! \remarks Please note that this will completely overwrite the existing
  //! register mask, use `addDirtyRegs()` to modify the existing register
  //! mask.
  inline void setDirtyRegs(uint32_t group, uint32_t regs) noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    _dirtyRegs[group] = regs;
  }

  //! Adds which registers (as a mask) are modified by the function.
  inline void addDirtyRegs(uint32_t group, uint32_t regs) noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    _dirtyRegs[group] |= regs;
  }

  //! \overload
  inline void addDirtyRegs(const BaseReg& reg) noexcept {
    ASMJIT_ASSERT(reg.id() < Globals::kMaxPhysRegs);
    addDirtyRegs(reg.group(), Support::bitMask(reg.id()));
  }

  //! \overload
  template<typename... Args>
  ASMJIT_INLINE void addDirtyRegs(const BaseReg& reg, Args&&... args) noexcept {
    addDirtyRegs(reg);
    addDirtyRegs(std::forward<Args>(args)...);
  }

  inline void setAllDirty() noexcept {
    for (size_t i = 0; i < ASMJIT_ARRAY_SIZE(_dirtyRegs); i++)
      _dirtyRegs[i] = 0xFFFFFFFFu;
  }

  inline void setAllDirty(uint32_t group) noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    _dirtyRegs[group] = 0xFFFFFFFFu;
  }

  //! Returns a calculated mask of registers of the given `group` that will be
  //! saved and restored in the function's prolog and epilog, respectively. The
  //! register mask is calculated from both `dirtyRegs` (provided by user) and
  //! `preservedMask` (provided by the calling convention).
  inline uint32_t savedRegs(uint32_t group) const noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    return _dirtyRegs[group] & _preservedRegs[group];
  }

  //! Returns the mask of preserved registers of the given register `group`.
  //!
  //! Preserved registers are those that must survive the function call
  //! unmodified. The function can only modify preserved registers it they
  //! are saved and restored in funciton's prolog and epilog, respectively.
  inline uint32_t preservedRegs(uint32_t group) const noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    return _preservedRegs[group];
  }

  inline uint32_t saveRestoreRegSize(uint32_t group) const noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    return _saveRestoreRegSize[group];
  }

  inline uint32_t saveRestoreAlignment(uint32_t group) const noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    return _saveRestoreAlignment[group];
  }

  inline bool hasSARegId() const noexcept { return _saRegId != BaseReg::kIdBad; }
  inline uint32_t saRegId() const noexcept { return _saRegId; }
  inline void setSARegId(uint32_t regId) { _saRegId = uint8_t(regId); }
  inline void resetSARegId() { setSARegId(BaseReg::kIdBad); }

  //! Returns stack size required to save/restore registers via push/pop.
  inline uint32_t pushPopSaveSize() const noexcept { return _pushPopSaveSize; }
  //! Returns an offset to the stack where registers are saved via push/pop.
  inline uint32_t pushPopSaveOffset() const noexcept { return _pushPopSaveOffset; }

  //! Returns stack size required to save/restore extra registers that don't
  //! use push/pop/
  //!
  //! \note On X86 this covers all registers except GP registers, on other
  //! architectures it can be always zero (for example AArch64 saves all
  //! registers via push/pop like instructions, so this would be zero).
  inline uint32_t extraRegSaveSize() const noexcept { return _extraRegSaveSize; }
  //! Returns an offset to the stack where extra registers are saved.
  inline uint32_t extraRegSaveOffset() const noexcept { return _extraRegSaveOffset; }

  //! Tests whether the functions contains stack adjustment.
  inline bool hasStackAdjustment() const noexcept { return _stackAdjustment != 0; }
  //! Returns function's stack adjustment used in function's prolog and epilog.
  //!
  //! If the returned value is zero it means that the stack is not adjusted.
  //! This can mean both that the stack is not used and/or the stack is only
  //! adjusted by instructions that pust/pop registers into/from stack.
  inline uint32_t stackAdjustment() const noexcept { return _stackAdjustment; }

  //! \}

  //! \name Finaliztion
  //! \{

  ASMJIT_API Error finalize() noexcept;

  //! \}
};

// ============================================================================
// [asmjit::FuncArgsAssignment]
// ============================================================================

//! A helper class that can be used to assign a physical register for each
//! function argument. Use with `BaseEmitter::emitArgsAssignment()`.
class FuncArgsAssignment {
public:
  //! Function detail.
  const FuncDetail* _funcDetail;
  //! Register that can be used to access arguments passed by stack.
  uint8_t _saRegId;
  //! Reserved for future use.
  uint8_t _reserved[3];
  //! Mapping of each function argument.
  FuncValuePack _argPacks[Globals::kMaxFuncArgs];

  //! \name Construction & Destruction
  //! \{

  inline explicit FuncArgsAssignment(const FuncDetail* fd = nullptr) noexcept { reset(fd); }

  inline FuncArgsAssignment(const FuncArgsAssignment& other) noexcept {
    memcpy(this, &other, sizeof(*this));
  }

  inline void reset(const FuncDetail* fd = nullptr) noexcept {
    _funcDetail = fd;
    _saRegId = uint8_t(BaseReg::kIdBad);
    memset(_reserved, 0, sizeof(_reserved));
    memset(_argPacks, 0, sizeof(_argPacks));
  }

  //! \}

  //! \name Accessors
  //! \{

  inline const FuncDetail* funcDetail() const noexcept { return _funcDetail; }
  inline void setFuncDetail(const FuncDetail* fd) noexcept { _funcDetail = fd; }

  inline bool hasSARegId() const noexcept { return _saRegId != BaseReg::kIdBad; }
  inline uint32_t saRegId() const noexcept { return _saRegId; }
  inline void setSARegId(uint32_t regId) { _saRegId = uint8_t(regId); }
  inline void resetSARegId() { _saRegId = uint8_t(BaseReg::kIdBad); }

  inline FuncValue& arg(size_t argIndex, size_t valueIndex) noexcept {
    ASMJIT_ASSERT(argIndex < ASMJIT_ARRAY_SIZE(_argPacks));
    return _argPacks[argIndex][valueIndex];
  }
  inline const FuncValue& arg(size_t argIndex, size_t valueIndex) const noexcept {
    ASMJIT_ASSERT(argIndex < ASMJIT_ARRAY_SIZE(_argPacks));
    return _argPacks[argIndex][valueIndex];
  }

  inline bool isAssigned(size_t argIndex, size_t valueIndex) const noexcept {
    ASMJIT_ASSERT(argIndex < ASMJIT_ARRAY_SIZE(_argPacks));
    return _argPacks[argIndex][valueIndex].isAssigned();
  }

  inline void assignReg(size_t argIndex, const BaseReg& reg, uint32_t typeId = Type::kIdVoid) noexcept {
    ASMJIT_ASSERT(argIndex < ASMJIT_ARRAY_SIZE(_argPacks));
    ASMJIT_ASSERT(reg.isPhysReg());
    _argPacks[argIndex][0].initReg(reg.type(), reg.id(), typeId);
  }

  inline void assignReg(size_t argIndex, uint32_t regType, uint32_t regId, uint32_t typeId = Type::kIdVoid) noexcept {
    ASMJIT_ASSERT(argIndex < ASMJIT_ARRAY_SIZE(_argPacks));
    _argPacks[argIndex][0].initReg(regType, regId, typeId);
  }

  inline void assignStack(size_t argIndex, int32_t offset, uint32_t typeId = Type::kIdVoid) noexcept {
    ASMJIT_ASSERT(argIndex < ASMJIT_ARRAY_SIZE(_argPacks));
    _argPacks[argIndex][0].initStack(offset, typeId);
  }

  inline void assignRegInPack(size_t argIndex, size_t valueIndex, const BaseReg& reg, uint32_t typeId = Type::kIdVoid) noexcept {
    ASMJIT_ASSERT(argIndex < ASMJIT_ARRAY_SIZE(_argPacks));
    ASMJIT_ASSERT(reg.isPhysReg());
    _argPacks[argIndex][valueIndex].initReg(reg.type(), reg.id(), typeId);
  }

  inline void assignRegInPack(size_t argIndex, size_t valueIndex, uint32_t regType, uint32_t regId, uint32_t typeId = Type::kIdVoid) noexcept {
    ASMJIT_ASSERT(argIndex < ASMJIT_ARRAY_SIZE(_argPacks));
    _argPacks[argIndex][valueIndex].initReg(regType, regId, typeId);
  }

  inline void assignStackInPack(size_t argIndex, size_t valueIndex, int32_t offset, uint32_t typeId = Type::kIdVoid) noexcept {
    ASMJIT_ASSERT(argIndex < ASMJIT_ARRAY_SIZE(_argPacks));
    _argPacks[argIndex][valueIndex].initStack(offset, typeId);
  }

  // NOTE: All `assignAll()` methods are shortcuts to assign all arguments at
  // once, however, since registers are passed all at once these initializers
  // don't provide any way to pass TypeId and/or to keep any argument between
  // the arguments passed unassigned.
  inline void _assignAllInternal(size_t argIndex, const BaseReg& reg) noexcept {
    assignReg(argIndex, reg);
  }

  template<typename... Args>
  inline void _assignAllInternal(size_t argIndex, const BaseReg& reg, Args&&... args) noexcept {
    assignReg(argIndex, reg);
    _assignAllInternal(argIndex + 1, std::forward<Args>(args)...);
  }

  template<typename... Args>
  inline void assignAll(Args&&... args) noexcept {
    _assignAllInternal(0, std::forward<Args>(args)...);
  }

  //! \}

  //! \name Utilities
  //! \{

  //! Update `FuncFrame` based on function's arguments assignment.
  //!
  //! \note You MUST call this in orher to use `BaseEmitter::emitArgsAssignment()`,
  //! otherwise the FuncFrame would not contain the information necessary to
  //! assign all arguments into the registers and/or stack specified.
  ASMJIT_API Error updateFuncFrame(FuncFrame& frame) const noexcept;

  //! \}
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_FUNC_H_INCLUDED

