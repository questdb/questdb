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

#ifndef ASMJIT_CORE_GLOBALS_H_INCLUDED
#define ASMJIT_CORE_GLOBALS_H_INCLUDED

#include "../core/api-config.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::Support]
// ============================================================================

//! \cond INTERNAL
//! \addtogroup asmjit_utilities
//! \{
namespace Support {
  //! Cast designed to cast between function and void* pointers.
  template<typename Dst, typename Src>
  static inline Dst ptr_cast_impl(Src p) noexcept { return (Dst)p; }
} // {Support}

#if defined(ASMJIT_NO_STDCXX)
namespace Support {
  ASMJIT_INLINE void* operatorNew(size_t n) noexcept { return malloc(n); }
  ASMJIT_INLINE void operatorDelete(void* p) noexcept { if (p) free(p); }
} // {Support}

#define ASMJIT_BASE_CLASS(TYPE)                                               \
  ASMJIT_INLINE void* operator new(size_t n) noexcept {                       \
    return Support::operatorNew(n);                                           \
  }                                                                           \
                                                                              \
  ASMJIT_INLINE void  operator delete(void* p) noexcept {                     \
    Support::operatorDelete(p);                                               \
  }                                                                           \
                                                                              \
  ASMJIT_INLINE void* operator new(size_t, void* p) noexcept { return p; }    \
  ASMJIT_INLINE void  operator delete(void*, void*) noexcept {}
#else
#define ASMJIT_BASE_CLASS(TYPE)
#endif

//! \}
//! \endcond

// ============================================================================
// [asmjit::Globals]
// ============================================================================

//! \addtogroup asmjit_core
//! \{

//! Contains typedefs, constants, and variables used globally by AsmJit.
namespace Globals {

// ============================================================================
// [asmjit::Globals::<global>]
// ============================================================================

//! Host memory allocator overhead.
static constexpr uint32_t kAllocOverhead = uint32_t(sizeof(intptr_t) * 4);

//! Host memory allocator alignment.
static constexpr uint32_t kAllocAlignment = 8;

//! Aggressive growing strategy threshold.
static constexpr uint32_t kGrowThreshold = 1024 * 1024 * 16;

//! Maximum depth of RB-Tree is:
//!
//!   `2 * log2(n + 1)`
//!
//! Size of RB node is at least two pointers (without data),
//! so a theoretical architecture limit would be:
//!
//!   `2 * log2(addressableMemorySize / sizeof(Node) + 1)`
//!
//! Which yields 30 on 32-bit arch and 61 on 64-bit arch.
//! The final value was adjusted by +1 for safety reasons.
static constexpr uint32_t kMaxTreeHeight = (ASMJIT_ARCH_BITS == 32 ? 30 : 61) + 1;

//! Maximum number of operands per a single instruction.
static constexpr uint32_t kMaxOpCount = 6;

//! Maximum arguments of a function supported by the Compiler / Function API.
static constexpr uint32_t kMaxFuncArgs = 16;

//! The number of values that can be assigned to a single function argument or
//! return value.
static constexpr uint32_t kMaxValuePack = 4;

//! Maximum number of physical registers AsmJit can use per register group.
static constexpr uint32_t kMaxPhysRegs = 32;

//! Maximum alignment.
static constexpr uint32_t kMaxAlignment = 64;

//! Maximum label or symbol size in bytes.
static constexpr uint32_t kMaxLabelNameSize = 2048;

//! Maximum section name size.
static constexpr uint32_t kMaxSectionNameSize = 35;

//! Maximum size of comment.
static constexpr uint32_t kMaxCommentSize = 1024;

//! Invalid identifier.
static constexpr uint32_t kInvalidId = 0xFFFFFFFFu;

//! Returned by `indexOf()` and similar when working with containers that use 32-bit index/size.
static constexpr uint32_t kNotFound = 0xFFFFFFFFu;

//! Invalid base address.
static constexpr uint64_t kNoBaseAddress = ~uint64_t(0);

// ============================================================================
// [asmjit::Globals::ResetPolicy]
// ============================================================================

//! Reset policy used by most `reset()` functions.
enum ResetPolicy : uint32_t {
  //! Soft reset, doesn't deallocate memory (default).
  kResetSoft = 0,
  //! Hard reset, releases all memory used, if any.
  kResetHard = 1
};

// ============================================================================
// [asmjit::Globals::Link]
// ============================================================================

enum Link : uint32_t {
  kLinkLeft  = 0,
  kLinkRight = 1,

  kLinkPrev  = 0,
  kLinkNext  = 1,

  kLinkFirst = 0,
  kLinkLast  = 1,

  kLinkCount = 2
};

struct Init_ {};
struct NoInit_ {};

static const constexpr Init_ Init {};
static const constexpr NoInit_ NoInit {};

} // {Globals}

// ============================================================================
// [asmjit::ByteOrder]
// ============================================================================

//! Byte order.
namespace ByteOrder {
  enum : uint32_t {
    kLE      = 0,
    kBE      = 1,
    kNative  = ASMJIT_ARCH_LE ? kLE : kBE,
    kSwapped = ASMJIT_ARCH_LE ? kBE : kLE
  };
}

// ============================================================================
// [asmjit::ptr_as_func / func_as_ptr]
// ============================================================================

template<typename Func>
static inline Func ptr_as_func(void* func) noexcept { return Support::ptr_cast_impl<Func, void*>(func); }

template<typename Func>
static inline void* func_as_ptr(Func func) noexcept { return Support::ptr_cast_impl<void*, Func>(func); }

//! \}

// ============================================================================
// [asmjit::Error]
// ============================================================================

//! \addtogroup asmjit_error_handling
//! \{

//! AsmJit error type (uint32_t).
typedef uint32_t Error;

//! AsmJit error codes.
enum ErrorCode : uint32_t {
  // @EnumValuesBegin{"enum": "ErrorCode"}@

  //! No error (success).
  kErrorOk = 0,

  //! Out of memory.
  kErrorOutOfMemory,

  //! Invalid argument.
  kErrorInvalidArgument,

  //! Invalid state.
  //!
  //! If this error is returned it means that either you are doing something
  //! wrong or AsmJit caught itself by doing something wrong. This error should
  //! never be ignored.
  kErrorInvalidState,

  //! Invalid or incompatible architecture.
  kErrorInvalidArch,

  //! The object is not initialized.
  kErrorNotInitialized,
  //! The object is already initialized.
  kErrorAlreadyInitialized,

  //! Built-in feature was disabled at compile time and it's not available.
  kErrorFeatureNotEnabled,

  //! Too many handles (Windows) or file descriptors (Unix/Posix).
  kErrorTooManyHandles,
  //! Code generated is larger than allowed.
  kErrorTooLarge,

  //! No code generated.
  //!
  //! Returned by runtime if the \ref CodeHolder contains no code.
  kErrorNoCodeGenerated,

  //! Invalid directive.
  kErrorInvalidDirective,
  //! Attempt to use uninitialized label.
  kErrorInvalidLabel,
  //! Label index overflow - a single \ref BaseAssembler instance can hold
  //! almost 2^32 (4 billion) labels. If there is an attempt to create more
  //! labels then this error is returned.
  kErrorTooManyLabels,
  //! Label is already bound.
  kErrorLabelAlreadyBound,
  //! Label is already defined (named labels).
  kErrorLabelAlreadyDefined,
  //! Label name is too long.
  kErrorLabelNameTooLong,
  //! Label must always be local if it's anonymous (without a name).
  kErrorInvalidLabelName,
  //! Parent id passed to \ref CodeHolder::newNamedLabelEntry() was invalid.
  kErrorInvalidParentLabel,
  //! Parent id specified for a non-local (global) label.
  kErrorNonLocalLabelCannotHaveParent,

  //! Invalid section.
  kErrorInvalidSection,
  //! Too many sections (section index overflow).
  kErrorTooManySections,
  //! Invalid section name (most probably too long).
  kErrorInvalidSectionName,

  //! Relocation index overflow (too many relocations).
  kErrorTooManyRelocations,
  //! Invalid relocation entry.
  kErrorInvalidRelocEntry,
  //! Reloc entry contains address that is out of range (unencodable).
  kErrorRelocOffsetOutOfRange,

  //! Invalid assignment to a register, function argument, or function return value.
  kErrorInvalidAssignment,
  //! Invalid instruction.
  kErrorInvalidInstruction,
  //! Invalid register type.
  kErrorInvalidRegType,
  //! Invalid register group.
  kErrorInvalidRegGroup,
  //! Invalid physical register id.
  kErrorInvalidPhysId,
  //! Invalid virtual register id.
  kErrorInvalidVirtId,
  //! Invalid element index (ARM).
  kErrorInvalidElementIndex,
  //! Invalid prefix combination (X86|X64).
  kErrorInvalidPrefixCombination,
  //! Invalid LOCK prefix (X86|X64).
  kErrorInvalidLockPrefix,
  //! Invalid XACQUIRE prefix (X86|X64).
  kErrorInvalidXAcquirePrefix,
  //! Invalid XRELEASE prefix (X86|X64).
  kErrorInvalidXReleasePrefix,
  //! Invalid REP prefix (X86|X64).
  kErrorInvalidRepPrefix,
  //! Invalid REX prefix (X86|X64).
  kErrorInvalidRexPrefix,
  //! Invalid {...} register (X86|X64).
  kErrorInvalidExtraReg,
  //! Invalid {k} use (not supported by the instruction) (X86|X64).
  kErrorInvalidKMaskUse,
  //! Invalid {k}{z} use (not supported by the instruction) (X86|X64).
  kErrorInvalidKZeroUse,
  //! Invalid broadcast - Currently only related to invalid use of AVX-512 {1tox} (X86|X64).
  kErrorInvalidBroadcast,
  //! Invalid 'embedded-rounding' {er} or 'suppress-all-exceptions' {sae} (AVX-512) (X86|X64).
  kErrorInvalidEROrSAE,
  //! Invalid address used (not encodable).
  kErrorInvalidAddress,
  //! Invalid index register used in memory address (not encodable).
  kErrorInvalidAddressIndex,
  //! Invalid address scale (not encodable).
  kErrorInvalidAddressScale,
  //! Invalid use of 64-bit address.
  kErrorInvalidAddress64Bit,
  //! Invalid use of 64-bit address that require 32-bit zero-extension (X64).
  kErrorInvalidAddress64BitZeroExtension,
  //! Invalid displacement (not encodable).
  kErrorInvalidDisplacement,
  //! Invalid segment (X86).
  kErrorInvalidSegment,

  //! Invalid immediate (out of bounds on X86 and invalid pattern on ARM).
  kErrorInvalidImmediate,

  //! Invalid operand size.
  kErrorInvalidOperandSize,
  //! Ambiguous operand size (memory has zero size while it's required to determine the operation type.
  kErrorAmbiguousOperandSize,
  //! Mismatching operand size (size of multiple operands doesn't match the operation size).
  kErrorOperandSizeMismatch,

  //! Invalid option.
  kErrorInvalidOption,
  //! Option already defined.
  kErrorOptionAlreadyDefined,

  //! Invalid TypeId.
  kErrorInvalidTypeId,
  //! Invalid use of a 8-bit GPB-HIGH register.
  kErrorInvalidUseOfGpbHi,
  //! Invalid use of a 64-bit GPQ register in 32-bit mode.
  kErrorInvalidUseOfGpq,
  //! Invalid use of an 80-bit float (\ref Type::kIdF80).
  kErrorInvalidUseOfF80,
  //! Some registers in the instruction muse be consecutive (some ARM and AVX512
  //! neural-net instructions).
  kErrorNotConsecutiveRegs,

  //! Illegal virtual register - reported by instruction validation.
  kErrorIllegalVirtReg,
  //! AsmJit cannot create more virtual registers.
  kErrorTooManyVirtRegs,

  //! AsmJit requires a physical register, but no one is available.
  kErrorNoMorePhysRegs,
  //! A variable has been assigned more than once to a function argument (BaseCompiler).
  kErrorOverlappedRegs,
  //! Invalid register to hold stack arguments offset.
  kErrorOverlappingStackRegWithRegArg,

  //! Unbound label cannot be evaluated by expression.
  kErrorExpressionLabelNotBound,
  //! Arithmetic overflow during expression evaluation.
  kErrorExpressionOverflow,

  //! Failed to open anonymous memory handle or file descriptor.
  kErrorFailedToOpenAnonymousMemory,

  // @EnumValuesEnd@

  //! Count of AsmJit error codes.
  kErrorCount
};

// ============================================================================
// [asmjit::DebugUtils]
// ============================================================================

//! Debugging utilities.
namespace DebugUtils {

//! \cond INTERNAL
//! Used to silence warnings about unused arguments or variables.
template<typename... Args>
static ASMJIT_INLINE void unused(Args&&...) noexcept {}
//! \endcond

//! Returns the error `err` passed.
//!
//! Provided for debugging purposes. Putting a breakpoint inside `errored` can
//! help with tracing the origin of any error reported / returned by AsmJit.
static constexpr Error errored(Error err) noexcept { return err; }

//! Returns a printable version of `asmjit::Error` code.
ASMJIT_API const char* errorAsString(Error err) noexcept;

//! Called to output debugging message(s).
ASMJIT_API void debugOutput(const char* str) noexcept;

//! Called on assertion failure.
//!
//! \param file Source file name where it happened.
//! \param line Line in the source file.
//! \param msg Message to display.
//!
//! If you have problems with assertion failures a breakpoint can be put
//! at \ref assertionFailed() function (asmjit/core/globals.cpp). A call stack
//! will be available when such assertion failure is triggered. AsmJit always
//! returns errors on failures, assertions are a last resort and usually mean
//! unrecoverable state due to out of range array access or totally invalid
//! arguments like nullptr where a valid pointer should be provided, etc...
ASMJIT_API void ASMJIT_NORETURN assertionFailed(const char* file, int line, const char* msg) noexcept;

} // {DebugUtils}

//! \def ASMJIT_ASSERT(...)
//!
//! AsmJit's own assert macro used in AsmJit code-base.
#if defined(ASMJIT_BUILD_DEBUG)
#define ASMJIT_ASSERT(...)                                                     \
  do {                                                                         \
    if (ASMJIT_LIKELY(__VA_ARGS__))                                            \
      break;                                                                   \
    ::asmjit::DebugUtils::assertionFailed(__FILE__, __LINE__, #__VA_ARGS__);   \
  } while (0)
#else
#define ASMJIT_ASSERT(...) ((void)0)
#endif

//! \def ASMJIT_PROPAGATE(...)
//!
//! Propagates a possible `Error` produced by `...` to the caller by returning
//! the error immediately. Used by AsmJit internally, but kept public for users
//! that want to use the same technique to propagate errors to the caller.
#define ASMJIT_PROPAGATE(...)               \
  do {                                      \
    ::asmjit::Error _err = __VA_ARGS__;     \
    if (ASMJIT_UNLIKELY(_err))              \
      return _err;                          \
  } while (0)

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_GLOBALS_H_INCLUDED
