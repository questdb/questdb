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

#ifndef ASMJIT_CORE_TARGET_H_INCLUDED
#define ASMJIT_CORE_TARGET_H_INCLUDED

#include "../core/archtraits.h"
#include "../core/func.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_core
//! \{

// ============================================================================
// [asmjit::CodeInfo]
// ============================================================================

#ifndef ASMJIT_NO_DEPRECATED
//! Basic information about a code (or target). It describes its architecture,
//! code generation mode (or optimization level), and base address.
class ASMJIT_DEPRECATED_STRUCT("Use Environment instead of CodeInfo") CodeInfo {
public:
  //!< Environment information.
  Environment _environment;
  //! Base address.
  uint64_t _baseAddress;

  //! \name Construction & Destruction
  //! \{

  inline CodeInfo() noexcept
    : _environment(),
      _baseAddress(Globals::kNoBaseAddress) {}

  inline explicit CodeInfo(uint32_t arch, uint32_t subArch = 0, uint64_t baseAddress = Globals::kNoBaseAddress) noexcept
    : _environment(arch, subArch),
      _baseAddress(baseAddress) {}

  inline explicit CodeInfo(const Environment& environment, uint64_t baseAddress = Globals::kNoBaseAddress) noexcept
    : _environment(environment),
      _baseAddress(baseAddress) {}


  inline CodeInfo(const CodeInfo& other) noexcept { init(other); }

  inline bool isInitialized() const noexcept {
    return _environment.arch() != Environment::kArchUnknown;
  }

  inline void init(const CodeInfo& other) noexcept {
    *this = other;
  }

  inline void init(uint32_t arch, uint32_t subArch = 0, uint64_t baseAddress = Globals::kNoBaseAddress) noexcept {
    _environment.init(arch, subArch);
    _baseAddress = baseAddress;
  }

  inline void reset() noexcept {
    _environment.reset();
    _baseAddress = Globals::kNoBaseAddress;
  }

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline CodeInfo& operator=(const CodeInfo& other) noexcept = default;

  inline bool operator==(const CodeInfo& other) const noexcept { return ::memcmp(this, &other, sizeof(*this)) == 0; }
  inline bool operator!=(const CodeInfo& other) const noexcept { return ::memcmp(this, &other, sizeof(*this)) != 0; }

  //! \}

  //! \name Accessors
  //! \{

  //! Returns the target environment information, see \ref Environment.
  inline const Environment& environment() const noexcept { return _environment; }

  //! Returns the target architecture, see \ref Environment::Arch.
  inline uint32_t arch() const noexcept { return _environment.arch(); }
  //! Returns the target sub-architecture, see \ref Environment::SubArch.
  inline uint32_t subArch() const noexcept { return _environment.subArch(); }
  //! Returns the native size of the target's architecture GP register.
  inline uint32_t gpSize() const noexcept { return _environment.registerSize(); }

  //! Tests whether this CodeInfo has a base address set.
  inline bool hasBaseAddress() const noexcept { return _baseAddress != Globals::kNoBaseAddress; }
  //! Returns the base address or \ref Globals::kNoBaseAddress if it's not set.
  inline uint64_t baseAddress() const noexcept { return _baseAddress; }
  //! Sets base address to `p`.
  inline void setBaseAddress(uint64_t p) noexcept { _baseAddress = p; }
  //! Resets base address (implicitly sets it to \ref Globals::kNoBaseAddress).
  inline void resetBaseAddress() noexcept { _baseAddress = Globals::kNoBaseAddress; }

  //! \}
};
#endif // !ASMJIT_NO_DEPRECATED

// ============================================================================
// [asmjit::Target]
// ============================================================================

//! Target is an abstract class that describes a machine code target.
class ASMJIT_VIRTAPI Target {
public:
  ASMJIT_BASE_CLASS(Target)
  ASMJIT_NONCOPYABLE(Target)

  //! Target environment information.
  Environment _environment;

  //! \name Construction & Destruction
  //! \{

  //! Creates a `Target` instance.
  ASMJIT_API Target() noexcept;
  //! Destroys the `Target` instance.
  ASMJIT_API virtual ~Target() noexcept;

  //! \}

  //! \name Accessors
  //! \{

  //! Returns CodeInfo of this target.
  //!
  //! CodeInfo can be used to setup a CodeHolder in case you plan to generate a
  //! code compatible and executable by this Runtime.
  inline const Environment& environment() const noexcept { return _environment; }

  //! Returns the target architecture, see \ref Environment::Arch.
  inline uint32_t arch() const noexcept { return _environment.arch(); }
  //! Returns the target sub-architecture, see \ref Environment::SubArch.
  inline uint32_t subArch() const noexcept { return _environment.subArch(); }

#ifndef ASMJIT_NO_DEPRECATED
  ASMJIT_DEPRECATED("Use environment() instead")
  inline CodeInfo codeInfo() const noexcept { return CodeInfo(_environment); }

  ASMJIT_DEPRECATED("Use environment().format() instead")
  inline uint32_t targetType() const noexcept { return _environment.format(); }
#endif // !ASMJIT_NO_DEPRECATED

  //! \}
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_TARGET_H_INCLUDED
