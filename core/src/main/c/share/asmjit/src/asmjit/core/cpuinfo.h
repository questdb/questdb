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

#ifndef ASMJIT_CORE_CPUINFO_H_INCLUDED
#define ASMJIT_CORE_CPUINFO_H_INCLUDED

#include "../core/archtraits.h"
#include "../core/features.h"
#include "../core/globals.h"
#include "../core/string.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_core
//! \{

// ============================================================================
// [asmjit::CpuInfo]
// ============================================================================

//! CPU information.
class CpuInfo {
public:
  //! Architecture.
  uint8_t _arch;
  //! Sub-architecture.
  uint8_t _subArch;
  //! Reserved for future use.
  uint16_t _reserved;
  //! CPU family ID.
  uint32_t _familyId;
  //! CPU model ID.
  uint32_t _modelId;
  //! CPU brand ID.
  uint32_t _brandId;
  //! CPU stepping.
  uint32_t _stepping;
  //! Processor type.
  uint32_t _processorType;
  //! Maximum number of addressable IDs for logical processors.
  uint32_t _maxLogicalProcessors;
  //! Cache line size (in bytes).
  uint32_t _cacheLineSize;
  //! Number of hardware threads.
  uint32_t _hwThreadCount;

  //! CPU vendor string.
  FixedString<16> _vendor;
  //! CPU brand string.
  FixedString<64> _brand;
  //! CPU features.
  BaseFeatures _features;

  //! \name Construction & Destruction
  //! \{

  inline CpuInfo() noexcept { reset(); }
  inline CpuInfo(const CpuInfo& other) noexcept = default;

  inline explicit CpuInfo(Globals::NoInit_) noexcept
    : _features(Globals::NoInit) {};

  //! Returns the host CPU information.
  ASMJIT_API static const CpuInfo& host() noexcept;

  //! Initializes CpuInfo to the given architecture, see \ref Environment.
  inline void initArch(uint32_t arch, uint32_t subArch = 0u) noexcept {
    _arch = uint8_t(arch);
    _subArch = uint8_t(subArch);
  }

  inline void reset() noexcept { memset(this, 0, sizeof(*this)); }

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline CpuInfo& operator=(const CpuInfo& other) noexcept = default;

  //! \}

  //! \name Accessors
  //! \{

  //! Returns the CPU architecture id, see \ref Environment::Arch.
  inline uint32_t arch() const noexcept { return _arch; }
  //! Returns the CPU architecture sub-id, see \ref Environment::SubArch.
  inline uint32_t subArch() const noexcept { return _subArch; }

  //! Returns the CPU family ID.
  inline uint32_t familyId() const noexcept { return _familyId; }
  //! Returns the CPU model ID.
  inline uint32_t modelId() const noexcept { return _modelId; }
  //! Returns the CPU brand id.
  inline uint32_t brandId() const noexcept { return _brandId; }
  //! Returns the CPU stepping.
  inline uint32_t stepping() const noexcept { return _stepping; }
  //! Returns the processor type.
  inline uint32_t processorType() const noexcept { return _processorType; }
  //! Returns the number of maximum logical processors.
  inline uint32_t maxLogicalProcessors() const noexcept { return _maxLogicalProcessors; }

  //! Returns the size of a cache line flush.
  inline uint32_t cacheLineSize() const noexcept { return _cacheLineSize; }
  //! Returns number of hardware threads available.
  inline uint32_t hwThreadCount() const noexcept { return _hwThreadCount; }

  //! Returns the CPU vendor.
  inline const char* vendor() const noexcept { return _vendor.str; }
  //! Tests whether the CPU vendor is equal to `s`.
  inline bool isVendor(const char* s) const noexcept { return _vendor.eq(s); }

  //! Returns the CPU brand string.
  inline const char* brand() const noexcept { return _brand.str; }

  //! Returns all CPU features as `BaseFeatures`, cast to your arch-specific class
  //! if needed.
  template<typename T = BaseFeatures>
  inline const T& features() const noexcept { return _features.as<T>(); }

  //! Tests whether the CPU has the given `feature`.
  inline bool hasFeature(uint32_t featureId) const noexcept { return _features.has(featureId); }
  //! Adds the given CPU `feature` to the list of this CpuInfo features.
  inline CpuInfo& addFeature(uint32_t featureId) noexcept { _features.add(featureId); return *this; }

  //! \}
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_CPUINFO_H_INCLUDED
