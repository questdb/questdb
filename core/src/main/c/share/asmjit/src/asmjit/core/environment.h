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

#ifndef ASMJIT_CORE_ENVIRONMENT_H_INCLUDED
#define ASMJIT_CORE_ENVIRONMENT_H_INCLUDED

#include "../core/globals.h"

#if defined(__APPLE__)
  #include <TargetConditionals.h>
#endif

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_core
//! \{

// ============================================================================
// [asmjit::Environment]
// ============================================================================

//! Represents an environment, which is usually related to a \ref Target.
//!
//! Environment has usually an 'arch-subarch-vendor-os-abi' format, which is
//! sometimes called "Triple" (historically it used to be 3 only parts) or
//! "Tuple", which is a convention used by Debian Linux.
//!
//! AsmJit doesn't support all possible combinations or architectures and ABIs,
//! however, it models the environment similarly to other compilers for future
//! extensibility.
class Environment {
public:
  //! Architecture type, see \ref Arch.
  uint8_t _arch;
  //! Sub-architecture type, see \ref SubArch.
  uint8_t _subArch;
  //! Vendor type, see \ref Vendor.
  uint8_t _vendor;
  //! Platform type, see \ref Platform.
  uint8_t _platform;
  //! ABI type, see \ref Abi.
  uint8_t _abi;
  //! Object format, see \ref Format.
  uint8_t _format;
  //! Reserved for future use, must be zero.
  uint16_t _reserved;

  //! Architecture.
  enum Arch : uint32_t {
    //! Unknown or uninitialized architecture.
    kArchUnknown = 0,

    //! Mask used by 32-bit architectures (odd are 32-bit, even are 64-bit).
    kArch32BitMask = 0x01,
    //! Mask used by big-endian architectures.
    kArchBigEndianMask = 0x80u,

    //! 32-bit X86 architecture.
    kArchX86 = 1,
    //! 64-bit X86 architecture also known as X86_64 and AMD64.
    kArchX64 = 2,

    //! 32-bit RISC-V architecture.
    kArchRISCV32 = 3,
    //! 64-bit RISC-V architecture.
    kArchRISCV64 = 4,

    //! 32-bit ARM architecture (little endian).
    kArchARM = 5,
    //! 32-bit ARM architecture (big endian).
    kArchARM_BE = kArchARM | kArchBigEndianMask,
    //! 64-bit ARM architecture in (little endian).
    kArchAArch64 = 6,
    //! 64-bit ARM architecture in (big endian).
    kArchAArch64_BE = kArchAArch64 | kArchBigEndianMask,
    //! 32-bit ARM in Thumb mode (little endian).
    kArchThumb = 7,
    //! 32-bit ARM in Thumb mode (big endian).
    kArchThumb_BE = kArchThumb | kArchBigEndianMask,

    // 8 is not used, even numbers are 64-bit architectures.

    //! 32-bit MIPS architecture in (little endian).
    kArchMIPS32_LE = 9,
    //! 32-bit MIPS architecture in (big endian).
    kArchMIPS32_BE = kArchMIPS32_LE | kArchBigEndianMask,
    //! 64-bit MIPS architecture in (little endian).
    kArchMIPS64_LE = 10,
    //! 64-bit MIPS architecture in (big endian).
    kArchMIPS64_BE = kArchMIPS64_LE | kArchBigEndianMask,

    //! Count of architectures.
    kArchCount = 11
  };

  //! Sub-architecture.
  enum SubArch : uint32_t {
    //! Unknown or uninitialized architecture sub-type.
    kSubArchUnknown = 0,

    //! Count of sub-architectures.
    kSubArchCount
  };

  //! Vendor.
  //!
  //! \note AsmJit doesn't use vendor information at the moment. It's provided
  //! for future use, if required.
  enum Vendor : uint32_t {
    //! Unknown or uninitialized vendor.
    kVendorUnknown = 0,

    //! Count of vendor identifiers.
    kVendorCount
  };

  //! Platform / OS.
  enum Platform : uint32_t {
    //! Unknown or uninitialized platform.
    kPlatformUnknown = 0,

    //! Windows OS.
    kPlatformWindows,

    //! Other platform, most likely POSIX based.
    kPlatformOther,

    //! Linux OS.
    kPlatformLinux,
    //! GNU/Hurd OS.
    kPlatformHurd,

    //! FreeBSD OS.
    kPlatformFreeBSD,
    //! OpenBSD OS.
    kPlatformOpenBSD,
    //! NetBSD OS.
    kPlatformNetBSD,
    //! DragonFly BSD OS.
    kPlatformDragonFlyBSD,

    //! Haiku OS.
    kPlatformHaiku,

    //! Apple OSX.
    kPlatformOSX,
    //! Apple iOS.
    kPlatformIOS,
    //! Apple TVOS.
    kPlatformTVOS,
    //! Apple WatchOS.
    kPlatformWatchOS,

    //! Emscripten platform.
    kPlatformEmscripten,

    //! Count of platform identifiers.
    kPlatformCount
  };

  //! ABI.
  enum Abi : uint32_t {
    //! Unknown or uninitialied environment.
    kAbiUnknown = 0,
    //! Microsoft ABI.
    kAbiMSVC,
    //! GNU ABI.
    kAbiGNU,
    //! Android Environment / ABI.
    kAbiAndroid,
    //! Cygwin ABI.
    kAbiCygwin,

    //! Count of known ABI types.
    kAbiCount
  };

  //! Object format.
  //!
  //! \note AsmJit doesn't really use anything except \ref kFormatUnknown and
  //! \ref kFormatJIT at the moment. Object file formats are provided for
  //! future extensibility and a possibility to generate object files at some
  //! point.
  enum Format : uint32_t {
    //! Unknown or uninitialized object format.
    kFormatUnknown = 0,

    //! JIT code generation object, most likely \ref JitRuntime or a custom
    //! \ref Target implementation.
    kFormatJIT,

    //! Executable and linkable format (ELF).
    kFormatELF,
    //! Common object file format.
    kFormatCOFF,
    //! Extended COFF object format.
    kFormatXCOFF,
    //! Mach object file format.
    kFormatMachO,

    //! Count of object format types.
    kFormatCount
  };

  //! \name Environment Detection
  //! \{

#ifdef _DOXYGEN
  //! Architecture detected at compile-time (architecture of the host).
  static constexpr Arch kArchHost = DETECTED_AT_COMPILE_TIME;
  //! Sub-architecture detected at compile-time (sub-architecture of the host).
  static constexpr SubArch kSubArchHost = DETECTED_AT_COMPILE_TIME;
  //! Vendor detected at compile-time (vendor of the host).
  static constexpr Vendor kVendorHost = DETECTED_AT_COMPILE_TIME;
  //! Platform detected at compile-time (platform of the host).
  static constexpr Platform kPlatformHost = DETECTED_AT_COMPILE_TIME;
  //! ABI detected at compile-time (ABI of the host).
  static constexpr Abi kAbiHost = DETECTED_AT_COMPILE_TIME;
#else
  static constexpr Arch kArchHost =
    ASMJIT_ARCH_X86 == 32 ? kArchX86 :
    ASMJIT_ARCH_X86 == 64 ? kArchX64 :

    ASMJIT_ARCH_ARM == 32 && ASMJIT_ARCH_LE ? kArchARM :
    ASMJIT_ARCH_ARM == 32 && ASMJIT_ARCH_BE ? kArchARM_BE :
    ASMJIT_ARCH_ARM == 64 && ASMJIT_ARCH_LE ? kArchAArch64 :
    ASMJIT_ARCH_ARM == 64 && ASMJIT_ARCH_BE ? kArchAArch64_BE :

    ASMJIT_ARCH_MIPS == 32 && ASMJIT_ARCH_LE ? kArchMIPS32_LE :
    ASMJIT_ARCH_MIPS == 32 && ASMJIT_ARCH_BE ? kArchMIPS32_BE :
    ASMJIT_ARCH_MIPS == 64 && ASMJIT_ARCH_LE ? kArchMIPS64_LE :
    ASMJIT_ARCH_MIPS == 64 && ASMJIT_ARCH_BE ? kArchMIPS64_BE :

    kArchUnknown;

  static constexpr SubArch kSubArchHost =
    kSubArchUnknown;

  static constexpr Vendor kVendorHost =
    kVendorUnknown;

  static constexpr Platform kPlatformHost =
#if defined(__EMSCRIPTEN__)
    kPlatformEmscripten
#elif defined(_WIN32)
    kPlatformWindows
#elif defined(__linux__)
    kPlatformLinux
#elif defined(__gnu_hurd__)
    kPlatformHurd
#elif defined(__FreeBSD__)
    kPlatformFreeBSD
#elif defined(__OpenBSD__)
    kPlatformOpenBSD
#elif defined(__NetBSD__)
    kPlatformNetBSD
#elif defined(__DragonFly__)
    kPlatformDragonFlyBSD
#elif defined(__HAIKU__)
    kPlatformHaiku
#elif defined(__APPLE__) && TARGET_OS_OSX
    kPlatformOSX
#elif defined(__APPLE__) && TARGET_OS_TV
    kPlatformTVOS
#elif defined(__APPLE__) && TARGET_OS_WATCH
    kPlatformWatchOS
#elif defined(__APPLE__) && TARGET_OS_IPHONE
    kPlatformIOS
#else
    kPlatformOther
#endif
    ;

  static constexpr Abi kAbiHost =
#if defined(_MSC_VER)
    kAbiMSVC
#elif defined(__CYGWIN__)
    kAbiCygwin
#elif defined(__MINGW32__) || defined(__GLIBC__)
    kAbiGNU
#elif defined(__ANDROID__)
    kAbiAndroid
#else
    kAbiUnknown
#endif
    ;

#endif

  //! \}

  //! \name Construction / Destruction
  //! \{

  inline Environment() noexcept :
    _arch(uint8_t(kArchUnknown)),
    _subArch(uint8_t(kSubArchUnknown)),
    _vendor(uint8_t(kVendorUnknown)),
    _platform(uint8_t(kPlatformUnknown)),
    _abi(uint8_t(kAbiUnknown)),
    _format(uint8_t(kFormatUnknown)),
    _reserved(0) {}

  inline Environment(const Environment& other) noexcept = default;

  inline explicit Environment(uint32_t arch,
                              uint32_t subArch = kSubArchUnknown,
                              uint32_t vendor = kVendorUnknown,
                              uint32_t platform = kPlatformUnknown,
                              uint32_t abi = kAbiUnknown,
                              uint32_t format = kFormatUnknown) noexcept {
    init(arch, subArch, vendor, platform, abi, format);
  }

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline Environment& operator=(const Environment& other) noexcept = default;

  inline bool operator==(const Environment& other) const noexcept { return  equals(other); }
  inline bool operator!=(const Environment& other) const noexcept { return !equals(other); }

  //! \}

  //! \name Accessors
  //! \{

  //! Tests whether the environment is not set up.
  //!
  //! Returns true if all members are zero, and thus unknown.
  inline bool empty() const noexcept {
    // Unfortunately compilers won't optimize fields are checked one by one...
    return _packed() == 0;
  }

  //! Tests whether the environment is intialized, which means it must have
  //! a valid architecture.
  inline bool isInitialized() const noexcept {
    return _arch != kArchUnknown;
  }

  inline uint64_t _packed() const noexcept {
    uint64_t x;
    memcpy(&x, this, 8);
    return x;
  }

  //! Resets all members of the environment to zero / unknown.
  inline void reset() noexcept {
    _arch = uint8_t(kArchUnknown);
    _subArch = uint8_t(kSubArchUnknown);
    _vendor = uint8_t(kVendorUnknown);
    _platform = uint8_t(kPlatformUnknown);
    _abi = uint8_t(kAbiUnknown);
    _format = uint8_t(kFormatUnknown);
    _reserved = 0;
  }

  inline bool equals(const Environment& other) const noexcept {
    return _packed() == other._packed();
  }

  //! Returns the architecture, see \ref Arch.
  inline uint32_t arch() const noexcept { return _arch; }
  //! Returns the sub-architecture, see \ref SubArch.
  inline uint32_t subArch() const noexcept { return _subArch; }
  //! Returns vendor, see \ref Vendor.
  inline uint32_t vendor() const noexcept { return _vendor; }
  //! Returns target's platform or operating system, see \ref Platform.
  inline uint32_t platform() const noexcept { return _platform; }
  //! Returns target's ABI, see \ref Abi.
  inline uint32_t abi() const noexcept { return _abi; }
  //! Returns target's object format, see \ref Format.
  inline uint32_t format() const noexcept { return _format; }

  inline void init(uint32_t arch,
                   uint32_t subArch = kSubArchUnknown,
                   uint32_t vendor = kVendorUnknown,
                   uint32_t platform = kPlatformUnknown,
                   uint32_t abi = kAbiUnknown,
                   uint32_t format = kFormatUnknown) noexcept {
    _arch = uint8_t(arch);
    _subArch = uint8_t(subArch);
    _vendor = uint8_t(vendor);
    _platform = uint8_t(platform);
    _abi = uint8_t(abi);
    _format = uint8_t(format);
    _reserved = 0;
  }

  inline bool isArchX86() const noexcept { return _arch == kArchX86; }
  inline bool isArchX64() const noexcept { return _arch == kArchX64; }
  inline bool isArchRISCV32() const noexcept { return _arch == kArchRISCV32; }
  inline bool isArchRISCV64() const noexcept { return _arch == kArchRISCV64; }
  inline bool isArchARM() const noexcept { return (_arch & ~kArchBigEndianMask) == kArchARM; }
  inline bool isArchThumb() const noexcept { return (_arch & ~kArchBigEndianMask) == kArchThumb; }
  inline bool isArchAArch64() const noexcept { return (_arch & ~kArchBigEndianMask) == kArchAArch64; }
  inline bool isArchMIPS32() const noexcept { return (_arch & ~kArchBigEndianMask) == kArchMIPS32_LE; }
  inline bool isArchMIPS64() const noexcept { return (_arch & ~kArchBigEndianMask) == kArchMIPS64_LE; }

  //! Tests whether the architecture is 32-bit.
  inline bool is32Bit() const noexcept { return is32Bit(_arch); }
  //! Tests whether the architecture is 64-bit.
  inline bool is64Bit() const noexcept { return is64Bit(_arch); }

  //! Tests whether the architecture is little endian.
  inline bool isLittleEndian() const noexcept { return isLittleEndian(_arch); }
  //! Tests whether the architecture is big endian.
  inline bool isBigEndian() const noexcept { return isBigEndian(_arch); }

  //! Tests whether this architecture is of X86 family.
  inline bool isFamilyX86() const noexcept { return isFamilyX86(_arch); }
  //! Tests whether this architecture family is RISC-V (both 32-bit and 64-bit).
  inline bool isFamilyRISCV() const noexcept { return isFamilyRISCV(_arch); }
  //! Tests whether this architecture family is ARM, Thumb, or AArch64.
  inline bool isFamilyARM() const noexcept { return isFamilyARM(_arch); }
  //! Tests whether this architecture family is MISP or MIPS64.
  inline bool isFamilyMIPS() const noexcept { return isFamilyMIPS(_arch); }

  //! Tests whether the environment platform is Windows.
  inline bool isPlatformWindows() const noexcept { return _platform == kPlatformWindows; }

  //! Tests whether the environment platform is Linux.
  inline bool isPlatformLinux() const noexcept { return _platform == kPlatformLinux; }

  //! Tests whether the environment platform is Hurd.
  inline bool isPlatformHurd() const noexcept { return _platform == kPlatformHurd; }

  //! Tests whether the environment platform is Haiku.
  inline bool isPlatformHaiku() const noexcept { return _platform == kPlatformHaiku; }

  //! Tests whether the environment platform is any BSD.
  inline bool isPlatformBSD() const noexcept {
    return _platform == kPlatformFreeBSD ||
           _platform == kPlatformOpenBSD ||
           _platform == kPlatformNetBSD ||
           _platform == kPlatformDragonFlyBSD;
  }

  //! Tests whether the environment platform is any Apple platform (OSX, iOS, TVOS, WatchOS).
  inline bool isPlatformApple() const noexcept {
    return _platform == kPlatformOSX ||
           _platform == kPlatformIOS ||
           _platform == kPlatformTVOS ||
           _platform == kPlatformWatchOS;
  }

  //! Tests whether the ABI is MSVC.
  inline bool isAbiMSVC() const noexcept { return _abi == kAbiMSVC; }
  //! Tests whether the ABI is GNU.
  inline bool isAbiGNU() const noexcept { return _abi == kAbiGNU; }

  //! Returns a calculated stack alignment for this environment.
  ASMJIT_API uint32_t stackAlignment() const noexcept;

  //! Returns a native register size of this architecture.
  uint32_t registerSize() const noexcept { return registerSizeFromArch(_arch); }

  //! Sets the architecture to `arch`.
  inline void setArch(uint32_t arch) noexcept { _arch = uint8_t(arch); }
  //! Sets the sub-architecture to `subArch`.
  inline void setSubArch(uint32_t subArch) noexcept { _subArch = uint8_t(subArch); }
  //! Sets the vendor to `vendor`.
  inline void setVendor(uint32_t vendor) noexcept { _vendor = uint8_t(vendor); }
  //! Sets the platform to `platform`.
  inline void setPlatform(uint32_t platform) noexcept { _platform = uint8_t(platform); }
  //! Sets the ABI to `abi`.
  inline void setAbi(uint32_t abi) noexcept { _abi = uint8_t(abi); }
  //! Sets the object format to `format`.
  inline void setFormat(uint32_t format) noexcept { _format = uint8_t(format); }

  //! \}

  //! \name Static Utilities
  //! \{

  static inline bool isValidArch(uint32_t arch) noexcept {
    return (arch & ~kArchBigEndianMask) != 0 &&
           (arch & ~kArchBigEndianMask) < kArchCount;
  }

  //! Tests whether the given architecture `arch` is 32-bit.
  static inline bool is32Bit(uint32_t arch) noexcept {
    return (arch & kArch32BitMask) == kArch32BitMask;
  }

  //! Tests whether the given architecture `arch` is 64-bit.
  static inline bool is64Bit(uint32_t arch) noexcept {
    return (arch & kArch32BitMask) == 0;
  }

  //! Tests whether the given architecture `arch` is little endian.
  static inline bool isLittleEndian(uint32_t arch) noexcept {
    return (arch & kArchBigEndianMask) == 0;
  }

  //! Tests whether the given architecture `arch` is big endian.
  static inline bool isBigEndian(uint32_t arch) noexcept {
    return (arch & kArchBigEndianMask) == kArchBigEndianMask;
  }

  //! Tests whether the given architecture is AArch64.
  static inline bool isArchAArch64(uint32_t arch) noexcept {
    arch &= ~kArchBigEndianMask;
    return arch == kArchAArch64;
  }

  //! Tests whether the given architecture family is X86 or X64.
  static inline bool isFamilyX86(uint32_t arch) noexcept {
    return arch == kArchX86 ||
           arch == kArchX64;
  }

  //! Tests whether the given architecture family is RISC-V (both 32-bit and 64-bit).
  static inline bool isFamilyRISCV(uint32_t arch) noexcept {
    return arch == kArchRISCV32 ||
           arch == kArchRISCV64;
  }

  //! Tests whether the given architecture family is ARM, Thumb, or AArch64.
  static inline bool isFamilyARM(uint32_t arch) noexcept {
    arch &= ~kArchBigEndianMask;
    return arch == kArchARM ||
           arch == kArchAArch64 ||
           arch == kArchThumb;
  }

  //! Tests whether the given architecture family is MISP or MIPS64.
  static inline bool isFamilyMIPS(uint32_t arch) noexcept {
    arch &= ~kArchBigEndianMask;
    return arch == kArchMIPS32_LE ||
           arch == kArchMIPS64_LE;
  }

  //! Returns a native general purpose register size from the given architecture.
  static uint32_t registerSizeFromArch(uint32_t arch) noexcept {
    return is32Bit(arch) ? 4u : 8u;
  }

  //! \}
};

//! Returns the host environment constructed from preprocessor macros defined
//! by the compiler.
//!
//! The returned environment should precisely match the target host architecture,
//! sub-architecture, platform, and ABI.
static ASMJIT_INLINE Environment hostEnvironment() noexcept {
  return Environment(Environment::kArchHost,
                     Environment::kSubArchHost,
                     Environment::kVendorHost,
                     Environment::kPlatformHost,
                     Environment::kAbiHost,
                     Environment::kFormatUnknown);
}

static_assert(sizeof(Environment) == 8,
              "Environment must occupy exactly 8 bytes.");

//! \}

#ifndef ASMJIT_NO_DEPRECATED
class ASMJIT_DEPRECATED_STRUCT("Use Environment instead") ArchInfo : public Environment {
public:
  inline ArchInfo() noexcept : Environment() {}

  inline ArchInfo(const Environment& other) noexcept : Environment(other) {}
  inline explicit ArchInfo(uint32_t arch, uint32_t subArch = kSubArchUnknown) noexcept
    : Environment(arch, subArch) {}

  enum Id : uint32_t {
    kIdNone = Environment::kArchUnknown,
    kIdX86 = Environment::kArchX86,
    kIdX64 = Environment::kArchX64,
    kIdA32 = Environment::kArchARM,
    kIdA64 = Environment::kArchAArch64,
    kIdHost = Environment::kArchHost
  };

  enum SubType : uint32_t {
    kSubIdNone = Environment::kSubArchUnknown
  };

  static inline ArchInfo host() noexcept { return ArchInfo(hostEnvironment()); }
};
#endif // !ASMJIT_NO_DEPRECATED

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_ENVIRONMENT_H_INCLUDED
