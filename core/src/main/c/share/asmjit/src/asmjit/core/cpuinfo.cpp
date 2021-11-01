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

#include "../core/api-build_p.h"
#include "../core/cpuinfo.h"

#if !defined(_WIN32)
  #include <errno.h>
  #include <sys/utsname.h>
  #include <unistd.h>
#endif

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::CpuInfo - Detect - CPU NumThreads]
// ============================================================================

#if defined(_WIN32)
static inline uint32_t detectHWThreadCount() noexcept {
  SYSTEM_INFO info;
  ::GetSystemInfo(&info);
  return info.dwNumberOfProcessors;
}
#elif defined(_SC_NPROCESSORS_ONLN)
static inline uint32_t detectHWThreadCount() noexcept {
  long res = ::sysconf(_SC_NPROCESSORS_ONLN);
  return res <= 0 ? uint32_t(1) : uint32_t(res);
}
#else
static inline uint32_t detectHWThreadCount() noexcept {
  return 1;
}
#endif

// ============================================================================
// [asmjit::CpuInfo - Detect - CPU Features]
// ============================================================================

#if !defined(ASMJIT_NO_X86) && ASMJIT_ARCH_X86
namespace x86 { void detectCpu(CpuInfo& cpu) noexcept; }
#endif

#if defined(ASMJIT_BUILD_ARM) && ASMJIT_ARCH_ARM
namespace arm { void detectCpu(CpuInfo& cpu) noexcept; }
#endif

// ============================================================================
// [asmjit::CpuInfo - Detect - Static Initializer]
// ============================================================================

static uint32_t cpuInfoInitialized;
static CpuInfo cpuInfoGlobal(Globals::NoInit);

const CpuInfo& CpuInfo::host() noexcept {
  // This should never cause a problem as the resulting information should
  // always be the same.
  if (!cpuInfoInitialized) {
    CpuInfo cpuInfoLocal;

#if !defined(ASMJIT_NO_X86) && ASMJIT_ARCH_X86
    x86::detectCpu(cpuInfoLocal);
#endif

#if defined(ASMJIT_BUILD_ARM) && ASMJIT_ARCH_ARM
    arm::detectCpu(cpuInfoLocal);
#endif

    cpuInfoLocal._hwThreadCount = detectHWThreadCount();
    cpuInfoGlobal = cpuInfoLocal;
    cpuInfoInitialized = 1;
  }

  return cpuInfoGlobal;
}

ASMJIT_END_NAMESPACE
