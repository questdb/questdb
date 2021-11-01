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

#ifndef ASMJIT_CORE_OSUTILS_H_INCLUDED
#define ASMJIT_CORE_OSUTILS_H_INCLUDED

#include "../core/globals.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_utilities
//! \{

// ============================================================================
// [asmjit::OSUtils]
// ============================================================================

//! Operating system utilities.
namespace OSUtils {
  //! Gets the current CPU tick count, used for benchmarking (1ms resolution).
  ASMJIT_API uint32_t getTickCount() noexcept;
};

// ============================================================================
// [asmjit::Lock]
// ============================================================================

//! \cond INTERNAL

//! Lock.
//!
//! Lock is internal, it cannot be used outside of AsmJit, however, its internal
//! layout is exposed as it's used by some other classes, which are public.
class Lock {
public:
  ASMJIT_NONCOPYABLE(Lock)

#if defined(_WIN32)
#pragma pack(push, 8)
  struct ASMJIT_MAY_ALIAS Handle {
    void* DebugInfo;
    long LockCount;
    long RecursionCount;
    void* OwningThread;
    void* LockSemaphore;
    unsigned long* SpinCount;
  };
  Handle _handle;
#pragma pack(pop)
#elif !defined(__EMSCRIPTEN__)
  typedef pthread_mutex_t Handle;
  Handle _handle;
#endif

  inline Lock() noexcept;
  inline ~Lock() noexcept;

  inline void lock() noexcept;
  inline void unlock() noexcept;
};
//! \endcond

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_OSUTILS_H_INCLUDED
