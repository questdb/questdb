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

#ifndef ASMJIT_CORE_OSUTILS_P_H_INCLUDED
#define ASMJIT_CORE_OSUTILS_P_H_INCLUDED

#include "../core/osutils.h"

ASMJIT_BEGIN_NAMESPACE

//! \cond INTERNAL
//! \addtogroup asmjit_utilities
//! \{

// ============================================================================
// [asmjit::Lock]
// ============================================================================

#if defined(_WIN32)

// Windows implementation.
static_assert(sizeof(Lock::Handle) == sizeof(CRITICAL_SECTION), "asmjit::Lock::Handle layout must match CRITICAL_SECTION");
static_assert(alignof(Lock::Handle) == alignof(CRITICAL_SECTION), "asmjit::Lock::Handle alignment must match CRITICAL_SECTION");

inline Lock::Lock() noexcept { InitializeCriticalSection(reinterpret_cast<CRITICAL_SECTION*>(&_handle)); }
inline Lock::~Lock() noexcept { DeleteCriticalSection(reinterpret_cast<CRITICAL_SECTION*>(&_handle)); }
inline void Lock::lock() noexcept { EnterCriticalSection(reinterpret_cast<CRITICAL_SECTION*>(&_handle)); }
inline void Lock::unlock() noexcept { LeaveCriticalSection(reinterpret_cast<CRITICAL_SECTION*>(&_handle)); }

#elif !defined(__EMSCRIPTEN__)

// PThread implementation.
#ifdef PTHREAD_MUTEX_INITIALIZER
inline Lock::Lock() noexcept : _handle(PTHREAD_MUTEX_INITIALIZER) {}
#else
inline Lock::Lock() noexcept { pthread_mutex_init(&_handle, nullptr); }
#endif
inline Lock::~Lock() noexcept { pthread_mutex_destroy(&_handle); }
inline void Lock::lock() noexcept { pthread_mutex_lock(&_handle); }
inline void Lock::unlock() noexcept { pthread_mutex_unlock(&_handle); }

#else

// Dummy implementation - Emscripten or other unsupported platform.
inline Lock::Lock() noexcept {}
inline Lock::~Lock() noexcept {}
inline void Lock::lock() noexcept {}
inline void Lock::unlock() noexcept {}

#endif

// ============================================================================
// [asmjit::LockGuard]
// ============================================================================

//! Scoped lock.
class LockGuard {
public:
  ASMJIT_NONCOPYABLE(LockGuard)

  Lock& _target;

  inline LockGuard(Lock& target) noexcept
    : _target(target) { _target.lock(); }
  inline ~LockGuard() noexcept { _target.unlock(); }
};

//! \}
//! \endcond

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_OSUTILS_P_H_INCLUDED

