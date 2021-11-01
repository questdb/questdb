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
#ifndef ASMJIT_NO_JIT

#include "../core/cpuinfo.h"
#include "../core/jitruntime.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::JitRuntime - Utilities]
// ============================================================================

// Only useful on non-x86 architectures.
static inline void JitRuntime_flushInstructionCache(const void* p, size_t size) noexcept {
#if ASMJIT_ARCH_X86
  DebugUtils::unused(p, size);
#else
# if defined(_WIN32)
  // Windows has a built-in support in `kernel32.dll`.
  ::FlushInstructionCache(::GetCurrentProcess(), p, size);
# elif defined(__GNUC__)
  char* start = static_cast<char*>(const_cast<void*>(p));
  char* end = start + size;
  __builtin___clear_cache(start, end);
# else
  DebugUtils::unused(p, size);
# endif
#endif
}

// ============================================================================
// [asmjit::JitRuntime - Construction / Destruction]
// ============================================================================

JitRuntime::JitRuntime(const JitAllocator::CreateParams* params) noexcept
  : _allocator(params) {
  _environment = hostEnvironment();
  _environment.setFormat(Environment::kFormatJIT);
}

JitRuntime::~JitRuntime() noexcept {}

// ============================================================================
// [asmjit::JitRuntime - Interface]
// ============================================================================

Error JitRuntime::_add(void** dst, CodeHolder* code) noexcept {
  *dst = nullptr;

  ASMJIT_PROPAGATE(code->flatten());
  ASMJIT_PROPAGATE(code->resolveUnresolvedLinks());

  size_t estimatedCodeSize = code->codeSize();
  if (ASMJIT_UNLIKELY(estimatedCodeSize == 0))
    return DebugUtils::errored(kErrorNoCodeGenerated);

  uint8_t* ro;
  uint8_t* rw;
  ASMJIT_PROPAGATE(_allocator.alloc((void**)&ro, (void**)&rw, estimatedCodeSize));

  // Relocate the code.
  Error err = code->relocateToBase(uintptr_t((void*)ro));
  if (ASMJIT_UNLIKELY(err)) {
    _allocator.release(ro);
    return err;
  }

  // Recalculate the final code size and shrink the memory we allocated for it
  // in case that some relocations didn't require records in an address table.
  size_t codeSize = code->codeSize();

  for (Section* section : code->_sections) {
    size_t offset = size_t(section->offset());
    size_t bufferSize = size_t(section->bufferSize());
    size_t virtualSize = size_t(section->virtualSize());

    ASMJIT_ASSERT(offset + bufferSize <= codeSize);
    memcpy(rw + offset, section->data(), bufferSize);

    if (virtualSize > bufferSize) {
      ASMJIT_ASSERT(offset + virtualSize <= codeSize);
      memset(rw + offset + bufferSize, 0, virtualSize - bufferSize);
    }
  }

  if (codeSize < estimatedCodeSize)
    _allocator.shrink(ro, codeSize);

  flush(ro, codeSize);
  *dst = ro;

  return kErrorOk;
}

Error JitRuntime::_release(void* p) noexcept {
  return _allocator.release(p);
}

void JitRuntime::flush(const void* p, size_t size) noexcept {
  JitRuntime_flushInstructionCache(p, size);
}

ASMJIT_END_NAMESPACE

#endif
