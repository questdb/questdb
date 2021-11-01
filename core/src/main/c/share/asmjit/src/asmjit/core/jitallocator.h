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

#ifndef ASMJIT_CORE_JITALLOCATOR_H_INCLUDED
#define ASMJIT_CORE_JITALLOCATOR_H_INCLUDED

#include "../core/api-config.h"
#ifndef ASMJIT_NO_JIT

#include "../core/globals.h"
#include "../core/virtmem.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_virtual_memory
//! \{

// ============================================================================
// [asmjit::JitAllocator]
// ============================================================================

//! A simple implementation of memory manager that uses `asmjit::VirtMem`
//! functions to manage virtual memory for JIT compiled code.
//!
//! Implementation notes:
//!
//! - Granularity of allocated blocks is different than granularity for a typical
//!   C malloc. In addition, the allocator can use several memory pools having a
//!   different granularity to minimize the maintenance overhead. Multiple pools
//!   feature requires `kFlagUseMultiplePools` flag to be set.
//!
//! - The allocator doesn't store any information in executable memory, instead,
//!   the implementation uses two bit-vectors to manage allocated memory of each
//!   allocator-block. The first bit-vector called 'used' is used to track used
//!   memory (where each bit represents memory size defined by granularity) and
//!   the second bit vector called 'stop' is used as a sentinel to mark where
//!   the allocated area ends.
//!
//! - Internally, the allocator also uses RB tree to keep track of all blocks
//!   across all pools. Each inserted block is added to the tree so it can be
//!   matched fast during `release()` and `shrink()`.
class JitAllocator {
public:
  ASMJIT_NONCOPYABLE(JitAllocator)

  struct Impl {
    //! Allocator options, see \ref JitAllocator::Options.
    uint32_t options;
    //! Base block size (0 if the allocator is not initialized).
    uint32_t blockSize;
    //! Base granularity (0 if the allocator is not initialized).
    uint32_t granularity;
    //! A pattern that is used to fill unused memory if secure mode is enabled.
    uint32_t fillPattern;
  };

  //! Allocator implementation (private).
  Impl* _impl;

  enum Options : uint32_t {
    //! Enables the use of an anonymous memory-mapped memory that is mapped into
    //! two buffers having a different pointer. The first buffer has read and
    //! execute permissions and the second buffer has read+write permissions.
    //!
    //! See \ref VirtMem::allocDualMapping() for more details about this feature.
    kOptionUseDualMapping = 0x00000001u,

    //! Enables the use of multiple pools with increasing granularity instead of
    //! a single pool. This flag would enable 3 internal pools in total having
    //! 64, 128, and 256 bytes granularity.
    //!
    //! This feature is only recommended for users that generate a lot of code
    //! and would like to minimize the overhead of `JitAllocator` itself by
    //! having blocks of different allocation granularities. Using this feature
    //! only for few allocations won't pay off as the allocator may need to
    //! create more blocks initially before it can take the advantage of
    //! variable block granularity.
    kOptionUseMultiplePools = 0x00000002u,

    //! Always fill reserved memory by a fill-pattern.
    //!
    //! Causes a new block to be cleared by the fill pattern and freshly
    //! released memory to be cleared before making it ready for another use.
    kOptionFillUnusedMemory = 0x00000004u,

    //! When this flag is set the allocator would immediately release unused
    //! blocks during `release()` or `reset()`. When this flag is not set the
    //! allocator would keep one empty block in each pool to prevent excessive
    //! virtual memory allocations and deallocations in border cases, which
    //! involve constantly allocating and deallocating a single block caused
    //! by repetitive calling `alloc()` and `release()` when the allocator has
    //! either no blocks or have all blocks fully occupied.
    kOptionImmediateRelease = 0x00000008u,

    //! Use a custom fill pattern, must be combined with `kFlagFillUnusedMemory`.
    kOptionCustomFillPattern = 0x10000000u
  };

  //! \name Construction & Destruction
  //! \{

  //! Parameters that can be passed to `JitAllocator` constructor.
  //!
  //! Use it like this:
  //!
  //! ```
  //! // Zero initialize (zero means the default value) and change what you need.
  //! JitAllocator::CreateParams params {};
  //! params.blockSize = 1024 * 1024;
  //!
  //! // Create the allocator.
  //! JitAllocator allocator(&params);
  //! ```
  struct CreateParams {
    //! Allocator options, see \ref JitAllocator::Options.
    //!
    //! No options are used by default.
    uint32_t options;

    //! Base size of a single block in bytes (default 64kB).
    //!
    //! \remarks Block size must be equal or greater to page size and must be
    //! power of 2. If the input is not valid then the default block size will
    //! be used instead.
    uint32_t blockSize;

    //! Base granularity (and also natural alignment) of allocations in bytes
    //! (default 64).
    //!
    //! Since the `JitAllocator` uses bit-arrays to mark used memory the
    //! granularity also specifies how many bytes correspond to a single bit in
    //! such bit-array. Higher granularity means more waste of virtual memory
    //! (as it increases the natural alignment), but smaller bit-arrays as less
    //! bits would be required per a single block.
    uint32_t granularity;

    //! Patter to use to fill unused memory.
    //!
    //! Only used if \ref kOptionCustomFillPattern is set.
    uint32_t fillPattern;

    // Reset the content of `CreateParams`.
    inline void reset() noexcept { memset(this, 0, sizeof(*this)); }
  };

  //! Creates a `JitAllocator` instance.
  explicit ASMJIT_API JitAllocator(const CreateParams* params = nullptr) noexcept;
  //! Destroys the `JitAllocator` instance and release all blocks held.
  ASMJIT_API ~JitAllocator() noexcept;

  inline bool isInitialized() const noexcept { return _impl->blockSize == 0; }

  //! Free all allocated memory - makes all pointers returned by `alloc()` invalid.
  //!
  //! \remarks This function is not thread-safe as it's designed to be used when
  //! nobody else is using allocator. The reason is that there is no point of
  //1 calling `reset()` when the allocator is still in use.
  ASMJIT_API void reset(uint32_t resetPolicy = Globals::kResetSoft) noexcept;

  //! \}

  //! \name Accessors
  //! \{

  //! Returns allocator options, see `Flags`.
  inline uint32_t options() const noexcept { return _impl->options; }
  //! Tests whether the allocator has the given `option` set.
  inline bool hasOption(uint32_t option) const noexcept { return (_impl->options & option) != 0; }

  //! Returns a base block size (a minimum size of block that the allocator would allocate).
  inline uint32_t blockSize() const noexcept { return _impl->blockSize; }
  //! Returns granularity of the allocator.
  inline uint32_t granularity() const noexcept { return _impl->granularity; }
  //! Returns pattern that is used to fill unused memory if `kFlagUseFillPattern` is set.
  inline uint32_t fillPattern() const noexcept { return _impl->fillPattern; }

  //! \}

  //! \name Alloc & Release
  //! \{

  //! Allocate `size` bytes of virtual memory.
  //!
  //! \remarks This function is thread-safe.
  ASMJIT_API Error alloc(void** roPtrOut, void** rwPtrOut, size_t size) noexcept;

  //! Release a memory returned by `alloc()`.
  //!
  //! \remarks This function is thread-safe.
  ASMJIT_API Error release(void* roPtr) noexcept;

  //! Free extra memory allocated with `p` by restricting it to `newSize` size.
  //!
  //! \remarks This function is thread-safe.
  ASMJIT_API Error shrink(void* roPtr, size_t newSize) noexcept;

  //! \}

  //! \name Statistics
  //! \{

  //! Statistics about `JitAllocator`.
  struct Statistics {
    //! Number of blocks `JitAllocator` maintains.
    size_t _blockCount;
    //! How many bytes are currently used / allocated.
    size_t _usedSize;
    //! How many bytes are currently reserved by the allocator.
    size_t _reservedSize;
    //! Allocation overhead (in bytes) required to maintain all blocks.
    size_t _overheadSize;

    inline void reset() noexcept {
      _blockCount = 0;
      _usedSize = 0;
      _reservedSize = 0;
      _overheadSize = 0;
    }

    //! Returns count of blocks managed by `JitAllocator` at the moment.
    inline size_t blockCount() const noexcept { return _blockCount; }

    //! Returns how many bytes are currently used.
    inline size_t usedSize() const noexcept { return _usedSize; }
    //! Returns the number of bytes unused by the allocator at the moment.
    inline size_t unusedSize() const noexcept { return _reservedSize - _usedSize; }
    //! Returns the total number of bytes bytes reserved by the allocator (sum of sizes of all blocks).
    inline size_t reservedSize() const noexcept { return _reservedSize; }
    //! Returns the number of bytes the allocator needs to manage the allocated memory.
    inline size_t overheadSize() const noexcept { return _overheadSize; }

    inline double usedSizeAsPercent() const noexcept {
      return (double(usedSize()) / (double(reservedSize()) + 1e-16)) * 100.0;
    }

    inline double unusedSizeAsPercent() const noexcept {
      return (double(unusedSize()) / (double(reservedSize()) + 1e-16)) * 100.0;
    }

    inline double overheadSizeAsPercent() const noexcept {
      return (double(overheadSize()) / (double(reservedSize()) + 1e-16)) * 100.0;
    }
  };

  //! Returns JIT allocator statistics.
  //!
  //! \remarks This function is thread-safe.
  ASMJIT_API Statistics statistics() const noexcept;

  //! \}
};

//! \}

ASMJIT_END_NAMESPACE

#endif
#endif
