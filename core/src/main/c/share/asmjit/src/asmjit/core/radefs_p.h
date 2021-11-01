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

#ifndef ASMJIT_CORE_RADEFS_P_H_INCLUDED
#define ASMJIT_CORE_RADEFS_P_H_INCLUDED

#include "../core/api-config.h"
#include "../core/archtraits.h"
#include "../core/compilerdefs.h"
#include "../core/logger.h"
#include "../core/operand.h"
#include "../core/support.h"
#include "../core/type.h"
#include "../core/zone.h"
#include "../core/zonevector.h"

ASMJIT_BEGIN_NAMESPACE

//! \cond INTERNAL
//! \addtogroup asmjit_ra
//! \{

// ============================================================================
// [Logging]
// ============================================================================

#ifndef ASMJIT_NO_LOGGING
# define ASMJIT_RA_LOG_FORMAT(...)  \
  do {                              \
    if (logger)                     \
      logger->logf(__VA_ARGS__);    \
  } while (0)
# define ASMJIT_RA_LOG_COMPLEX(...) \
  do {                              \
    if (logger) {                   \
      __VA_ARGS__                   \
    }                               \
  } while (0)
#else
# define ASMJIT_RA_LOG_FORMAT(...) ((void)0)
# define ASMJIT_RA_LOG_COMPLEX(...) ((void)0)
#endif

// ============================================================================
// [Forward Declarations]
// ============================================================================

class BaseRAPass;
class RABlock;
class BaseNode;
struct RAStackSlot;

typedef ZoneVector<RABlock*> RABlocks;
typedef ZoneVector<RAWorkReg*> RAWorkRegs;

// ============================================================================
// [asmjit::RAConstraints]
// ============================================================================

class RAConstraints {
public:
  uint32_t _availableRegs[BaseReg::kGroupVirt] {};

  inline RAConstraints() noexcept {}

  ASMJIT_NOINLINE Error init(uint32_t arch) noexcept {
    switch (arch) {
      case Environment::kArchX86:
      case Environment::kArchX64: {
        uint32_t registerCount = arch == Environment::kArchX86 ? 8 : 16;
        _availableRegs[BaseReg::kGroupGp] = Support::lsbMask<uint32_t>(registerCount) & ~Support::bitMask(4u);
        _availableRegs[BaseReg::kGroupVec] = Support::lsbMask<uint32_t>(registerCount);
        _availableRegs[BaseReg::kGroupOther0] = Support::lsbMask<uint32_t>(8);
        _availableRegs[BaseReg::kGroupOther1] = Support::lsbMask<uint32_t>(8);
        return kErrorOk;
      }

      case Environment::kArchAArch64: {
        _availableRegs[BaseReg::kGroupGp] = 0xFFFFFFFFu & ~Support::bitMask(18, 31u);
        _availableRegs[BaseReg::kGroupVec] = 0xFFFFFFFFu;
        _availableRegs[BaseReg::kGroupOther0] = 0;
        _availableRegs[BaseReg::kGroupOther1] = 0;
        return kErrorOk;
      }

      default:
        return DebugUtils::errored(kErrorInvalidArch);
    }
  }

  inline uint32_t availableRegs(uint32_t group) const noexcept { return _availableRegs[group]; }
};

// ============================================================================
// [asmjit::RAStrategy]
// ============================================================================

struct RAStrategy {
  uint8_t _type;

  enum StrategyType : uint32_t {
    kStrategySimple  = 0,
    kStrategyComplex = 1
  };

  inline RAStrategy() noexcept { reset(); }
  inline void reset() noexcept { memset(this, 0, sizeof(*this)); }

  inline uint32_t type() const noexcept { return _type; }
  inline void setType(uint32_t type) noexcept { _type = uint8_t(type); }

  inline bool isSimple() const noexcept { return _type == kStrategySimple; }
  inline bool isComplex() const noexcept { return _type >= kStrategyComplex; }
};


// ============================================================================
// [asmjit::RARegCount]
// ============================================================================

//! Count of virtual or physical registers per group.
//!
//! \note This class uses 8-bit integers to represent counters, it's only used
//! in places where this is sufficient - for example total count of machine's
//! physical registers, count of virtual registers per instruction, etc. There
//! is also `RALiveCount`, which uses 32-bit integers and is indeed much safer.
struct RARegCount {
  union {
    uint8_t _regs[4];
    uint32_t _packed;
  };

  //! \name Construction & Destruction
  //! \{

  //! Resets all counters to zero.
  inline void reset() noexcept { _packed = 0; }

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline uint8_t& operator[](uint32_t index) noexcept {
    ASMJIT_ASSERT(index < BaseReg::kGroupVirt);
    return _regs[index];
  }

  inline const uint8_t& operator[](uint32_t index) const noexcept {
    ASMJIT_ASSERT(index < BaseReg::kGroupVirt);
    return _regs[index];
  }

  inline RARegCount& operator=(const RARegCount& other) noexcept = default;

  inline bool operator==(const RARegCount& other) const noexcept { return _packed == other._packed; }
  inline bool operator!=(const RARegCount& other) const noexcept { return _packed != other._packed; }

  //! \}

  //! \name Utilities
  //! \{

  //! Returns the count of registers by the given register `group`.
  inline uint32_t get(uint32_t group) const noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);

    uint32_t shift = Support::byteShiftOfDWordStruct(group);
    return (_packed >> shift) & uint32_t(0xFF);
  }

  //! Sets the register count by a register `group`.
  inline void set(uint32_t group, uint32_t n) noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    ASMJIT_ASSERT(n <= 0xFF);

    uint32_t shift = Support::byteShiftOfDWordStruct(group);
    _packed = (_packed & ~uint32_t(0xFF << shift)) + (n << shift);
  }

  //! Adds the register count by a register `group`.
  inline void add(uint32_t group, uint32_t n = 1) noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    ASMJIT_ASSERT(0xFF - uint32_t(_regs[group]) >= n);

    uint32_t shift = Support::byteShiftOfDWordStruct(group);
    _packed += n << shift;
  }

  //! \}
};

// ============================================================================
// [asmjit::RARegIndex]
// ============================================================================

struct RARegIndex : public RARegCount {
  //! Build register indexes based on the given `count` of registers.
  inline void buildIndexes(const RARegCount& count) noexcept {
    uint32_t x = uint32_t(count._regs[0]);
    uint32_t y = uint32_t(count._regs[1]) + x;
    uint32_t z = uint32_t(count._regs[2]) + y;

    ASMJIT_ASSERT(y <= 0xFF);
    ASMJIT_ASSERT(z <= 0xFF);
    _packed = Support::bytepack32_4x8(0, x, y, z);
  }
};

// ============================================================================
// [asmjit::RARegMask]
// ============================================================================

//! Registers mask.
struct RARegMask {
  uint32_t _masks[BaseReg::kGroupVirt];

  //! \name Construction & Destruction
  //! \{

  inline void init(const RARegMask& other) noexcept {
    for (uint32_t i = 0; i < BaseReg::kGroupVirt; i++)
      _masks[i] = other._masks[i];
  }

  //! Reset all register masks to zero.
  inline void reset() noexcept {
    for (uint32_t i = 0; i < BaseReg::kGroupVirt; i++)
      _masks[i] = 0;
  }

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline RARegMask& operator=(const RARegMask& other) noexcept = default;

  inline bool operator==(const RARegMask& other) const noexcept {
    return _masks[0] == other._masks[0] &&
           _masks[1] == other._masks[1] &&
           _masks[2] == other._masks[2] &&
           _masks[3] == other._masks[3] ;
  }

  inline bool operator!=(const RARegMask& other) const noexcept {
    return !operator==(other);
  }

  inline uint32_t& operator[](uint32_t index) noexcept {
    ASMJIT_ASSERT(index < BaseReg::kGroupVirt);
    return _masks[index];
  }

  inline const uint32_t& operator[](uint32_t index) const noexcept {
    ASMJIT_ASSERT(index < BaseReg::kGroupVirt);
    return _masks[index];
  }

  //! \}

  //! \name Utilities
  //! \{

  //! Tests whether all register masks are zero (empty).
  inline bool empty() const noexcept {
    uint32_t m = 0;
    for (uint32_t i = 0; i < BaseReg::kGroupVirt; i++)
      m |= _masks[i];
    return m == 0;
  }

  inline bool has(uint32_t group, uint32_t mask = 0xFFFFFFFFu) const noexcept {
    ASMJIT_ASSERT(group < BaseReg::kGroupVirt);
    return (_masks[group] & mask) != 0;
  }

  template<class Operator>
  inline void op(const RARegMask& other) noexcept {
    for (uint32_t i = 0; i < BaseReg::kGroupVirt; i++)
      _masks[i] = Operator::op(_masks[i], other._masks[i]);
  }

  template<class Operator>
  inline void op(uint32_t group, uint32_t input) noexcept {
    _masks[group] = Operator::op(_masks[group], input);
  }

  //! \}
};

// ============================================================================
// [asmjit::RARegsStats]
// ============================================================================

//! Information associated with each instruction, propagated to blocks, loops,
//! and the whole function. This information can be used to do minor decisions
//! before the register allocator tries to do its job. For example to use fast
//! register allocation inside a block or loop it cannot have clobbered and/or
//! fixed registers, etc...
class RARegsStats {
public:
  uint32_t _packed = 0;

  enum Index : uint32_t {
    kIndexUsed       = 0,
    kIndexFixed      = 8,
    kIndexClobbered  = 16
  };

  enum Mask : uint32_t {
    kMaskUsed        = 0xFFu << kIndexUsed,
    kMaskFixed       = 0xFFu << kIndexFixed,
    kMaskClobbered   = 0xFFu << kIndexClobbered
  };

  inline void reset() noexcept { _packed = 0; }
  inline void combineWith(const RARegsStats& other) noexcept { _packed |= other._packed; }

  inline bool hasUsed() const noexcept { return (_packed & kMaskUsed) != 0u; }
  inline bool hasUsed(uint32_t group) const noexcept { return (_packed & Support::bitMask(kIndexUsed + group)) != 0u; }
  inline void makeUsed(uint32_t group) noexcept { _packed |= Support::bitMask(kIndexUsed + group); }

  inline bool hasFixed() const noexcept { return (_packed & kMaskFixed) != 0u; }
  inline bool hasFixed(uint32_t group) const noexcept { return (_packed & Support::bitMask(kIndexFixed + group)) != 0u; }
  inline void makeFixed(uint32_t group) noexcept { _packed |= Support::bitMask(kIndexFixed + group); }

  inline bool hasClobbered() const noexcept { return (_packed & kMaskClobbered) != 0u; }
  inline bool hasClobbered(uint32_t group) const noexcept { return (_packed & Support::bitMask(kIndexClobbered + group)) != 0u; }
  inline void makeClobbered(uint32_t group) noexcept { _packed |= Support::bitMask(kIndexClobbered + group); }
};

// ============================================================================
// [asmjit::RALiveCount]
// ============================================================================

//! Count of live registers, per group.
class RALiveCount {
public:
  uint32_t n[BaseReg::kGroupVirt] {};

  //! \name Construction & Destruction
  //! \{

  inline RALiveCount() noexcept = default;
  inline RALiveCount(const RALiveCount& other) noexcept = default;

  inline void init(const RALiveCount& other) noexcept {
    for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++)
      n[group] = other.n[group];
  }

  inline void reset() noexcept {
    for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++)
      n[group] = 0;
  }

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline RALiveCount& operator=(const RALiveCount& other) noexcept = default;

  inline uint32_t& operator[](uint32_t group) noexcept { return n[group]; }
  inline const uint32_t& operator[](uint32_t group) const noexcept { return n[group]; }

  //! \}

  //! \name Utilities
  //! \{

  template<class Operator>
  inline void op(const RALiveCount& other) noexcept {
    for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++)
      n[group] = Operator::op(n[group], other.n[group]);
  }

  //! \}
};

// ============================================================================
// [asmjit::RALiveInterval]
// ============================================================================

struct RALiveInterval {
  uint32_t a, b;

  enum Misc : uint32_t {
    kNaN = 0,
    kInf = 0xFFFFFFFFu
  };

  //! \name Construction & Destruction
  //! \{

  inline RALiveInterval() noexcept : a(0), b(0) {}
  inline RALiveInterval(uint32_t a, uint32_t b) noexcept : a(a), b(b) {}
  inline RALiveInterval(const RALiveInterval& other) noexcept : a(other.a), b(other.b) {}

  inline void init(uint32_t aVal, uint32_t bVal) noexcept {
    a = aVal;
    b = bVal;
  }
  inline void init(const RALiveInterval& other) noexcept { init(other.a, other.b); }
  inline void reset() noexcept { init(0, 0); }

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline RALiveInterval& operator=(const RALiveInterval& other) = default;

  //! \}

  //! \name Accessors
  //! \{

  inline bool isValid() const noexcept { return a < b; }
  inline uint32_t width() const noexcept { return b - a; }

  //! \}
};

// ============================================================================
// [asmjit::RALiveSpan<T>]
// ============================================================================

template<typename T>
class RALiveSpan : public RALiveInterval, public T {
public:
  typedef T DataType;

  //! \name Construction & Destruction
  //! \{

  inline RALiveSpan() noexcept : RALiveInterval(), T() {}
  inline RALiveSpan(const RALiveSpan<T>& other) noexcept : RALiveInterval(other), T() {}
  inline RALiveSpan(const RALiveInterval& interval, const T& data) noexcept : RALiveInterval(interval), T(data) {}
  inline RALiveSpan(uint32_t a, uint32_t b) noexcept : RALiveInterval(a, b), T() {}
  inline RALiveSpan(uint32_t a, uint32_t b, const T& data) noexcept : RALiveInterval(a, b), T(data) {}

  inline void init(const RALiveSpan<T>& other) noexcept {
    RALiveInterval::init(static_cast<const RALiveInterval&>(other));
    T::init(static_cast<const T&>(other));
  }

  inline void init(const RALiveSpan<T>& span, const T& data) noexcept {
    RALiveInterval::init(static_cast<const RALiveInterval&>(span));
    T::init(data);
  }

  inline void init(const RALiveInterval& interval, const T& data) noexcept {
    RALiveInterval::init(interval);
    T::init(data);
  }

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline RALiveSpan& operator=(const RALiveSpan& other) {
    init(other);
    return *this;
  }

  //! \}
};

// ============================================================================
// [asmjit::RALiveSpans<T>]
// ============================================================================

template<typename T>
class RALiveSpans {
public:
  ASMJIT_NONCOPYABLE(RALiveSpans)

  typedef typename T::DataType DataType;
  ZoneVector<T> _data;

  //! \name Construction & Destruction
  //! \{

  inline RALiveSpans() noexcept : _data() {}

  inline void reset() noexcept { _data.reset(); }
  inline void release(ZoneAllocator* allocator) noexcept { _data.release(allocator); }

  //! \}

  //! \name Accessors
  //! \{

  inline bool empty() const noexcept { return _data.empty(); }
  inline uint32_t size() const noexcept { return _data.size(); }

  inline T* data() noexcept { return _data.data(); }
  inline const T* data() const noexcept { return _data.data(); }

  inline bool isOpen() const noexcept {
    uint32_t size = _data.size();
    return size > 0 && _data[size - 1].b == RALiveInterval::kInf;
  }

  //! \}

  //! \name Utilities
  //! \{

  inline void swap(RALiveSpans<T>& other) noexcept { _data.swap(other._data); }

  //! Open the current live span.
  ASMJIT_INLINE Error openAt(ZoneAllocator* allocator, uint32_t start, uint32_t end) noexcept {
    bool wasOpen;
    return openAt(allocator, start, end, wasOpen);
  }

  ASMJIT_INLINE Error openAt(ZoneAllocator* allocator, uint32_t start, uint32_t end, bool& wasOpen) noexcept {
    uint32_t size = _data.size();
    wasOpen = false;

    if (size > 0) {
      T& last = _data[size - 1];
      if (last.b >= start) {
        wasOpen = last.b > start;
        last.b = end;
        return kErrorOk;
      }
    }

    return _data.append(allocator, T(start, end));
  }

  inline void closeAt(uint32_t end) noexcept {
    ASMJIT_ASSERT(!empty());

    uint32_t size = _data.size();
    _data[size - 1].b = end;
  }

  //! Returns the sum of width of all spans.
  //!
  //! \note Don't overuse, this iterates over all spans so it's O(N).
  //! It should be only called once and then cached.
  ASMJIT_INLINE uint32_t width() const noexcept {
    uint32_t width = 0;
    for (const T& span : _data)
      width += span.width();
    return width;
  }

  inline T& operator[](uint32_t index) noexcept { return _data[index]; }
  inline const T& operator[](uint32_t index) const noexcept { return _data[index]; }

  inline bool intersects(const RALiveSpans<T>& other) const noexcept {
    return intersects(*this, other);
  }

  ASMJIT_INLINE Error nonOverlappingUnionOf(ZoneAllocator* allocator, const RALiveSpans<T>& x, const RALiveSpans<T>& y, const DataType& yData) noexcept {
    uint32_t finalSize = x.size() + y.size();
    ASMJIT_PROPAGATE(_data.reserve(allocator, finalSize));

    T* dstPtr = _data.data();
    const T* xSpan = x.data();
    const T* ySpan = y.data();

    const T* xEnd = xSpan + x.size();
    const T* yEnd = ySpan + y.size();

    // Loop until we have intersection or either `xSpan == xEnd` or `ySpan == yEnd`,
    // which means that there is no intersection. We advance either `xSpan` or `ySpan`
    // depending on their ranges.
    if (xSpan != xEnd && ySpan != yEnd) {
      uint32_t xa, ya;
      xa = xSpan->a;
      for (;;) {
        while (ySpan->b <= xa) {
          dstPtr->init(*ySpan, yData);
          dstPtr++;
          if (++ySpan == yEnd)
            goto Done;
        }

        ya = ySpan->a;
        while (xSpan->b <= ya) {
          *dstPtr++ = *xSpan;
          if (++xSpan == xEnd)
            goto Done;
        }

        // We know that `xSpan->b > ySpan->a`, so check if `ySpan->b > xSpan->a`.
        xa = xSpan->a;
        if (ySpan->b > xa)
          return 0xFFFFFFFFu;
      }
    }

  Done:
    while (xSpan != xEnd) {
      *dstPtr++ = *xSpan++;
    }

    while (ySpan != yEnd) {
      dstPtr->init(*ySpan, yData);
      dstPtr++;
      ySpan++;
    }

    _data._setEndPtr(dstPtr);
    return kErrorOk;
  }

  static ASMJIT_INLINE bool intersects(const RALiveSpans<T>& x, const RALiveSpans<T>& y) noexcept {
    const T* xSpan = x.data();
    const T* ySpan = y.data();

    const T* xEnd = xSpan + x.size();
    const T* yEnd = ySpan + y.size();

    // Loop until we have intersection or either `xSpan == xEnd` or `ySpan == yEnd`,
    // which means that there is no intersection. We advance either `xSpan` or `ySpan`
    // depending on their end positions.
    if (xSpan == xEnd || ySpan == yEnd)
      return false;

    uint32_t xa, ya;
    xa = xSpan->a;

    for (;;) {
      while (ySpan->b <= xa)
        if (++ySpan == yEnd)
          return false;

      ya = ySpan->a;
      while (xSpan->b <= ya)
        if (++xSpan == xEnd)
          return false;

      // We know that `xSpan->b > ySpan->a`, so check if `ySpan->b > xSpan->a`.
      xa = xSpan->a;
      if (ySpan->b > xa)
        return true;
    }
  }

  //! \}
};

// ============================================================================
// [asmjit::RALiveStats]
// ============================================================================

//! Statistics about a register liveness.
class RALiveStats {
public:
  uint32_t _width = 0;
  float _freq = 0.0f;
  float _priority = 0.0f;

  //! \name Accessors
  //! \{

  inline uint32_t width() const noexcept { return _width; }
  inline float freq() const noexcept { return _freq; }
  inline float priority() const noexcept { return _priority; }

  //! \}
};

// ============================================================================
// [asmjit::LiveRegData]
// ============================================================================

struct LiveRegData {
  uint32_t id;

  inline explicit LiveRegData(uint32_t id = BaseReg::kIdBad) noexcept : id(id) {}
  inline LiveRegData(const LiveRegData& other) noexcept : id(other.id) {}

  inline void init(const LiveRegData& other) noexcept { id = other.id; }

  inline bool operator==(const LiveRegData& other) const noexcept { return id == other.id; }
  inline bool operator!=(const LiveRegData& other) const noexcept { return id != other.id; }
};

typedef RALiveSpan<LiveRegData> LiveRegSpan;
typedef RALiveSpans<LiveRegSpan> LiveRegSpans;

// ============================================================================
// [asmjit::RATiedReg]
// ============================================================================

//! Tied register merges one ore more register operand into a single entity. It
//! contains information about its access (Read|Write) and allocation slots
//! (Use|Out) that are used by the register allocator and liveness analysis.
struct RATiedReg {
  //! WorkReg id.
  uint32_t _workId;
  //! Allocation flags.
  uint32_t _flags;
  //! Registers where input {R|X} can be allocated to.
  uint32_t _allocableRegs;
  //! Indexes used to rewrite USE regs.
  uint32_t _useRewriteMask;
  //! Indexes used to rewrite OUT regs.
  uint32_t _outRewriteMask;

  union {
    struct {
      //! How many times the VirtReg is referenced in all operands.
      uint8_t _refCount;
      //! Physical register for use operation (ReadOnly / ReadWrite).
      uint8_t _useId;
      //! Physical register for out operation (WriteOnly).
      uint8_t _outId;
      //! Reserved for future use (padding).
      uint8_t _rmSize;
    };
    //! Packed data.
    uint32_t _packed;
  };

  //! Flags.
  //!
  //! Register access information is encoded in 4 flags in total:
  //!
  //!   - `kRead`  - Register is Read    (ReadWrite if combined with `kWrite`).
  //!   - `kWrite` - Register is Written (ReadWrite if combined with `kRead`).
  //!   - `kUse`   - Encoded as Read or ReadWrite.
  //!   - `kOut`   - Encoded as WriteOnly.
  //!
  //! Let's describe all of these on two X86 instructions:
  //!
  //!   - ADD x{R|W|Use},  x{R|Use}              -> {x:R|W|Use            }
  //!   - LEA x{  W|Out}, [x{R|Use} + x{R|Out}]  -> {x:R|W|Use|Out        }
  //!   - ADD x{R|W|Use},  y{R|Use}              -> {x:R|W|Use     y:R|Use}
  //!   - LEA x{  W|Out}, [x{R|Use} + y{R|Out}]  -> {x:R|W|Use|Out y:R|Use}
  //!
  //! It should be obvious from the example above how these flags get created.
  //! Each operand contains READ/WRITE information, which is then merged to
  //! RATiedReg's flags. However, we also need to represent the possitility to
  //! use see the operation as two independent operations - USE and OUT, because
  //! the register allocator will first allocate USE registers, and then assign
  //! OUT registers independently of USE registers.
  enum Flags : uint32_t {
    kRead         = OpRWInfo::kRead,     //!< Register is read.
    kWrite        = OpRWInfo::kWrite,    //!< Register is written.
    kRW           = OpRWInfo::kRW,       //!< Register both read and written.

    kUse          = 0x00000100u,         //!< Register has a USE slot (read/rw).
    kOut          = 0x00000200u,         //!< Register has an OUT slot (write-only).
    kUseRM        = 0x00000400u,         //!< Register in USE slot can be patched to memory.
    kOutRM        = 0x00000800u,         //!< Register in OUT slot can be patched to memory.

    kUseFixed     = 0x00001000u,         //!< Register has a fixed USE slot.
    kOutFixed     = 0x00002000u,         //!< Register has a fixed OUT slot.
    kUseDone      = 0x00004000u,         //!< Register USE slot has been allocated.
    kOutDone      = 0x00008000u,         //!< Register OUT slot has been allocated.

    kDuplicate    = 0x00010000u,         //!< Register must be duplicated (function call only).
    kLast         = 0x00020000u,         //!< Last occurrence of this VirtReg in basic block.
    kKill         = 0x00040000u,         //!< Kill this VirtReg after use.

    // Architecture specific flags are used during RATiedReg building to ensure
    // that architecture-specific constraints are handled properly. These flags
    // are not really needed after RATiedReg[] is built and copied to `RAInst`.

    kX86Gpb       = 0x01000000u          //!< This RATiedReg references GPB-LO or GPB-HI.
  };

  static_assert(kRead  == 0x1, "RATiedReg::kRead flag must be 0x1");
  static_assert(kWrite == 0x2, "RATiedReg::kWrite flag must be 0x2");
  static_assert(kRW    == 0x3, "RATiedReg::kRW combination must be 0x3");

  //! \name Construction & Destruction
  //! \{

  ASMJIT_INLINE void init(uint32_t workId, uint32_t flags, uint32_t allocableRegs, uint32_t useId, uint32_t useRewriteMask, uint32_t outId, uint32_t outRewriteMask, uint32_t rmSize = 0) noexcept {
    _workId = workId;
    _flags = flags;
    _allocableRegs = allocableRegs;
    _useRewriteMask = useRewriteMask;
    _outRewriteMask = outRewriteMask;
    _refCount = 1;
    _useId = uint8_t(useId);
    _outId = uint8_t(outId);
    _rmSize = uint8_t(rmSize);
  }

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline RATiedReg& operator=(const RATiedReg& other) noexcept = default;

  //! \}

  //! \name Accessors
  //! \{

  //! Returns the associated WorkReg id.
  inline uint32_t workId() const noexcept { return _workId; }

  //! Checks if the given `flag` is set, see `Flags`.
  inline bool hasFlag(uint32_t flag) const noexcept { return (_flags & flag) != 0; }

  //! Returns TiedReg flags, see `RATiedReg::Flags`.
  inline uint32_t flags() const noexcept { return _flags; }
  //! Adds tied register flags, see `Flags`.
  inline void addFlags(uint32_t flags) noexcept { _flags |= flags; }

  //! Tests whether the register is read (writes `true` also if it's Read/Write).
  inline bool isRead() const noexcept { return hasFlag(kRead); }
  //! Tests whether the register is written (writes `true` also if it's Read/Write).
  inline bool isWrite() const noexcept { return hasFlag(kWrite); }
  //! Tests whether the register is read only.
  inline bool isReadOnly() const noexcept { return (_flags & kRW) == kRead; }
  //! Tests whether the register is write only.
  inline bool isWriteOnly() const noexcept { return (_flags & kRW) == kWrite; }
  //! Tests whether the register is read and written.
  inline bool isReadWrite() const noexcept { return (_flags & kRW) == kRW; }

  //! Tests whether the tied register has use operand (Read/ReadWrite).
  inline bool isUse() const noexcept { return hasFlag(kUse); }
  //! Tests whether the tied register has out operand (Write).
  inline bool isOut() const noexcept { return hasFlag(kOut); }

  //! Tests whether the USE slot can be patched to memory operand.
  inline bool hasUseRM() const noexcept { return hasFlag(kUseRM); }
  //! Tests whether the OUT slot can be patched to memory operand.
  inline bool hasOutRM() const noexcept { return hasFlag(kOutRM); }

  inline uint32_t rmSize() const noexcept { return _rmSize; }

  inline void makeReadOnly() noexcept {
    _flags = (_flags & ~(kOut | kWrite)) | kUse;
    _useRewriteMask |= _outRewriteMask;
    _outRewriteMask = 0;
  }

  inline void makeWriteOnly() noexcept {
    _flags = (_flags & ~(kUse | kRead)) | kOut;
    _outRewriteMask |= _useRewriteMask;
    _useRewriteMask = 0;
  }

  //! Tests whether the register would duplicate.
  inline bool isDuplicate() const noexcept { return hasFlag(kDuplicate); }

  //! Tests whether the register (and the instruction it's part of) appears last in the basic block.
  inline bool isLast() const noexcept { return hasFlag(kLast); }
  //! Tests whether the register should be killed after USEd and/or OUTed.
  inline bool isKill() const noexcept { return hasFlag(kKill); }

  //! Tests whether the register is OUT or KILL (used internally by local register allocator).
  inline bool isOutOrKill() const noexcept { return hasFlag(kOut | kKill); }

  inline uint32_t allocableRegs() const noexcept { return _allocableRegs; }

  inline uint32_t refCount() const noexcept { return _refCount; }
  inline void addRefCount(uint32_t n = 1) noexcept { _refCount = uint8_t(_refCount + n); }

  //! Tests whether the register must be allocated to a fixed physical register before it's used.
  inline bool hasUseId() const noexcept { return _useId != BaseReg::kIdBad; }
  //! Tests whether the register must be allocated to a fixed physical register before it's written.
  inline bool hasOutId() const noexcept { return _outId != BaseReg::kIdBad; }

  //! Returns a physical register id used for 'use' operation.
  inline uint32_t useId() const noexcept { return _useId; }
  //! Returns a physical register id used for 'out' operation.
  inline uint32_t outId() const noexcept { return _outId; }

  inline uint32_t useRewriteMask() const noexcept { return _useRewriteMask; }
  inline uint32_t outRewriteMask() const noexcept { return _outRewriteMask; }

  //! Sets a physical register used for 'use' operation.
  inline void setUseId(uint32_t index) noexcept { _useId = uint8_t(index); }
  //! Sets a physical register used for 'out' operation.
  inline void setOutId(uint32_t index) noexcept { _outId = uint8_t(index); }

  inline bool isUseDone() const noexcept { return hasFlag(kUseDone); }
  inline bool isOutDone() const noexcept { return hasFlag(kUseDone); }

  inline void markUseDone() noexcept { addFlags(kUseDone); }
  inline void markOutDone() noexcept { addFlags(kUseDone); }

  //! \}
};

// ============================================================================
// [asmjit::RAWorkReg]
// ============================================================================

class RAWorkReg {
public:
  ASMJIT_NONCOPYABLE(RAWorkReg)

  //! RAPass specific ID used during analysis and allocation.
  uint32_t _workId = 0;
  //! Copy of ID used by \ref VirtReg.
  uint32_t _virtId = 0;

  //! Permanent association with \ref VirtReg.
  VirtReg* _virtReg = nullptr;
  //! Temporary association with \ref RATiedReg.
  RATiedReg* _tiedReg = nullptr;
  //! Stack slot associated with the register.
  RAStackSlot* _stackSlot = nullptr;

  //! Copy of a signature used by \ref VirtReg.
  RegInfo _info {};
  //! RAPass specific flags used during analysis and allocation.
  uint32_t _flags = 0;
  //! IDs of all physical registers this WorkReg has been allocated to.
  uint32_t _allocatedMask = 0;
  //! IDs of all physical registers that are clobbered during the lifetime of
  //! this WorkReg.
  //!
  //! This mask should be updated by `RAPass::buildLiveness()`, because it's
  //! global and should be updated after unreachable code has been removed.
  uint32_t _clobberSurvivalMask = 0;

  //! A byte-mask where each bit represents one valid byte of the register.
  uint64_t _regByteMask = 0;

  //! Argument index (or `kNoArgIndex` if none).
  uint8_t _argIndex = kNoArgIndex;
  //! Argument value index in the pack (0 by default).
  uint8_t _argValueIndex = 0;
  //! Global home register ID (if any, assigned by RA).
  uint8_t _homeRegId = BaseReg::kIdBad;
  //! Global hint register ID (provided by RA or user).
  uint8_t _hintRegId = BaseReg::kIdBad;

  //! Live spans of the `VirtReg`.
  LiveRegSpans _liveSpans {};
  //! Live statistics.
  RALiveStats _liveStats {};

  //! All nodes that read/write this VirtReg/WorkReg.
  ZoneVector<BaseNode*> _refs;
  //! All nodes that write to this VirtReg/WorkReg.
  ZoneVector<BaseNode*> _writes;

  enum Ids : uint32_t {
    kIdNone = 0xFFFFFFFFu
  };

  enum Flags : uint32_t {
    //! Has been coalesced to another WorkReg.
    kFlagCoalesced = 0x00000001u,
    //! Stack slot has to be allocated.
    kFlagStackUsed = 0x00000002u,
    //! Stack allocation is preferred.
    kFlagStackPreferred = 0x00000004u,
    //! Marked for stack argument reassignment.
    kFlagStackArgToStack = 0x00000008u
  };

  enum ArgIndex : uint32_t {
    kNoArgIndex = 0xFFu
  };

  //! \name Construction & Destruction
  //! \{

  ASMJIT_INLINE RAWorkReg(VirtReg* vReg, uint32_t workId) noexcept
    : _workId(workId),
      _virtId(vReg->id()),
      _virtReg(vReg),
      _info(vReg->info()) {}

  //! \}

  //! \name Accessors
  //! \{

  inline uint32_t workId() const noexcept { return _workId; }
  inline uint32_t virtId() const noexcept { return _virtId; }

  inline const char* name() const noexcept { return _virtReg->name(); }
  inline uint32_t nameSize() const noexcept { return _virtReg->nameSize(); }

  inline uint32_t typeId() const noexcept { return _virtReg->typeId(); }

  inline bool hasFlag(uint32_t flag) const noexcept { return (_flags & flag) != 0; }
  inline uint32_t flags() const noexcept { return _flags; }
  inline void addFlags(uint32_t flags) noexcept { _flags |= flags; }

  inline bool isStackUsed() const noexcept { return hasFlag(kFlagStackUsed); }
  inline void markStackUsed() noexcept { addFlags(kFlagStackUsed); }

  inline bool isStackPreferred() const noexcept { return hasFlag(kFlagStackPreferred); }
  inline void markStackPreferred() noexcept { addFlags(kFlagStackPreferred); }

  //! Tests whether this RAWorkReg has been coalesced with another one (cannot be used anymore).
  inline bool isCoalesced() const noexcept { return hasFlag(kFlagCoalesced); }

  inline const RegInfo& info() const noexcept { return _info; }
  inline uint32_t group() const noexcept { return _info.group(); }
  inline uint32_t signature() const noexcept { return _info.signature(); }

  inline VirtReg* virtReg() const noexcept { return _virtReg; }

  inline bool hasTiedReg() const noexcept { return _tiedReg != nullptr; }
  inline RATiedReg* tiedReg() const noexcept { return _tiedReg; }
  inline void setTiedReg(RATiedReg* tiedReg) noexcept { _tiedReg = tiedReg; }
  inline void resetTiedReg() noexcept { _tiedReg = nullptr; }

  inline bool hasStackSlot() const noexcept { return _stackSlot != nullptr; }
  inline RAStackSlot* stackSlot() const noexcept { return _stackSlot; }

  inline LiveRegSpans& liveSpans() noexcept { return _liveSpans; }
  inline const LiveRegSpans& liveSpans() const noexcept { return _liveSpans; }

  inline RALiveStats& liveStats() noexcept { return _liveStats; }
  inline const RALiveStats& liveStats() const noexcept { return _liveStats; }

  inline bool hasArgIndex() const noexcept { return _argIndex != kNoArgIndex; }
  inline uint32_t argIndex() const noexcept { return _argIndex; }
  inline uint32_t argValueIndex() const noexcept { return _argValueIndex; }

  inline void setArgIndex(uint32_t argIndex, uint32_t valueIndex) noexcept {
    _argIndex = uint8_t(argIndex);
    _argValueIndex = uint8_t(valueIndex);
  }

  inline bool hasHomeRegId() const noexcept { return _homeRegId != BaseReg::kIdBad; }
  inline uint32_t homeRegId() const noexcept { return _homeRegId; }
  inline void setHomeRegId(uint32_t physId) noexcept { _homeRegId = uint8_t(physId); }

  inline bool hasHintRegId() const noexcept { return _hintRegId != BaseReg::kIdBad; }
  inline uint32_t hintRegId() const noexcept { return _hintRegId; }
  inline void setHintRegId(uint32_t physId) noexcept { _hintRegId = uint8_t(physId); }

  inline uint32_t allocatedMask() const noexcept { return _allocatedMask; }
  inline void addAllocatedMask(uint32_t mask) noexcept { _allocatedMask |= mask; }

  inline uint32_t clobberSurvivalMask() const noexcept { return _clobberSurvivalMask; }
  inline void addClobberSurvivalMask(uint32_t mask) noexcept { _clobberSurvivalMask |= mask; }

  inline uint64_t regByteMask() const noexcept { return _regByteMask; }
  inline void setRegByteMask(uint64_t mask) noexcept { _regByteMask = mask; }

  //! \}
};

//! \}
//! \endcond

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_RADEFS_P_H_INCLUDED
