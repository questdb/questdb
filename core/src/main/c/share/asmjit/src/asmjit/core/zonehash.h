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

#ifndef ASMJIT_CORE_ZONEHASH_H_INCLUDED
#define ASMJIT_CORE_ZONEHASH_H_INCLUDED

#include "../core/zone.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_zone
//! \{

// ============================================================================
// [asmjit::ZoneHashNode]
// ============================================================================

//! Node used by \ref ZoneHash template.
//!
//! You must provide function `bool eq(const Key& key)` in order to make
//! `ZoneHash::get()` working.
class ZoneHashNode {
public:
  ASMJIT_NONCOPYABLE(ZoneHashNode)

  inline ZoneHashNode(uint32_t hashCode = 0) noexcept
    : _hashNext(nullptr),
      _hashCode(hashCode),
      _customData(0) {}

  //! Next node in the chain, null if it terminates the chain.
  ZoneHashNode* _hashNext;
  //! Precalculated hash-code of key.
  uint32_t _hashCode;
  //! Padding, can be reused by any Node that inherits `ZoneHashNode`.
  uint32_t _customData;
};

// ============================================================================
// [asmjit::ZoneHashBase]
// ============================================================================

//! Base class used by \ref ZoneHash template
class ZoneHashBase {
public:
  ASMJIT_NONCOPYABLE(ZoneHashBase)

  //! Buckets data.
  ZoneHashNode** _data;
  //! Count of records inserted into the hash table.
  size_t _size;
  //! Count of hash buckets.
  uint32_t _bucketsCount;
  //! When buckets array should grow (only checked after insertion).
  uint32_t _bucketsGrow;
  //! Reciprocal value of `_bucketsCount`.
  uint32_t _rcpValue;
  //! How many bits to shift right when hash is multiplied with `_rcpValue`.
  uint8_t _rcpShift;
  //! Prime value index in internal prime array.
  uint8_t _primeIndex;

  //! Embedded data, used by empty hash tables.
  ZoneHashNode* _embedded[1];

  //! \name Construction & Destruction
  //! \{

  inline ZoneHashBase() noexcept {
    reset();
  }

  inline ZoneHashBase(ZoneHashBase&& other) noexcept {
    _data = other._data;
    _size = other._size;
    _bucketsCount = other._bucketsCount;
    _bucketsGrow = other._bucketsGrow;
    _rcpValue = other._rcpValue;
    _rcpShift = other._rcpShift;
    _primeIndex = other._primeIndex;
    _embedded[0] = other._embedded[0];

    if (_data == other._embedded) _data = _embedded;
  }

  inline void reset() noexcept {
    _data = _embedded;
    _size = 0;
    _bucketsCount = 1;
    _bucketsGrow = 1;
    _rcpValue = 1;
    _rcpShift = 0;
    _primeIndex = 0;
    _embedded[0] = nullptr;
  }

  inline void release(ZoneAllocator* allocator) noexcept {
    ZoneHashNode** oldData = _data;
    if (oldData != _embedded)
      allocator->release(oldData, _bucketsCount * sizeof(ZoneHashNode*));
    reset();
  }

  //! \}

  //! \name Accessors
  //! \{

  inline bool empty() const noexcept { return _size == 0; }
  inline size_t size() const noexcept { return _size; }

  //! \}

  //! \name Utilities
  //! \{

  inline void _swap(ZoneHashBase& other) noexcept {
    std::swap(_data, other._data);
    std::swap(_size, other._size);
    std::swap(_bucketsCount, other._bucketsCount);
    std::swap(_bucketsGrow, other._bucketsGrow);
    std::swap(_rcpValue, other._rcpValue);
    std::swap(_rcpShift, other._rcpShift);
    std::swap(_primeIndex, other._primeIndex);
    std::swap(_embedded[0], other._embedded[0]);

    if (_data == other._embedded) _data = _embedded;
    if (other._data == _embedded) other._data = other._embedded;
  }

  //! \cond INTERNAL
  inline uint32_t _calcMod(uint32_t hash) const noexcept {
    uint32_t x = uint32_t((uint64_t(hash) * _rcpValue) >> _rcpShift);
    return hash - x * _bucketsCount;
  }

  ASMJIT_API void _rehash(ZoneAllocator* allocator, uint32_t newCount) noexcept;
  ASMJIT_API ZoneHashNode* _insert(ZoneAllocator* allocator, ZoneHashNode* node) noexcept;
  ASMJIT_API ZoneHashNode* _remove(ZoneAllocator* allocator, ZoneHashNode* node) noexcept;
  //! \endcond

  //! \}
};

// ============================================================================
// [asmjit::ZoneHash]
// ============================================================================

//! Low-level hash table specialized for storing string keys and POD values.
//!
//! This hash table allows duplicates to be inserted (the API is so low
//! level that it's up to you if you allow it or not, as you should first
//! `get()` the node and then modify it or insert a new node by using `insert()`,
//! depending on the intention).
template<typename NodeT>
class ZoneHash : public ZoneHashBase {
public:
  ASMJIT_NONCOPYABLE(ZoneHash)

  typedef NodeT Node;

  //! \name Construction & Destruction
  //! \{

  inline ZoneHash() noexcept
    : ZoneHashBase() {}

  inline ZoneHash(ZoneHash&& other) noexcept
    : ZoneHash(other) {}

  //! \}

  //! \name Utilities
  //! \{

  inline void swap(ZoneHash& other) noexcept { ZoneHashBase::_swap(other); }

  template<typename KeyT>
  inline NodeT* get(const KeyT& key) const noexcept {
    uint32_t hashMod = _calcMod(key.hashCode());
    NodeT* node = static_cast<NodeT*>(_data[hashMod]);

    while (node && !key.matches(node))
      node = static_cast<NodeT*>(node->_hashNext);
    return node;
  }

  inline NodeT* insert(ZoneAllocator* allocator, NodeT* node) noexcept { return static_cast<NodeT*>(_insert(allocator, node)); }
  inline NodeT* remove(ZoneAllocator* allocator, NodeT* node) noexcept { return static_cast<NodeT*>(_remove(allocator, node)); }

  //! \}
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_ZONEHASH_H_INCLUDED
