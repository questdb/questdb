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

#ifndef ASMJIT_CORE_ZONEVECTOR_H_INCLUDED
#define ASMJIT_CORE_ZONEVECTOR_H_INCLUDED

#include "../core/support.h"
#include "../core/zone.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_zone
//! \{

// ============================================================================
// [asmjit::ZoneVectorBase]
// ============================================================================

//! Base class used by \ref ZoneVector template.
class ZoneVectorBase {
public:
  ASMJIT_NONCOPYABLE(ZoneVectorBase)

  // STL compatibility;
  typedef uint32_t size_type;
  typedef ptrdiff_t difference_type;

  //! Vector data (untyped).
  void* _data = nullptr;
  //! Size of the vector.
  size_type _size = 0;
  //! Capacity of the vector.
  size_type _capacity = 0;

protected:
  //! \name Construction & Destruction
  //! \{

  //! Creates a new instance of `ZoneVectorBase`.
  inline ZoneVectorBase() noexcept {}

  inline ZoneVectorBase(ZoneVectorBase&& other) noexcept
    : _data(other._data),
      _size(other._size),
      _capacity(other._capacity) {}

  //! \}

  //! \cond INTERNAL
  //! \name Internal
  //! \{

  inline void _release(ZoneAllocator* allocator, uint32_t sizeOfT) noexcept {
    if (_data != nullptr) {
      allocator->release(_data, _capacity * sizeOfT);
      reset();
    }
  }

  ASMJIT_API Error _grow(ZoneAllocator* allocator, uint32_t sizeOfT, uint32_t n) noexcept;
  ASMJIT_API Error _resize(ZoneAllocator* allocator, uint32_t sizeOfT, uint32_t n) noexcept;
  ASMJIT_API Error _reserve(ZoneAllocator* allocator, uint32_t sizeOfT, uint32_t n) noexcept;

  inline void _swap(ZoneVectorBase& other) noexcept {
    std::swap(_data, other._data);
    std::swap(_size, other._size);
    std::swap(_capacity, other._capacity);
  }

  //! \}
  //! \endcond

public:
  //! \name Accessors
  //! \{

  //! Tests whether the vector is empty.
  inline bool empty() const noexcept { return _size == 0; }
  //! Returns the vector size.
  inline size_type size() const noexcept { return _size; }
  //! Returns the vector capacity.
  inline size_type capacity() const noexcept { return _capacity; }

  //! \}

  //! \name Utilities
  //! \{

  //! Makes the vector empty (won't change the capacity or data pointer).
  inline void clear() noexcept { _size = 0; }
  //! Resets the vector data and set its `size` to zero.
  inline void reset() noexcept {
    _data = nullptr;
    _size = 0;
    _capacity = 0;
  }

  //! Truncates the vector to at most `n` items.
  inline void truncate(size_type n) noexcept {
    _size = Support::min(_size, n);
  }

  //! Sets size of the vector to `n`. Used internally by some algorithms.
  inline void _setSize(size_type n) noexcept {
    ASMJIT_ASSERT(n <= _capacity);
    _size = n;
  }

  //! \}
};

// ============================================================================
// [asmjit::ZoneVector<T>]
// ============================================================================

//! Template used to store and manage array of Zone allocated data.
//!
//! This template has these advantages over other std::vector<>:
//! - Always non-copyable (designed to be non-copyable, we want it).
//! - Optimized for working only with POD types.
//! - Uses ZoneAllocator, thus small vectors are almost for free.
//! - Explicit allocation, ZoneAllocator is not part of the data.
template <typename T>
class ZoneVector : public ZoneVectorBase {
public:
  ASMJIT_NONCOPYABLE(ZoneVector)

  // STL compatibility;
  typedef T value_type;
  typedef T* pointer;
  typedef const T* const_pointer;
  typedef T& reference;
  typedef const T& const_reference;

  typedef T* iterator;
  typedef const T* const_iterator;
  typedef std::reverse_iterator<iterator> reverse_iterator;
  typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

  //! \name Construction & Destruction
  //! \{

  inline ZoneVector() noexcept : ZoneVectorBase() {}
  inline ZoneVector(ZoneVector&& other) noexcept : ZoneVector(other) {}

  //! \}

  //! \name Accessors
  //! \{

  //! Returns vector data.
  inline T* data() noexcept { return static_cast<T*>(_data); }
  //! Returns vector data (const)
  inline const T* data() const noexcept { return static_cast<const T*>(_data); }

  //! Returns item at the given index `i` (const).
  inline const T& at(size_t i) const noexcept {
    ASMJIT_ASSERT(i < _size);
    return data()[i];
  }

  inline void _setEndPtr(T* p) noexcept {
    ASMJIT_ASSERT(p >= data() && p <= data() + _capacity);
    _setSize(uint32_t((uintptr_t)(p - data())));
  }

  //! \}

  //! \name STL Compatibility (Iterators)
  //! \{

  inline iterator begin() noexcept { return iterator(data()); };
  inline const_iterator begin() const noexcept { return const_iterator(data()); };

  inline iterator end() noexcept { return iterator(data() + _size); };
  inline const_iterator end() const noexcept { return const_iterator(data() + _size); };

  inline reverse_iterator rbegin() noexcept { return reverse_iterator(end()); };
  inline const_reverse_iterator rbegin() const noexcept { return const_reverse_iterator(end()); };

  inline reverse_iterator rend() noexcept { return reverse_iterator(begin()); };
  inline const_reverse_iterator rend() const noexcept { return const_reverse_iterator(begin()); };

  inline const_iterator cbegin() const noexcept { return const_iterator(data()); };
  inline const_iterator cend() const noexcept { return const_iterator(data() + _size); };

  inline const_reverse_iterator crbegin() const noexcept { return const_reverse_iterator(cend()); };
  inline const_reverse_iterator crend() const noexcept { return const_reverse_iterator(cbegin()); };

  //! \}

  //! \name Utilities
  //! \{

  //! Swaps this vector with `other`.
  inline void swap(ZoneVector<T>& other) noexcept { _swap(other); }

  //! Prepends `item` to the vector.
  inline Error prepend(ZoneAllocator* allocator, const T& item) noexcept {
    if (ASMJIT_UNLIKELY(_size == _capacity))
      ASMJIT_PROPAGATE(grow(allocator, 1));

    ::memmove(static_cast<T*>(_data) + 1, _data, size_t(_size) * sizeof(T));
    memcpy(_data, &item, sizeof(T));

    _size++;
    return kErrorOk;
  }

  //! Inserts an `item` at the specified `index`.
  inline Error insert(ZoneAllocator* allocator, size_t index, const T& item) noexcept {
    ASMJIT_ASSERT(index <= _size);

    if (ASMJIT_UNLIKELY(_size == _capacity))
      ASMJIT_PROPAGATE(grow(allocator, 1));

    T* dst = static_cast<T*>(_data) + index;
    ::memmove(dst + 1, dst, size_t(_size - index) * sizeof(T));
    memcpy(dst, &item, sizeof(T));
    _size++;

    return kErrorOk;
  }

  //! Appends `item` to the vector.
  inline Error append(ZoneAllocator* allocator, const T& item) noexcept {
    if (ASMJIT_UNLIKELY(_size == _capacity))
      ASMJIT_PROPAGATE(grow(allocator, 1));

    memcpy(static_cast<T*>(_data) + _size, &item, sizeof(T));
    _size++;

    return kErrorOk;
  }

  //! Appends `other` vector at the end of this vector.
  inline Error concat(ZoneAllocator* allocator, const ZoneVector<T>& other) noexcept {
    uint32_t size = other._size;
    if (_capacity - _size < size)
      ASMJIT_PROPAGATE(grow(allocator, size));

    if (size) {
      memcpy(static_cast<T*>(_data) + _size, other._data, size_t(size) * sizeof(T));
      _size += size;
    }

    return kErrorOk;
  }

  //! Prepends `item` to the vector (unsafe case).
  //!
  //! Can only be used together with `willGrow()`. If `willGrow(N)` returns
  //! `kErrorOk` then N elements can be added to the vector without checking
  //! if there is a place for them. Used mostly internally.
  inline void prependUnsafe(const T& item) noexcept {
    ASMJIT_ASSERT(_size < _capacity);
    T* data = static_cast<T*>(_data);

    if (_size)
      ::memmove(data + 1, data, size_t(_size) * sizeof(T));

    memcpy(data, &item, sizeof(T));
    _size++;
  }

  //! Append s`item` to the vector (unsafe case).
  //!
  //! Can only be used together with `willGrow()`. If `willGrow(N)` returns
  //! `kErrorOk` then N elements can be added to the vector without checking
  //! if there is a place for them. Used mostly internally.
  inline void appendUnsafe(const T& item) noexcept {
    ASMJIT_ASSERT(_size < _capacity);

    memcpy(static_cast<T*>(_data) + _size, &item, sizeof(T));
    _size++;
  }

  //! Inserts an `item` at the specified `index` (unsafe case).
  inline void insertUnsafe(size_t index, const T& item) noexcept {
    ASMJIT_ASSERT(_size < _capacity);
    ASMJIT_ASSERT(index <= _size);

    T* dst = static_cast<T*>(_data) + index;
    ::memmove(dst + 1, dst, size_t(_size - index) * sizeof(T));
    memcpy(dst, &item, sizeof(T));
    _size++;
  }
  //! Concatenates all items of `other` at the end of the vector.
  inline void concatUnsafe(const ZoneVector<T>& other) noexcept {
    uint32_t size = other._size;
    ASMJIT_ASSERT(_capacity - _size >= size);

    if (size) {
      memcpy(static_cast<T*>(_data) + _size, other._data, size_t(size) * sizeof(T));
      _size += size;
    }
  }

  //! Returns index of the given `val` or `Globals::kNotFound` if it doesn't exist.
  inline uint32_t indexOf(const T& val) const noexcept {
    const T* data = static_cast<const T*>(_data);
    uint32_t size = _size;

    for (uint32_t i = 0; i < size; i++)
      if (data[i] == val)
        return i;
    return Globals::kNotFound;
  }

  //! Tests whether the vector contains `val`.
  inline bool contains(const T& val) const noexcept {
    return indexOf(val) != Globals::kNotFound;
  }

  //! Removes item at index `i`.
  inline void removeAt(size_t i) noexcept {
    ASMJIT_ASSERT(i < _size);

    T* data = static_cast<T*>(_data) + i;
    size_t size = --_size - i;

    if (size)
      ::memmove(data, data + 1, size_t(size) * sizeof(T));
  }

  //! Pops the last element from the vector and returns it.
  inline T pop() noexcept {
    ASMJIT_ASSERT(_size > 0);

    uint32_t index = --_size;
    return data()[index];
  }

  template<typename CompareT = Support::Compare<Support::kSortAscending>>
  inline void sort(const CompareT& cmp = CompareT()) noexcept {
    Support::qSort<T, CompareT>(data(), size(), cmp);
  }

  //! Returns item at index `i`.
  inline T& operator[](size_t i) noexcept {
    ASMJIT_ASSERT(i < _size);
    return data()[i];
  }

  //! Returns item at index `i`.
  inline const T& operator[](size_t i) const noexcept {
    ASMJIT_ASSERT(i < _size);
    return data()[i];
  }

  //! Returns a reference to the first element of the vector.
  //!
  //! \note The vector must have at least one element. Attempting to use
  //! `first()` on empty vector will trigger an assertion failure in debug
  //! builds.
  inline T& first() noexcept { return operator[](0); }
  //! \overload
  inline const T& first() const noexcept { return operator[](0); }

  //! Returns a reference to the last element of the vector.
  //!
  //! \note The vector must have at least one element. Attempting to use
  //! `last()` on empty vector will trigger an assertion failure in debug
  //! builds.
  inline T& last() noexcept { return operator[](_size - 1); }
  //! \overload
  inline const T& last() const noexcept { return operator[](_size - 1); }

  //! \}

  //! \name Memory Management
  //! \{

  //! Releases the memory held by `ZoneVector<T>` back to the `allocator`.
  inline void release(ZoneAllocator* allocator) noexcept {
    _release(allocator, sizeof(T));
  }

  //! Called to grow the buffer to fit at least `n` elements more.
  inline Error grow(ZoneAllocator* allocator, uint32_t n) noexcept {
    return ZoneVectorBase::_grow(allocator, sizeof(T), n);
  }

  //! Resizes the vector to hold `n` elements.
  //!
  //! If `n` is greater than the current size then the additional elements'
  //! content will be initialized to zero. If `n` is less than the current
  //! size then the vector will be truncated to exactly `n` elements.
  inline Error resize(ZoneAllocator* allocator, uint32_t n) noexcept {
    return ZoneVectorBase::_resize(allocator, sizeof(T), n);
  }

  //! Reallocates the internal array to fit at least `n` items.
  inline Error reserve(ZoneAllocator* allocator, uint32_t n) noexcept {
    return n > _capacity ? ZoneVectorBase::_reserve(allocator, sizeof(T), n) : Error(kErrorOk);
  }

  inline Error willGrow(ZoneAllocator* allocator, uint32_t n = 1) noexcept {
    return _capacity - _size < n ? grow(allocator, n) : Error(kErrorOk);
  }

  //! \}
};

// ============================================================================
// [asmjit::ZoneBitVector]
// ============================================================================

//! Zone-allocated bit vector.
class ZoneBitVector {
public:
  typedef Support::BitWord BitWord;
  static constexpr uint32_t kBitWordSizeInBits = Support::kBitWordSizeInBits;

  //! Bits.
  BitWord* _data = nullptr;
  //! Size of the bit-vector (in bits).
  uint32_t _size = 0;
  //! Capacity of the bit-vector (in bits).
  uint32_t _capacity = 0;

  ASMJIT_NONCOPYABLE(ZoneBitVector)

  //! \cond INTERNAL
  //! \name Internal
  //! \{

  static inline uint32_t _wordsPerBits(uint32_t nBits) noexcept {
    return ((nBits + kBitWordSizeInBits - 1) / kBitWordSizeInBits);
  }

  static inline void _zeroBits(BitWord* dst, uint32_t nBitWords) noexcept {
    for (uint32_t i = 0; i < nBitWords; i++)
      dst[i] = 0;
  }

  static inline void _fillBits(BitWord* dst, uint32_t nBitWords) noexcept {
    for (uint32_t i = 0; i < nBitWords; i++)
      dst[i] = ~BitWord(0);
  }

  static inline void _copyBits(BitWord* dst, const BitWord* src, uint32_t nBitWords) noexcept {
    for (uint32_t i = 0; i < nBitWords; i++)
      dst[i] = src[i];
  }

  //! \}
  //! \endcond

  //! \name Construction & Destruction
  //! \{

  inline ZoneBitVector() noexcept {}

  inline ZoneBitVector(ZoneBitVector&& other) noexcept
    : _data(other._data),
      _size(other._size),
      _capacity(other._capacity) {}

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline bool operator==(const ZoneBitVector& other) const noexcept { return  eq(other); }
  inline bool operator!=(const ZoneBitVector& other) const noexcept { return !eq(other); }

  //! \}

  //! \name Accessors
  //! \{

  //! Tests whether the bit-vector is empty (has no bits).
  inline bool empty() const noexcept { return _size == 0; }
  //! Returns the size of this bit-vector (in bits).
  inline uint32_t size() const noexcept { return _size; }
  //! Returns the capacity of this bit-vector (in bits).
  inline uint32_t capacity() const noexcept { return _capacity; }

  //! Returns the size of the `BitWord[]` array in `BitWord` units.
  inline uint32_t sizeInBitWords() const noexcept { return _wordsPerBits(_size); }
  //! Returns the capacity of the `BitWord[]` array in `BitWord` units.
  inline uint32_t capacityInBitWords() const noexcept { return _wordsPerBits(_capacity); }

  //! REturns bit-vector data as `BitWord[]`.
  inline BitWord* data() noexcept { return _data; }
  //! \overload
  inline const BitWord* data() const noexcept { return _data; }

  //! \}

  //! \name Utilities
  //! \{

  inline void swap(ZoneBitVector& other) noexcept {
    std::swap(_data, other._data);
    std::swap(_size, other._size);
    std::swap(_capacity, other._capacity);
  }

  inline void clear() noexcept {
    _size = 0;
  }

  inline void reset() noexcept {
    _data = nullptr;
    _size = 0;
    _capacity = 0;
  }

  inline void truncate(uint32_t newSize) noexcept {
    _size = Support::min(_size, newSize);
    _clearUnusedBits();
  }

  inline bool bitAt(uint32_t index) const noexcept {
    ASMJIT_ASSERT(index < _size);
    return Support::bitVectorGetBit(_data, index);
  }

  inline void setBit(uint32_t index, bool value) noexcept {
    ASMJIT_ASSERT(index < _size);
    Support::bitVectorSetBit(_data, index, value);
  }

  inline void flipBit(uint32_t index) noexcept {
    ASMJIT_ASSERT(index < _size);
    Support::bitVectorFlipBit(_data, index);
  }

  ASMJIT_INLINE Error append(ZoneAllocator* allocator, bool value) noexcept {
    uint32_t index = _size;
    if (ASMJIT_UNLIKELY(index >= _capacity))
      return _append(allocator, value);

    uint32_t idx = index / kBitWordSizeInBits;
    uint32_t bit = index % kBitWordSizeInBits;

    if (bit == 0)
      _data[idx] = BitWord(value) << bit;
    else
      _data[idx] |= BitWord(value) << bit;

    _size++;
    return kErrorOk;
  }

  ASMJIT_API Error copyFrom(ZoneAllocator* allocator, const ZoneBitVector& other) noexcept;

  inline void clearAll() noexcept {
    _zeroBits(_data, _wordsPerBits(_size));
  }

  inline void fillAll() noexcept {
    _fillBits(_data, _wordsPerBits(_size));
    _clearUnusedBits();
  }

  inline void clearBits(uint32_t start, uint32_t count) noexcept {
    ASMJIT_ASSERT(start <= _size);
    ASMJIT_ASSERT(_size - start >= count);

    Support::bitVectorClear(_data, start, count);
  }

  inline void fillBits(uint32_t start, uint32_t count) noexcept {
    ASMJIT_ASSERT(start <= _size);
    ASMJIT_ASSERT(_size - start >= count);

    Support::bitVectorFill(_data, start, count);
  }

  //! Performs a logical bitwise AND between bits specified in this array and bits
  //! in `other`. If `other` has less bits than `this` then all remaining bits are
  //! set to zero.
  //!
  //! \note The size of the BitVector is unaffected by this operation.
  inline void and_(const ZoneBitVector& other) noexcept {
    BitWord* dst = _data;
    const BitWord* src = other._data;

    uint32_t thisBitWordCount = sizeInBitWords();
    uint32_t otherBitWordCount = other.sizeInBitWords();
    uint32_t commonBitWordCount = Support::min(thisBitWordCount, otherBitWordCount);

    uint32_t i = 0;
    while (i < commonBitWordCount) {
      dst[i] = dst[i] & src[i];
      i++;
    }

    while (i < thisBitWordCount) {
      dst[i] = 0;
      i++;
    }
  }

  //! Performs a logical bitwise AND between bits specified in this array and
  //! negated bits in `other`. If `other` has less bits than `this` then all
  //! remaining bits are kept intact.
  //!
  //! \note The size of the BitVector is unaffected by this operation.
  inline void andNot(const ZoneBitVector& other) noexcept {
    BitWord* dst = _data;
    const BitWord* src = other._data;

    uint32_t commonBitWordCount = _wordsPerBits(Support::min(_size, other._size));
    for (uint32_t i = 0; i < commonBitWordCount; i++)
      dst[i] = dst[i] & ~src[i];
  }

  //! Performs a logical bitwise OP between bits specified in this array and bits
  //! in `other`. If `other` has less bits than `this` then all remaining bits
  //! are kept intact.
  //!
  //! \note The size of the BitVector is unaffected by this operation.
  inline void or_(const ZoneBitVector& other) noexcept {
    BitWord* dst = _data;
    const BitWord* src = other._data;

    uint32_t commonBitWordCount = _wordsPerBits(Support::min(_size, other._size));
    for (uint32_t i = 0; i < commonBitWordCount; i++)
      dst[i] = dst[i] | src[i];
    _clearUnusedBits();
  }

  inline void _clearUnusedBits() noexcept {
    uint32_t idx = _size / kBitWordSizeInBits;
    uint32_t bit = _size % kBitWordSizeInBits;

    if (!bit) return;
    _data[idx] &= (BitWord(1) << bit) - 1u;
  }

  inline bool eq(const ZoneBitVector& other) const noexcept {
    if (_size != other._size)
      return false;

    const BitWord* aData = _data;
    const BitWord* bData = other._data;
    uint32_t numBitWords = _wordsPerBits(_size);

    for (uint32_t i = 0; i < numBitWords; i++)
      if (aData[i] != bData[i])
        return false;
    return true;
  }

  //! \}

  //! \name Memory Management
  //! \{

  inline void release(ZoneAllocator* allocator) noexcept {
    if (!_data) return;
    allocator->release(_data, _capacity / 8);
    reset();
  }

  inline Error resize(ZoneAllocator* allocator, uint32_t newSize, bool newBitsValue = false) noexcept {
    return _resize(allocator, newSize, newSize, newBitsValue);
  }

  ASMJIT_API Error _resize(ZoneAllocator* allocator, uint32_t newSize, uint32_t idealCapacity, bool newBitsValue) noexcept;
  ASMJIT_API Error _append(ZoneAllocator* allocator, bool value) noexcept;

  //! \}

  //! \name Iterators
  //! \{

  class ForEachBitSet : public Support::BitVectorIterator<BitWord> {
  public:
    ASMJIT_INLINE explicit ForEachBitSet(const ZoneBitVector& bitVector) noexcept
      : Support::BitVectorIterator<BitWord>(bitVector.data(), bitVector.sizeInBitWords()) {}
  };

  template<class Operator>
  class ForEachBitOp : public Support::BitVectorOpIterator<BitWord, Operator> {
  public:
    ASMJIT_INLINE ForEachBitOp(const ZoneBitVector& a, const ZoneBitVector& b) noexcept
      : Support::BitVectorOpIterator<BitWord, Operator>(a.data(), b.data(), a.sizeInBitWords()) {
      ASMJIT_ASSERT(a.size() == b.size());
    }
  };

  //! \}
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_ZONEVECTOR_H_INCLUDED
