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

#ifndef ASMJIT_CORE_STRING_H_INCLUDED
#define ASMJIT_CORE_STRING_H_INCLUDED

#include "../core/support.h"
#include "../core/zone.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_utilities
//! \{

// ============================================================================
// [asmjit::FixedString]
// ============================================================================

//! A fixed string - only useful for strings that would never exceed `N - 1`
//! characters; always null-terminated.
template<size_t N>
union FixedString {
  enum : uint32_t {
    kNumU32 = uint32_t((N + sizeof(uint32_t) - 1) / sizeof(uint32_t))
  };

  char str[kNumU32 * sizeof(uint32_t)];
  uint32_t u32[kNumU32];

  //! \name Utilities
  //! \{

  inline bool eq(const char* other) const noexcept {
    return strcmp(str, other) == 0;
  }

  //! \}
};
// ============================================================================
// [asmjit::String]
// ============================================================================

//! A simple non-reference counted string that uses small string optimization (SSO).
//!
//! This string has 3 allocation possibilities:
//!
//!   1. Small    - embedded buffer is used for up to `kSSOCapacity` characters.
//!                 This should handle most small strings and thus avoid dynamic
//!                 memory allocation for most use-cases.
//!
//!   2. Large    - string that doesn't fit into an embedded buffer (or string
//!                 that was truncated from a larger buffer) and is owned by
//!                 AsmJit. When you destroy the string AsmJit would automatically
//!                 release the large buffer.
//!
//!   3. External - like Large (2), however, the large buffer is not owned by
//!                 AsmJit and won't be released when the string is destroyed
//!                 or reallocated. This is mostly useful for working with
//!                 larger temporary strings allocated on stack or with immutable
//!                 strings.
class String {
public:
  ASMJIT_NONCOPYABLE(String)

  //! String operation.
  enum Op : uint32_t {
    //! Assignment - a new content replaces the current one.
    kOpAssign = 0,
    //! Append - a new content is appended to the string.
    kOpAppend = 1
  };

  //! String format flags.
  enum FormatFlags : uint32_t {
    kFormatShowSign  = 0x00000001u,
    kFormatShowSpace = 0x00000002u,
    kFormatAlternate = 0x00000004u,
    kFormatSigned    = 0x80000000u
  };

  //! \cond INTERNAL
  enum : uint32_t {
    kLayoutSize = 32,
    kSSOCapacity = kLayoutSize - 2
  };

  //! String type.
  enum Type : uint8_t {
    kTypeLarge    = 0x1Fu, //!< Large string (owned by String).
    kTypeExternal = 0x20u  //!< External string (zone allocated or not owned by String).
  };

  union Raw {
    uint8_t u8[kLayoutSize];
    uint64_t u64[kLayoutSize / sizeof(uint64_t)];
    uintptr_t uptr[kLayoutSize / sizeof(uintptr_t)];
  };

  struct Small {
    uint8_t type;
    char data[kSSOCapacity + 1u];
  };

  struct Large {
    uint8_t type;
    uint8_t reserved[sizeof(uintptr_t) - 1];
    size_t size;
    size_t capacity;
    char* data;
  };

  union {
    uint8_t _type;
    Raw _raw;
    Small _small;
    Large _large;
  };
  //! \endcond

  //! \name Construction & Destruction
  //! \{

  //! Creates a default-initialized string if zero length.
  inline String() noexcept
    : _small {} {}

  //! Creates a string that takes ownership of the content of the `other` string.
  inline String(String&& other) noexcept {
    _raw = other._raw;
    other._resetInternal();
  }

  inline ~String() noexcept {
    reset();
  }

  //! Reset the string into a construction state.
  ASMJIT_API Error reset() noexcept;

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline String& operator=(String&& other) noexcept {
    swap(other);
    other.reset();
    return *this;
  }

  inline bool operator==(const char* other) const noexcept { return  eq(other); }
  inline bool operator!=(const char* other) const noexcept { return !eq(other); }

  inline bool operator==(const String& other) const noexcept { return  eq(other); }
  inline bool operator!=(const String& other) const noexcept { return !eq(other); }

  //! \}

  //! \name Accessors
  //! \{

  inline bool isLarge() const noexcept { return _type >= kTypeLarge; }
  inline bool isExternal() const noexcept { return _type == kTypeExternal; }

  //! Tests whether the string is empty.
  inline bool empty() const noexcept { return size() == 0; }
  //! Returns the size of the string.
  inline size_t size() const noexcept { return isLarge() ? size_t(_large.size) : size_t(_type); }
  //! Returns the capacity of the string.
  inline size_t capacity() const noexcept { return isLarge() ? _large.capacity : size_t(kSSOCapacity); }

  //! Returns the data of the string.
  inline char* data() noexcept { return isLarge() ? _large.data : _small.data; }
  //! \overload
  inline const char* data() const noexcept { return isLarge() ? _large.data : _small.data; }

  inline char* start() noexcept { return data(); }
  inline const char* start() const noexcept { return data(); }

  inline char* end() noexcept { return data() + size(); }
  inline const char* end() const noexcept { return data() + size(); }

  //! \}

  //! \name String Operations
  //! \{

  //! Swaps the content of this string with `other`.
  inline void swap(String& other) noexcept {
    std::swap(_raw, other._raw);
  }

  //! Clears the content of the string.
  ASMJIT_API Error clear() noexcept;

  ASMJIT_API char* prepare(uint32_t op, size_t size) noexcept;

  ASMJIT_API Error _opString(uint32_t op, const char* str, size_t size = SIZE_MAX) noexcept;
  ASMJIT_API Error _opChar(uint32_t op, char c) noexcept;
  ASMJIT_API Error _opChars(uint32_t op, char c, size_t n) noexcept;
  ASMJIT_API Error _opNumber(uint32_t op, uint64_t i, uint32_t base = 0, size_t width = 0, uint32_t flags = 0) noexcept;
  ASMJIT_API Error _opHex(uint32_t op, const void* data, size_t size, char separator = '\0') noexcept;
  ASMJIT_API Error _opFormat(uint32_t op, const char* fmt, ...) noexcept;
  ASMJIT_API Error _opVFormat(uint32_t op, const char* fmt, va_list ap) noexcept;

  //! Replaces the current of the string with `data` of the given `size`.
  //!
  //! Null terminated strings can set `size` to `SIZE_MAX`.
  ASMJIT_API Error assign(const char* data, size_t size = SIZE_MAX) noexcept;

  //! Replaces the current of the string with `other` string.
  inline Error assign(const String& other) noexcept {
    return assign(other.data(), other.size());
  }

  //! Replaces the current of the string by a single `c` character.
  inline Error assign(char c) noexcept {
    return _opChar(kOpAssign, c);
  }

  //! Replaces the current of the string by a `c` character, repeated `n` times.
  inline Error assignChars(char c, size_t n) noexcept {
    return _opChars(kOpAssign, c, n);
  }

  //! Replaces the current of the string by a formatted integer `i` (signed).
  inline Error assignInt(int64_t i, uint32_t base = 0, size_t width = 0, uint32_t flags = 0) noexcept {
    return _opNumber(kOpAssign, uint64_t(i), base, width, flags | kFormatSigned);
  }

  //! Replaces the current of the string by a formatted integer `i` (unsigned).
  inline Error assignUInt(uint64_t i, uint32_t base = 0, size_t width = 0, uint32_t flags = 0) noexcept {
    return _opNumber(kOpAssign, i, base, width, flags);
  }

  //! Replaces the current of the string by the given `data` converted to a HEX string.
  inline Error assignHex(const void* data, size_t size, char separator = '\0') noexcept {
    return _opHex(kOpAssign, data, size, separator);
  }

  //! Replaces the current of the string by a formatted string `fmt`.
  template<typename... Args>
  inline Error assignFormat(const char* fmt, Args&&... args) noexcept {
    return _opFormat(kOpAssign, fmt, std::forward<Args>(args)...);
  }

  //! Replaces the current of the string by a formatted string `fmt` (va_list version).
  inline Error assignVFormat(const char* fmt, va_list ap) noexcept {
    return _opVFormat(kOpAssign, fmt, ap);
  }

  //! Appends `str` having the given size `size` to the string.
  //!
  //! Null terminated strings can set `size` to `SIZE_MAX`.
  inline Error append(const char* str, size_t size = SIZE_MAX) noexcept {
    return _opString(kOpAppend, str, size);
  }

  //! Appends `other` string to this string.
  inline Error append(const String& other) noexcept {
    return append(other.data(), other.size());
  }

  //! Appends a single `c` character.
  inline Error append(char c) noexcept {
    return _opChar(kOpAppend, c);
  }

  //! Appends `c` character repeated `n` times.
  inline Error appendChars(char c, size_t n) noexcept {
    return _opChars(kOpAppend, c, n);
  }

  //! Appends a formatted integer `i` (signed).
  inline Error appendInt(int64_t i, uint32_t base = 0, size_t width = 0, uint32_t flags = 0) noexcept {
    return _opNumber(kOpAppend, uint64_t(i), base, width, flags | kFormatSigned);
  }

  //! Appends a formatted integer `i` (unsigned).
  inline Error appendUInt(uint64_t i, uint32_t base = 0, size_t width = 0, uint32_t flags = 0) noexcept {
    return _opNumber(kOpAppend, i, base, width, flags);
  }

  //! Appends the given `data` converted to a HEX string.
  inline Error appendHex(const void* data, size_t size, char separator = '\0') noexcept {
    return _opHex(kOpAppend, data, size, separator);
  }

  //! Appends a formatted string `fmt` with `args`.
  template<typename... Args>
  inline Error appendFormat(const char* fmt, Args&&... args) noexcept {
    return _opFormat(kOpAppend, fmt, std::forward<Args>(args)...);
  }

  //! Appends a formatted string `fmt` (va_list version).
  inline Error appendVFormat(const char* fmt, va_list ap) noexcept {
    return _opVFormat(kOpAppend, fmt, ap);
  }

  ASMJIT_API Error padEnd(size_t n, char c = ' ') noexcept;

  //! Truncate the string length into `newSize`.
  ASMJIT_API Error truncate(size_t newSize) noexcept;

  ASMJIT_API bool eq(const char* other, size_t size = SIZE_MAX) const noexcept;
  inline bool eq(const String& other) const noexcept { return eq(other.data(), other.size()); }

  //! \}

  //! \name Internal Functions
  //! \{

  //! Resets string to embedded and makes it empty (zero length, zero first char)
  //!
  //! \note This is always called internally after an external buffer was released
  //! as it zeroes all bytes used by String's embedded storage.
  inline void _resetInternal() noexcept {
    for (size_t i = 0; i < ASMJIT_ARRAY_SIZE(_raw.uptr); i++)
      _raw.uptr[i] = 0;
  }

  inline void _setSize(size_t newSize) noexcept {
    if (isLarge())
      _large.size = newSize;
    else
      _small.type = uint8_t(newSize);
  }

  //! \}

#ifndef ASMJIT_NO_DEPRECATED
  ASMJIT_DEPRECATED("Use assign() instead of assignString()")
  inline Error assignString(const char* data, size_t size = SIZE_MAX) noexcept { return assign(data, size); }

  ASMJIT_DEPRECATED("Use assign() instead of assignChar()")
  inline Error assignChar(char c) noexcept { return assign(c); }

  ASMJIT_DEPRECATED("Use append() instead of appendString()")
  inline Error appendString(const char* data, size_t size = SIZE_MAX) noexcept { return append(data, size); }

  ASMJIT_DEPRECATED("Use append() instead of appendChar()")
  inline Error appendChar(char c) noexcept { return append(c); }
#endif // !ASMJIT_NO_DEPRECATED
};

// ============================================================================
// [asmjit::StringTmp]
// ============================================================================

//! Temporary string builder, has statically allocated `N` bytes.
template<size_t N>
class StringTmp : public String {
public:
  ASMJIT_NONCOPYABLE(StringTmp)

  //! Embedded data.
  char _embeddedData[Support::alignUp(N + 1, sizeof(size_t))];

  //! \name Construction & Destruction
  //! \{

  inline StringTmp() noexcept {
    _resetToTemporary();
  }

  inline void _resetToTemporary() noexcept {
    _large.type = kTypeExternal;
    _large.capacity = ASMJIT_ARRAY_SIZE(_embeddedData) - 1;
    _large.data = _embeddedData;
    _embeddedData[0] = '\0';
  }

  //! \}
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_STRING_H_INCLUDED
