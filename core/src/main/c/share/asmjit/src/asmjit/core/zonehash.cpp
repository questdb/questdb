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
#include "../core/support.h"
#include "../core/zone.h"
#include "../core/zonehash.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::ZoneHashBase - Helpers]
// ============================================================================

#define ASMJIT_POPULATE_PRIMES(ENTRY) \
  ENTRY(2         , 0x80000000, 32), /* [N * 0x80000000 >> 32] (rcp=2147483648) */ \
  ENTRY(11        , 0xBA2E8BA3, 35), /* [N * 0xBA2E8BA3 >> 35] (rcp=3123612579) */ \
  ENTRY(29        , 0x8D3DCB09, 36), /* [N * 0x8D3DCB09 >> 36] (rcp=2369637129) */ \
  ENTRY(41        , 0xC7CE0C7D, 37), /* [N * 0xC7CE0C7D >> 37] (rcp=3352169597) */ \
  ENTRY(59        , 0x8AD8F2FC, 37), /* [N * 0x8AD8F2FC >> 37] (rcp=2329473788) */ \
  ENTRY(83        , 0xC565C87C, 38), /* [N * 0xC565C87C >> 38] (rcp=3311782012) */ \
  ENTRY(131       , 0xFA232CF3, 39), /* [N * 0xFA232CF3 >> 39] (rcp=4196609267) */ \
  ENTRY(191       , 0xAB8F69E3, 39), /* [N * 0xAB8F69E3 >> 39] (rcp=2878302691) */ \
  ENTRY(269       , 0xF3A0D52D, 40), /* [N * 0xF3A0D52D >> 40] (rcp=4087403821) */ \
  ENTRY(383       , 0xAB1CBDD4, 40), /* [N * 0xAB1CBDD4 >> 40] (rcp=2870787540) */ \
  ENTRY(541       , 0xF246FACC, 41), /* [N * 0xF246FACC >> 41] (rcp=4064737996) */ \
  ENTRY(757       , 0xAD2589A4, 41), /* [N * 0xAD2589A4 >> 41] (rcp=2904918436) */ \
  ENTRY(1061      , 0xF7129426, 42), /* [N * 0xF7129426 >> 42] (rcp=4145189926) */ \
  ENTRY(1499      , 0xAEE116B7, 42), /* [N * 0xAEE116B7 >> 42] (rcp=2933986999) */ \
  ENTRY(2099      , 0xF9C7A737, 43), /* [N * 0xF9C7A737 >> 43] (rcp=4190611255) */ \
  ENTRY(2939      , 0xB263D25C, 43), /* [N * 0xB263D25C >> 43] (rcp=2992886364) */ \
  ENTRY(4111      , 0xFF10E02E, 44), /* [N * 0xFF10E02E >> 44] (rcp=4279296046) */ \
  ENTRY(5779      , 0xB5722823, 44), /* [N * 0xB5722823 >> 44] (rcp=3044157475) */ \
  ENTRY(8087      , 0x81A97405, 44), /* [N * 0x81A97405 >> 44] (rcp=2175366149) */ \
  ENTRY(11321     , 0xB93E91DB, 45), /* [N * 0xB93E91DB >> 45] (rcp=3107885531) */ \
  ENTRY(15859     , 0x843CC26B, 45), /* [N * 0x843CC26B >> 45] (rcp=2218574443) */ \
  ENTRY(22189     , 0xBD06B9EA, 46), /* [N * 0xBD06B9EA >> 46] (rcp=3171334634) */ \
  ENTRY(31051     , 0x8713F186, 46), /* [N * 0x8713F186 >> 46] (rcp=2266231174) */ \
  ENTRY(43451     , 0xC10F1CB9, 47), /* [N * 0xC10F1CB9 >> 47] (rcp=3238993081) */ \
  ENTRY(60869     , 0x89D06A86, 47), /* [N * 0x89D06A86 >> 47] (rcp=2312137350) */ \
  ENTRY(85159     , 0xC502AF3B, 48), /* [N * 0xC502AF3B >> 48] (rcp=3305287483) */ \
  ENTRY(102107    , 0xA44F65AE, 48), /* [N * 0xA44F65AE >> 48] (rcp=2756666798) */ \
  ENTRY(122449    , 0x89038F77, 48), /* [N * 0x89038F77 >> 48] (rcp=2298711927) */ \
  ENTRY(146819    , 0xE48AF7E9, 49), /* [N * 0xE48AF7E9 >> 49] (rcp=3834312681) */ \
  ENTRY(176041    , 0xBE9B145B, 49), /* [N * 0xBE9B145B >> 49] (rcp=3197834331) */ \
  ENTRY(211073    , 0x9EF882BA, 49), /* [N * 0x9EF882BA >> 49] (rcp=2667086522) */ \
  ENTRY(253081    , 0x849571AB, 49), /* [N * 0x849571AB >> 49] (rcp=2224386475) */ \
  ENTRY(303469    , 0xDD239C97, 50), /* [N * 0xDD239C97 >> 50] (rcp=3710098583) */ \
  ENTRY(363887    , 0xB86C196D, 50), /* [N * 0xB86C196D >> 50] (rcp=3094092141) */ \
  ENTRY(436307    , 0x99CFA4E9, 50), /* [N * 0x99CFA4E9 >> 50] (rcp=2580522217) */ \
  ENTRY(523177    , 0x804595C0, 50), /* [N * 0x804595C0 >> 50] (rcp=2152043968) */ \
  ENTRY(627293    , 0xD5F69FCF, 51), /* [N * 0xD5F69FCF >> 51] (rcp=3589709775) */ \
  ENTRY(752177    , 0xB27063BA, 51), /* [N * 0xB27063BA >> 51] (rcp=2993710010) */ \
  ENTRY(901891    , 0x94D170AC, 51), /* [N * 0x94D170AC >> 51] (rcp=2496753836) */ \
  ENTRY(1081369   , 0xF83C9767, 52), /* [N * 0xF83C9767 >> 52] (rcp=4164720487) */ \
  ENTRY(1296563   , 0xCF09435D, 52), /* [N * 0xCF09435D >> 52] (rcp=3473490781) */ \
  ENTRY(1554583   , 0xACAC7198, 52), /* [N * 0xACAC7198 >> 52] (rcp=2896982424) */ \
  ENTRY(1863971   , 0x90033EE3, 52), /* [N * 0x90033EE3 >> 52] (rcp=2416131811) */ \
  ENTRY(2234923   , 0xF0380EBD, 53), /* [N * 0xF0380EBD >> 53] (rcp=4030205629) */ \
  ENTRY(2679673   , 0xC859731E, 53), /* [N * 0xC859731E >> 53] (rcp=3361305374) */ \
  ENTRY(3212927   , 0xA718DE27, 53), /* [N * 0xA718DE27 >> 53] (rcp=2803424807) */ \
  ENTRY(3852301   , 0x8B5D1B4B, 53), /* [N * 0x8B5D1B4B >> 53] (rcp=2338134859) */ \
  ENTRY(4618921   , 0xE8774804, 54), /* [N * 0xE8774804 >> 54] (rcp=3900131332) */ \
  ENTRY(5076199   , 0xD386574E, 54), /* [N * 0xD386574E >> 54] (rcp=3548796750) */ \
  ENTRY(5578757   , 0xC0783FE1, 54), /* [N * 0xC0783FE1 >> 54] (rcp=3229106145) */ \
  ENTRY(6131057   , 0xAF21B08F, 54), /* [N * 0xAF21B08F >> 54] (rcp=2938220687) */ \
  ENTRY(6738031   , 0x9F5AFD6E, 54), /* [N * 0x9F5AFD6E >> 54] (rcp=2673540462) */ \
  ENTRY(7405163   , 0x90FFC3B9, 54), /* [N * 0x90FFC3B9 >> 54] (rcp=2432680889) */ \
  ENTRY(8138279   , 0x83EFECFC, 54), /* [N * 0x83EFECFC >> 54] (rcp=2213539068) */ \
  ENTRY(8943971   , 0xF01AA2EF, 55), /* [N * 0xF01AA2EF >> 55] (rcp=4028277487) */ \
  ENTRY(9829447   , 0xDA7979B2, 55), /* [N * 0xDA7979B2 >> 55] (rcp=3665394098) */ \
  ENTRY(10802581  , 0xC6CB2771, 55), /* [N * 0xC6CB2771 >> 55] (rcp=3335202673) */ \
  ENTRY(11872037  , 0xB4E2C7DD, 55), /* [N * 0xB4E2C7DD >> 55] (rcp=3034761181) */ \
  ENTRY(13047407  , 0xA4974124, 55), /* [N * 0xA4974124 >> 55] (rcp=2761376036) */ \
  ENTRY(14339107  , 0x95C39CF1, 55), /* [N * 0x95C39CF1 >> 55] (rcp=2512624881) */ \
  ENTRY(15758737  , 0x8845C763, 55), /* [N * 0x8845C763 >> 55] (rcp=2286274403) */ \
  ENTRY(17318867  , 0xF7FE593F, 56), /* [N * 0xF7FE593F >> 56] (rcp=4160641343) */ \
  ENTRY(19033439  , 0xE1A75D93, 56), /* [N * 0xE1A75D93 >> 56] (rcp=3785842067) */ \
  ENTRY(20917763  , 0xCD5389B3, 56), /* [N * 0xCD5389B3 >> 56] (rcp=3444804019) */ \
  ENTRY(22988621  , 0xBAD4841A, 56), /* [N * 0xBAD4841A >> 56] (rcp=3134489626) */ \
  ENTRY(25264543  , 0xA9FFF2FF, 56), /* [N * 0xA9FFF2FF >> 56] (rcp=2852123391) */ \
  ENTRY(27765763  , 0x9AAF8BF3, 56), /* [N * 0x9AAF8BF3 >> 56] (rcp=2595195891) */ \
  ENTRY(30514607  , 0x8CC04E18, 56), /* [N * 0x8CC04E18 >> 56] (rcp=2361413144) */ \
  ENTRY(33535561  , 0x80127068, 56), /* [N * 0x80127068 >> 56] (rcp=2148692072) */ \
  ENTRY(36855587  , 0xE911F0BB, 57), /* [N * 0xE911F0BB >> 57] (rcp=3910267067) */ \
  ENTRY(38661533  , 0xDE2ED7BE, 57), /* [N * 0xDE2ED7BE >> 57] (rcp=3727611838) */ \
  ENTRY(40555961  , 0xD3CDF2FD, 57), /* [N * 0xD3CDF2FD >> 57] (rcp=3553489661) */ \
  ENTRY(42543269  , 0xC9E9196C, 57), /* [N * 0xC9E9196C >> 57] (rcp=3387496812) */ \
  ENTRY(44627909  , 0xC07A9EB6, 57), /* [N * 0xC07A9EB6 >> 57] (rcp=3229261494) */ \
  ENTRY(46814687  , 0xB77CEF65, 57), /* [N * 0xB77CEF65 >> 57] (rcp=3078418277) */ \
  ENTRY(49108607  , 0xAEEAC65C, 57), /* [N * 0xAEEAC65C >> 57] (rcp=2934621788) */ \
  ENTRY(51514987  , 0xA6BF0EF0, 57), /* [N * 0xA6BF0EF0 >> 57] (rcp=2797539056) */ \
  ENTRY(54039263  , 0x9EF510B5, 57), /* [N * 0x9EF510B5 >> 57] (rcp=2666860725) */ \
  ENTRY(56687207  , 0x97883B42, 57), /* [N * 0x97883B42 >> 57] (rcp=2542287682) */ \
  ENTRY(59464897  , 0x907430ED, 57), /* [N * 0x907430ED >> 57] (rcp=2423533805) */ \
  ENTRY(62378699  , 0x89B4CA91, 57), /* [N * 0x89B4CA91 >> 57] (rcp=2310326929) */ \
  ENTRY(65435273  , 0x83461568, 57), /* [N * 0x83461568 >> 57] (rcp=2202408296) */ \
  ENTRY(68641607  , 0xFA489AA8, 58), /* [N * 0xFA489AA8 >> 58] (rcp=4199062184) */ \
  ENTRY(72005051  , 0xEE97B1C5, 58), /* [N * 0xEE97B1C5 >> 58] (rcp=4002918853) */ \
  ENTRY(75533323  , 0xE3729293, 58), /* [N * 0xE3729293 >> 58] (rcp=3815936659) */ \
  ENTRY(79234469  , 0xD8D2BBA3, 58), /* [N * 0xD8D2BBA3 >> 58] (rcp=3637689251) */ \
  ENTRY(83116967  , 0xCEB1F196, 58), /* [N * 0xCEB1F196 >> 58] (rcp=3467768214) */ \
  ENTRY(87189709  , 0xC50A4426, 58), /* [N * 0xC50A4426 >> 58] (rcp=3305784358) */ \
  ENTRY(91462061  , 0xBBD6052B, 58), /* [N * 0xBBD6052B >> 58] (rcp=3151365419) */ \
  ENTRY(95943737  , 0xB30FD999, 58), /* [N * 0xB30FD999 >> 58] (rcp=3004160409) */ \
  ENTRY(100644991 , 0xAAB29CED, 58), /* [N * 0xAAB29CED >> 58] (rcp=2863832301) */ \
  ENTRY(105576619 , 0xA2B96421, 58), /* [N * 0xA2B96421 >> 58] (rcp=2730058785) */ \
  ENTRY(110749901 , 0x9B1F8434, 58), /* [N * 0x9B1F8434 >> 58] (rcp=2602533940) */ \
  ENTRY(116176651 , 0x93E08B4A, 58), /* [N * 0x93E08B4A >> 58] (rcp=2480966474) */ \
  ENTRY(121869317 , 0x8CF837E0, 58), /* [N * 0x8CF837E0 >> 58] (rcp=2365077472) */ \
  ENTRY(127840913 , 0x86627F01, 58), /* [N * 0x86627F01 >> 58] (rcp=2254601985) */ \
  ENTRY(134105159 , 0x801B8178, 58), /* [N * 0x801B8178 >> 58] (rcp=2149286264) */ \
  ENTRY(140676353 , 0xF43F294F, 59), /* [N * 0xF43F294F >> 59] (rcp=4097780047) */ \
  ENTRY(147569509 , 0xE8D67089, 59), /* [N * 0xE8D67089 >> 59] (rcp=3906367625) */ \
  ENTRY(154800449 , 0xDDF6243C, 59), /* [N * 0xDDF6243C >> 59] (rcp=3723895868) */ \
  ENTRY(162385709 , 0xD397E6AE, 59), /* [N * 0xD397E6AE >> 59] (rcp=3549947566) */ \
  ENTRY(170342629 , 0xC9B5A65A, 59), /* [N * 0xC9B5A65A >> 59] (rcp=3384125018) */ \
  ENTRY(178689419 , 0xC0499865, 59), /* [N * 0xC0499865 >> 59] (rcp=3226048613) */ \
  ENTRY(187445201 , 0xB74E35FA, 59), /* [N * 0xB74E35FA >> 59] (rcp=3075356154) */ \
  ENTRY(196630033 , 0xAEBE3AC1, 59), /* [N * 0xAEBE3AC1 >> 59] (rcp=2931702465) */ \
  ENTRY(206264921 , 0xA694A37F, 59), /* [N * 0xA694A37F >> 59] (rcp=2794759039) */ \
  ENTRY(216371963 , 0x9ECCA59F, 59), /* [N * 0x9ECCA59F >> 59] (rcp=2664211871) */ \
  ENTRY(226974197 , 0x9761B6AE, 59), /* [N * 0x9761B6AE >> 59] (rcp=2539763374) */ \
  ENTRY(238095983 , 0x904F79A1, 59), /* [N * 0x904F79A1 >> 59] (rcp=2421127585) */ \
  ENTRY(249762697 , 0x8991CD1F, 59), /* [N * 0x8991CD1F >> 59] (rcp=2308033823) */ \
  ENTRY(262001071 , 0x8324BCA5, 59), /* [N * 0x8324BCA5 >> 59] (rcp=2200222885) */ \
  ENTRY(274839137 , 0xFA090732, 60), /* [N * 0xFA090732 >> 60] (rcp=4194895666) */ \
  ENTRY(288306269 , 0xEE5B16ED, 60), /* [N * 0xEE5B16ED >> 60] (rcp=3998947053) */ \
  ENTRY(302433337 , 0xE338CE49, 60), /* [N * 0xE338CE49 >> 60] (rcp=3812150857) */ \
  ENTRY(317252587 , 0xD89BABC0, 60), /* [N * 0xD89BABC0 >> 60] (rcp=3634080704) */ \
  ENTRY(374358107 , 0xB790EF43, 60), /* [N * 0xB790EF43 >> 60] (rcp=3079728963) */ \
  ENTRY(441742621 , 0x9B908414, 60), /* [N * 0x9B908414 >> 60] (rcp=2609939476) */ \
  ENTRY(521256293 , 0x83D596FA, 60), /* [N * 0x83D596FA >> 60] (rcp=2211813114) */ \
  ENTRY(615082441 , 0xDF72B16E, 61), /* [N * 0xDF72B16E >> 61] (rcp=3748835694) */ \
  ENTRY(725797313 , 0xBD5CDB3B, 61), /* [N * 0xBD5CDB3B >> 61] (rcp=3176979259) */ \
  ENTRY(856440829 , 0xA07A14E9, 61), /* [N * 0xA07A14E9 >> 61] (rcp=2692355305) */ \
  ENTRY(1010600209, 0x87FF5289, 61), /* [N * 0x87FF5289 >> 61] (rcp=2281656969) */ \
  ENTRY(1192508257, 0xE6810540, 62), /* [N * 0xE6810540 >> 62] (rcp=3867215168) */ \
  ENTRY(1407159797, 0xC357A480, 62), /* [N * 0xC357A480 >> 62] (rcp=3277300864) */ \
  ENTRY(1660448617, 0xA58B5B4F, 62), /* [N * 0xA58B5B4F >> 62] (rcp=2777373519) */ \
  ENTRY(1959329399, 0x8C4AB55F, 62), /* [N * 0x8C4AB55F >> 62] (rcp=2353706335) */ \
  ENTRY(2312008693, 0xEDC86320, 63), /* [N * 0xEDC86320 >> 63] (rcp=3989332768) */ \
  ENTRY(2728170257, 0xC982C4D2, 63), /* [N * 0xC982C4D2 >> 63] (rcp=3380790482) */ \
  ENTRY(3219240923, 0xAAC599B6, 63)  /* [N * 0xAAC599B6 >> 63] (rcp=2865076662) */


struct HashPrime {
  //! Prime number
  uint32_t prime;
  //! Reciprocal to turn division into multiplication.
  uint32_t rcp;
};

static const HashPrime ZoneHash_primeArray[] = {
  #define E(PRIME, RCP, SHIFT) { PRIME, RCP }
  ASMJIT_POPULATE_PRIMES(E)
  #undef E
};

static const uint8_t ZoneHash_primeShift[] = {
  #define E(PRIME, RCP, SHIFT) uint8_t(SHIFT)
  ASMJIT_POPULATE_PRIMES(E)
  #undef E
};

// ============================================================================
// [asmjit::ZoneHashBase - Rehash]
// ============================================================================

void ZoneHashBase::_rehash(ZoneAllocator* allocator, uint32_t primeIndex) noexcept {
  ASMJIT_ASSERT(primeIndex < ASMJIT_ARRAY_SIZE(ZoneHash_primeArray));
  uint32_t newCount = ZoneHash_primeArray[primeIndex].prime;

  ZoneHashNode** oldData = _data;
  ZoneHashNode** newData = reinterpret_cast<ZoneHashNode**>(
    allocator->allocZeroed(size_t(newCount) * sizeof(ZoneHashNode*)));

  // We can still store nodes into the table, but it will degrade.
  if (ASMJIT_UNLIKELY(newData == nullptr))
    return;

  uint32_t i;
  uint32_t oldCount = _bucketsCount;

  _data = newData;
  _bucketsCount = newCount;
  _bucketsGrow = uint32_t(newCount * 0.9);
  _rcpValue = ZoneHash_primeArray[primeIndex].rcp;
  _rcpShift = ZoneHash_primeShift[primeIndex];
  _primeIndex = uint8_t(primeIndex);

  for (i = 0; i < oldCount; i++) {
    ZoneHashNode* node = oldData[i];
    while (node) {
      ZoneHashNode* next = node->_hashNext;
      uint32_t hashMod = _calcMod(node->_hashCode);

      node->_hashNext = newData[hashMod];
      newData[hashMod] = node;
      node = next;
    }
  }

  if (oldData != _embedded)
    allocator->release(oldData, oldCount * sizeof(ZoneHashNode*));
}

// ============================================================================
// [asmjit::ZoneHashBase - Ops]
// ============================================================================

ZoneHashNode* ZoneHashBase::_insert(ZoneAllocator* allocator, ZoneHashNode* node) noexcept {
  uint32_t hashMod = _calcMod(node->_hashCode);
  ZoneHashNode* next = _data[hashMod];

  node->_hashNext = next;
  _data[hashMod] = node;

  if (++_size > _bucketsGrow) {
    uint32_t primeIndex = Support::min<uint32_t>(_primeIndex + 2, ASMJIT_ARRAY_SIZE(ZoneHash_primeArray) - 1);
    if (primeIndex > _primeIndex)
      _rehash(allocator, primeIndex);
  }

  return node;
}

ZoneHashNode* ZoneHashBase::_remove(ZoneAllocator* allocator, ZoneHashNode* node) noexcept {
  DebugUtils::unused(allocator);
  uint32_t hashMod = _calcMod(node->_hashCode);

  ZoneHashNode** pPrev = &_data[hashMod];
  ZoneHashNode* p = *pPrev;

  while (p) {
    if (p == node) {
      *pPrev = p->_hashNext;
      _size--;
      return node;
    }

    pPrev = &p->_hashNext;
    p = *pPrev;
  }

  return nullptr;
}

// ============================================================================
// [asmjit::ZoneHash - Unit]
// ============================================================================

#if defined(ASMJIT_TEST)
struct MyHashNode : public ZoneHashNode {
  inline MyHashNode(uint32_t key) noexcept
    : ZoneHashNode(key),
      _key(key) {}

  uint32_t _key;
};

struct MyKeyMatcher {
  inline MyKeyMatcher(uint32_t key) noexcept
    : _key(key) {}

  inline uint32_t hashCode() const noexcept { return _key; }
  inline bool matches(const MyHashNode* node) const noexcept { return node->_key == _key; }

  uint32_t _key;
};

UNIT(zone_hash) {
  uint32_t kCount = BrokenAPI::hasArg("--quick") ? 1000 : 10000;

  Zone zone(4096);
  ZoneAllocator allocator(&zone);

  ZoneHash<MyHashNode> hashTable;

  uint32_t key;
  INFO("Inserting %u elements to HashTable", unsigned(kCount));
  for (key = 0; key < kCount; key++) {
    hashTable.insert(&allocator, zone.newT<MyHashNode>(key));
  }

  uint32_t count = kCount;
  INFO("Removing %u elements from HashTable and validating each operation", unsigned(kCount));
  do {
    MyHashNode* node;

    for (key = 0; key < count; key++) {
      node = hashTable.get(MyKeyMatcher(key));
      EXPECT(node != nullptr);
      EXPECT(node->_key == key);
    }

    {
      count--;
      node = hashTable.get(MyKeyMatcher(count));
      hashTable.remove(&allocator, node);

      node = hashTable.get(MyKeyMatcher(count));
      EXPECT(node == nullptr);
    }
  } while (count);

  EXPECT(hashTable.empty());
}
#endif

ASMJIT_END_NAMESPACE
