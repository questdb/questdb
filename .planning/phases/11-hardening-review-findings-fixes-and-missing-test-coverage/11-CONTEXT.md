# Phase 11 Context: Hardening — Review Findings & Missing Test Coverage

## Domain Boundary

Fix 6 specific findings from code review. Verification: re-run /review-pr to confirm no moderate+ findings remain.

## Fixes

### Fix 1: UUID key FILL_KEY dispatch (review finding #1)
getLong128Hi, getLong128Lo, getDecimal128, getDecimal256, getLong256/A/B — add FILL_KEY check.

### Fix 2: Geo null sentinels (review finding #2)
getGeoByte: 0 → GeoHashes.BYTE_NULL
getGeoShort: 0 → GeoHashes.SHORT_NULL
getGeoInt: Numbers.INT_NULL → GeoHashes.INT_NULL
getGeoLong: Numbers.LONG_NULL → GeoHashes.NULL

### Fix 3: Decimal8/16 null sentinels (review finding #10)
getDecimal8: 0 → Decimals.DECIMAL8_NULL (or equivalent)
getDecimal16: 0 → Decimals.DECIMAL16_NULL (or equivalent)

### Tests to add

### Test 4: NULL key value (review finding #7)
Insert NULL into SYMBOL key column, verify it forms its own group with independent fill.

### Test 5: CTE/subquery FILL_KEY reclassification (review finding #8)
Wrap SAMPLE BY FILL in WITH ... AS, verify SYMBOL key column reclassified correctly.

### Test 6: Sparse DST test (review finding #9)
Sparse data around DST fall-back, verify fill rows generated during transition hour.
