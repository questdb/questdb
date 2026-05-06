# CompiledTickExpression IR Format

The `CompiledTickExpression` stores all pre-parsed tick expression data in a
single `long[]` called `ir`. This eliminates per-field array allocations and
keeps the runtime `evaluate()` path free of string parsing — pure long
arithmetic only.

## Overall Layout

```
ir[0]                          header (counts + flags)
ir[1]                          numericTzOffset
ir[2 .. 2+D)                   duration parts          (D longs)
ir[2+D .. 2+D+3T)              time override triples   (T triples, 3 longs each)
ir[2+D+3T .. end)              elements                (variable-length)
```

`D`, `T`, and the element count are all encoded in the header.

## Header — `ir[0]`

```
bit 63                                                              bit 0
 ├─ elemCount ─┤─ toCount ──┤─ durCount ─┤          ┤DWE┤          ┤dayMask┤
 [63      56]   [55      48] [47      40] [39 .. 24] [23] [22 .. 7] [6 .. 0]
```

| Bits    | Name                     | Description                              |
|---------|--------------------------|------------------------------------------|
| 63 – 56 | `elemCount`             | Number of elements (max 255)             |
| 55 – 48 | `timeOverrideCount`     | Number of time override pairs            |
| 47 – 40 | `durationPartCount`     | Number of duration (unit, value) parts   |
| 23      | `hasDurationWithExchange`| Duration applied after exchange schedule |
| 6 – 0   | `dayFilterMask`         | Bitmask of allowed days (Mon=bit 0, Sun=bit 6) |

Bits 39–24 and 22–7 are reserved (zero).

## Timezone Offset — `ir[1]`

A pre-computed numeric timezone offset in the driver's time units (micros or
nanos), or `Long.MIN_VALUE` if the timezone is not a fixed numeric offset.

Named timezones (e.g. `Europe/London`) are stored separately as a
`TimeZoneRules` object reference on the `CompiledTickExpression` instance, since
DST rules cannot be encoded as a single long.

## Duration Parts — `ir[2 .. 2+D)`

Each duration part (e.g. the `6h30m` in `;6h30m`) is encoded as one long:

```
bit 63                                    bit 0
                   ┤── unit ──┤           ┤──── value ────┤
 [63 .. 48]        [47     32]  [31 .. 0]
   (unused)          char         int
```

| Bits    | Field  | Description                              |
|---------|--------|------------------------------------------|
| 47 – 32 | `unit` | Duration unit char (`h`, `m`, `s`, `d`, `M`, `y`, etc.) |
| 31 – 0  | `value`| Duration amount (signed int stored as 32-bit pattern) |

## Time Override Triples — `ir[2+D .. 2+D+3T)`

Each time override (e.g. `T09:30` or each entry in `T[09:00,14:00]`) is stored
as three consecutive longs:

```
ir[offset + 0] = timeOffset       offset from midnight (in driver units)
ir[offset + 1] = precisionWidth   width derived from string precision
ir[offset + 2] = zoneMatch        result of dateLocale.matchZone(), or Long.MIN_VALUE
```

For example, `T09:30` produces `timeOffset = micros(9h30m)` from epoch midnight
and `precisionWidth = micros(1 minute) - 1` (minute precision).

Numeric timezone offsets (e.g. `@+05:00`) are pre-applied to `timeOffset` at
compile time (adjusting both `timeOffset` and `precisionWidth`), so
`zoneMatch = Long.MIN_VALUE`.

Named timezones (e.g. `@Europe/London`) store the raw `matchZone()` result in
`zoneMatch`. At construction time, the `CompiledTickExpression` resolves each
non-`MIN_VALUE` entry to a `TimeZoneRules` object via
`dateLocale.getZoneRules()`, and at runtime applies `driver.toUTC()` when the
actual date is known (for correct DST handling).

## Elements

Elements are packed sequentially. Each element starts with a long whose top 2
bits (63–62) encode the tag. The walker reads the tag to determine the element
type and how many longs to consume.

### Tag Values

| Bits 63–62 | Tag            | Longs | Description          |
|------------|----------------|-------|----------------------|
| `00`       | `TAG_SINGLE_VAR` | 1   | Single `$variable`   |
| `01`       | `TAG_STATIC`     | 3   | Pre-computed [lo, hi]|
| `10`       | `TAG_RANGE`      | 2   | Range `$start..$end` |

### Expr Encoding (bits 59–0)

A `DateVariableExpr` (`$variable ± offset`) fits in a single long. The encoding
occupies bits 59–0, leaving bits 63–60 free for tag/flags:

```
bit 63                                                              bit 0
 ┤tag/flags┤varType┤BD┤      ┤── offsetUnit ──┤          ┤──── offsetValue ────┤
 [63    60] [59 58] [57] [56] [55          40]  [39 .. 32] [31                0]
```

| Bits    | Field          | Description                              |
|---------|----------------|------------------------------------------|
| 59 – 58 | `varType`     | `0`=NOW, `1`=TODAY, `2`=TOMORROW, `3`=YESTERDAY |
| 57      | `isBusinessDays` | Offset is in business days            |
| 55 – 40 | `offsetUnit`  | Unit char (`h`, `d`, `M`, etc.) or `0` if no offset |
| 31 – 0  | `offsetValue` | Signed int offset amount (stored as 32-bit pattern) |

Bits 63–60 and 56 and 39–32 are available for element-level flags.

### SINGLE_VAR — 1 long

```
long[0]:  [63-62] = 00 (TAG_SINGLE_VAR)
          [59-0]  = encoded expr
```

At runtime, `evaluateExpr(long)` decodes the variable type and offset, computes
the base timestamp (`$now` → now, `$today` → start of day, etc.), then applies
the offset.

### STATIC — 3 longs

```
long[0]:  [63-62] = 01 (TAG_STATIC), remaining bits unused
long[1]:  lo  (interval start, fully resolved at compile time)
long[2]:  hi  (interval end, fully resolved at compile time)
```

Static elements have all suffix processing (time override, duration, timezone,
day filter, exchange schedule) applied at compile time. They are emitted
directly into the output without further computation.

### RANGE — 2 longs

```
long[0]:  [63-62] = 10 (TAG_RANGE)
          [61]    = isBusinessDay (skip weekends when iterating)
          [59-0]  = encoded start expr
long[1]:  [59-0]  = encoded end expr
```

At runtime, both expressions are evaluated. If both have sub-day time
components, a single interval `[start, end]` is emitted. Otherwise, the range
iterates day-by-day from `startDay` to `endDay`, emitting per-day intervals
(with optional business-day filtering).

## Section Offsets

Computed once in the constructor from the header:

```
durationOff      = 2
toOff            = 2 + durationPartCount
elemOff          = toOff + timeOverrideCount * 3
```

## Non-IR Fields

These cannot be encoded in `long[]` and remain as object references:

| Field              | Type               | When present                         |
|--------------------|--------------------|--------------------------------------|
| `timestampDriver`  | `TimestampDriver`  | Always (micro or nano driver)        |
| `tzRules`          | `TimeZoneRules`    | Global named timezone (e.g. `@Europe/London`) |
| `elemTzRules`      | `TimeZoneRules[]`  | Per-element named timezones in time list entries |
| `exchangeSchedule` | `LongList`         | Exchange calendar (e.g. `#NYSE`)     |

The `expression` string is also kept for `toPlan()` display only.

## Evaluate Algorithm

```
1. Walk elements (elemOff → end of ir):
   - SINGLE_VAR: decode expr → evaluate → emit day/point interval
   - STATIC:     emit pre-computed [lo, hi]
   - RANGE:      decode both exprs → evaluate → emit day-by-day or single interval

2. Apply day filter (bitmask check on day-of-week of each lo)

3. Apply timezone:
   - Numeric: subtract fixed offset from all timestamps
   - Named:   DST-aware conversion via driver.toUTC(timestamp, tzRules)

4. Apply exchange schedule (if present):
   - Filter intervals to exchange trading hours
   - Optionally apply duration to filtered intervals

5. Sort and merge overlapping intervals
```

All phases operate on the output `LongList` in-place. No intermediate
allocations.

## Compile-Time Construction

`IntervalUtils.compileTickExpr()` builds the IR into a single `long[]`:

1. Allocate one `long[64]`
2. Write duration parts at `ir[0..D)`
3. Write time override triples at `ir[D..D+3T)`
4. **Shift** suffix data right by 2 (`arraycopy`) to insert header + tz offset
5. Append elements at `ir[2+D+3T..)`
6. Write header at `ir[0]`, trim array to exact size

This produces exactly **one `long[]` allocation** for the stored IR. Compile-time
temporaries (two `StringSink`, one `LongList`) are reused across all parsing
phases via `clear()` between uses.
