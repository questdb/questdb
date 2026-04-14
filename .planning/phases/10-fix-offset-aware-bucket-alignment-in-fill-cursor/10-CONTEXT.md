# Phase 10 Context: Fix Offset-Aware Bucket Alignment

## Domain Boundary

Fix infinite fill when ALIGN TO CALENDAR WITH OFFSET is used without TO clause. The sampler's bucket grid doesn't match timestamp_floor_utc's grid because setStart() uses raw FROM without offset.

## Decisions

### Root cause

`timestamp_floor_utc` computes `effectiveOffset = from + offset` (AbstractTimestampFloorFromOffsetFunctionFactory.java:215). Our fill cursor calls `timestampSampler.setStart(fromTs)` without offset (SampleByFillRecordCursorFactory.java:547). The sampler generates buckets at e.g. `00:00` while GROUP BY produces buckets at `10:00`. They never match → every bucket is a "gap" → infinite fill.

### Fix: pass offset to fill cursor, compute from + offset in setStart()

**Decision**: In `generateFill()`, parse the offset from the SQL model (`curr.getSampleByOffset()`). Parse it using `Dates.parseOffset()` → convert to microseconds via `timestampDriver.fromMinutes()`. Pass the parsed `long offset` to `SampleByFillRecordCursorFactory` constructor. In `initialize()`, call `timestampSampler.setStart(fromTs + offset)` instead of `timestampSampler.setStart(fromTs)`.

Also adjust `nextBucketTimestamp = fromTs + offset` when FROM is set.

When no offset (offset == 0), behavior is unchanged.

**Implementation**:
1. `SqlCodeGenerator.generateFill()`: parse `curr.getSampleByOffset()` using same logic as AbstractTimestampFloorFromOffsetFunctionFactory (Dates.parseOffset → fromMinutes). Pass as `long calendarOffset` to factory.
2. `SampleByFillRecordCursorFactory`: new `calendarOffset` field. In `initialize()`, `fromTs` adjusted: `fromTs = fromTs + calendarOffset` before `setStart()`.
3. When no FROM but offset exists: `nextBucketTimestamp = firstTs` is already rounded by the sampler. Need `setOffset(calendarOffset)` before `round(firstTs)`.

### Edge cases

- No offset: `calendarOffset = 0`, no behavior change
- No FROM but with offset: need `setOffset(calendarOffset)` then `round(firstTs)` for first bucket
- FROM + offset: `setStart(fromTs + calendarOffset)`, `nextBucketTimestamp = fromTs + calendarOffset`
- FROM + offset + TO: same adjustment, maxTimestamp from TO is already correct

## Canonical References

- `core/src/main/java/io/questdb/griffin/engine/functions/date/AbstractTimestampFloorFromOffsetFunctionFactory.java:215` — `effectiveOffset = from + offset`
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java:547` — `setStart(nextBucketTimestamp)`
- `core/src/main/java/io/questdb/griffin/engine/groupby/SimpleTimestampSampler.java:82-88` — `round()` uses `start`
