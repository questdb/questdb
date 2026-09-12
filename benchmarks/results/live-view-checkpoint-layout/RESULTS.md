# Performance acceptance matrix: measured run

Baseline `6a2c65602856f0e3d9699c167f3c9515ae4419d1` against the candidate at
`4056b2515c` on `puzpuzpuz_live_view`. Both revisions ran on the same machine,
filesystem, JVM, heap, worker count, input and maintenance settings, through
`run-matrix.sh`, which fixes every one of those but the machine. 360 runs, none
failed.

A second measurement followed - the failed gates re-measured with 15 runs per
cell, the 1,000,000-key scaling run and the closed-segment repair cell, 480 more
runs, none failed - and is reported in its own section below the first. Read the
first run's "Failed gates" section together with "Second measurement": the three
gates it left open are attributed there and the requirements revised explicitly.

A third measurement - the add/remove-keys run across anchor boundaries and the
cold-cache restore, the two readings the first two left unmeasured, 720 runs plus
a 15-run re-measure - follows the second. It finds one regression, on the
anchor-only shapes when every anchor moves, attributes it and revises a fourth
requirement.

## Environment

```
cpu:    24 logical cores
memory: 61 GiB
os:     Ubuntu 24.04.4 LTS
jvm:    openjdk version "25.0.4" 2026-07-21
disk:   NVMe SSD, ext4
heap:   -Xmx8g (10,000 and 100,000 keys)
```

## Protocol

36 cells: 9 shapes x {fusion on, fusion off} x {10,000, 100,000 live keys}. Five
independent JVM runs per cell per revision, each over a fresh database.

A run seeds `K` rows into `K` round-robin accounts, so the view holds exactly `K`
live keys and nothing ever falls behind the frontier, then ingests 110 commits of
1000 rows. Consecutive rows take consecutive accounts, so one commit touches
exactly 1000 distinct existing keys - the changed-key domain stays at 1000 while
`K` moves - and `--checkpoint-rows=1000` makes one commit seal one boundary, so
one batch row of the output is one seal. The first 10 batches are dropped as
warm-up, leaving 100 measured incremental seals per run. `--restart=true` adds a
restore and its first reseal from the state those batches left behind.

A run's figure is a median (or a p95) over its 100 measured seals; a cell's
figure is the median over its five runs. Ratios below are candidate / baseline.

## Headline: ratio distribution across the 36 cells

```
metric                        min   median      max   worst cell
seal_ms_median              0.147    0.332    1.007   ('residual-heavy', 'true', 10000) = 1.007
seal_ms_p95                 0.155    0.324    1.012   ('narrow-sum', 'true', 10000) = 1.012
refresh_ms_median           0.320    0.549    1.006   ('residual-heavy', 'true', 10000) = 1.006
rows_per_sec_median         0.994    1.822    3.129   ('residual-heavy', 'true', 10000) = 0.994
refresh_peak_mb_median      0.952    1.000    1.000   ('wide-below-budget', 'true', 100000) = 1.000
alloc_mb_median             0.833    1.000    1.000   ('wide-below-budget', 'true', 100000) = 1.000
state_bytes_last            0.334    1.000    1.000   ('wide-below-budget', 'true', 100000) = 1.000
meta_bytes_median           0.312    1.000    1.107   ('narrow-sum', 'false', 10000) = 1.107
data_bytes_median           1.000    1.000    1.000   ('residual-heavy', 'true', 100000) = 1.000
meta_segs_total             0.200    1.000    1.000   ('wide-below-budget', 'true', 100000) = 1.000
data_segs_total             1.000    1.000    1.000   ('residual-heavy', 'true', 100000) = 1.000
complete_seal_ms            0.251    0.985    1.135   ('anchor-only-unfused-control', 'false', 10000) = 1.135
restore_ms                  0.475    0.960    1.147   ('anchor-only-decimal', 'true', 10000) = 1.147
first_reseal_ms             0.136    0.471    1.032   ('narrow-sum', 'true', 100000) = 1.032

cells with no failed gate: 24 / 36
   ('anchor-only-decimal', 'false', 10000) -> meta_bytes_median
   ('anchor-only-decimal', 'false', 100000) -> meta_bytes_median,restore_ms
   ('anchor-only-decimal', 'true', 10000) -> meta_bytes_median,restore_ms
   ('anchor-only-decimal', 'true', 100000) -> meta_bytes_median,restore_ms
   ('anchor-only-unfused-control', 'false', 10000) -> meta_bytes_median,complete_seal_ms,restore_ms
   ('anchor-only-unfused-control', 'false', 100000) -> meta_bytes_median,restore_ms
   ('anchor-only-unfused-control', 'true', 10000) -> meta_bytes_median,restore_ms
   ('anchor-only-unfused-control', 'true', 100000) -> meta_bytes_median
   ('narrow-sum', 'false', 10000) -> meta_bytes_median
   ('narrow-sum', 'false', 100000) -> meta_bytes_median
   ('residual-heavy', 'false', 10000) -> meta_bytes_median
   ('residual-heavy', 'false', 100000) -> meta_bytes_median
```

The median cell seals 3x faster, refreshes at 1.8x the throughput and reseals
after a restore 2x faster. The `min` column is where the layout removal does the
most work: those are fusion-off cells, where the baseline published an anchor
root plus one function root per projection and the candidate publishes one window
root. `meta_segs_total` reaching 0.200 is five metadata segments per seal becoming
one; `state_bytes_last` reaching 0.334 is the same state in a third of the logical
bytes, because one key is stored once instead of once per root.

Neither memory reading regresses anywhere: peak native per refresh and Java
allocation per refresh both top out at 1.000.

The steady seal never regresses by more than 1.2% in any cell
(`seal_ms_median` max 1.007, `seal_ms_p95` max 1.012), comfortably inside the
105% and 110% limits.

## Failed gates

24 of 36 cells pass every gate. The 12 that do not fail on three metrics.

### `meta_bytes_median`, 12 cells

Per-seal metadata bytes. Three distinct magnitudes, which the flat 1.00 limit in
the aggregator does not distinguish:

- **Anchor-only shapes, 8 cells: exactly +86 bytes per seal** (+0.1%), the same
  fixed figure in every one, with `meta_segs_total` and `state_bytes_last`
  unchanged to the byte. This is the anchor-only window-root header and
  zero-component manifest replacing an anchor-root header, which the matrix
  admits as an explicit exception provided it is fixed per root with no per-key
  payload growth and no extra function roots. All three conditions hold, so the
  measured allowance is **86 bytes per state root**.
- **`narrow-sum` with fusion off, 2 cells: +10.7% at 10,000 keys and +7.4% at
  100,000.** Not covered by any exception. The candidate publishes one window
  root carrying a 24-byte payload where the baseline published an anchor root
  with an 8-byte payload plus a function root with a 16-byte one. It writes one
  fewer metadata segment per seal (5 -> 4) and holds 32% fewer logical state
  bytes (675,560 -> 457,780), but the single wider tree's copy-on-write rewrite
  of 1000 dirty keys costs more bytes than the two narrower trees' did.
- **`residual-heavy` with fusion off, 2 cells: +0.33% and +0.03%**, against 12.5%
  fewer metadata segments (8 -> 7) and 2.6-3.1% fewer state bytes.

### `restore_ms`, 6 cells

Reading the checkpoint back on the anchor-only shapes: +5.2% to +14.7%. The
candidate restores a window root with an identity, a key schema and a manifest
where the baseline restored an anchor root.

Read beside the reseal it enables, the pair is faster in every one of those
cells: restore plus first reseal is 0.82 to 0.93 of baseline, because the first
reseal after the restore runs 2 to 3x faster. The regression is real and is
reported as a failed gate; it is not net-negative for a restart.

### `complete_seal_ms`, 1 cell

`anchor-only-unfused-control` with fusion off, at 10,000 keys: +13.5%
(24.33 ms -> 27.62 ms), with no overlap between the two revisions' five runs. Its
fusion-on twin is +4.3% and passes. The candidate builds a window root - identity,
key schema, manifest, an 8-byte payload per key - beside the function root, where
the baseline built an anchor root. Across all 36 cells the complete seal's median
ratio is 0.985 and its minimum 0.251.

## Structural gate

Candidate-only: the baseline has no capture ledger, so this is a claim about the
candidate rather than a comparison. Median per measured seal, run 1 of each cell.
`map` is the live key domain, `wvis` the rows the window walk read, `wimg` the
keys it imaged, `fvis` the rows the function roots' walks read.

```
cell                                                   map  wcap  winc    wvis    wimg  wrem   fn  finc     fvis  flt
anchor-only-decimal f=false K=10000                  10000     1     1    1000    1000     0    1     1     1000    0
anchor-only-decimal f=false K=100000                100000     1     1    1000    1000     0    1     1     1000    0
anchor-only-decimal f=true K=10000                   10000     1     1    1000    1000     0    1     1     1000    0
anchor-only-decimal f=true K=100000                 100000     1     1    1000    1000     0    1     1     1000    0
anchor-only-unfused-control f=false K=10000          10000     1     1    1000    1000     0    1     1     1000    0
anchor-only-unfused-control f=false K=100000        100000     1     1    1000    1000     0    1     1     1000    0
anchor-only-unfused-control f=true K=10000           10000     1     1    1000    1000     0    1     1     1000    0
anchor-only-unfused-control f=true K=100000         100000     1     1    1000    1000     0    1     1     1000    0
narrow-count-star-key f=false K=10000                 9501     1     1     951     951     0    0     0        0    0
narrow-count-star-key f=false K=100000               95001     1     1     951     951     0    0     0        0    0
narrow-count-star-key f=true K=10000                  9501     1     1     951     951     0    0     0        0    0
narrow-count-star-key f=true K=100000                95001     1     1     951     951     0    0     0        0    0
narrow-sum f=false K=10000                           10000     1     1    1000    1000     0    0     0        0    0
narrow-sum f=false K=100000                         100000     1     1    1000    1000     0    0     0        0    0
narrow-sum f=true K=10000                            10000     1     1    1000    1000     0    0     0        0    0
narrow-sum f=true K=100000                          100000     1     1    1000    1000     0    0     0        0    0
narrow-sum-avg-count f=false K=10000                 10000     1     1    1000    1000     0    0     0        0    0
narrow-sum-avg-count f=false K=100000               100000     1     1    1000    1000     0    0     0        0    0
narrow-sum-avg-count f=true K=10000                  10000     1     1    1000    1000     0    0     0        0    0
narrow-sum-avg-count f=true K=100000                100000     1     1    1000    1000     0    0     0        0    0
residual-heavy f=false K=10000                       10000     1     1    1000    1000     0    3     1    21000    0
residual-heavy f=false K=100000                     100000     1     1    1000    1000     0    3     1   201000    0
residual-heavy f=true K=10000                        10000     1     1    1000    1000     0    3     1    21000    0
residual-heavy f=true K=100000                      100000     1     1    1000    1000     0    3     1   201000    0
wide-above-budget f=false K=10000                    10000     1     1    1000    1000     0    1     1     1000    0
wide-above-budget f=false K=100000                  100000     1     1    1000    1000     0    1     1     1000    0
wide-above-budget f=true K=10000                     10000     1     1    1000    1000     0    1     1     1000    0
wide-above-budget f=true K=100000                   100000     1     1    1000    1000     0    1     1     1000    0
wide-at-budget f=false K=10000                       10000     1     1    1000    1000     0    0     0        0    0
wide-at-budget f=false K=100000                     100000     1     1    1000    1000     0    0     0        0    0
wide-at-budget f=true K=10000                        10000     1     1    1000    1000     0    0     0        0    0
wide-at-budget f=true K=100000                      100000     1     1    1000    1000     0    0     0        0    0
wide-below-budget f=false K=10000                    10000     1     1    1000    1000     0    0     0        0    0
wide-below-budget f=false K=100000                  100000     1     1    1000    1000     0    0     0        0    0
wide-below-budget f=true K=10000                     10000     1     1    1000    1000     0    0     0        0    0
wide-below-budget f=true K=100000                   100000     1     1    1000    1000     0    0     0        0    0
```

Every cell, both modes, both cardinalities: one window capture per seal, always
incremental, reading and imaging exactly the 1000 keys the commit changed against
a domain 10x or 100x larger, with no removals and no refresh faults. The window
capture never walks the full anchor map, and where components sit in private maps
it never walks those either.

`narrow-count-star-key` reads 951 rather than 1000 because 5% of rows carry a
NULL partition key and all of them collapse into one key, so a 1000-row commit
touches 951 distinct keys.

Two exemptions the matrix grants show up exactly as specified. `residual-heavy`
keeps three function roots of which one is incremental: `fvis` is
1000 + K + K, the DECIMAL sum's dirty keys plus complete scans of the bounded
ROWS frame and the ring-backed RANGE frame, which `freezeFunction` excludes from
dirty-key capture. `wide-above-budget` keeps exactly one function root, and it is
incremental over 1000 keys in both modes - the overflow projection is a
runtime-only member of the group, not a residual.

## Storage and restore, per cell

```
cell                                                             meta_b        segs              state_b        rest+reseal
anchor-only-decimal f=false K=10000                     80265->   80351   500-> 500    845560->   845560    38.1->  31.1 (0.816)
anchor-only-decimal f=false K=100000                    89657->   89743   500-> 500   8855560->  8855560    85.7->  79.7 (0.929)
anchor-only-decimal f=true K=10000                      80265->   80351   500-> 500    845560->   845560    35.3->  32.0 (0.908)
anchor-only-decimal f=true K=100000                     89657->   89743   500-> 500   8855560->  8855560    85.0->  78.4 (0.923)
anchor-only-unfused-control f=false K=10000             62743->   62829   500-> 500    675560->   675560    35.1->  29.8 (0.849)
anchor-only-unfused-control f=false K=100000            72203->   72289   500-> 500   7155560->  7155560    81.3->  73.7 (0.907)
anchor-only-unfused-control f=true K=10000              62743->   62829   500-> 500    675560->   675560    36.1->  30.6 (0.848)
anchor-only-unfused-control f=true K=100000             72203->   72289   500-> 500   7155560->  7155560    82.3->  72.5 (0.881)
narrow-count-star-key f=false K=10000                  102693->   63001   600-> 400    848736->   358920    41.7->  26.7 (0.641)
narrow-count-star-key f=false K=100000                 121667->   73189   600-> 400   9056736->  3778920   105.3->  58.3 (0.554)
narrow-count-star-key f=true K=10000                    63001->   63001   400-> 400    358920->   358920    24.1->  24.6 (1.023)
narrow-count-star-key f=true K=100000                   73189->   73189   400-> 400   3778920->  3778920    45.7->  45.6 (0.999)
narrow-sum f=false K=10000                              62743->   69487   500-> 400    675560->   457780    35.0->  27.5 (0.786)
narrow-sum f=false K=100000                             72203->   77559   500-> 400   7155560->  4777780    81.5->  55.1 (0.676)
narrow-sum f=true K=10000                               69487->   69487   400-> 400    457780->   457780    25.3->  23.8 (0.942)
narrow-sum f=true K=100000                              77559->   77559   400-> 400   4777780->  4777780    48.5->  46.9 (0.966)
narrow-sum-avg-count f=false K=10000                   167555->   69487   700-> 400   1351120->   457780    52.1->  27.4 (0.525)
narrow-sum-avg-count f=false K=100000                  190903->   77559   700-> 400  14311120->  4777780   154.6->  70.9 (0.458)
narrow-sum-avg-count f=true K=10000                     69487->   69487   400-> 400    457780->   457780    26.4->  24.7 (0.937)
narrow-sum-avg-count f=true K=100000                    77559->   77559   400-> 400   4777780->  4777780    49.8->  47.8 (0.960)
residual-heavy f=false K=10000                        1778460-> 1784371   800-> 700   8458900->  8241120   148.3-> 134.0 (0.903)
residual-heavy f=false K=100000                      16044509->16049967   800-> 700  75988900-> 73611120   717.8-> 666.1 (0.928)
residual-heavy f=true K=10000                         1784371-> 1784371   700-> 700   8241120->  8241120   136.2-> 136.2 (1.000)
residual-heavy f=true K=100000                       16049967->16049967   700-> 700  73611120-> 73611120   660.4-> 655.2 (0.992)
wide-above-budget f=false K=10000                      907479->  355521  2000-> 500   6342260->  3075560   129.2->  58.7 (0.454)
wide-above-budget f=false K=100000                    1022447->  372071  2000-> 500  66822260-> 31155560   522.1-> 245.5 (0.470)
wide-above-budget f=true K=10000                       355521->  355521   500-> 500   3075560->  3075560    45.5->  43.5 (0.955)
wide-above-budget f=true K=100000                      372071->  372071   500-> 500  31155560-> 31155560   130.6-> 129.2 (0.989)
wide-at-budget f=false K=10000                         899295->  307819  2000-> 400   6262260->  2777780   139.7->  51.2 (0.367)
wide-at-budget f=false K=100000                       1014263->  316809  2000-> 400  66022260-> 27977780   531.9-> 240.7 (0.453)
wide-at-budget f=true K=10000                          307819->  307819   400-> 400   2777780->  2777780    38.8->  36.8 (0.950)
wide-at-budget f=true K=100000                         316809->  316809   400-> 400  27977780-> 27977780   104.8-> 105.5 (1.007)
wide-below-budget f=false K=10000                      851153->  299583  1900-> 400   5964480->  2697780   122.3->  51.0 (0.417)
wide-below-budget f=false K=100000                     959239->  308573  1900-> 400  62844480-> 27177780   489.8-> 226.1 (0.462)
wide-below-budget f=true K=10000                       299583->  299583   400-> 400   2697780->  2697780    36.1->  36.2 (1.004)
wide-below-budget f=true K=100000                      308573->  308573   400-> 400  27177780-> 27177780    98.1->  97.7 (0.996)
```

## Second measurement: the failed gates and the remaining cells

Everything below was measured after the run above, on the same machine, with the
same protocol, against the same baseline `6a2c656028`, from the candidate at the
commit that adds this section. Raw files are again not committed; `run-matrix.sh`
regenerates them. Where a number here disagrees with the first run, both are
reported.

### Where narrow-sum's extra metadata bytes come from

The partition map splits a leaf by entry count, not by bytes, so a leaf's rewrite
costs its entry count times the entry width. `narrow-sum` with fusion off is the
one row where the baseline published the anchor value and the accumulator in two
separate trees and the candidate publishes them in one:

- Baseline, fusion off: an anchor root whose entries carry 8 bytes, plus a
  function root whose entries carry 16. Under the daily anchor every run stays
  inside one anchor period, so no key's anchor value moves and the anchor tree is
  never rewritten: the copy-on-write writer drops a put equal to the stored
  entry. A seal rewrites 1000 dirty keys through the 16-byte tree only.
- Candidate, either mode: one window root whose entries carry 24 bytes, the
  anchor value leading them. The SUM changes on every dirty key, so the whole
  24-byte entry is rewritten, and the 8 anchor bytes ride along on every one.

The ceiling is therefore `ANCHOR_STATE_BYTES x keys imaged` = 8,000 bytes per
seal, less what the single root saves: one metadata segment and one root page
fewer per seal. Measured: +6,744 bytes at 10,000 keys and +5,356 at 100,000, the
difference between the two being the wider tree's larger interior rewrite at the
larger domain. This is inherent to a fused entry - the fusion-on baseline wrote
exactly these bytes and passes because the candidate matches it byte for byte -
and there is no encoding change short of splitting the anchor back into a tree of
its own that removes it. The requirement is revised below rather than the code.

### Restore: a one-shot reading, and what it varies by

`restore_ms` is derived: the wall time of the restart turn minus the reseal's own
timer. To see where the time goes, both revisions were rebuilt with a throwaway
timer around `restoreLatestCompatible` and `replayToApplied` in
`tryRestoreFromTimeline`, printed to stderr, and run five times each on the two
10,000-key cells that failed and once more at 100,000 keys. The instrumentation
is not committed; it adds one `System.nanoTime()` pair per phase and one print
per restart.

```
cell                                            restoreLatestCompatible ms, five runs        median   read_back median
anchor-only-decimal f=true K=10000     baseline  14.83 14.67 13.72 16.48 13.99               14.67    23.76
                                       candidate 18.57 14.75 13.58 14.09 13.72               14.09    23.15
anchor-only-unfused f=false K=10000    baseline  12.88 13.22 14.90 14.18 12.57               13.22    23.43
                                       candidate 16.39 13.84 16.42 13.13 16.72               16.39    26.78
anchor-only-decimal f=false K=100000   baseline  64.13 65.72 64.21 68.99 70.70               65.72    71.07
                                       candidate 64.23 66.03 66.93 66.40 68.37               66.40    72.26
```

`replayToApplied` is 30 to 70 us on both sides in every run, and the rest of the
turn - reading the probe rows and everything outside the restore and the reseal -
is 9.1 ms against 9.4 ms at 10,000 keys. The restore call itself is where the
runs differ, and at 10,000 keys it is bimodal on both revisions: most runs read
the root back in 13 to 15 ms, some in 16 to 18.5, with the slow mode landing on
the candidate in four of ten runs and on the baseline in one. Five samples of
that distribution put the median on either side by chance. At 100,000 keys,
where the first run read +7.8% for this cell, these five reruns read +1.0% on the
restore call and +1.7% on the derived figure.

Five runs were not enough to settle it either way, so the cells were re-measured
with 15 interleaved runs, below. **That re-measure shows the difference is
real**: about 2.3 ms per restart at 10,000 keys and 1.5 to 4.3 ms at 100,000, on
the anchor-only shapes only, and not on `narrow-sum`. The code path is the same
walk in both shapes - a validating pass and a restoring pass over the same
partition-map leaves - and class loading is not the difference: a whole
10,000-key run loads 8,103 classes on the baseline and 8,107 on the candidate,
199 of them under `cairo.lv` on both. What distinguishes the anchor-only shapes
is which code the restore runs cold: the baseline restores them through the
anchor-root decoder and `restoreCheckpointEntry`, the candidate through the
window-root decoder, the manifest comparison and `restoreCheckpointWindowEntry`,
a wider path that runs exactly once per JVM in this harness. `narrow-sum` fusion
on runs the same window-root path on both revisions and reads 0.99 to 1.05. That
is the explanation the evidence points at, and it is not proven: a `-Xcomp` run
puts the compiler inside the timed region and cannot separate the two, and no
harness knob restores twice in one JVM.

### Complete seal: the JVM's first seal against a warm one

The same runs give the seed seal, which is the first seal the JVM performs and
runs on cold code:

```
cell                                            seed seal ms, five runs                       median   ratio
anchor-only-decimal f=true K=10000     baseline  24.39 24.87 25.99 24.32 24.59               24.59
                                       candidate 27.36 25.88 27.10 29.27 26.35               27.10    1.102
anchor-only-unfused f=false K=10000    baseline  24.73 27.41 23.94 25.54 24.18               24.73
                                       candidate 26.51 26.48 28.18 27.91 25.26               26.51    1.072
anchor-only-decimal f=false K=100000   baseline  185.1 185.9 181.2 179.7 186.1               185.1
                                       candidate 201.9 195.6 182.8 181.5 188.0               188.0    1.016
```

An extra 1.5 to 2.5 ms on the first seal at 10,000 keys, and about 3 ms at
100,000: a fixed cost per JVM rather than a per-key one, which is what a cold
code path costs and a wider one costs more of. To read a complete seal on warm
code, the same shape was run with a one-minute anchor, 1000 rows per minute and
compaction thresholds low enough for the frontier sweep to fire every batch; the
seal after a sweep is a complete freeze of the live domain, about 1,250 keys,
carrying about 1,000 removals. Three runs each, mean over 110 such seals per run:

```
warm complete seal after a sweep       baseline  4.188 4.223 4.359   median 4.223
                                       candidate 4.370 4.345 4.538   median 4.370   ratio 1.035
```

Warm, a complete window-root seal costs 3.5% more than a complete anchor-root
seal over the same keys, inside the limit. The 5 to 12% on the first seal at
10,000 keys and the 2.5 to 4% at 100,000 (15-run figures below) are therefore
mostly the cold first seal of a wider code path, with a per-key component of a
few percent under it; at 1,000,000 keys, where the seal runs 2.3 to 2.5 s, the
same cells read 0.990 to 1.043.

Two things this diagnostic run also showed, reported because they were seen and
not because they were measured under the protocol: with the anchor moving every
batch, so that every touched key's entry changes, the steady incremental seal of
the anchor-only shape read 3.65 to 3.71 ms on the baseline and 3.95 to 4.02 ms on
the candidate over the run's 110 seals, warm-up included; and the sweep itself
read 33 to 37 ms on both. Three runs at diagnostic settings are not a matrix
cell. The cross-anchor-boundary run the matrix names beside the steady rows would
measure this properly and has not been run.

### Re-measured: anchor-only and single-SUM cells, 15 runs

Fifteen independent JVMs per cell per revision, interleaved run by run (baseline
run 1, candidate run 1, baseline run 2, ...), for the three shapes whose one-shot
readings failed or whose metadata bytes did, at 10,000 and 100,000 keys. Steady
seal, metadata bytes per seal, complete seal, restore and restore-plus-first-reseal,
as candidate / baseline of the medians over 15 runs:

```
cell                                          seal   meta_b  cseal  restore rest+reseal   (candidate/baseline, medians over 15 runs)
anchor-only-decimal f=false K=10000           0.332  1.001   1.118  1.110   0.874
anchor-only-decimal f=false K=100000          0.321  1.001   1.040  1.049   0.917
anchor-only-decimal f=true K=10000            0.327  1.001   1.051  1.100   0.865
anchor-only-decimal f=true K=100000           0.325  1.001   1.041  1.021   0.897
anchor-only-unfused-control f=false K=10000   0.317  1.001   1.080  1.101   0.874
anchor-only-unfused-control f=false K=100000  0.311  1.001   1.027  1.065   0.921
anchor-only-unfused-control f=true K=10000    0.323  1.001   1.097  1.036   0.831
anchor-only-unfused-control f=true K=100000   0.301  1.001   1.025  1.035   0.901
narrow-sum f=false K=10000                    0.239  1.107   0.689  0.993   0.764
narrow-sum f=false K=100000                   0.219  1.074   0.577  0.781   0.680
narrow-sum f=true K=10000                     1.008  1.000   0.965  1.049   1.039
narrow-sum f=true K=100000                    1.014  1.000   1.014  1.023   1.021
```

The steady figures reproduce the first run: the anchor-only seal is 0.30 to 0.33 of
baseline, `narrow-sum` with fusion off 0.22 to 0.24, fusion on 1.008 to 1.014; the
+86 bytes per seal (1.001) on the anchor-only cells and the +10.7% / +7.4% on
`narrow-sum` fusion off are byte-identical to before.

The one-shot readings do **not** average away with 15 runs. In absolute terms:

```
cell                                             restore ms                complete seal ms
anchor-only-decimal f=false K=10000       22.38 -> 24.84  (+2.46)     25.3 -> 28.3  (+3.0)
anchor-only-decimal f=false K=100000      69.99 -> 73.42  (+3.43)    179.9 -> 187.1 (+7.2)
anchor-only-decimal f=true  K=10000       22.77 -> 25.04  (+2.27)     25.8 -> 27.1  (+1.3)
anchor-only-decimal f=true  K=100000      71.65 -> 73.13  (+1.48)    179.0 -> 186.4 (+7.4)
anchor-only-unfused f=false K=10000       22.54 -> 24.81  (+2.27)     24.4 -> 26.4  (+1.9)
anchor-only-unfused f=false K=100000      66.31 -> 70.61  (+4.30)    170.8 -> 175.5 (+4.7)
anchor-only-unfused f=true  K=10000       22.99 -> 23.81  (+0.82)     24.8 -> 27.2  (+2.4)
anchor-only-unfused f=true  K=100000      67.79 -> 70.19  (+2.40)    170.9 -> 175.1 (+4.2)
narrow-sum f=false          K=10000       22.32 -> 22.15  (-0.17)     24.9 -> 17.1  (-7.7)
narrow-sum f=true           K=10000       19.59 -> 20.55  (+0.96)     15.6 -> 15.1  (-0.5)
narrow-sum f=true           K=100000      43.55 -> 44.56  (+1.01)     89.5 -> 90.8  (+1.3)
```

Read as absolute deltas the pattern is a cost of one to four milliseconds per
restart and per first seal that appears on the two shapes whose baseline state root
was an anchor root, grows only weakly with the key count (10x the keys, roughly 1.5
to 2.5x the delta), and is absent on `narrow-sum`, where the fusion-on baseline ran
the same window-root code the candidate runs. Restore plus its first reseal stays
0.83 to 0.92 of baseline on every anchor-only cell, because the incremental first
reseal is half the baseline's; the restore reading on its own is over the 105% limit
in five of the eight anchor-only cells (1.049 to 1.110) and the complete seal in
four (1.051 to 1.118), with the four 100,000-key complete seals at 1.025 to 1.041.

Taken together with the warm complete seal at 1.035 and the 1,000,000-key cells
below, the picture is a fixed cost of one to four milliseconds paid once per JVM
on the two one-shot operations of the anchor-only shapes, most likely the cold
first execution of the window-root path where the baseline ran the narrower
anchor-root one, plus a per-key cost of a few percent on the complete seal. It is
a genuine regression of the restore reading on those shapes and is reported as
one; a restart is still 8 to 17% faster end to end because the first reseal after
it is incremental.

### 1,000,000 live keys

Five independent JVMs per cell per revision, interleaved, `-Xmx24g`, for the
anchor-only and single-SUM shapes. A run seeds 1,000,000 rows into 1,000,000
accounts (5 to 8.5 s), seals the seeded state once (the complete seal, 1.2 to 3.0 s),
then runs the same 110 batches of 1000 rows over 1000 distinct existing keys each.
Candidate / baseline of the medians:

```
cell                                          seal   p95    refresh  thrpt  state_b  meta_b  segs   cseal  restore rest+reseal
anchor-only-decimal f=false K=1000000         0.496  0.528  0.551    1.816  1.000    1.001   1.000  1.041  1.015   0.999
anchor-only-decimal f=true  K=1000000         0.494  0.485  0.541    1.850  1.000    1.001   1.000  0.990  0.988   0.973
anchor-only-unfused f=false K=1000000         0.490  0.474  0.540    1.853  1.000    1.001   1.000  0.999  1.010   0.992
anchor-only-unfused f=true  K=1000000         0.487  0.476  0.538    1.860  1.000    1.001   1.000  1.043  1.060   1.041
narrow-sum f=false          K=1000000         0.232  0.220  0.305    3.282  0.659    1.071   0.800  0.531  0.766   0.752
narrow-sum f=true           K=1000000         0.999  1.041  0.989    1.011  1.000    1.000   1.000  1.031  1.046   1.045
```

Peak native memory and Java allocation per refresh are 1.000 in every cell. The
structural gate holds at this size too: one window capture per seal, always
incremental, 1000 keys visited and 1000 imaged against a live domain of
1,000,000, no removals, no faults, and the anchor-only shapes' one function root
incremental over the same 1000 keys.

Scaling from 100,000 to 1,000,000 keys, the steady seal ratio on the anchor-only
shapes moves from 0.31 to 0.49 - both revisions' seals grow with the tree height
and the candidate's grew more, from 3.5 to 7.1 ms against the baseline's 11.3 to
14.5 - while `narrow-sum` fusion off holds at 0.23 and fusion on at parity. The
metadata bytes per seal reproduce byte for byte: +86 on the anchor-only cells
and +7.1% on `narrow-sum` fusion off, against a fifth fewer segments and a third
fewer logical bytes.

The one-shot readings at this size, in absolute terms:

```
cell                                          restore ms                  complete seal ms
anchor-only-decimal f=false                609.4 -> 618.3  (+8.9)      2418 -> 2516  (+98)
anchor-only-decimal f=true                 623.4 -> 615.9  (-7.5)      2538 -> 2513  (-25)
anchor-only-unfused f=false                560.3 -> 566.0  (+5.7)      2419 -> 2416  (-3)
anchor-only-unfused f=true                 552.6 -> 585.7  (+33.1)     2233 -> 2328  (+95)
narrow-sum f=false                         564.6 -> 432.8  (-131.8)    2421 -> 1285  (-1136)
narrow-sum f=true                          327.0 -> 342.0  (+15.0)     1216 -> 1254  (+38)
```

The fixed cost seen at 10,000 and 100,000 keys does not scale with the keys: at
a million keys the anchor-only restore reads 0.988 to 1.060 and the complete seal
0.990 to 1.043, with the five per-run readings of the two revisions overlapping in
every cell but the one at 1.060 (`anchor-only-unfused-control`, fusion on:
baseline 544.5 to 604.7 ms, candidate 561.7 to 589.5). That cell is the one
failed timing gate at this size, and its restore plus first reseal is 1.041.

### Closed-segment repair

Five runs per cell per revision at 1,000 and 10,000 keys, both fusion modes. Per
repair batch the harness's `refresh_ms` is the repair's latency: one late row per
commit, ten minutes behind, into a closed one-minute anchor segment with ten
checkpoint boundaries sealed above it. 90 corrections per run are measured (batches
20 to 109). Every one of the 40 runs ends with the result oracle reporting
`match`: 38,120 rows at 1,000 keys and 42,620 at 10,000, none only in the view,
none only in the oracle.

The baseline runs the whole-range control only. The candidate's whole-range cell
is the like-for-like comparison; its keyed cell is read against the same baseline
cell and is a route comparison.

```
cell                                   route          repair ms median    p95        replayed rows   keyed segs   peak MB
                                                      base -> cand  ratio  ratio     base -> cand    per run      base -> cand
repair-closed-whole f=false K=1000     whole -> whole 10.02 -> 9.14  0.912  0.842    1999 -> 1999    0            1.0 -> 1.0
repair-closed-whole f=false K=10000    whole -> whole 14.81 -> 12.11 0.818  0.675    1999 -> 1999    0            1.3 -> 1.3
repair-closed-whole f=true  K=1000     whole -> whole  8.92 -> 9.13  1.023  1.076    1999 -> 1999    0            1.0 -> 1.0
repair-closed-whole f=true  K=10000    whole -> whole 11.79 -> 11.85 1.005  0.984    1999 -> 1999    0            1.3 -> 1.3
repair-closed-keyed f=false K=1000     whole -> keyed 10.02 -> 9.05  0.904  0.825    1999 -> 1000    90           1.0 -> 1.0
repair-closed-keyed f=false K=10000    whole -> keyed 14.81 -> 12.07 0.815  0.470    1999 -> 1000    90           1.3 -> 1.8
repair-closed-keyed f=true  K=1000     whole -> keyed  8.92 -> 8.79  0.985  0.874    1999 -> 1000    90           1.0 -> 1.0
repair-closed-keyed f=true  K=10000    whole -> keyed 11.79 -> 11.60 0.984  0.677    1999 -> 1000    90           1.3 -> 1.3
```

The repair latency gate passes in all eight comparisons, 0.815 to 1.023. Both
revisions take the same publication route on every one of the 450 corrections per
cell, `resume from anchor / resume cheaper`; the keyed cell additionally follows
its key through the posting index on all 90 corrected closed segments per run,
which is what halves the rows its replay reads (the 1000 forward rows of the
commit stay). The corrected output size is the same on both sides and both routes:
the live view is partitioned by hour and the correction lands inside a closed
partition, so the publication rewrites that partition whole - 38,120 rows at 1,000
keys and 42,620 at 10,000 per repair, the write amplification the harness reports
as `lv_phys_rows`. A sparse publication needs the view's own dedup keys and is not
part of this cell.

Fusion off is where the difference is: the baseline's repair re-versioned an
anchor root plus a function root per boundary and the candidate re-versions one
window root, so at 10,000 keys the repair runs 18% faster, writes 35% fewer
metadata bytes per batch and 15% fewer segments, and the same live state occupies
32% fewer logical bytes. Fusion on is at parity, within 2.3%.

Three gates outside the repair latency fail in these cells, and they are the
three already attributed above rather than new findings: the restore reading in
the two keyed fusion-on cells (1.053 and 1.095; the whole-range cells of the same
runs read 0.981 and 1.032, and the keyed cell's restore is the same code as the
whole-range cell's), the first complete seal in one fusion-on cell (1.061, +0.9
ms at 10,000 keys), and peak native memory per refresh in the keyed fusion-off
10,000-key cell, 1.3 -> 1.8 MB, which is a route difference: the keyed replay
maps the posting index and the corrected segment's checkpoint leaves beside the
forward refresh, where the whole-range control streams the segment. Its fusion-on
twin reads 1.3 -> 1.3, and both candidate whole-range cells read 1.000, so the
layout is not what moved it. Java allocation per refresh is 1.000 in every repair
cell.

At 10,000 keys an account appears once in ten minutes and falls behind the frontier
in between, but the default compaction thresholds (100,000 stale entries, 50%) are
never reached in 110 batches: no sweep fires, no key is evicted, and the map holds
all 10,000 keys throughout on both revisions. The add/remove-keys regime the matrix
names is therefore not covered by this cell.

### Revised requirements

The plan asks for a failed gate to be fixed or for the requirement to be revised
explicitly with its measured impact. Three are revised; nothing else in the limits
table moves.

1. **Per-seal metadata bytes for a shape whose baseline split the anchor and its
   accumulators across trees.** The fused entry may add at most
   `ANCHOR_STATE_BYTES` (8) bytes per key imaged, less the segment and root page
   the single root saves. Measured: +10.7% at 10,000 keys and +7.4% at 100,000
   for `narrow-sum` with fusion off, against one metadata segment fewer per seal
   and 32% fewer logical state bytes. The fusion-on comparison for the same shape
   is unchanged at 1.000 and stays under the flat limit.
2. **Restore, for a shape whose baseline state root was an anchor root.** The
   restore may cost up to 5 ms more per restart, a fixed amount rather than a
   per-key one, provided restore plus first reseal stays under 100% of baseline.
   Measured over 15 runs: +0.8 to +2.5 ms at 10,000 keys (1.036 to 1.110), +1.5
   to +4.3 ms at 100,000 (1.021 to 1.065), -7.5 to +33 ms at 1,000,000 (0.988
   to 1.060, five runs), with restore plus first reseal at 0.83 to 0.92, 0.90 to
   0.92 and 0.97 to 1.04 respectively. The 105% limit stands unchanged for every
   other shape, and `narrow-sum` meets it in all six cells.
3. **The JVM's first complete seal, for the same shapes.** It may cost up to 5 ms
   more, again fixed. Measured: +1.3 to +3.0 ms at 10,000 keys (1.051 to 1.118),
   +4.2 to +7.4 ms at 100,000 (1.025 to 1.041), -25 to +98 ms at 1,000,000
   (0.990 to 1.043). A complete seal on warm code, which is what every complete
   seal after the first is, measured 1.035 and stays under the 105% limit.

## Third measurement: keys added and removed across anchor boundaries, and cold-cache restore

The two readings the matrix names beside the steady rows and the second
measurement left open. Baseline `6a2c656028` against the branch at the commit
this section lands in, same machine, JVM, heap and settings as the first two;
720 runs in all, 360 per revision, plus a 15-run re-measure of the cells whose
five-run spreads overlapped. Other sessions were using the machine while these
ran, so the two revisions were interleaved run by run - every run index went to
the baseline and then to the candidate before the next index started - and the
one-minute load average was logged before each revision's turn: 1.0 to 5.4,
with both revisions seeing the same range. Three driver runs failed: two were
the runs this session interrupted to reschedule the residual-heavy cell, which
the driver then regenerated, and one candidate run of that cell failed on its
own and is reported below.

### The churn cell

`run-matrix.sh ... churn`. The steady rows recycle `K` accounts forever, so no
key is ever added or evicted. The churn rows slide a `K`-account window
(`--account-window=K`) over an anchor bucket of exactly `K` rows - a
`K/1000`-minute anchor at 1000 rows per minute - which the harness moves forward
by `K/2` accounts per bucket: half of a bucket's accounts recur from the bucket
before and half are new, and the half left behind falls behind the frontier and
is evicted by the sweep at the next bucket boundary. `K/2` keys added and `K/2`
evicted per bucket, over a live domain moving between `K` and `1.5 K`. The
compaction thresholds are lowered to `--compact-threshold=1000
--compact-stale-percent=25` so the sweep fires at all - the shipped defaults need
100,000 stale keys and never do at these sizes. Six buckets per run: 110 batches
at 10,000 keys (10 sweeps, the first inside the warm-up), 610 at 100,000 (5
measured sweeps). Every run ends with the result oracle. The residual-heavy
shape runs two buckets at 100,000 keys, for the reason given in its own section.

Every seal of a churn run is one of three kinds and all three are in the steady
gates: over existing keys, over keys the batch just added, or - once per bucket
- the seal after a sweep, which stays incremental and carries one removal per
evicted key on top of the keys it imaged. That last kind is gated separately as
`swept_seal_ms_median`; the sweep itself (`sweep_ms`) is runtime map work the
layout does not touch and is reported, not gated.

The oracle matched in all 360 churn runs on both revisions: at 10,000 keys
121,000 rows, at 100,000 keys 711,000, none only in the view, none only in the
oracle. Eviction dropped nothing the view still needed.

### Churn: the fused shapes

27 cells: the seven shapes carrying at least one inline component, both modes,
both sizes. Every timing, memory, allocation and storage gate passes, in every
cell, with two exceptions taken up under "Re-measured" below.

```
metric                    min    median   max     limit
incremental seal         0.172   0.988   1.026    1.05
incremental seal p95     0.088   0.973   1.048    1.10
seal after a sweep       0.057   0.960   1.081    1.05   (two cells above, re-measured below)
refresh                  0.306   0.968   1.035    1.05
throughput               0.966   1.033   3.268    0.95 floor
peak native / refresh    0.989   1.000   1.000    1.05
java alloc / refresh     0.947   1.000   1.000    1.05
logical state bytes      0.312   0.988   1.000    1.00
metadata bytes / seal    0.245   0.993   1.000    1.00
metadata segments        0.200   0.875   1.000    1.00
restore                  0.487   0.958   1.074    1.05   (one cell above, re-measured below)
first reseal             0.091   0.952   1.045    1.05
```

Fusion on runs the window-root path on both revisions and reads at parity:
seals 0.99 to 1.02, the seal after a sweep 0.93 to 1.02 outside the two
re-measured cells, sweeps 0.98 to 1.06. Fusion off is where the layout does its
work, and the churn regime shows it more sharply than the steady one did: the
baseline re-versioned an anchor root plus one function root per projection on
every seal, and a sweep's removals had to be applied to each of them. With one
window root the seal after a 5,000-key sweep at 10,000 keys costs 0.09 to 0.33
of baseline on the narrow and wide shapes (`wide-at-budget` 132 ms -> 11.6 ms),
and after a 50,000-key sweep at 100,000 keys 0.06 to 0.38 (`wide-below-budget`
1,139 ms -> 65 ms). The steady seal reads 0.17 to 0.59, logical state bytes
0.31 to 0.63, metadata segments 0.20 to 0.80. Restore of a churned timeline reads
0.49 to 0.79 with fusion off and 0.95 to 1.07 with it on.

The sweep itself costs the same on both revisions in every cell - 0.41 to 42 ms
at 10,000 keys and 6.4 to 199 ms at 100,000 depending on the shape, within 8%
either way - which is as expected: it rebuilds the runtime map and records the
evicted keys in the dirty set, and neither is layout work.

### Churn: the anchor-only shapes

8 cells: the anchored DECIMAL SUM and the expression control, whose window root
carries only the eight-byte anchor value, both modes, both sizes. Here the
candidate's steady incremental seal is slower, and it is the one regression this
measurement finds:

```
cell                                             seal ms      ratio   p95     refresh  throughput
anchor-only-decimal          f=true  K=10000     4.62 -> 4.95  1.072   1.043   1.047    0.951
anchor-only-decimal          f=false K=10000     4.70 -> 4.07  0.867   0.725   0.962    1.039
anchor-only-decimal          f=true  K=100000    3.94 -> 4.33  1.099   1.133   1.072    0.933
anchor-only-decimal          f=false K=100000    3.88 -> 4.33  1.116   1.123   1.081    0.925
anchor-only-unfused-control  f=true  K=10000     4.70 -> 4.92  1.047   1.017   1.057    0.946
anchor-only-unfused-control  f=false K=10000     4.59 -> 4.93  1.073   1.011   1.035    0.966
anchor-only-unfused-control  f=true  K=100000    3.85 -> 4.24  1.100   1.101   1.076    0.929
anchor-only-unfused-control  f=false K=100000    3.85 -> 4.27  1.109   1.087   1.087    0.920
```

At 100,000 keys the two revisions' five runs do not overlap in any of the four
cells - the baseline's medians run 3.83 to 3.97 ms and the candidate's 4.06 to
4.45 - so the +0.39 to +0.45 ms per seal is real: about 400 ns per imaged key,
10 to 12%. At 10,000 keys every one of the four cells is bimodal on both sides,
with one mode near 4.0 ms and one near 4.9, and five runs land the median on
either mode by chance (`anchor-only-decimal` with fusion off reads 0.867 that
way); those four cells are re-measured with 15 runs below. Peak native memory,
Java allocation, logical state bytes and segment counts are 1.000 in all eight
cells; metadata bytes read 1.000 to 1.001, the +86 bytes per seal the first
measurement attributed to the anchor-only header and manifest. The seal after a
sweep - 1,000 keys imaged plus 5,000 or 50,000 removals - reads 0.98 to 1.02,
and the sweep itself 0.97 to 1.10.

**Where the time goes.** The steady matrix measured the same shapes' incremental
seal at 0.31 to 0.34 of baseline (9.0 ms -> 3.0 ms at 10,000 keys), and the
difference between that reading and this one is what changes between the two
regimes: under a daily anchor no key's anchor value moves during a run, under
the churn cell every imaged key's does. The window-root freeze
(`freezeWindowState`) looks each imaged key up in the predecessor root -
`previousBoundary.findWindowState(key)`, a descent of the partition map with a
decoded-node memo - and elides the put when the predecessor already holds the
same bytes, which is what makes the anchor-only seal three times cheaper when
anchors hold: the baseline's anchor-root path (`FrozenAnchor`, deleted with the
layout) staged every key's put without looking and let the partition-map writer
drop the equal ones one layer down, after a descent of its own. When every
anchor moves the lookup finds nothing to elide and the mutation is staged
anyway, so the seal pays the descent twice. A shape with an inline component
pays that lookup on the baseline too - fusion on ran the same freeze there -
which is why the seven fused shapes read at parity in the same regime and why
the eight anchor-only cells are the only ones that move.

This attribution rests on the code and on the contrast between the two regimes
- the same shapes, the same seal, 0.31 to 0.34 with anchors held and 1.10 to
1.12 with every anchor moved - and not on a run with the lookup disabled, which
was not made. It is the explanation the evidence points at, as the second
measurement's restore attribution was, and is reported with the same
qualification.

The first complete seal of the JVM reads +1.6 and +2.1 ms at 10,000 keys
(1.058, 1.077), inside revised requirement 3's 5 ms, and at 100,000 keys +20.6 ms
with fusion off (1.088) against -16.0 ms with fusion on (0.933) on the same code
path, a one-shot at 230 ms whose five-run spreads overlap on both sides (217 to
246 against 220 to 234, and 214 to 239 against 248 to 265); neither sign holds
across the two modes, and the reading is left as inconclusive rather than added
to that requirement.

### Churn: the residual-heavy shape at 100,000 keys

This shape's ring-backed bounded RANGE residual scans its whole map on every
seal - the exemption the plan grants and the first measurement confirmed - so
at 100,000 keys a seal costs 0.8 s and a six-bucket run would take twelve
minutes; twenty of them, four hours. The cell was run with two buckets instead
(`CHURN_BUCKETS=2`: 210 batches, 200 measured seals, one measured sweep per run
evicting 82,749 keys), five runs per revision, both modes, interleaved.

```
metric                       f=false            f=true
incremental seal             797.8 -> 798.0     805.4 -> 805.9     1.000 / 1.001
seal after the sweep         1167  -> 1050      1047  -> 1061      0.900 / 1.013
sweep                        69.6  -> 74.9      57.8  -> 57.2
refresh                      1.000              1.000
peak native, java alloc      1.000              1.000
logical state bytes          0.970              1.000
metadata segments            0.875              1.000
restore                      569.9 -> 495.2     518.0 -> 508.7     0.869 / 0.982
first reseal                 1082  -> 1146      1117  -> 1106      1.059 / 0.991
complete seal                713.1 -> 677.7     638.6 -> 671.2     0.950 / 1.051
```

Two one-shot readings sit just over their limits and both have overlapping
five-run spreads: the first reseal after restore with fusion off (1,070 to 1,139
ms against 1,106 to 1,153, with restore plus reseal at 0.990) and the JVM's first
seal with fusion on (585 to 850 ms against 602 to 780). The same two readings on
the other mode read 0.991 and 0.950. At four minutes per run this cell was not
re-measured, and the two are reported as inconclusive rather than as a pass or a
regression; the steady seal, the seal after the sweep, memory, allocation and
storage all pass, and the oracle matched in every run.

### The structural gate under churn

Read from the candidate's capture ledger over every measured batch of all 180
churn runs - 59,000 seals - and checked mechanically rather than by median:
every seal is exactly one window capture and it is incremental; keys visited
equal keys imaged plus keys removed in every batch; a batch without a sweep
images exactly the 1,000 keys the commit touched (951 on `count-star-key`, whose
NULL-key rows fold into one partition) and names no removal; a batch with a
sweep names exactly as many removals as the sweep evicted; no refresh fault
anywhere. The residual-heavy shape keeps its three function roots with one
incremental, and `wide-above-budget` its one, as in the steady matrix.

So a seal after a sweep of 50,000 keys visits 51,000 rows against a domain of
100,000 to 150,000: the removals are read out of the dirty set the sweep recorded
them in, not discovered by walking the map. This is what the "seal after a sweep"
gate prices, and on the fused shapes it is the reading that moved most.

### Cold-cache restore

`run-matrix.sh ... cold-restore`: the steady rows again, with the restart at the
end of each run dropping the database's files from the page cache first. The
harness releases the engine's pooled readers and writers, fsyncs every file
under the database root and advises it `POSIX_FADV_DONTNEED`, then rebuilds the
view. `posix_fadvise` is advice, so the eviction was checked once with
`residency.py` (mmap plus `mincore`) against a run paused after it: the
checkpoint tree read 0 of 14.4 MB resident at 100,000 keys, the whole root 14%,
and what stayed resident were the symbol-map files of the base and the view
(about 12 MB), which the WAL writer pool keeps mapped on both revisions and which
the restore does not read. Every one of the 360 runs reported `cache=cold` and
the bytes it advised, 29 MB to 2.1 GB depending on the shape and size, the same
on both revisions to within a megabyte.

36 cells, five runs each per revision. Restore passes in all 36: 0.376 to 1.044,
median 0.974.

```
                              restore, cold cache            restore + first reseal
anchor-only shapes, K=10000   1.021 to 1.044                 0.83 to 0.88
anchor-only shapes, K=100000  0.938 to 0.981                 0.83 to 0.86
fused shapes, fusion on       0.957 to 1.025                 0.95 to 1.02
fused shapes, fusion off      0.376 to 0.900                 0.33 to 0.93
```

Cold, the anchor-only restore that the second measurement found 1.04 to 1.11
warm at 10,000 keys reads 1.02 to 1.04, and at 100,000 keys 0.94 to 0.98: the
disk read is the same on both sides and the fixed cost of the wider restore path
is a smaller share of it. With fusion off the candidate reads back one window
root where the baseline read an anchor root plus a function root per projection,
which is 0.38 to 0.55 on the wide shapes (746 to 778 ms -> 280 to 341 ms at
100,000 keys). The first reseal after a cold restore is 0.14 to 0.47 wherever the
baseline's was a complete freeze and 0.97 to 1.03 where both are incremental,
with two exceptions at 100,000 keys with fusion on, `count-star-key` (3.87 ->
4.14 ms, 1.069) and `sum-avg-count` (3.72 -> 4.28 ms, 1.148), whose five-run
spreads overlap fully (3.44 to 4.29 against 3.59 to 4.35, and 3.52 to 4.43
against 3.63 to 4.32) and which are re-measured below. Their restore plus
reseal reads 0.993 and 1.006.

### Re-measured, 15 runs

Ten more interleaved runs on each of the cells whose five-run spreads overlapped,
15 per cell per revision.

The six fused-shape readings that stood over their limit on five runs are under
it on fifteen: the seal after a sweep at 100,000 keys with fusion on reads 1.042
on `narrow-sum-avg-count` (46.8 -> 48.8 ms) and 1.002 on `wide-below-budget`
(62.2 -> 62.4 ms); the first reseal after a cold restore at 100,000 keys with
fusion on reads 0.996 on `count-star-key` and 1.014 on `sum-avg-count`; and
`narrow-sum` with fusion on at 10,000 keys reads 1.040 on the JVM's first seal
and 1.011 on restore, `wide-above-budget` 1.016 on the first seal. Every one of
them is a fusion-on cell, where both revisions run the same window-root code,
and their five-run readings were the tails of overlapping distributions.

The four anchor-only cells at 10,000 keys stay bimodal on fifteen runs, on both
revisions and in both modes: a run's median seal sits either near 3.5 to 4.1 ms
or near 4.6 to 5.0, for the whole run, and which mode a JVM lands in is not
decided by the revision - the baseline has 3 to 8 fast runs of 15 per cell and
the candidate 3 to 7. Read mode against mode the candidate is +0.25 to +0.35 ms
per seal in both, the same 6 to 10% the 100,000-key cells read; read as a
median of the mixture it is 1.038, 1.041 and 1.057 in three cells and 1.218 in
`anchor-only-unfused-control` with fusion off, whose baseline landed eight runs
in the fast mode against the candidate's four. The seal after a sweep in that
cell reads 1.238 for the same reason (14.0 to 21.3 ms on both sides, in two
modes); in the other three it reads 0.99 to 1.03.

Three one-shot readings on `anchor-only-unfused-control` with fusion on at
10,000 keys also stand over the limit on fifteen runs: the JVM's first seal
27.4 -> 29.2 ms (1.069), restore 27.5 -> 29.4 ms (1.071) and the first reseal
8.4 -> 9.0 ms (1.064), with restore plus first reseal at 1.060. The first two are
the +1.9 ms that revised requirements 2 and 3 already admit; the reseal is a seal
over the probe's keys in the churn regime, where their anchors moved, and falls
under the fourth revision below rather than under requirement 2's proviso, which
was stated for the held-anchor regime in which the reseal is half the baseline's.
The other seven anchor-only churn cells read restore 0.95 to 1.02, first reseal
0.93 to 1.01 and restore plus reseal 0.96 to 1.04.

### The failed run

One candidate run failed on its own: `residual-heavy-churn`, fusion off, 100,000
keys, run 1, in the first pass, after about three minutes of a run that takes
four. The driver discarded the JVM's stderr at the time, so its stack trace is
lost, and it deleted the partial output. The driver's second pass regenerated the
run, which passed with the oracle matching, and two further attempts of the same
configuration with stderr captured passed as well, 311,000 rows each, oracle
`match`, no refresh fault. Three clean runs out of three do not prove the failure
was environmental, and it is reported as unreproduced and unexplained rather than
dismissed. The driver now writes each run's stderr to a `.err` file beside its
output and keeps a failed run's partial output under `.failed.tsv`, so a recurrence
carries its cause.

### Revised requirement

A fourth revision, alongside the three of the second measurement; nothing else
in the limits table moves.

4. **The steady incremental seal of an anchored window with no inline
   component, in a batch where every imaged key's anchor value moved.** It may
   cost up to 0.5 ms more per 1,000 keys imaged - the predecessor lookup that
   elides unchanged entries, paid when there are none - provided the same shape's
   seal under a held anchor stays under 50% of baseline. Measured: +0.39 to
   +0.45 ms per seal at 100,000 keys (1.099 to 1.116) in four cells;
   +0.25 to +0.35 ms
   mode against mode at 10,000 keys over 15 runs (1.038 to 1.218 as medians of
   bimodal mixtures); against 0.31 to 0.34
   under a daily anchor in the steady matrix. The seal after a sweep on the same
   shapes, memory, allocation and storage are unchanged. Every shape with an
   inline component is unaffected because the baseline paid the same lookup
   there, and the 105% limit stands for them. The first reseal after a restore
   in the same regime is such a seal, and requirement 2's proviso - restore plus
   first reseal under baseline - is read in the held-anchor regime it was stated
   for; measured 1.060 in one cell here, 0.96 to 1.04 in the other seven.

## Fourth measurement: skipping the elision lookup where nothing can be elided

The third measurement's fourth revised requirement covers a steady incremental
seal that costs more on an anchored window with no inline component when every
imaged key's anchor moved: the freeze looks the predecessor's entry up for every
key it images, and in that regime the lookup never finds an entry it can leave
standing. `LiveViewWindow` now records in the checkpoint dirty set whether the
row that named a key moved its anchor value, and `freezeWindowState` skips the
lookup for those keys and for the keys the predecessor root does not hold at all.
Nothing else moves: the elision itself, the bytes published and the
`isUnchanged` verdict per key are what they were, since a key whose anchor moved
never compared equal anyway.

### Protocol

`before` is the branch head `40bdedc5ce`, `after` is that head plus the change,
built into two jars from the same tree and run on the same machine, interleaved
run by run: one full sweep of the cells per jar, five sweeps in all, and three
more sweeps for the 10,000-key cells. Five runs per cell at 100,000 keys, twenty
at 10,000, `WARMUP=10` batches dropped as everywhere else.

Ratios against `6a2c656028` reuse the third measurement's baseline runs rather
than re-running them. What licenses that is the `before` column: it reproduces
the third measurement's candidate figure to within 0.5% in all four 100,000-key
cells (4.315 against 4.332, 4.350 against 4.329, 4.246 against 4.267, 4.245
against 4.237), so the machine has not moved under the pairing. It is still a
cross-session comparison and is marked as such.

### 100,000 live keys, five runs per side

Seal median in milliseconds, 1,000 keys imaged per seal.

| Shape | Fusion | Baseline | Before | After | Before/base | After/base |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| anchor-only-decimal | off | 3.883 | 4.315 | 4.171 | 1.111 | 1.074 |
| anchor-only-decimal | on | 3.939 | 4.350 | 4.146 | 1.104 | 1.053 |
| anchor-only-unfused-control | off | 3.846 | 4.246 | 4.011 | 1.104 | 1.043 |
| anchor-only-unfused-control | on | 3.853 | 4.245 | 4.037 | 1.102 | 1.048 |

The excess over baseline per 1,000 keys imaged falls from +0.39 to +0.43 ms to
+0.17 to +0.29 ms. Two of the four cells come under the flat 105% limit, one sits
at 1.053 and one at 1.074, so the lookup was between 40% and 60% of what the
regime cost - not all of it. What remains is unattributed here.

### 10,000 live keys, twenty runs per side

Every run of these cells is bimodal on all three revisions, at about 3.7 ms and
about 4.9 ms, and which mode a run lands in varies run to run (the low mode holds
3 of 15 baseline runs in one cell and 17 of 20 in another). A median across that
mixture reads the mode split rather than the seal, so the cells are compared mode
against mode, as the third measurement compared them.

| Shape | Fusion | Mode | Baseline | Before | After | Before/base | After/base |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| anchor-only-decimal | off | low | 3.665 | 3.916 | 3.675 | 1.068 | 1.003 |
| anchor-only-decimal | off | high | 4.760 | 5.054 | 4.836 | 1.062 | 1.016 |
| anchor-only-decimal | on | high | 4.719 | 5.081 | 4.891 | 1.077 | 1.037 |
| anchor-only-unfused-control | off | low | 3.533 | 3.774 | 3.626 | 1.068 | 1.026 |
| anchor-only-unfused-control | off | high | 4.675 | 4.968 | 4.730 | 1.063 | 1.012 |
| anchor-only-unfused-control | on | low | 3.532 | 3.749 | 3.673 | 1.062 | 1.040 |
| anchor-only-unfused-control | on | high | 4.672 | 4.982 | 4.755 | 1.066 | 1.018 |

The `anchor-only-decimal` fusion-on low mode is left out: one baseline run landed
in it. Every reading that is here passes 105% after the change and none did
before it. The medians of the whole mixture move the same way - 0.960, 0.957,
0.942 and 0.754 of `before` - but that last figure is a mode split moving, not a
25% seal, which is why the mixture medians are not the reading.

### The held-anchor regime is untouched

The steady `anchor-only-decimal` cell at 10,000 keys, where a key's anchor holds
across the seal and the elision is what makes that seal 0.31 to 0.34 of baseline:

| Fusion | Metric | Before | After | Ratio |
| --- | --- | ---: | ---: | ---: |
| off | seal median | 3.083 | 3.075 | 0.997 |
| off | refresh median | 5.996 | 5.984 | 0.998 |
| on | seal median | 3.074 | 3.087 | 1.004 |
| on | refresh median | 5.986 | 6.011 | 1.004 |

### Structural evidence

`win_probes` is the new capture-ledger column: predecessor entries the window
capture looked up. Across the `after` runs it reads a median of 0 per seal
against 1,000 keys imaged in every churn cell, and 1,000 against 1,000 in every
steady cell. That is the claim the timings above are attributed to, read directly
rather than inferred from a run with the lookup disabled - which is what the
third measurement listed as not measured.

Memory, allocation, published state bytes, metadata bytes and segment counts are
identical between `before` and `after` in every cell. The independent result
oracle matched in all 200 churn runs and no run recorded a refresh fault.

### Revised requirement, restated

Requirement 4 of the third measurement narrows to:

4. **The steady incremental seal of an anchored window with no inline
   component, in a batch where every imaged key's anchor value moved.** It may
   cost up to 0.3 ms more per 1,000 keys imaged, provided the same shape's seal
   under a held anchor stays under 50% of baseline. Measured: +0.17 to +0.29 ms
   per seal at 100,000 keys (1.043 to 1.074) in four cells; 1.003 to 1.040 mode
   against mode at 10,000 keys over 20 runs. The 0.5 ms and the 10,000-key
   readings of the third measurement's requirement 4 are superseded. The seal
   after a sweep, memory, allocation and storage remain unchanged, and every
   shape with an inline component remains under the 105% limit.

## Not measured

- The residual-heavy churn cell at 100,000 keys was run with two anchor buckets
  rather than six, so its seal after a sweep rests on one sweep per run over five
  runs, and its two one-shot readings just over the limit were not re-measured.
- Cross-mode comparisons are reported by the aggregator but are not substitutes
  for the paired ones and are not claimed as such.
- The fourth measurement covers the two anchor-only churn shapes and one steady
  cell. The other seven shapes were not re-run against the change: their seals
  hold a predecessor entry the lookup can elide, `win_probes` reads the imaged
  count for them, and the code the change adds is a branch they do not take.
  That is an argument from the mechanism and the ledger, not a measurement.
- What remains of the 100,000-key excess after the change - 1.043 to 1.074 of
  baseline - is not attributed to a mechanism.

Nothing here extrapolates to unmeasured supported queries.

## Appendix: every gate


```
| Shape | Fusion | Keys | Metric | Baseline | Candidate | Ratio | Limit | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| anchor-only-decimal | false | 10000 | seal_ms_median | 9.032 | 3.021 | 0.334 | 1.05 | pass |
| anchor-only-decimal | false | 10000 | seal_ms_p95 | 11.63 | 3.668 | 0.315 | 1.10 | pass |
| anchor-only-decimal | false | 10000 | refresh_ms_median | 12.03 | 5.957 | 0.495 | 1.05 | pass |
| anchor-only-decimal | false | 10000 | rows_per_sec_median | 8.316e+04 | 1.679e+05 | 2.019 | 0.95 | pass |
| anchor-only-decimal | false | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| anchor-only-decimal | false | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-decimal | false | 10000 | state_bytes_last | 8.456e+05 | 8.456e+05 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 10000 | meta_bytes_median | 8.026e+04 | 8.035e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-decimal | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 10000 | complete_seal_ms | 25.57 | 26.4 | 1.033 | 1.05 | pass |
| anchor-only-decimal | false | 10000 | restore_ms | 23.43 | 24.38 | 1.040 | 1.05 | pass |
| anchor-only-decimal | false | 10000 | first_reseal_ms | 14.72 | 6.752 | 0.459 | 1.05 | pass |
| anchor-only-decimal | false | 100000 | seal_ms_median | 10.97 | 3.606 | 0.329 | 1.05 | pass |
| anchor-only-decimal | false | 100000 | seal_ms_p95 | 12.38 | 3.967 | 0.320 | 1.10 | pass |
| anchor-only-decimal | false | 100000 | refresh_ms_median | 16.94 | 9.538 | 0.563 | 1.05 | pass |
| anchor-only-decimal | false | 100000 | rows_per_sec_median | 5.905e+04 | 1.048e+05 | 1.776 | 0.95 | pass |
| anchor-only-decimal | false | 100000 | refresh_peak_mb_median | 6.2 | 6.2 | 1.000 | 1.05 | pass |
| anchor-only-decimal | false | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-decimal | false | 100000 | state_bytes_last | 8.856e+06 | 8.856e+06 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 100000 | meta_bytes_median | 8.966e+04 | 8.974e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-decimal | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 100000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 100000 | complete_seal_ms | 180.7 | 189.6 | 1.049 | 1.05 | pass |
| anchor-only-decimal | false | 100000 | restore_ms | 69.48 | 74.93 | 1.078 | 1.05 | FAIL |
| anchor-only-decimal | false | 100000 | first_reseal_ms | 16.25 | 4.727 | 0.291 | 1.05 | pass |
| anchor-only-decimal | true | 10000 | seal_ms_median | 9.069 | 3.072 | 0.339 | 1.05 | pass |
| anchor-only-decimal | true | 10000 | seal_ms_p95 | 11.72 | 3.896 | 0.332 | 1.10 | pass |
| anchor-only-decimal | true | 10000 | refresh_ms_median | 12.05 | 5.999 | 0.498 | 1.05 | pass |
| anchor-only-decimal | true | 10000 | rows_per_sec_median | 8.299e+04 | 1.667e+05 | 2.009 | 0.95 | pass |
| anchor-only-decimal | true | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| anchor-only-decimal | true | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-decimal | true | 10000 | state_bytes_last | 8.456e+05 | 8.456e+05 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 10000 | meta_bytes_median | 8.026e+04 | 8.035e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-decimal | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 10000 | complete_seal_ms | 26.15 | 26.73 | 1.022 | 1.05 | pass |
| anchor-only-decimal | true | 10000 | restore_ms | 22.01 | 25.26 | 1.147 | 1.05 | FAIL |
| anchor-only-decimal | true | 10000 | first_reseal_ms | 13.25 | 6.771 | 0.511 | 1.05 | pass |
| anchor-only-decimal | true | 100000 | seal_ms_median | 11.47 | 3.599 | 0.314 | 1.05 | pass |
| anchor-only-decimal | true | 100000 | seal_ms_p95 | 12.22 | 3.89 | 0.318 | 1.10 | pass |
| anchor-only-decimal | true | 100000 | refresh_ms_median | 17.37 | 9.571 | 0.551 | 1.05 | pass |
| anchor-only-decimal | true | 100000 | rows_per_sec_median | 5.759e+04 | 1.045e+05 | 1.814 | 0.95 | pass |
| anchor-only-decimal | true | 100000 | refresh_peak_mb_median | 6.2 | 6.2 | 1.000 | 1.05 | pass |
| anchor-only-decimal | true | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-decimal | true | 100000 | state_bytes_last | 8.856e+06 | 8.856e+06 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 100000 | meta_bytes_median | 8.966e+04 | 8.974e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-decimal | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 100000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 100000 | complete_seal_ms | 182 | 182.7 | 1.004 | 1.05 | pass |
| anchor-only-decimal | true | 100000 | restore_ms | 69.44 | 73.7 | 1.061 | 1.05 | FAIL |
| anchor-only-decimal | true | 100000 | first_reseal_ms | 15.55 | 4.723 | 0.304 | 1.05 | pass |
| anchor-only-unfused-control | false | 10000 | seal_ms_median | 8.974 | 2.818 | 0.314 | 1.05 | pass |
| anchor-only-unfused-control | false | 10000 | seal_ms_p95 | 11.26 | 3.569 | 0.317 | 1.10 | pass |
| anchor-only-unfused-control | false | 10000 | refresh_ms_median | 11.9 | 5.675 | 0.477 | 1.05 | pass |
| anchor-only-unfused-control | false | 10000 | rows_per_sec_median | 8.401e+04 | 1.762e+05 | 2.097 | 0.95 | pass |
| anchor-only-unfused-control | false | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | false | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | false | 10000 | state_bytes_last | 6.756e+05 | 6.756e+05 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 10000 | meta_bytes_median | 6.274e+04 | 6.283e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 10000 | complete_seal_ms | 24.33 | 27.62 | 1.135 | 1.05 | FAIL |
| anchor-only-unfused-control | false | 10000 | restore_ms | 21.76 | 23.56 | 1.083 | 1.05 | FAIL |
| anchor-only-unfused-control | false | 10000 | first_reseal_ms | 13.35 | 6.259 | 0.469 | 1.05 | pass |
| anchor-only-unfused-control | false | 100000 | seal_ms_median | 10.93 | 3.419 | 0.313 | 1.05 | pass |
| anchor-only-unfused-control | false | 100000 | seal_ms_p95 | 13.38 | 3.842 | 0.287 | 1.10 | pass |
| anchor-only-unfused-control | false | 100000 | refresh_ms_median | 16.95 | 9.195 | 0.543 | 1.05 | pass |
| anchor-only-unfused-control | false | 100000 | rows_per_sec_median | 5.901e+04 | 1.088e+05 | 1.843 | 0.95 | pass |
| anchor-only-unfused-control | false | 100000 | refresh_peak_mb_median | 6.2 | 6.2 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | false | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | false | 100000 | state_bytes_last | 7.156e+06 | 7.156e+06 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 100000 | meta_bytes_median | 7.22e+04 | 7.229e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 100000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 100000 | complete_seal_ms | 170.3 | 177.3 | 1.041 | 1.05 | pass |
| anchor-only-unfused-control | false | 100000 | restore_ms | 65.75 | 69.2 | 1.052 | 1.05 | FAIL |
| anchor-only-unfused-control | false | 100000 | first_reseal_ms | 15.52 | 4.499 | 0.290 | 1.05 | pass |
| anchor-only-unfused-control | true | 10000 | seal_ms_median | 9.175 | 2.829 | 0.308 | 1.05 | pass |
| anchor-only-unfused-control | true | 10000 | seal_ms_p95 | 11.38 | 3.736 | 0.328 | 1.10 | pass |
| anchor-only-unfused-control | true | 10000 | refresh_ms_median | 12.09 | 5.674 | 0.469 | 1.05 | pass |
| anchor-only-unfused-control | true | 10000 | rows_per_sec_median | 8.268e+04 | 1.763e+05 | 2.132 | 0.95 | pass |
| anchor-only-unfused-control | true | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | true | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | true | 10000 | state_bytes_last | 6.756e+05 | 6.756e+05 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 10000 | meta_bytes_median | 6.274e+04 | 6.283e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 10000 | complete_seal_ms | 25.29 | 26.38 | 1.043 | 1.05 | pass |
| anchor-only-unfused-control | true | 10000 | restore_ms | 22.48 | 24.18 | 1.076 | 1.05 | FAIL |
| anchor-only-unfused-control | true | 10000 | first_reseal_ms | 13.66 | 6.467 | 0.473 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | seal_ms_median | 11.14 | 3.436 | 0.309 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | seal_ms_p95 | 12.39 | 3.711 | 0.299 | 1.10 | pass |
| anchor-only-unfused-control | true | 100000 | refresh_ms_median | 16.91 | 9.24 | 0.546 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | rows_per_sec_median | 5.912e+04 | 1.082e+05 | 1.831 | 0.95 | pass |
| anchor-only-unfused-control | true | 100000 | refresh_peak_mb_median | 6.2 | 6.2 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | state_bytes_last | 7.156e+06 | 7.156e+06 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 100000 | meta_bytes_median | 7.22e+04 | 7.229e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 100000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 100000 | complete_seal_ms | 171.1 | 174.9 | 1.022 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | restore_ms | 66.76 | 67.99 | 1.018 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | first_reseal_ms | 15.59 | 4.54 | 0.291 | 1.05 | pass |
| narrow-count-star-key | false | 10000 | seal_ms_median | 8.518 | 2.022 | 0.237 | 1.05 | pass |
| narrow-count-star-key | false | 10000 | seal_ms_p95 | 9.439 | 2.654 | 0.281 | 1.10 | pass |
| narrow-count-star-key | false | 10000 | refresh_ms_median | 11.35 | 4.819 | 0.424 | 1.05 | pass |
| narrow-count-star-key | false | 10000 | rows_per_sec_median | 8.808e+04 | 2.075e+05 | 2.356 | 0.95 | pass |
| narrow-count-star-key | false | 10000 | refresh_peak_mb_median | 2.4 | 2.3 | 0.958 | 1.05 | pass |
| narrow-count-star-key | false | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-count-star-key | false | 10000 | state_bytes_last | 8.487e+05 | 3.589e+05 | 0.423 | 1.00 | pass |
| narrow-count-star-key | false | 10000 | meta_bytes_median | 1.027e+05 | 6.3e+04 | 0.613 | 1.00 | pass |
| narrow-count-star-key | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key | false | 10000 | meta_segs_total | 600 | 400 | 0.667 | 1.00 | pass |
| narrow-count-star-key | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key | false | 10000 | complete_seal_ms | 32.05 | 17.14 | 0.535 | 1.05 | pass |
| narrow-count-star-key | false | 10000 | restore_ms | 26.77 | 21.54 | 0.804 | 1.05 | pass |
| narrow-count-star-key | false | 10000 | first_reseal_ms | 14.94 | 5.209 | 0.349 | 1.05 | pass |
| narrow-count-star-key | false | 100000 | seal_ms_median | 13.91 | 2.272 | 0.163 | 1.05 | pass |
| narrow-count-star-key | false | 100000 | seal_ms_p95 | 14.78 | 2.625 | 0.178 | 1.10 | pass |
| narrow-count-star-key | false | 100000 | refresh_ms_median | 19.7 | 8.03 | 0.408 | 1.05 | pass |
| narrow-count-star-key | false | 100000 | rows_per_sec_median | 5.077e+04 | 1.245e+05 | 2.453 | 0.95 | pass |
| narrow-count-star-key | false | 100000 | refresh_peak_mb_median | 6.3 | 6.3 | 1.000 | 1.05 | pass |
| narrow-count-star-key | false | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-count-star-key | false | 100000 | state_bytes_last | 9.057e+06 | 3.779e+06 | 0.417 | 1.00 | pass |
| narrow-count-star-key | false | 100000 | meta_bytes_median | 1.217e+05 | 7.319e+04 | 0.602 | 1.00 | pass |
| narrow-count-star-key | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key | false | 100000 | meta_segs_total | 600 | 400 | 0.667 | 1.00 | pass |
| narrow-count-star-key | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key | false | 100000 | complete_seal_ms | 228.4 | 82.77 | 0.362 | 1.05 | pass |
| narrow-count-star-key | false | 100000 | restore_ms | 88.04 | 54.8 | 0.622 | 1.05 | pass |
| narrow-count-star-key | false | 100000 | first_reseal_ms | 17.21 | 3.488 | 0.203 | 1.05 | pass |
| narrow-count-star-key | true | 10000 | seal_ms_median | 2.025 | 2.03 | 1.002 | 1.05 | pass |
| narrow-count-star-key | true | 10000 | seal_ms_p95 | 2.596 | 2.557 | 0.985 | 1.10 | pass |
| narrow-count-star-key | true | 10000 | refresh_ms_median | 4.598 | 4.514 | 0.982 | 1.05 | pass |
| narrow-count-star-key | true | 10000 | rows_per_sec_median | 2.175e+05 | 2.216e+05 | 1.019 | 0.95 | pass |
| narrow-count-star-key | true | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| narrow-count-star-key | true | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-count-star-key | true | 10000 | state_bytes_last | 3.589e+05 | 3.589e+05 | 1.000 | 1.00 | pass |
| narrow-count-star-key | true | 10000 | meta_bytes_median | 6.3e+04 | 6.3e+04 | 1.000 | 1.00 | pass |
| narrow-count-star-key | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key | true | 10000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| narrow-count-star-key | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key | true | 10000 | complete_seal_ms | 14.08 | 14.37 | 1.021 | 1.05 | pass |
| narrow-count-star-key | true | 10000 | restore_ms | 19.37 | 19.89 | 1.027 | 1.05 | pass |
| narrow-count-star-key | true | 10000 | first_reseal_ms | 4.705 | 4.745 | 1.009 | 1.05 | pass |
| narrow-count-star-key | true | 100000 | seal_ms_median | 2.24 | 2.173 | 0.970 | 1.05 | pass |
| narrow-count-star-key | true | 100000 | seal_ms_p95 | 2.56 | 2.461 | 0.961 | 1.10 | pass |
| narrow-count-star-key | true | 100000 | refresh_ms_median | 7.71 | 7.457 | 0.967 | 1.05 | pass |
| narrow-count-star-key | true | 100000 | rows_per_sec_median | 1.297e+05 | 1.341e+05 | 1.034 | 0.95 | pass |
| narrow-count-star-key | true | 100000 | refresh_peak_mb_median | 6.3 | 6 | 0.952 | 1.05 | pass |
| narrow-count-star-key | true | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-count-star-key | true | 100000 | state_bytes_last | 3.779e+06 | 3.779e+06 | 1.000 | 1.00 | pass |
| narrow-count-star-key | true | 100000 | meta_bytes_median | 7.319e+04 | 7.319e+04 | 1.000 | 1.00 | pass |
| narrow-count-star-key | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key | true | 100000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| narrow-count-star-key | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key | true | 100000 | complete_seal_ms | 77.33 | 76.91 | 0.995 | 1.05 | pass |
| narrow-count-star-key | true | 100000 | restore_ms | 42.14 | 42.1 | 0.999 | 1.05 | pass |
| narrow-count-star-key | true | 100000 | first_reseal_ms | 3.529 | 3.52 | 0.997 | 1.05 | pass |
| narrow-sum | false | 10000 | seal_ms_median | 8.918 | 2.154 | 0.242 | 1.05 | pass |
| narrow-sum | false | 10000 | seal_ms_p95 | 11.34 | 2.725 | 0.240 | 1.10 | pass |
| narrow-sum | false | 10000 | refresh_ms_median | 11.85 | 4.787 | 0.404 | 1.05 | pass |
| narrow-sum | false | 10000 | rows_per_sec_median | 8.44e+04 | 2.089e+05 | 2.475 | 0.95 | pass |
| narrow-sum | false | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| narrow-sum | false | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-sum | false | 10000 | state_bytes_last | 6.756e+05 | 4.578e+05 | 0.678 | 1.00 | pass |
| narrow-sum | false | 10000 | meta_bytes_median | 6.274e+04 | 6.949e+04 | 1.107 | 1.00 | FAIL |
| narrow-sum | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | false | 10000 | meta_segs_total | 500 | 400 | 0.800 | 1.00 | pass |
| narrow-sum | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | false | 10000 | complete_seal_ms | 24.56 | 17.18 | 0.699 | 1.05 | pass |
| narrow-sum | false | 10000 | restore_ms | 21.65 | 22.72 | 1.050 | 1.05 | pass |
| narrow-sum | false | 10000 | first_reseal_ms | 13.34 | 4.797 | 0.360 | 1.05 | pass |
| narrow-sum | false | 100000 | seal_ms_median | 11.16 | 2.383 | 0.213 | 1.05 | pass |
| narrow-sum | false | 100000 | seal_ms_p95 | 13.88 | 2.834 | 0.204 | 1.10 | pass |
| narrow-sum | false | 100000 | refresh_ms_median | 16.9 | 8.303 | 0.491 | 1.05 | pass |
| narrow-sum | false | 100000 | rows_per_sec_median | 5.916e+04 | 1.204e+05 | 2.036 | 0.95 | pass |
| narrow-sum | false | 100000 | refresh_peak_mb_median | 6.2 | 6.2 | 1.000 | 1.05 | pass |
| narrow-sum | false | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-sum | false | 100000 | state_bytes_last | 7.156e+06 | 4.778e+06 | 0.668 | 1.00 | pass |
| narrow-sum | false | 100000 | meta_bytes_median | 7.22e+04 | 7.756e+04 | 1.074 | 1.00 | FAIL |
| narrow-sum | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | false | 100000 | meta_segs_total | 500 | 400 | 0.800 | 1.00 | pass |
| narrow-sum | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | false | 100000 | complete_seal_ms | 167.8 | 93.31 | 0.556 | 1.05 | pass |
| narrow-sum | false | 100000 | restore_ms | 66.68 | 51.41 | 0.771 | 1.05 | pass |
| narrow-sum | false | 100000 | first_reseal_ms | 14.85 | 3.681 | 0.248 | 1.05 | pass |
| narrow-sum | true | 10000 | seal_ms_median | 2.095 | 2.056 | 0.981 | 1.05 | pass |
| narrow-sum | true | 10000 | seal_ms_p95 | 2.608 | 2.64 | 1.012 | 1.10 | pass |
| narrow-sum | true | 10000 | refresh_ms_median | 4.753 | 4.614 | 0.971 | 1.05 | pass |
| narrow-sum | true | 10000 | rows_per_sec_median | 2.104e+05 | 2.167e+05 | 1.030 | 0.95 | pass |
| narrow-sum | true | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| narrow-sum | true | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-sum | true | 10000 | state_bytes_last | 4.578e+05 | 4.578e+05 | 1.000 | 1.00 | pass |
| narrow-sum | true | 10000 | meta_bytes_median | 6.949e+04 | 6.949e+04 | 1.000 | 1.00 | pass |
| narrow-sum | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | true | 10000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| narrow-sum | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | true | 10000 | complete_seal_ms | 14.97 | 15.48 | 1.034 | 1.05 | pass |
| narrow-sum | true | 10000 | restore_ms | 20.13 | 19.21 | 0.954 | 1.05 | pass |
| narrow-sum | true | 10000 | first_reseal_ms | 5.178 | 4.632 | 0.895 | 1.05 | pass |
| narrow-sum | true | 100000 | seal_ms_median | 2.305 | 2.301 | 0.998 | 1.05 | pass |
| narrow-sum | true | 100000 | seal_ms_p95 | 2.723 | 2.593 | 0.952 | 1.10 | pass |
| narrow-sum | true | 100000 | refresh_ms_median | 8.072 | 7.888 | 0.977 | 1.05 | pass |
| narrow-sum | true | 100000 | rows_per_sec_median | 1.239e+05 | 1.268e+05 | 1.023 | 0.95 | pass |
| narrow-sum | true | 100000 | refresh_peak_mb_median | 6.2 | 6.2 | 1.000 | 1.05 | pass |
| narrow-sum | true | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-sum | true | 100000 | state_bytes_last | 4.778e+06 | 4.778e+06 | 1.000 | 1.00 | pass |
| narrow-sum | true | 100000 | meta_bytes_median | 7.756e+04 | 7.756e+04 | 1.000 | 1.00 | pass |
| narrow-sum | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | true | 100000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| narrow-sum | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | true | 100000 | complete_seal_ms | 90.34 | 90.3 | 1.000 | 1.05 | pass |
| narrow-sum | true | 100000 | restore_ms | 45.06 | 43.29 | 0.961 | 1.05 | pass |
| narrow-sum | true | 100000 | first_reseal_ms | 3.482 | 3.595 | 1.032 | 1.05 | pass |
| narrow-sum-avg-count | false | 10000 | seal_ms_median | 11.34 | 2.124 | 0.187 | 1.05 | pass |
| narrow-sum-avg-count | false | 10000 | seal_ms_p95 | 13.85 | 2.562 | 0.185 | 1.10 | pass |
| narrow-sum-avg-count | false | 10000 | refresh_ms_median | 14.58 | 5.152 | 0.353 | 1.05 | pass |
| narrow-sum-avg-count | false | 10000 | rows_per_sec_median | 6.858e+04 | 1.941e+05 | 2.830 | 0.95 | pass |
| narrow-sum-avg-count | false | 10000 | refresh_peak_mb_median | 2.6 | 2.6 | 1.000 | 1.05 | pass |
| narrow-sum-avg-count | false | 10000 | alloc_mb_median | 0.08 | 0.07 | 0.875 | 1.05 | pass |
| narrow-sum-avg-count | false | 10000 | state_bytes_last | 1.351e+06 | 4.578e+05 | 0.339 | 1.00 | pass |
| narrow-sum-avg-count | false | 10000 | meta_bytes_median | 1.676e+05 | 6.949e+04 | 0.415 | 1.00 | pass |
| narrow-sum-avg-count | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | false | 10000 | meta_segs_total | 700 | 400 | 0.571 | 1.00 | pass |
| narrow-sum-avg-count | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | false | 10000 | complete_seal_ms | 44.67 | 15.7 | 0.351 | 1.05 | pass |
| narrow-sum-avg-count | false | 10000 | restore_ms | 32.73 | 22.76 | 0.695 | 1.05 | pass |
| narrow-sum-avg-count | false | 10000 | first_reseal_ms | 19.39 | 4.598 | 0.237 | 1.05 | pass |
| narrow-sum-avg-count | false | 100000 | seal_ms_median | 14.65 | 2.409 | 0.164 | 1.05 | pass |
| narrow-sum-avg-count | false | 100000 | seal_ms_p95 | 15.47 | 2.683 | 0.173 | 1.10 | pass |
| narrow-sum-avg-count | false | 100000 | refresh_ms_median | 20.97 | 8.611 | 0.411 | 1.05 | pass |
| narrow-sum-avg-count | false | 100000 | rows_per_sec_median | 4.769e+04 | 1.161e+05 | 2.435 | 0.95 | pass |
| narrow-sum-avg-count | false | 100000 | refresh_peak_mb_median | 6.3 | 6.3 | 1.000 | 1.05 | pass |
| narrow-sum-avg-count | false | 100000 | alloc_mb_median | 0.08 | 0.07 | 0.875 | 1.05 | pass |
| narrow-sum-avg-count | false | 100000 | state_bytes_last | 1.431e+07 | 4.778e+06 | 0.334 | 1.00 | pass |
| narrow-sum-avg-count | false | 100000 | meta_bytes_median | 1.909e+05 | 7.756e+04 | 0.406 | 1.00 | pass |
| narrow-sum-avg-count | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | false | 100000 | meta_segs_total | 700 | 400 | 0.571 | 1.00 | pass |
| narrow-sum-avg-count | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | false | 100000 | complete_seal_ms | 359.7 | 90.37 | 0.251 | 1.05 | pass |
| narrow-sum-avg-count | false | 100000 | restore_ms | 135.9 | 67.32 | 0.496 | 1.05 | pass |
| narrow-sum-avg-count | false | 100000 | first_reseal_ms | 18.79 | 3.568 | 0.190 | 1.05 | pass |
| narrow-sum-avg-count | true | 10000 | seal_ms_median | 2.078 | 2.049 | 0.986 | 1.05 | pass |
| narrow-sum-avg-count | true | 10000 | seal_ms_p95 | 2.809 | 2.773 | 0.987 | 1.10 | pass |
| narrow-sum-avg-count | true | 10000 | refresh_ms_median | 4.821 | 4.802 | 0.996 | 1.05 | pass |
| narrow-sum-avg-count | true | 10000 | rows_per_sec_median | 2.074e+05 | 2.082e+05 | 1.004 | 0.95 | pass |
| narrow-sum-avg-count | true | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| narrow-sum-avg-count | true | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-sum-avg-count | true | 10000 | state_bytes_last | 4.578e+05 | 4.578e+05 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | true | 10000 | meta_bytes_median | 6.949e+04 | 6.949e+04 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | true | 10000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | true | 10000 | complete_seal_ms | 15.54 | 15.61 | 1.004 | 1.05 | pass |
| narrow-sum-avg-count | true | 10000 | restore_ms | 21.87 | 20.21 | 0.924 | 1.05 | pass |
| narrow-sum-avg-count | true | 10000 | first_reseal_ms | 4.514 | 4.522 | 1.002 | 1.05 | pass |
| narrow-sum-avg-count | true | 100000 | seal_ms_median | 2.306 | 2.263 | 0.981 | 1.05 | pass |
| narrow-sum-avg-count | true | 100000 | seal_ms_p95 | 2.596 | 2.519 | 0.970 | 1.10 | pass |
| narrow-sum-avg-count | true | 100000 | refresh_ms_median | 8.079 | 7.806 | 0.966 | 1.05 | pass |
| narrow-sum-avg-count | true | 100000 | rows_per_sec_median | 1.238e+05 | 1.281e+05 | 1.035 | 0.95 | pass |
| narrow-sum-avg-count | true | 100000 | refresh_peak_mb_median | 6.3 | 6.3 | 1.000 | 1.05 | pass |
| narrow-sum-avg-count | true | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-sum-avg-count | true | 100000 | state_bytes_last | 4.778e+06 | 4.778e+06 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | true | 100000 | meta_bytes_median | 7.756e+04 | 7.756e+04 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | true | 100000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count | true | 100000 | complete_seal_ms | 89.25 | 88.55 | 0.992 | 1.05 | pass |
| narrow-sum-avg-count | true | 100000 | restore_ms | 46.22 | 44.3 | 0.958 | 1.05 | pass |
| narrow-sum-avg-count | true | 100000 | first_reseal_ms | 3.577 | 3.52 | 0.984 | 1.05 | pass |
| residual-heavy | false | 10000 | seal_ms_median | 65.55 | 59.78 | 0.912 | 1.05 | pass |
| residual-heavy | false | 10000 | seal_ms_p95 | 69.27 | 62.9 | 0.908 | 1.10 | pass |
| residual-heavy | false | 10000 | refresh_ms_median | 69.27 | 63.3 | 0.914 | 1.05 | pass |
| residual-heavy | false | 10000 | rows_per_sec_median | 1.444e+04 | 1.58e+04 | 1.094 | 0.95 | pass |
| residual-heavy | false | 10000 | refresh_peak_mb_median | 2.6 | 2.6 | 1.000 | 1.05 | pass |
| residual-heavy | false | 10000 | alloc_mb_median | 5.58 | 5.58 | 1.000 | 1.05 | pass |
| residual-heavy | false | 10000 | state_bytes_last | 8.459e+06 | 8.241e+06 | 0.974 | 1.00 | pass |
| residual-heavy | false | 10000 | meta_bytes_median | 1.778e+06 | 1.784e+06 | 1.003 | 1.00 | FAIL |
| residual-heavy | false | 10000 | data_bytes_median | 1.059e+06 | 1.059e+06 | 1.000 | 1.00 | pass |
| residual-heavy | false | 10000 | meta_segs_total | 800 | 700 | 0.875 | 1.00 | pass |
| residual-heavy | false | 10000 | data_segs_total | 100 | 100 | 1.000 | 1.00 | pass |
| residual-heavy | false | 10000 | complete_seal_ms | 64.76 | 59.68 | 0.922 | 1.05 | pass |
| residual-heavy | false | 10000 | restore_ms | 68.03 | 60.15 | 0.884 | 1.05 | pass |
| residual-heavy | false | 10000 | first_reseal_ms | 80.31 | 73.8 | 0.919 | 1.05 | pass |
| residual-heavy | false | 100000 | seal_ms_median | 387.5 | 372.1 | 0.960 | 1.05 | pass |
| residual-heavy | false | 100000 | seal_ms_p95 | 405.1 | 390.9 | 0.965 | 1.10 | pass |
| residual-heavy | false | 100000 | refresh_ms_median | 395 | 379.6 | 0.961 | 1.05 | pass |
| residual-heavy | false | 100000 | rows_per_sec_median | 2532 | 2634 | 1.040 | 0.95 | pass |
| residual-heavy | false | 100000 | refresh_peak_mb_median | 6.4 | 6.4 | 1.000 | 1.05 | pass |
| residual-heavy | false | 100000 | alloc_mb_median | 55.04 | 55.04 | 1.000 | 1.05 | pass |
| residual-heavy | false | 100000 | state_bytes_last | 7.599e+07 | 7.361e+07 | 0.969 | 1.00 | pass |
| residual-heavy | false | 100000 | meta_bytes_median | 1.604e+07 | 1.605e+07 | 1.000 | 1.00 | FAIL |
| residual-heavy | false | 100000 | data_bytes_median | 2.128e+06 | 2.128e+06 | 1.000 | 1.00 | pass |
| residual-heavy | false | 100000 | meta_segs_total | 800 | 700 | 0.875 | 1.00 | pass |
| residual-heavy | false | 100000 | data_segs_total | 100 | 100 | 1.000 | 1.00 | pass |
| residual-heavy | false | 100000 | complete_seal_ms | 630.2 | 575.7 | 0.914 | 1.05 | pass |
| residual-heavy | false | 100000 | restore_ms | 285.1 | 257.3 | 0.903 | 1.05 | pass |
| residual-heavy | false | 100000 | first_reseal_ms | 432.7 | 408.7 | 0.945 | 1.05 | pass |
| residual-heavy | true | 10000 | seal_ms_median | 59.67 | 60.08 | 1.007 | 1.05 | pass |
| residual-heavy | true | 10000 | seal_ms_p95 | 62.99 | 62.74 | 0.996 | 1.10 | pass |
| residual-heavy | true | 10000 | refresh_ms_median | 62.99 | 63.35 | 1.006 | 1.05 | pass |
| residual-heavy | true | 10000 | rows_per_sec_median | 1.587e+04 | 1.578e+04 | 0.994 | 0.95 | pass |
| residual-heavy | true | 10000 | refresh_peak_mb_median | 2.6 | 2.6 | 1.000 | 1.05 | pass |
| residual-heavy | true | 10000 | alloc_mb_median | 5.58 | 5.58 | 1.000 | 1.05 | pass |
| residual-heavy | true | 10000 | state_bytes_last | 8.241e+06 | 8.241e+06 | 1.000 | 1.00 | pass |
| residual-heavy | true | 10000 | meta_bytes_median | 1.784e+06 | 1.784e+06 | 1.000 | 1.00 | pass |
| residual-heavy | true | 10000 | data_bytes_median | 1.059e+06 | 1.059e+06 | 1.000 | 1.00 | pass |
| residual-heavy | true | 10000 | meta_segs_total | 700 | 700 | 1.000 | 1.00 | pass |
| residual-heavy | true | 10000 | data_segs_total | 100 | 100 | 1.000 | 1.00 | pass |
| residual-heavy | true | 10000 | complete_seal_ms | 56.86 | 58.88 | 1.036 | 1.05 | pass |
| residual-heavy | true | 10000 | restore_ms | 60.32 | 60.71 | 1.007 | 1.05 | pass |
| residual-heavy | true | 10000 | first_reseal_ms | 75.89 | 75.52 | 0.995 | 1.05 | pass |
| residual-heavy | true | 100000 | seal_ms_median | 376.8 | 373.4 | 0.991 | 1.05 | pass |
| residual-heavy | true | 100000 | seal_ms_p95 | 390 | 387.8 | 0.994 | 1.10 | pass |
| residual-heavy | true | 100000 | refresh_ms_median | 383.7 | 380.4 | 0.991 | 1.05 | pass |
| residual-heavy | true | 100000 | rows_per_sec_median | 2606 | 2629 | 1.009 | 0.95 | pass |
| residual-heavy | true | 100000 | refresh_peak_mb_median | 6.4 | 6.4 | 1.000 | 1.05 | pass |
| residual-heavy | true | 100000 | alloc_mb_median | 55.04 | 55.04 | 1.000 | 1.05 | pass |
| residual-heavy | true | 100000 | state_bytes_last | 7.361e+07 | 7.361e+07 | 1.000 | 1.00 | pass |
| residual-heavy | true | 100000 | meta_bytes_median | 1.605e+07 | 1.605e+07 | 1.000 | 1.00 | pass |
| residual-heavy | true | 100000 | data_bytes_median | 2.128e+06 | 2.128e+06 | 1.000 | 1.00 | pass |
| residual-heavy | true | 100000 | meta_segs_total | 700 | 700 | 1.000 | 1.00 | pass |
| residual-heavy | true | 100000 | data_segs_total | 100 | 100 | 1.000 | 1.00 | pass |
| residual-heavy | true | 100000 | complete_seal_ms | 594.7 | 581.6 | 0.978 | 1.05 | pass |
| residual-heavy | true | 100000 | restore_ms | 251.4 | 248 | 0.987 | 1.05 | pass |
| residual-heavy | true | 100000 | first_reseal_ms | 409 | 407.2 | 0.995 | 1.05 | pass |
| wide-above-budget | false | 10000 | seal_ms_median | 26.78 | 6.081 | 0.227 | 1.05 | pass |
| wide-above-budget | false | 10000 | seal_ms_p95 | 29.78 | 6.987 | 0.235 | 1.10 | pass |
| wide-above-budget | false | 10000 | refresh_ms_median | 33.05 | 12.02 | 0.364 | 1.05 | pass |
| wide-above-budget | false | 10000 | rows_per_sec_median | 3.026e+04 | 8.318e+04 | 2.749 | 0.95 | pass |
| wide-above-budget | false | 10000 | refresh_peak_mb_median | 3.4 | 3.4 | 1.000 | 1.05 | pass |
| wide-above-budget | false | 10000 | alloc_mb_median | 0.12 | 0.1 | 0.833 | 1.05 | pass |
| wide-above-budget | false | 10000 | state_bytes_last | 6.342e+06 | 3.076e+06 | 0.485 | 1.00 | pass |
| wide-above-budget | false | 10000 | meta_bytes_median | 9.075e+05 | 3.555e+05 | 0.392 | 1.00 | pass |
| wide-above-budget | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget | false | 10000 | meta_segs_total | 2000 | 500 | 0.250 | 1.00 | pass |
| wide-above-budget | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget | false | 10000 | complete_seal_ms | 130.3 | 44.61 | 0.342 | 1.05 | pass |
| wide-above-budget | false | 10000 | restore_ms | 81.31 | 48.07 | 0.591 | 1.05 | pass |
| wide-above-budget | false | 10000 | first_reseal_ms | 47.94 | 10.63 | 0.222 | 1.05 | pass |
| wide-above-budget | false | 100000 | seal_ms_median | 38.9 | 7.535 | 0.194 | 1.05 | pass |
| wide-above-budget | false | 100000 | seal_ms_p95 | 40.41 | 8.196 | 0.203 | 1.10 | pass |
| wide-above-budget | false | 100000 | refresh_ms_median | 48.74 | 17.33 | 0.356 | 1.05 | pass |
| wide-above-budget | false | 100000 | rows_per_sec_median | 2.052e+04 | 5.77e+04 | 2.812 | 0.95 | pass |
| wide-above-budget | false | 100000 | refresh_peak_mb_median | 7.1 | 7.1 | 1.000 | 1.05 | pass |
| wide-above-budget | false | 100000 | alloc_mb_median | 0.11 | 0.1 | 0.909 | 1.05 | pass |
| wide-above-budget | false | 100000 | state_bytes_last | 6.682e+07 | 3.116e+07 | 0.466 | 1.00 | pass |
| wide-above-budget | false | 100000 | meta_bytes_median | 1.022e+06 | 3.721e+05 | 0.364 | 1.00 | pass |
| wide-above-budget | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget | false | 100000 | meta_segs_total | 2000 | 500 | 0.250 | 1.00 | pass |
| wide-above-budget | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget | false | 100000 | complete_seal_ms | 1414 | 563.8 | 0.399 | 1.05 | pass |
| wide-above-budget | false | 100000 | restore_ms | 473.4 | 236.8 | 0.500 | 1.05 | pass |
| wide-above-budget | false | 100000 | first_reseal_ms | 48.72 | 8.707 | 0.179 | 1.05 | pass |
| wide-above-budget | true | 10000 | seal_ms_median | 4.873 | 4.851 | 0.996 | 1.05 | pass |
| wide-above-budget | true | 10000 | seal_ms_p95 | 5.586 | 5.504 | 0.985 | 1.10 | pass |
| wide-above-budget | true | 10000 | refresh_ms_median | 8.3 | 8.243 | 0.993 | 1.05 | pass |
| wide-above-budget | true | 10000 | rows_per_sec_median | 1.205e+05 | 1.213e+05 | 1.007 | 0.95 | pass |
| wide-above-budget | true | 10000 | refresh_peak_mb_median | 3.4 | 3.4 | 1.000 | 1.05 | pass |
| wide-above-budget | true | 10000 | alloc_mb_median | 0.1 | 0.1 | 1.000 | 1.05 | pass |
| wide-above-budget | true | 10000 | state_bytes_last | 3.076e+06 | 3.076e+06 | 1.000 | 1.00 | pass |
| wide-above-budget | true | 10000 | meta_bytes_median | 3.555e+05 | 3.555e+05 | 1.000 | 1.00 | pass |
| wide-above-budget | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget | true | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| wide-above-budget | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget | true | 10000 | complete_seal_ms | 35.78 | 34.86 | 0.974 | 1.05 | pass |
| wide-above-budget | true | 10000 | restore_ms | 36.29 | 34.34 | 0.946 | 1.05 | pass |
| wide-above-budget | true | 10000 | first_reseal_ms | 9.257 | 9.167 | 0.990 | 1.05 | pass |
| wide-above-budget | true | 100000 | seal_ms_median | 5.663 | 5.648 | 0.997 | 1.05 | pass |
| wide-above-budget | true | 100000 | seal_ms_p95 | 6.069 | 6.036 | 0.995 | 1.10 | pass |
| wide-above-budget | true | 100000 | refresh_ms_median | 11.97 | 12 | 1.002 | 1.05 | pass |
| wide-above-budget | true | 100000 | rows_per_sec_median | 8.351e+04 | 8.336e+04 | 0.998 | 0.95 | pass |
| wide-above-budget | true | 100000 | refresh_peak_mb_median | 7.1 | 7.1 | 1.000 | 1.05 | pass |
| wide-above-budget | true | 100000 | alloc_mb_median | 0.1 | 0.1 | 1.000 | 1.05 | pass |
| wide-above-budget | true | 100000 | state_bytes_last | 3.116e+07 | 3.116e+07 | 1.000 | 1.00 | pass |
| wide-above-budget | true | 100000 | meta_bytes_median | 3.721e+05 | 3.721e+05 | 1.000 | 1.00 | pass |
| wide-above-budget | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget | true | 100000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| wide-above-budget | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget | true | 100000 | complete_seal_ms | 349.9 | 347.7 | 0.994 | 1.05 | pass |
| wide-above-budget | true | 100000 | restore_ms | 123.6 | 122.5 | 0.992 | 1.05 | pass |
| wide-above-budget | true | 100000 | first_reseal_ms | 7.042 | 6.649 | 0.944 | 1.05 | pass |
| wide-at-budget | false | 10000 | seal_ms_median | 26.64 | 4.573 | 0.172 | 1.05 | pass |
| wide-at-budget | false | 10000 | seal_ms_p95 | 30.93 | 5.36 | 0.173 | 1.10 | pass |
| wide-at-budget | false | 10000 | refresh_ms_median | 32.9 | 10.55 | 0.321 | 1.05 | pass |
| wide-at-budget | false | 10000 | rows_per_sec_median | 3.04e+04 | 9.478e+04 | 3.118 | 0.95 | pass |
| wide-at-budget | false | 10000 | refresh_peak_mb_median | 3.4 | 3.4 | 1.000 | 1.05 | pass |
| wide-at-budget | false | 10000 | alloc_mb_median | 0.12 | 0.1 | 0.833 | 1.05 | pass |
| wide-at-budget | false | 10000 | state_bytes_last | 6.262e+06 | 2.778e+06 | 0.444 | 1.00 | pass |
| wide-at-budget | false | 10000 | meta_bytes_median | 8.993e+05 | 3.078e+05 | 0.342 | 1.00 | pass |
| wide-at-budget | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget | false | 10000 | meta_segs_total | 2000 | 400 | 0.200 | 1.00 | pass |
| wide-at-budget | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget | false | 10000 | complete_seal_ms | 128.1 | 33.62 | 0.262 | 1.05 | pass |
| wide-at-budget | false | 10000 | restore_ms | 92.48 | 43.91 | 0.475 | 1.05 | pass |
| wide-at-budget | false | 10000 | first_reseal_ms | 47.22 | 7.302 | 0.155 | 1.05 | pass |
| wide-at-budget | false | 100000 | seal_ms_median | 39.12 | 5.756 | 0.147 | 1.05 | pass |
| wide-at-budget | false | 100000 | seal_ms_p95 | 40.73 | 6.322 | 0.155 | 1.10 | pass |
| wide-at-budget | false | 100000 | refresh_ms_median | 48.9 | 15.63 | 0.320 | 1.05 | pass |
| wide-at-budget | false | 100000 | rows_per_sec_median | 2.045e+04 | 6.399e+04 | 3.129 | 0.95 | pass |
| wide-at-budget | false | 100000 | refresh_peak_mb_median | 7.1 | 7.1 | 1.000 | 1.05 | pass |
| wide-at-budget | false | 100000 | alloc_mb_median | 0.11 | 0.1 | 0.909 | 1.05 | pass |
| wide-at-budget | false | 100000 | state_bytes_last | 6.602e+07 | 2.798e+07 | 0.424 | 1.00 | pass |
| wide-at-budget | false | 100000 | meta_bytes_median | 1.014e+06 | 3.168e+05 | 0.312 | 1.00 | pass |
| wide-at-budget | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget | false | 100000 | meta_segs_total | 2000 | 400 | 0.200 | 1.00 | pass |
| wide-at-budget | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget | false | 100000 | complete_seal_ms | 1394 | 468.6 | 0.336 | 1.05 | pass |
| wide-at-budget | false | 100000 | restore_ms | 482.9 | 234 | 0.485 | 1.05 | pass |
| wide-at-budget | false | 100000 | first_reseal_ms | 48.98 | 6.643 | 0.136 | 1.05 | pass |
| wide-at-budget | true | 10000 | seal_ms_median | 3.502 | 3.463 | 0.989 | 1.05 | pass |
| wide-at-budget | true | 10000 | seal_ms_p95 | 4.197 | 3.925 | 0.935 | 1.10 | pass |
| wide-at-budget | true | 10000 | refresh_ms_median | 7.082 | 6.864 | 0.969 | 1.05 | pass |
| wide-at-budget | true | 10000 | rows_per_sec_median | 1.412e+05 | 1.457e+05 | 1.032 | 0.95 | pass |
| wide-at-budget | true | 10000 | refresh_peak_mb_median | 3.4 | 3.4 | 1.000 | 1.05 | pass |
| wide-at-budget | true | 10000 | alloc_mb_median | 0.1 | 0.1 | 1.000 | 1.05 | pass |
| wide-at-budget | true | 10000 | state_bytes_last | 2.778e+06 | 2.778e+06 | 1.000 | 1.00 | pass |
| wide-at-budget | true | 10000 | meta_bytes_median | 3.078e+05 | 3.078e+05 | 1.000 | 1.00 | pass |
| wide-at-budget | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget | true | 10000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| wide-at-budget | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget | true | 10000 | complete_seal_ms | 23.68 | 24.13 | 1.019 | 1.05 | pass |
| wide-at-budget | true | 10000 | restore_ms | 32.41 | 30.43 | 0.939 | 1.05 | pass |
| wide-at-budget | true | 10000 | first_reseal_ms | 6.342 | 6.369 | 1.004 | 1.05 | pass |
| wide-at-budget | true | 100000 | seal_ms_median | 3.748 | 3.768 | 1.005 | 1.05 | pass |
| wide-at-budget | true | 100000 | seal_ms_p95 | 4.166 | 4.006 | 0.962 | 1.10 | pass |
| wide-at-budget | true | 100000 | refresh_ms_median | 10.4 | 10.27 | 0.988 | 1.05 | pass |
| wide-at-budget | true | 100000 | rows_per_sec_median | 9.615e+04 | 9.733e+04 | 1.012 | 0.95 | pass |
| wide-at-budget | true | 100000 | refresh_peak_mb_median | 7.1 | 7.1 | 1.000 | 1.05 | pass |
| wide-at-budget | true | 100000 | alloc_mb_median | 0.1 | 0.1 | 1.000 | 1.05 | pass |
| wide-at-budget | true | 100000 | state_bytes_last | 2.798e+07 | 2.798e+07 | 1.000 | 1.00 | pass |
| wide-at-budget | true | 100000 | meta_bytes_median | 3.168e+05 | 3.168e+05 | 1.000 | 1.00 | pass |
| wide-at-budget | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget | true | 100000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| wide-at-budget | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget | true | 100000 | complete_seal_ms | 267.1 | 246.6 | 0.923 | 1.05 | pass |
| wide-at-budget | true | 100000 | restore_ms | 99.69 | 100.4 | 1.007 | 1.05 | pass |
| wide-at-budget | true | 100000 | first_reseal_ms | 5.083 | 5.134 | 1.010 | 1.05 | pass |
| wide-below-budget | false | 10000 | seal_ms_median | 25.42 | 4.39 | 0.173 | 1.05 | pass |
| wide-below-budget | false | 10000 | seal_ms_p95 | 28.77 | 5.315 | 0.185 | 1.10 | pass |
| wide-below-budget | false | 10000 | refresh_ms_median | 31.48 | 10.37 | 0.329 | 1.05 | pass |
| wide-below-budget | false | 10000 | rows_per_sec_median | 3.176e+04 | 9.641e+04 | 3.036 | 0.95 | pass |
| wide-below-budget | false | 10000 | refresh_peak_mb_median | 3.3 | 3.3 | 1.000 | 1.05 | pass |
| wide-below-budget | false | 10000 | alloc_mb_median | 0.12 | 0.1 | 0.833 | 1.05 | pass |
| wide-below-budget | false | 10000 | state_bytes_last | 5.964e+06 | 2.698e+06 | 0.452 | 1.00 | pass |
| wide-below-budget | false | 10000 | meta_bytes_median | 8.512e+05 | 2.996e+05 | 0.352 | 1.00 | pass |
| wide-below-budget | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget | false | 10000 | meta_segs_total | 1900 | 400 | 0.211 | 1.00 | pass |
| wide-below-budget | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget | false | 10000 | complete_seal_ms | 120.9 | 35.35 | 0.292 | 1.05 | pass |
| wide-below-budget | false | 10000 | restore_ms | 76.89 | 43.52 | 0.566 | 1.05 | pass |
| wide-below-budget | false | 10000 | first_reseal_ms | 45.46 | 7.475 | 0.164 | 1.05 | pass |
| wide-below-budget | false | 100000 | seal_ms_median | 36.97 | 5.557 | 0.150 | 1.05 | pass |
| wide-below-budget | false | 100000 | seal_ms_p95 | 38.17 | 6.192 | 0.162 | 1.10 | pass |
| wide-below-budget | false | 100000 | refresh_ms_median | 46.45 | 14.92 | 0.321 | 1.05 | pass |
| wide-below-budget | false | 100000 | rows_per_sec_median | 2.153e+04 | 6.701e+04 | 3.112 | 0.95 | pass |
| wide-below-budget | false | 100000 | refresh_peak_mb_median | 7.1 | 7.1 | 1.000 | 1.05 | pass |
| wide-below-budget | false | 100000 | alloc_mb_median | 0.11 | 0.1 | 0.909 | 1.05 | pass |
| wide-below-budget | false | 100000 | state_bytes_last | 6.284e+07 | 2.718e+07 | 0.432 | 1.00 | pass |
| wide-below-budget | false | 100000 | meta_bytes_median | 9.592e+05 | 3.086e+05 | 0.322 | 1.00 | pass |
| wide-below-budget | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget | false | 100000 | meta_segs_total | 1900 | 400 | 0.211 | 1.00 | pass |
| wide-below-budget | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget | false | 100000 | complete_seal_ms | 1337 | 451.2 | 0.337 | 1.05 | pass |
| wide-below-budget | false | 100000 | restore_ms | 447.4 | 219.5 | 0.491 | 1.05 | pass |
| wide-below-budget | false | 100000 | first_reseal_ms | 42.43 | 6.577 | 0.155 | 1.05 | pass |
| wide-below-budget | true | 10000 | seal_ms_median | 3.427 | 3.385 | 0.988 | 1.05 | pass |
| wide-below-budget | true | 10000 | seal_ms_p95 | 4.13 | 3.953 | 0.957 | 1.10 | pass |
| wide-below-budget | true | 10000 | refresh_ms_median | 6.806 | 6.736 | 0.990 | 1.05 | pass |
| wide-below-budget | true | 10000 | rows_per_sec_median | 1.469e+05 | 1.485e+05 | 1.010 | 0.95 | pass |
| wide-below-budget | true | 10000 | refresh_peak_mb_median | 3.3 | 3.3 | 1.000 | 1.05 | pass |
| wide-below-budget | true | 10000 | alloc_mb_median | 0.1 | 0.1 | 1.000 | 1.05 | pass |
| wide-below-budget | true | 10000 | state_bytes_last | 2.698e+06 | 2.698e+06 | 1.000 | 1.00 | pass |
| wide-below-budget | true | 10000 | meta_bytes_median | 2.996e+05 | 2.996e+05 | 1.000 | 1.00 | pass |
| wide-below-budget | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget | true | 10000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| wide-below-budget | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget | true | 10000 | complete_seal_ms | 22.77 | 22.85 | 1.003 | 1.05 | pass |
| wide-below-budget | true | 10000 | restore_ms | 30.01 | 29.99 | 0.999 | 1.05 | pass |
| wide-below-budget | true | 10000 | first_reseal_ms | 6.084 | 6.249 | 1.027 | 1.05 | pass |
| wide-below-budget | true | 100000 | seal_ms_median | 3.702 | 3.674 | 0.992 | 1.05 | pass |
| wide-below-budget | true | 100000 | seal_ms_p95 | 3.981 | 3.876 | 0.974 | 1.10 | pass |
| wide-below-budget | true | 100000 | refresh_ms_median | 10.1 | 10.05 | 0.996 | 1.05 | pass |
| wide-below-budget | true | 100000 | rows_per_sec_median | 9.904e+04 | 9.948e+04 | 1.004 | 0.95 | pass |
| wide-below-budget | true | 100000 | refresh_peak_mb_median | 7.1 | 7.1 | 1.000 | 1.05 | pass |
| wide-below-budget | true | 100000 | alloc_mb_median | 0.1 | 0.1 | 1.000 | 1.05 | pass |
| wide-below-budget | true | 100000 | state_bytes_last | 2.718e+07 | 2.718e+07 | 1.000 | 1.00 | pass |
| wide-below-budget | true | 100000 | meta_bytes_median | 3.086e+05 | 3.086e+05 | 1.000 | 1.00 | pass |
| wide-below-budget | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget | true | 100000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| wide-below-budget | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget | true | 100000 | complete_seal_ms | 253.2 | 235.6 | 0.930 | 1.05 | pass |
| wide-below-budget | true | 100000 | restore_ms | 93.18 | 92.76 | 0.995 | 1.05 | pass |
| wide-below-budget | true | 100000 | first_reseal_ms | 4.904 | 4.982 | 1.016 | 1.05 | pass |

19 failed or incomplete gate(s)
```

## Appendix B: every gate of the second measurement

480 runs: the three re-measured shapes at 10,000 and 100,000 keys with 15 runs
per cell per revision, the same three at 1,000,000 keys with five, and the two
repair cells at 1,000 and 10,000 keys with five. The candidate's keyed repair cell
is read against the baseline's whole-range cell, as the label says. The flat 1.00
metadata limit again fails the +86-byte anchor-only cells (1.001) the allowance
admits, and the 105% restore and complete-seal limits fail the cells the revised
requirements above cover; every failure below is one of those or is discussed in
the repair section.

| Shape | Fusion | Keys | Metric | Baseline | Candidate | Ratio | Limit | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| anchor-only-decimal | false | 10000 | seal_ms_median | 9.168 | 3.045 | 0.332 | 1.05 | pass |
| anchor-only-decimal | false | 10000 | seal_ms_p95 | 11.81 | 3.788 | 0.321 | 1.10 | pass |
| anchor-only-decimal | false | 10000 | refresh_ms_median | 12.15 | 6 | 0.494 | 1.05 | pass |
| anchor-only-decimal | false | 10000 | rows_per_sec_median | 8.229e+04 | 1.667e+05 | 2.025 | 0.95 | pass |
| anchor-only-decimal | false | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| anchor-only-decimal | false | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-decimal | false | 10000 | state_bytes_last | 8.456e+05 | 8.456e+05 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 10000 | meta_bytes_median | 8.026e+04 | 8.035e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-decimal | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 10000 | complete_seal_ms | 25.34 | 28.32 | 1.118 | 1.05 | FAIL |
| anchor-only-decimal | false | 10000 | restore_ms | 22.38 | 24.84 | 1.110 | 1.05 | FAIL |
| anchor-only-decimal | false | 10000 | first_reseal_ms | 13.6 | 6.59 | 0.485 | 1.05 | pass |
| anchor-only-decimal | false | 100000 | seal_ms_median | 11.39 | 3.659 | 0.321 | 1.05 | pass |
| anchor-only-decimal | false | 100000 | seal_ms_p95 | 12.57 | 4.002 | 0.318 | 1.10 | pass |
| anchor-only-decimal | false | 100000 | refresh_ms_median | 17.46 | 9.776 | 0.560 | 1.05 | pass |
| anchor-only-decimal | false | 100000 | rows_per_sec_median | 5.726e+04 | 1.023e+05 | 1.786 | 0.95 | pass |
| anchor-only-decimal | false | 100000 | refresh_peak_mb_median | 6.2 | 6.2 | 1.000 | 1.05 | pass |
| anchor-only-decimal | false | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-decimal | false | 100000 | state_bytes_last | 8.856e+06 | 8.856e+06 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 100000 | meta_bytes_median | 8.966e+04 | 8.974e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-decimal | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 100000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 100000 | complete_seal_ms | 179.9 | 187.1 | 1.040 | 1.05 | pass |
| anchor-only-decimal | false | 100000 | restore_ms | 69.99 | 73.42 | 1.049 | 1.05 | pass |
| anchor-only-decimal | false | 100000 | first_reseal_ms | 15.14 | 4.638 | 0.306 | 1.05 | pass |
| anchor-only-decimal | false | 1000000 | seal_ms_median | 14.52 | 7.207 | 0.496 | 1.05 | pass |
| anchor-only-decimal | false | 1000000 | seal_ms_p95 | 15.08 | 7.968 | 0.528 | 1.10 | pass |
| anchor-only-decimal | false | 1000000 | refresh_ms_median | 16.13 | 8.884 | 0.551 | 1.05 | pass |
| anchor-only-decimal | false | 1000000 | rows_per_sec_median | 6.199e+04 | 1.126e+05 | 1.816 | 0.95 | pass |
| anchor-only-decimal | false | 1000000 | refresh_peak_mb_median | 0.2 | 0.2 | 1.000 | 1.05 | pass |
| anchor-only-decimal | false | 1000000 | alloc_mb_median | 0.08 | 0.08 | 1.000 | 1.05 | pass |
| anchor-only-decimal | false | 1000000 | state_bytes_last | 9.256e+07 | 9.256e+07 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 1000000 | meta_bytes_median | 9.118e+04 | 9.127e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-decimal | false | 1000000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 1000000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 1000000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | false | 1000000 | complete_seal_ms | 2418 | 2516 | 1.041 | 1.05 | pass |
| anchor-only-decimal | false | 1000000 | restore_ms | 609.4 | 618.3 | 1.015 | 1.05 | pass |
| anchor-only-decimal | false | 1000000 | first_reseal_ms | 13.85 | 4.49 | 0.324 | 1.05 | pass |
| anchor-only-decimal | true | 10000 | seal_ms_median | 9.254 | 3.025 | 0.327 | 1.05 | pass |
| anchor-only-decimal | true | 10000 | seal_ms_p95 | 12.09 | 3.872 | 0.320 | 1.10 | pass |
| anchor-only-decimal | true | 10000 | refresh_ms_median | 12.35 | 5.996 | 0.486 | 1.05 | pass |
| anchor-only-decimal | true | 10000 | rows_per_sec_median | 8.098e+04 | 1.668e+05 | 2.059 | 0.95 | pass |
| anchor-only-decimal | true | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| anchor-only-decimal | true | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-decimal | true | 10000 | state_bytes_last | 8.456e+05 | 8.456e+05 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 10000 | meta_bytes_median | 8.026e+04 | 8.035e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-decimal | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 10000 | complete_seal_ms | 25.75 | 27.08 | 1.051 | 1.05 | FAIL |
| anchor-only-decimal | true | 10000 | restore_ms | 22.77 | 25.04 | 1.100 | 1.05 | FAIL |
| anchor-only-decimal | true | 10000 | first_reseal_ms | 13.73 | 6.531 | 0.476 | 1.05 | pass |
| anchor-only-decimal | true | 100000 | seal_ms_median | 11.18 | 3.633 | 0.325 | 1.05 | pass |
| anchor-only-decimal | true | 100000 | seal_ms_p95 | 13.16 | 4.094 | 0.311 | 1.10 | pass |
| anchor-only-decimal | true | 100000 | refresh_ms_median | 17.18 | 9.775 | 0.569 | 1.05 | pass |
| anchor-only-decimal | true | 100000 | rows_per_sec_median | 5.819e+04 | 1.023e+05 | 1.758 | 0.95 | pass |
| anchor-only-decimal | true | 100000 | refresh_peak_mb_median | 6.2 | 6.2 | 1.000 | 1.05 | pass |
| anchor-only-decimal | true | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-decimal | true | 100000 | state_bytes_last | 8.856e+06 | 8.856e+06 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 100000 | meta_bytes_median | 8.966e+04 | 8.974e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-decimal | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 100000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 100000 | complete_seal_ms | 179 | 186.4 | 1.041 | 1.05 | pass |
| anchor-only-decimal | true | 100000 | restore_ms | 71.65 | 73.13 | 1.021 | 1.05 | pass |
| anchor-only-decimal | true | 100000 | first_reseal_ms | 15.04 | 4.655 | 0.310 | 1.05 | pass |
| anchor-only-decimal | true | 1000000 | seal_ms_median | 14.56 | 7.185 | 0.494 | 1.05 | pass |
| anchor-only-decimal | true | 1000000 | seal_ms_p95 | 15.74 | 7.632 | 0.485 | 1.10 | pass |
| anchor-only-decimal | true | 1000000 | refresh_ms_median | 16.41 | 8.869 | 0.541 | 1.05 | pass |
| anchor-only-decimal | true | 1000000 | rows_per_sec_median | 6.095e+04 | 1.128e+05 | 1.850 | 0.95 | pass |
| anchor-only-decimal | true | 1000000 | refresh_peak_mb_median | 0.2 | 0.2 | 1.000 | 1.05 | pass |
| anchor-only-decimal | true | 1000000 | alloc_mb_median | 0.08 | 0.08 | 1.000 | 1.05 | pass |
| anchor-only-decimal | true | 1000000 | state_bytes_last | 9.256e+07 | 9.256e+07 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 1000000 | meta_bytes_median | 9.118e+04 | 9.127e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-decimal | true | 1000000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 1000000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 1000000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal | true | 1000000 | complete_seal_ms | 2538 | 2513 | 0.990 | 1.05 | pass |
| anchor-only-decimal | true | 1000000 | restore_ms | 623.4 | 615.9 | 0.988 | 1.05 | pass |
| anchor-only-decimal | true | 1000000 | first_reseal_ms | 13.93 | 4.503 | 0.323 | 1.05 | pass |
| anchor-only-unfused-control | false | 10000 | seal_ms_median | 9.095 | 2.881 | 0.317 | 1.05 | pass |
| anchor-only-unfused-control | false | 10000 | seal_ms_p95 | 11.44 | 3.682 | 0.322 | 1.10 | pass |
| anchor-only-unfused-control | false | 10000 | refresh_ms_median | 12.12 | 5.785 | 0.477 | 1.05 | pass |
| anchor-only-unfused-control | false | 10000 | rows_per_sec_median | 8.253e+04 | 1.729e+05 | 2.094 | 0.95 | pass |
| anchor-only-unfused-control | false | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | false | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | false | 10000 | state_bytes_last | 6.756e+05 | 6.756e+05 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 10000 | meta_bytes_median | 6.274e+04 | 6.283e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 10000 | complete_seal_ms | 24.43 | 26.37 | 1.080 | 1.05 | FAIL |
| anchor-only-unfused-control | false | 10000 | restore_ms | 22.54 | 24.81 | 1.101 | 1.05 | FAIL |
| anchor-only-unfused-control | false | 10000 | first_reseal_ms | 13.35 | 6.558 | 0.491 | 1.05 | pass |
| anchor-only-unfused-control | false | 100000 | seal_ms_median | 11.21 | 3.487 | 0.311 | 1.05 | pass |
| anchor-only-unfused-control | false | 100000 | seal_ms_p95 | 12.37 | 3.802 | 0.307 | 1.10 | pass |
| anchor-only-unfused-control | false | 100000 | refresh_ms_median | 17.19 | 9.514 | 0.554 | 1.05 | pass |
| anchor-only-unfused-control | false | 100000 | rows_per_sec_median | 5.819e+04 | 1.051e+05 | 1.806 | 0.95 | pass |
| anchor-only-unfused-control | false | 100000 | refresh_peak_mb_median | 6.2 | 6.2 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | false | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | false | 100000 | state_bytes_last | 7.156e+06 | 7.156e+06 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 100000 | meta_bytes_median | 7.22e+04 | 7.229e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 100000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 100000 | complete_seal_ms | 170.8 | 175.5 | 1.027 | 1.05 | pass |
| anchor-only-unfused-control | false | 100000 | restore_ms | 66.31 | 70.61 | 1.065 | 1.05 | FAIL |
| anchor-only-unfused-control | false | 100000 | first_reseal_ms | 15.32 | 4.563 | 0.298 | 1.05 | pass |
| anchor-only-unfused-control | false | 1000000 | seal_ms_median | 14.31 | 7.018 | 0.490 | 1.05 | pass |
| anchor-only-unfused-control | false | 1000000 | seal_ms_p95 | 15.34 | 7.271 | 0.474 | 1.10 | pass |
| anchor-only-unfused-control | false | 1000000 | refresh_ms_median | 15.96 | 8.614 | 0.540 | 1.05 | pass |
| anchor-only-unfused-control | false | 1000000 | rows_per_sec_median | 6.267e+04 | 1.161e+05 | 1.853 | 0.95 | pass |
| anchor-only-unfused-control | false | 1000000 | refresh_peak_mb_median | 0.2 | 0.2 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | false | 1000000 | alloc_mb_median | 0.08 | 0.08 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | false | 1000000 | state_bytes_last | 7.556e+07 | 7.556e+07 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 1000000 | meta_bytes_median | 7.37e+04 | 7.378e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control | false | 1000000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 1000000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 1000000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | false | 1000000 | complete_seal_ms | 2419 | 2416 | 0.999 | 1.05 | pass |
| anchor-only-unfused-control | false | 1000000 | restore_ms | 560.3 | 566 | 1.010 | 1.05 | pass |
| anchor-only-unfused-control | false | 1000000 | first_reseal_ms | 14.12 | 4.032 | 0.286 | 1.05 | pass |
| anchor-only-unfused-control | true | 10000 | seal_ms_median | 9.067 | 2.927 | 0.323 | 1.05 | pass |
| anchor-only-unfused-control | true | 10000 | seal_ms_p95 | 11.45 | 3.864 | 0.338 | 1.10 | pass |
| anchor-only-unfused-control | true | 10000 | refresh_ms_median | 12.09 | 5.821 | 0.482 | 1.05 | pass |
| anchor-only-unfused-control | true | 10000 | rows_per_sec_median | 8.273e+04 | 1.718e+05 | 2.077 | 0.95 | pass |
| anchor-only-unfused-control | true | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | true | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | true | 10000 | state_bytes_last | 6.756e+05 | 6.756e+05 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 10000 | meta_bytes_median | 6.274e+04 | 6.283e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 10000 | complete_seal_ms | 24.79 | 27.18 | 1.097 | 1.05 | FAIL |
| anchor-only-unfused-control | true | 10000 | restore_ms | 22.99 | 23.81 | 1.036 | 1.05 | pass |
| anchor-only-unfused-control | true | 10000 | first_reseal_ms | 13.33 | 6.374 | 0.478 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | seal_ms_median | 11.47 | 3.457 | 0.301 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | seal_ms_p95 | 12.49 | 3.806 | 0.305 | 1.10 | pass |
| anchor-only-unfused-control | true | 100000 | refresh_ms_median | 17.42 | 9.572 | 0.549 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | rows_per_sec_median | 5.741e+04 | 1.045e+05 | 1.820 | 0.95 | pass |
| anchor-only-unfused-control | true | 100000 | refresh_peak_mb_median | 6.2 | 6.2 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | state_bytes_last | 7.156e+06 | 7.156e+06 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 100000 | meta_bytes_median | 7.22e+04 | 7.229e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 100000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 100000 | complete_seal_ms | 170.9 | 175.1 | 1.025 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | restore_ms | 67.79 | 70.19 | 1.035 | 1.05 | pass |
| anchor-only-unfused-control | true | 100000 | first_reseal_ms | 15.14 | 4.525 | 0.299 | 1.05 | pass |
| anchor-only-unfused-control | true | 1000000 | seal_ms_median | 14.57 | 7.088 | 0.487 | 1.05 | pass |
| anchor-only-unfused-control | true | 1000000 | seal_ms_p95 | 15.96 | 7.606 | 0.476 | 1.10 | pass |
| anchor-only-unfused-control | true | 1000000 | refresh_ms_median | 16.34 | 8.785 | 0.538 | 1.05 | pass |
| anchor-only-unfused-control | true | 1000000 | rows_per_sec_median | 6.121e+04 | 1.138e+05 | 1.860 | 0.95 | pass |
| anchor-only-unfused-control | true | 1000000 | refresh_peak_mb_median | 0.2 | 0.2 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | true | 1000000 | alloc_mb_median | 0.08 | 0.08 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control | true | 1000000 | state_bytes_last | 7.556e+07 | 7.556e+07 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 1000000 | meta_bytes_median | 7.37e+04 | 7.378e+04 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control | true | 1000000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 1000000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 1000000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control | true | 1000000 | complete_seal_ms | 2233 | 2328 | 1.043 | 1.05 | pass |
| anchor-only-unfused-control | true | 1000000 | restore_ms | 552.6 | 585.7 | 1.060 | 1.05 | FAIL |
| anchor-only-unfused-control | true | 1000000 | first_reseal_ms | 14.56 | 4.541 | 0.312 | 1.05 | pass |
| narrow-sum | false | 10000 | seal_ms_median | 9.142 | 2.189 | 0.239 | 1.05 | pass |
| narrow-sum | false | 10000 | seal_ms_p95 | 11.5 | 2.684 | 0.233 | 1.10 | pass |
| narrow-sum | false | 10000 | refresh_ms_median | 12.12 | 4.957 | 0.409 | 1.05 | pass |
| narrow-sum | false | 10000 | rows_per_sec_median | 8.249e+04 | 2.017e+05 | 2.446 | 0.95 | pass |
| narrow-sum | false | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| narrow-sum | false | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-sum | false | 10000 | state_bytes_last | 6.756e+05 | 4.578e+05 | 0.678 | 1.00 | pass |
| narrow-sum | false | 10000 | meta_bytes_median | 6.274e+04 | 6.949e+04 | 1.107 | 1.00 | FAIL |
| narrow-sum | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | false | 10000 | meta_segs_total | 500 | 400 | 0.800 | 1.00 | pass |
| narrow-sum | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | false | 10000 | complete_seal_ms | 24.85 | 17.12 | 0.689 | 1.05 | pass |
| narrow-sum | false | 10000 | restore_ms | 22.32 | 22.15 | 0.993 | 1.05 | pass |
| narrow-sum | false | 10000 | first_reseal_ms | 13.62 | 5.326 | 0.391 | 1.05 | pass |
| narrow-sum | false | 100000 | seal_ms_median | 11.16 | 2.44 | 0.219 | 1.05 | pass |
| narrow-sum | false | 100000 | seal_ms_p95 | 12.43 | 2.771 | 0.223 | 1.10 | pass |
| narrow-sum | false | 100000 | refresh_ms_median | 17.2 | 8.414 | 0.489 | 1.05 | pass |
| narrow-sum | false | 100000 | rows_per_sec_median | 5.816e+04 | 1.189e+05 | 2.044 | 0.95 | pass |
| narrow-sum | false | 100000 | refresh_peak_mb_median | 6.2 | 6.2 | 1.000 | 1.05 | pass |
| narrow-sum | false | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-sum | false | 100000 | state_bytes_last | 7.156e+06 | 4.778e+06 | 0.668 | 1.00 | pass |
| narrow-sum | false | 100000 | meta_bytes_median | 7.22e+04 | 7.756e+04 | 1.074 | 1.00 | FAIL |
| narrow-sum | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | false | 100000 | meta_segs_total | 500 | 400 | 0.800 | 1.00 | pass |
| narrow-sum | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | false | 100000 | complete_seal_ms | 166.3 | 95.93 | 0.577 | 1.05 | pass |
| narrow-sum | false | 100000 | restore_ms | 67.13 | 52.46 | 0.781 | 1.05 | pass |
| narrow-sum | false | 100000 | first_reseal_ms | 15.6 | 3.837 | 0.246 | 1.05 | pass |
| narrow-sum | false | 1000000 | seal_ms_median | 14.33 | 3.319 | 0.232 | 1.05 | pass |
| narrow-sum | false | 1000000 | seal_ms_p95 | 16.07 | 3.541 | 0.220 | 1.10 | pass |
| narrow-sum | false | 1000000 | refresh_ms_median | 16.07 | 4.898 | 0.305 | 1.05 | pass |
| narrow-sum | false | 1000000 | rows_per_sec_median | 6.221e+04 | 2.042e+05 | 3.282 | 0.95 | pass |
| narrow-sum | false | 1000000 | refresh_peak_mb_median | 0.2 | 0.2 | 1.000 | 1.05 | pass |
| narrow-sum | false | 1000000 | alloc_mb_median | 0.08 | 0.08 | 1.000 | 1.05 | pass |
| narrow-sum | false | 1000000 | state_bytes_last | 7.556e+07 | 4.978e+07 | 0.659 | 1.00 | pass |
| narrow-sum | false | 1000000 | meta_bytes_median | 7.37e+04 | 7.895e+04 | 1.071 | 1.00 | FAIL |
| narrow-sum | false | 1000000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | false | 1000000 | meta_segs_total | 500 | 400 | 0.800 | 1.00 | pass |
| narrow-sum | false | 1000000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | false | 1000000 | complete_seal_ms | 2421 | 1285 | 0.531 | 1.05 | pass |
| narrow-sum | false | 1000000 | restore_ms | 564.6 | 432.8 | 0.766 | 1.05 | pass |
| narrow-sum | false | 1000000 | first_reseal_ms | 15.15 | 3.495 | 0.231 | 1.05 | pass |
| narrow-sum | true | 10000 | seal_ms_median | 2.097 | 2.113 | 1.008 | 1.05 | pass |
| narrow-sum | true | 10000 | seal_ms_p95 | 2.628 | 2.608 | 0.992 | 1.10 | pass |
| narrow-sum | true | 10000 | refresh_ms_median | 4.755 | 4.739 | 0.996 | 1.05 | pass |
| narrow-sum | true | 10000 | rows_per_sec_median | 2.103e+05 | 2.11e+05 | 1.004 | 0.95 | pass |
| narrow-sum | true | 10000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| narrow-sum | true | 10000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-sum | true | 10000 | state_bytes_last | 4.578e+05 | 4.578e+05 | 1.000 | 1.00 | pass |
| narrow-sum | true | 10000 | meta_bytes_median | 6.949e+04 | 6.949e+04 | 1.000 | 1.00 | pass |
| narrow-sum | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | true | 10000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| narrow-sum | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | true | 10000 | complete_seal_ms | 15.61 | 15.07 | 0.965 | 1.05 | pass |
| narrow-sum | true | 10000 | restore_ms | 19.59 | 20.55 | 1.049 | 1.05 | pass |
| narrow-sum | true | 10000 | first_reseal_ms | 4.722 | 4.703 | 0.996 | 1.05 | pass |
| narrow-sum | true | 100000 | seal_ms_median | 2.325 | 2.358 | 1.014 | 1.05 | pass |
| narrow-sum | true | 100000 | seal_ms_p95 | 2.677 | 2.703 | 1.010 | 1.10 | pass |
| narrow-sum | true | 100000 | refresh_ms_median | 8.056 | 8.2 | 1.018 | 1.05 | pass |
| narrow-sum | true | 100000 | rows_per_sec_median | 1.241e+05 | 1.22e+05 | 0.982 | 0.95 | pass |
| narrow-sum | true | 100000 | refresh_peak_mb_median | 6.2 | 6.2 | 1.000 | 1.05 | pass |
| narrow-sum | true | 100000 | alloc_mb_median | 0.07 | 0.07 | 1.000 | 1.05 | pass |
| narrow-sum | true | 100000 | state_bytes_last | 4.778e+06 | 4.778e+06 | 1.000 | 1.00 | pass |
| narrow-sum | true | 100000 | meta_bytes_median | 7.756e+04 | 7.756e+04 | 1.000 | 1.00 | pass |
| narrow-sum | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | true | 100000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| narrow-sum | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | true | 100000 | complete_seal_ms | 89.54 | 90.83 | 1.014 | 1.05 | pass |
| narrow-sum | true | 100000 | restore_ms | 43.55 | 44.56 | 1.023 | 1.05 | pass |
| narrow-sum | true | 100000 | first_reseal_ms | 3.548 | 3.532 | 0.995 | 1.05 | pass |
| narrow-sum | true | 1000000 | seal_ms_median | 3.215 | 3.212 | 0.999 | 1.05 | pass |
| narrow-sum | true | 1000000 | seal_ms_p95 | 3.654 | 3.802 | 1.041 | 1.10 | pass |
| narrow-sum | true | 1000000 | refresh_ms_median | 4.624 | 4.575 | 0.989 | 1.05 | pass |
| narrow-sum | true | 1000000 | rows_per_sec_median | 2.162e+05 | 2.186e+05 | 1.011 | 0.95 | pass |
| narrow-sum | true | 1000000 | refresh_peak_mb_median | 0.2 | 0.2 | 1.000 | 1.05 | pass |
| narrow-sum | true | 1000000 | alloc_mb_median | 0.08 | 0.08 | 1.000 | 1.05 | pass |
| narrow-sum | true | 1000000 | state_bytes_last | 4.978e+07 | 4.978e+07 | 1.000 | 1.00 | pass |
| narrow-sum | true | 1000000 | meta_bytes_median | 7.895e+04 | 7.895e+04 | 1.000 | 1.00 | pass |
| narrow-sum | true | 1000000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | true | 1000000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| narrow-sum | true | 1000000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum | true | 1000000 | complete_seal_ms | 1216 | 1254 | 1.031 | 1.05 | pass |
| narrow-sum | true | 1000000 | restore_ms | 327 | 342 | 1.046 | 1.05 | pass |
| narrow-sum | true | 1000000 | first_reseal_ms | 3.468 | 3.414 | 0.984 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | seal_ms_median | 2.397 | 1.694 | 0.707 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | seal_ms_p95 | 3.489 | 2.422 | 0.694 | 1.10 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | refresh_ms_median | 10.02 | 9.053 | 0.904 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | rows_per_sec_median | 9.983e+04 | 1.105e+05 | 1.107 | 0.95 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | refresh_peak_mb_median | 1 | 1 | 1.000 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | alloc_mb_median | 0.21 | 0.21 | 1.000 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | state_bytes_last | 6.356e+04 | 4.378e+04 | 0.689 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | meta_bytes_median | 1.009e+05 | 6.664e+04 | 0.661 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | meta_segs_total | 681 | 575 | 0.844 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | complete_seal_ms | 5.1 | 4.159 | 0.815 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | restore_ms | 9.355 | 8.124 | 0.868 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | first_reseal_ms | 7.073 | 4.477 | 0.633 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 1000 | repair_ms_median | 10.02 | 9.053 | 0.904 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | seal_ms_median | 2.869 | 1.798 | 0.627 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | seal_ms_p95 | 3.502 | 2.352 | 0.672 | 1.10 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | refresh_ms_median | 14.81 | 12.07 | 0.815 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | rows_per_sec_median | 6.751e+04 | 8.288e+04 | 1.228 | 0.95 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | refresh_peak_mb_median | 1.3 | 1.8 | 1.385 | 1.05 | FAIL |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | alloc_mb_median | 0.22 | 0.22 | 1.000 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | state_bytes_last | 6.756e+05 | 4.578e+05 | 0.678 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | meta_bytes_median | 1.119e+05 | 7.223e+04 | 0.646 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | meta_segs_total | 685 | 575 | 0.839 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | complete_seal_ms | 24.88 | 16.75 | 0.673 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | restore_ms | 13.1 | 10.8 | 0.825 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | first_reseal_ms | 7.069 | 3.916 | 0.554 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | false | 10000 | repair_ms_median | 14.81 | 12.07 | 0.815 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | seal_ms_median | 1.653 | 1.648 | 0.997 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | seal_ms_p95 | 2.365 | 2.429 | 1.027 | 1.10 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | refresh_ms_median | 8.924 | 8.787 | 0.985 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | rows_per_sec_median | 1.121e+05 | 1.138e+05 | 1.016 | 0.95 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | refresh_peak_mb_median | 1 | 1 | 1.000 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | alloc_mb_median | 0.21 | 0.21 | 1.000 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | state_bytes_last | 4.378e+04 | 4.378e+04 | 1.000 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | meta_bytes_median | 6.664e+04 | 6.664e+04 | 1.000 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | meta_segs_total | 576 | 575 | 0.998 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | complete_seal_ms | 3.901 | 3.911 | 1.003 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | restore_ms | 7.997 | 8.418 | 1.053 | 1.05 | FAIL |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | first_reseal_ms | 4.569 | 4.39 | 0.961 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 1000 | repair_ms_median | 8.924 | 8.787 | 0.985 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | seal_ms_median | 1.76 | 1.765 | 1.003 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | seal_ms_p95 | 2.223 | 2.311 | 1.040 | 1.10 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | refresh_ms_median | 11.79 | 11.6 | 0.984 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | rows_per_sec_median | 8.483e+04 | 8.621e+04 | 1.016 | 0.95 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | refresh_peak_mb_median | 1.3 | 1.3 | 1.000 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | alloc_mb_median | 0.22 | 0.22 | 1.000 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | state_bytes_last | 4.578e+05 | 4.578e+05 | 1.000 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | meta_bytes_median | 7.223e+04 | 7.223e+04 | 1.000 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | meta_segs_total | 579 | 575 | 0.993 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | complete_seal_ms | 14.88 | 15.19 | 1.021 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | restore_ms | 10.82 | 11.86 | 1.095 | 1.05 | FAIL |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | first_reseal_ms | 3.942 | 4.11 | 1.043 | 1.05 | pass |
| repair-closed-keyed (vs repair-closed-whole) | true | 10000 | repair_ms_median | 11.79 | 11.6 | 0.984 | 1.05 | pass |
| repair-closed-whole | false | 1000 | seal_ms_median | 2.397 | 1.663 | 0.694 | 1.05 | pass |
| repair-closed-whole | false | 1000 | seal_ms_p95 | 3.489 | 2.39 | 0.685 | 1.10 | pass |
| repair-closed-whole | false | 1000 | refresh_ms_median | 10.02 | 9.14 | 0.912 | 1.05 | pass |
| repair-closed-whole | false | 1000 | rows_per_sec_median | 9.983e+04 | 1.094e+05 | 1.096 | 0.95 | pass |
| repair-closed-whole | false | 1000 | refresh_peak_mb_median | 1 | 1 | 1.000 | 1.05 | pass |
| repair-closed-whole | false | 1000 | alloc_mb_median | 0.21 | 0.21 | 1.000 | 1.05 | pass |
| repair-closed-whole | false | 1000 | state_bytes_last | 6.356e+04 | 4.378e+04 | 0.689 | 1.00 | pass |
| repair-closed-whole | false | 1000 | meta_bytes_median | 1.009e+05 | 6.664e+04 | 0.661 | 1.00 | pass |
| repair-closed-whole | false | 1000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-whole | false | 1000 | meta_segs_total | 681 | 576 | 0.846 | 1.00 | pass |
| repair-closed-whole | false | 1000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-whole | false | 1000 | complete_seal_ms | 5.1 | 3.988 | 0.782 | 1.05 | pass |
| repair-closed-whole | false | 1000 | restore_ms | 9.355 | 8.236 | 0.880 | 1.05 | pass |
| repair-closed-whole | false | 1000 | first_reseal_ms | 7.073 | 4.371 | 0.618 | 1.05 | pass |
| repair-closed-whole | false | 1000 | repair_ms_median | 10.02 | 9.14 | 0.912 | 1.05 | pass |
| repair-closed-whole | false | 10000 | seal_ms_median | 2.869 | 1.791 | 0.624 | 1.05 | pass |
| repair-closed-whole | false | 10000 | seal_ms_p95 | 3.502 | 2.328 | 0.665 | 1.10 | pass |
| repair-closed-whole | false | 10000 | refresh_ms_median | 14.81 | 12.11 | 0.818 | 1.05 | pass |
| repair-closed-whole | false | 10000 | rows_per_sec_median | 6.751e+04 | 8.256e+04 | 1.223 | 0.95 | pass |
| repair-closed-whole | false | 10000 | refresh_peak_mb_median | 1.3 | 1.3 | 1.000 | 1.05 | pass |
| repair-closed-whole | false | 10000 | alloc_mb_median | 0.22 | 0.22 | 1.000 | 1.05 | pass |
| repair-closed-whole | false | 10000 | state_bytes_last | 6.756e+05 | 4.578e+05 | 0.678 | 1.00 | pass |
| repair-closed-whole | false | 10000 | meta_bytes_median | 1.119e+05 | 7.223e+04 | 0.646 | 1.00 | pass |
| repair-closed-whole | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-whole | false | 10000 | meta_segs_total | 685 | 579 | 0.845 | 1.00 | pass |
| repair-closed-whole | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-whole | false | 10000 | complete_seal_ms | 24.88 | 17.99 | 0.723 | 1.05 | pass |
| repair-closed-whole | false | 10000 | restore_ms | 13.1 | 9.713 | 0.742 | 1.05 | pass |
| repair-closed-whole | false | 10000 | first_reseal_ms | 7.069 | 4.057 | 0.574 | 1.05 | pass |
| repair-closed-whole | false | 10000 | repair_ms_median | 14.81 | 12.11 | 0.818 | 1.05 | pass |
| repair-closed-whole | true | 1000 | seal_ms_median | 1.653 | 1.694 | 1.025 | 1.05 | pass |
| repair-closed-whole | true | 1000 | seal_ms_p95 | 2.365 | 2.414 | 1.021 | 1.10 | pass |
| repair-closed-whole | true | 1000 | refresh_ms_median | 8.924 | 9.128 | 1.023 | 1.05 | pass |
| repair-closed-whole | true | 1000 | rows_per_sec_median | 1.121e+05 | 1.096e+05 | 0.978 | 0.95 | pass |
| repair-closed-whole | true | 1000 | refresh_peak_mb_median | 1 | 1 | 1.000 | 1.05 | pass |
| repair-closed-whole | true | 1000 | alloc_mb_median | 0.21 | 0.21 | 1.000 | 1.05 | pass |
| repair-closed-whole | true | 1000 | state_bytes_last | 4.378e+04 | 4.378e+04 | 1.000 | 1.00 | pass |
| repair-closed-whole | true | 1000 | meta_bytes_median | 6.664e+04 | 6.664e+04 | 1.000 | 1.00 | pass |
| repair-closed-whole | true | 1000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-whole | true | 1000 | meta_segs_total | 576 | 576 | 1.000 | 1.00 | pass |
| repair-closed-whole | true | 1000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-whole | true | 1000 | complete_seal_ms | 3.901 | 3.85 | 0.987 | 1.05 | pass |
| repair-closed-whole | true | 1000 | restore_ms | 7.997 | 7.845 | 0.981 | 1.05 | pass |
| repair-closed-whole | true | 1000 | first_reseal_ms | 4.569 | 4.334 | 0.949 | 1.05 | pass |
| repair-closed-whole | true | 1000 | repair_ms_median | 8.924 | 9.128 | 1.023 | 1.05 | pass |
| repair-closed-whole | true | 10000 | seal_ms_median | 1.76 | 1.77 | 1.006 | 1.05 | pass |
| repair-closed-whole | true | 10000 | seal_ms_p95 | 2.223 | 2.298 | 1.034 | 1.10 | pass |
| repair-closed-whole | true | 10000 | refresh_ms_median | 11.79 | 11.85 | 1.005 | 1.05 | pass |
| repair-closed-whole | true | 10000 | rows_per_sec_median | 8.483e+04 | 8.439e+04 | 0.995 | 0.95 | pass |
| repair-closed-whole | true | 10000 | refresh_peak_mb_median | 1.3 | 1.3 | 1.000 | 1.05 | pass |
| repair-closed-whole | true | 10000 | alloc_mb_median | 0.22 | 0.22 | 1.000 | 1.05 | pass |
| repair-closed-whole | true | 10000 | state_bytes_last | 4.578e+05 | 4.578e+05 | 1.000 | 1.00 | pass |
| repair-closed-whole | true | 10000 | meta_bytes_median | 7.223e+04 | 7.223e+04 | 1.000 | 1.00 | pass |
| repair-closed-whole | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-whole | true | 10000 | meta_segs_total | 579 | 579 | 1.000 | 1.00 | pass |
| repair-closed-whole | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| repair-closed-whole | true | 10000 | complete_seal_ms | 14.88 | 15.79 | 1.061 | 1.05 | FAIL |
| repair-closed-whole | true | 10000 | restore_ms | 10.82 | 11.17 | 1.032 | 1.05 | pass |
| repair-closed-whole | true | 10000 | first_reseal_ms | 3.942 | 3.915 | 0.993 | 1.05 | pass |
| repair-closed-whole | true | 10000 | repair_ms_median | 11.79 | 11.85 | 1.005 | 1.05 | pass |

```
Repair diagnostics (reported, not gated):
  repair-closed-keyed (vs repair-closed-whole) fusion=false keys=1000 repair_ms_p95: baseline=16.3 candidate=13.45 ratio=0.825
  repair-closed-keyed (vs repair-closed-whole) fusion=false keys=1000 replayed_rows_median: baseline=1999 candidate=1000 ratio=0.500
  repair-closed-keyed (vs repair-closed-whole) fusion=false keys=1000 corrected_rows_median: baseline=3.812e+04 candidate=3.812e+04 ratio=1.000
  repair-closed-keyed (vs repair-closed-whole) fusion=false keys=1000 route: baseline=[resume from anchor/resume cheaperx450] candidate=[resume from anchor/resume cheaperx450]
  repair-closed-keyed (vs repair-closed-whole) fusion=false keys=1000 keyed_segments per run: baseline=0.0 candidate=90.0
  repair-closed-keyed (vs repair-closed-whole) fusion=false keys=1000 oracle: baseline=Counter({'match': 5}) candidate=Counter({'match': 5})
  repair-closed-keyed (vs repair-closed-whole) fusion=false keys=10000 repair_ms_p95: baseline=33.26 candidate=15.63 ratio=0.470
  repair-closed-keyed (vs repair-closed-whole) fusion=false keys=10000 replayed_rows_median: baseline=1999 candidate=1000 ratio=0.500
  repair-closed-keyed (vs repair-closed-whole) fusion=false keys=10000 corrected_rows_median: baseline=4.262e+04 candidate=4.262e+04 ratio=1.000
  repair-closed-keyed (vs repair-closed-whole) fusion=false keys=10000 route: baseline=[resume from anchor/resume cheaperx450] candidate=[resume from anchor/resume cheaperx450]
  repair-closed-keyed (vs repair-closed-whole) fusion=false keys=10000 keyed_segments per run: baseline=0.0 candidate=90.0
  repair-closed-keyed (vs repair-closed-whole) fusion=false keys=10000 oracle: baseline=Counter({'match': 5}) candidate=Counter({'match': 5})
  repair-closed-keyed (vs repair-closed-whole) fusion=true keys=1000 repair_ms_p95: baseline=14.08 candidate=12.31 ratio=0.874
  repair-closed-keyed (vs repair-closed-whole) fusion=true keys=1000 replayed_rows_median: baseline=1999 candidate=1000 ratio=0.500
  repair-closed-keyed (vs repair-closed-whole) fusion=true keys=1000 corrected_rows_median: baseline=3.812e+04 candidate=3.812e+04 ratio=1.000
  repair-closed-keyed (vs repair-closed-whole) fusion=true keys=1000 route: baseline=[resume from anchor/resume cheaperx450] candidate=[resume from anchor/resume cheaperx450]
  repair-closed-keyed (vs repair-closed-whole) fusion=true keys=1000 keyed_segments per run: baseline=0.0 candidate=90.0
  repair-closed-keyed (vs repair-closed-whole) fusion=true keys=1000 oracle: baseline=Counter({'match': 5}) candidate=Counter({'match': 5})
  repair-closed-keyed (vs repair-closed-whole) fusion=true keys=10000 repair_ms_p95: baseline=21.88 candidate=14.82 ratio=0.677
  repair-closed-keyed (vs repair-closed-whole) fusion=true keys=10000 replayed_rows_median: baseline=1999 candidate=1000 ratio=0.500
  repair-closed-keyed (vs repair-closed-whole) fusion=true keys=10000 corrected_rows_median: baseline=4.262e+04 candidate=4.262e+04 ratio=1.000
  repair-closed-keyed (vs repair-closed-whole) fusion=true keys=10000 route: baseline=[resume from anchor/resume cheaperx450] candidate=[resume from anchor/resume cheaperx450]
  repair-closed-keyed (vs repair-closed-whole) fusion=true keys=10000 keyed_segments per run: baseline=0.0 candidate=90.0
  repair-closed-keyed (vs repair-closed-whole) fusion=true keys=10000 oracle: baseline=Counter({'match': 5}) candidate=Counter({'match': 5})
  repair-closed-whole fusion=false keys=1000 repair_ms_p95: baseline=16.3 candidate=13.73 ratio=0.842
  repair-closed-whole fusion=false keys=1000 replayed_rows_median: baseline=1999 candidate=1999 ratio=1.000
  repair-closed-whole fusion=false keys=1000 corrected_rows_median: baseline=3.812e+04 candidate=3.812e+04 ratio=1.000
  repair-closed-whole fusion=false keys=1000 route: baseline=[resume from anchor/resume cheaperx450] candidate=[resume from anchor/resume cheaperx450]
  repair-closed-whole fusion=false keys=1000 keyed_segments per run: baseline=0.0 candidate=0.0
  repair-closed-whole fusion=false keys=1000 oracle: baseline=Counter({'match': 5}) candidate=Counter({'match': 5})
  repair-closed-whole fusion=false keys=10000 repair_ms_p95: baseline=33.26 candidate=22.46 ratio=0.675
  repair-closed-whole fusion=false keys=10000 replayed_rows_median: baseline=1999 candidate=1999 ratio=1.000
  repair-closed-whole fusion=false keys=10000 corrected_rows_median: baseline=4.262e+04 candidate=4.262e+04 ratio=1.000
  repair-closed-whole fusion=false keys=10000 route: baseline=[resume from anchor/resume cheaperx450] candidate=[resume from anchor/resume cheaperx450]
  repair-closed-whole fusion=false keys=10000 keyed_segments per run: baseline=0.0 candidate=0.0
  repair-closed-whole fusion=false keys=10000 oracle: baseline=Counter({'match': 5}) candidate=Counter({'match': 5})
  repair-closed-whole fusion=true keys=1000 repair_ms_p95: baseline=14.08 candidate=15.15 ratio=1.076
  repair-closed-whole fusion=true keys=1000 replayed_rows_median: baseline=1999 candidate=1999 ratio=1.000
  repair-closed-whole fusion=true keys=1000 corrected_rows_median: baseline=3.812e+04 candidate=3.812e+04 ratio=1.000
  repair-closed-whole fusion=true keys=1000 route: baseline=[resume from anchor/resume cheaperx450] candidate=[resume from anchor/resume cheaperx450]
  repair-closed-whole fusion=true keys=1000 keyed_segments per run: baseline=0.0 candidate=0.0
  repair-closed-whole fusion=true keys=1000 oracle: baseline=Counter({'match': 5}) candidate=Counter({'match': 5})
  repair-closed-whole fusion=true keys=10000 repair_ms_p95: baseline=21.88 candidate=21.52 ratio=0.984
  repair-closed-whole fusion=true keys=10000 replayed_rows_median: baseline=1999 candidate=1999 ratio=1.000
  repair-closed-whole fusion=true keys=10000 corrected_rows_median: baseline=4.262e+04 candidate=4.262e+04 ratio=1.000
  repair-closed-whole fusion=true keys=10000 route: baseline=[resume from anchor/resume cheaperx450] candidate=[resume from anchor/resume cheaperx450]
  repair-closed-whole fusion=true keys=10000 keyed_segments per run: baseline=0.0 candidate=0.0
  repair-closed-whole fusion=true keys=10000 oracle: baseline=Counter({'match': 5}) candidate=Counter({'match': 5})

28 failed or incomplete gate(s)
```

## Appendix C: every gate of the third measurement

The churn and cold-restore cells, 15 runs where re-measured and 5 otherwise. A
`FAIL` on an anchor-only churn cell is covered by revised requirement 4, on
`meta_bytes_median` by the +86-byte allowance, and on the two residual-heavy
one-shots is the inconclusive reading described above.

```
| Shape | Fusion | Keys | Metric | Baseline | Candidate | Ratio | Limit | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| anchor-only-decimal-churn | false | 10000 | seal_ms_median | 4.74 | 4.918 | 1.038 | 1.05 | pass |
| anchor-only-decimal-churn | false | 10000 | seal_ms_p95 | 20.64 | 20.58 | 0.997 | 1.10 | pass |
| anchor-only-decimal-churn | false | 10000 | refresh_ms_median | 8.608 | 8.589 | 0.998 | 1.05 | pass |
| anchor-only-decimal-churn | false | 10000 | rows_per_sec_median | 1.162e+05 | 1.164e+05 | 1.002 | 0.95 | pass |
| anchor-only-decimal-churn | false | 10000 | refresh_peak_mb_median | 4.4 | 4.4 | 1.000 | 1.05 | pass |
| anchor-only-decimal-churn | false | 10000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| anchor-only-decimal-churn | false | 10000 | state_bytes_last | 1.695e+06 | 1.695e+06 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | false | 10000 | meta_bytes_median | 1.617e+05 | 1.618e+05 | 1.001 | 1.00 | FAIL |
| anchor-only-decimal-churn | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | false | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | false | 10000 | complete_seal_ms | 29.51 | 30.98 | 1.050 | 1.05 | pass |
| anchor-only-decimal-churn | false | 10000 | restore_ms | 28.78 | 28.77 | 1.000 | 1.05 | pass |
| anchor-only-decimal-churn | false | 10000 | first_reseal_ms | 8.559 | 8.639 | 1.009 | 1.05 | pass |
| anchor-only-decimal-churn | false | 10000 | swept_seal_ms_median | 20.66 | 20.65 | 1.000 | 1.05 | pass |
| anchor-only-decimal-churn | false | 100000 | seal_ms_median | 3.883 | 4.332 | 1.116 | 1.05 | FAIL |
| anchor-only-decimal-churn | false | 100000 | seal_ms_p95 | 4.481 | 5.031 | 1.123 | 1.10 | FAIL |
| anchor-only-decimal-churn | false | 100000 | refresh_ms_median | 6.58 | 7.111 | 1.081 | 1.05 | FAIL |
| anchor-only-decimal-churn | false | 100000 | rows_per_sec_median | 1.52e+05 | 1.406e+05 | 0.925 | 0.95 | FAIL |
| anchor-only-decimal-churn | false | 100000 | refresh_peak_mb_median | 2.2 | 2.2 | 1.000 | 1.05 | pass |
| anchor-only-decimal-churn | false | 100000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| anchor-only-decimal-churn | false | 100000 | state_bytes_last | 1.695e+07 | 1.695e+07 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | false | 100000 | meta_bytes_median | 1.871e+05 | 1.872e+05 | 1.000 | 1.00 | FAIL |
| anchor-only-decimal-churn | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | false | 100000 | meta_segs_total | 3000 | 3000 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | false | 100000 | complete_seal_ms | 233.9 | 254.5 | 1.088 | 1.05 | FAIL |
| anchor-only-decimal-churn | false | 100000 | restore_ms | 135 | 128.6 | 0.952 | 1.05 | pass |
| anchor-only-decimal-churn | false | 100000 | first_reseal_ms | 17.66 | 17.62 | 0.998 | 1.05 | pass |
| anchor-only-decimal-churn | false | 100000 | swept_seal_ms_median | 125.9 | 128.7 | 1.022 | 1.05 | pass |
| anchor-only-decimal-churn | true | 10000 | seal_ms_median | 4.715 | 4.983 | 1.057 | 1.05 | FAIL |
| anchor-only-decimal-churn | true | 10000 | seal_ms_p95 | 20.58 | 20.57 | 1.000 | 1.10 | pass |
| anchor-only-decimal-churn | true | 10000 | refresh_ms_median | 8.596 | 8.704 | 1.013 | 1.05 | pass |
| anchor-only-decimal-churn | true | 10000 | rows_per_sec_median | 1.163e+05 | 1.149e+05 | 0.988 | 0.95 | pass |
| anchor-only-decimal-churn | true | 10000 | refresh_peak_mb_median | 4.4 | 4.4 | 1.000 | 1.05 | pass |
| anchor-only-decimal-churn | true | 10000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| anchor-only-decimal-churn | true | 10000 | state_bytes_last | 1.695e+06 | 1.695e+06 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | true | 10000 | meta_bytes_median | 1.617e+05 | 1.618e+05 | 1.001 | 1.00 | FAIL |
| anchor-only-decimal-churn | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | true | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | true | 10000 | complete_seal_ms | 29.11 | 30.21 | 1.038 | 1.05 | pass |
| anchor-only-decimal-churn | true | 10000 | restore_ms | 28.66 | 28.54 | 0.996 | 1.05 | pass |
| anchor-only-decimal-churn | true | 10000 | first_reseal_ms | 8.916 | 8.605 | 0.965 | 1.05 | pass |
| anchor-only-decimal-churn | true | 10000 | swept_seal_ms_median | 20.79 | 20.58 | 0.990 | 1.05 | pass |
| anchor-only-decimal-churn | true | 100000 | seal_ms_median | 3.939 | 4.329 | 1.099 | 1.05 | FAIL |
| anchor-only-decimal-churn | true | 100000 | seal_ms_p95 | 4.546 | 5.15 | 1.133 | 1.10 | FAIL |
| anchor-only-decimal-churn | true | 100000 | refresh_ms_median | 6.562 | 7.036 | 1.072 | 1.05 | FAIL |
| anchor-only-decimal-churn | true | 100000 | rows_per_sec_median | 1.524e+05 | 1.421e+05 | 0.933 | 0.95 | FAIL |
| anchor-only-decimal-churn | true | 100000 | refresh_peak_mb_median | 2.2 | 2.2 | 1.000 | 1.05 | pass |
| anchor-only-decimal-churn | true | 100000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| anchor-only-decimal-churn | true | 100000 | state_bytes_last | 1.695e+07 | 1.695e+07 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | true | 100000 | meta_bytes_median | 1.871e+05 | 1.872e+05 | 1.000 | 1.00 | FAIL |
| anchor-only-decimal-churn | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | true | 100000 | meta_segs_total | 3000 | 3000 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-decimal-churn | true | 100000 | complete_seal_ms | 239.2 | 223.3 | 0.933 | 1.05 | pass |
| anchor-only-decimal-churn | true | 100000 | restore_ms | 126.4 | 126.9 | 1.004 | 1.05 | pass |
| anchor-only-decimal-churn | true | 100000 | first_reseal_ms | 17.7 | 17.78 | 1.005 | 1.05 | pass |
| anchor-only-decimal-churn | true | 100000 | swept_seal_ms_median | 127 | 124.8 | 0.983 | 1.05 | pass |
| anchor-only-decimal-cold-restore | false | 10000 | restore_ms | 35.04 | 36.6 | 1.044 | 1.05 | pass |
| anchor-only-decimal-cold-restore | false | 10000 | first_reseal_ms | 16.52 | 7.714 | 0.467 | 1.05 | pass |
| anchor-only-decimal-cold-restore | false | 100000 | restore_ms | 109.9 | 107.6 | 0.979 | 1.05 | pass |
| anchor-only-decimal-cold-restore | false | 100000 | first_reseal_ms | 19.36 | 5.403 | 0.279 | 1.05 | pass |
| anchor-only-decimal-cold-restore | true | 10000 | restore_ms | 34.06 | 34.85 | 1.023 | 1.05 | pass |
| anchor-only-decimal-cold-restore | true | 10000 | first_reseal_ms | 16.29 | 7.495 | 0.460 | 1.05 | pass |
| anchor-only-decimal-cold-restore | true | 100000 | restore_ms | 117.3 | 110 | 0.938 | 1.05 | pass |
| anchor-only-decimal-cold-restore | true | 100000 | first_reseal_ms | 18.53 | 5.33 | 0.288 | 1.05 | pass |
| anchor-only-unfused-control-churn | false | 10000 | seal_ms_median | 4.031 | 4.912 | 1.218 | 1.05 | FAIL |
| anchor-only-unfused-control-churn | false | 10000 | seal_ms_p95 | 16.62 | 20.45 | 1.230 | 1.10 | FAIL |
| anchor-only-unfused-control-churn | false | 10000 | refresh_ms_median | 7.813 | 8.684 | 1.111 | 1.05 | FAIL |
| anchor-only-unfused-control-churn | false | 10000 | rows_per_sec_median | 1.28e+05 | 1.152e+05 | 0.900 | 0.95 | FAIL |
| anchor-only-unfused-control-churn | false | 10000 | refresh_peak_mb_median | 4.4 | 4.4 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control-churn | false | 10000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control-churn | false | 10000 | state_bytes_last | 1.44e+06 | 1.44e+06 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | false | 10000 | meta_bytes_median | 1.438e+05 | 1.439e+05 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control-churn | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | false | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | false | 10000 | complete_seal_ms | 28.31 | 30.06 | 1.062 | 1.05 | FAIL |
| anchor-only-unfused-control-churn | false | 10000 | restore_ms | 27.7 | 27.87 | 1.006 | 1.05 | pass |
| anchor-only-unfused-control-churn | false | 10000 | first_reseal_ms | 8.307 | 9.146 | 1.101 | 1.05 | FAIL |
| anchor-only-unfused-control-churn | false | 10000 | swept_seal_ms_median | 16.71 | 20.68 | 1.238 | 1.05 | FAIL |
| anchor-only-unfused-control-churn | false | 100000 | seal_ms_median | 3.846 | 4.267 | 1.109 | 1.05 | FAIL |
| anchor-only-unfused-control-churn | false | 100000 | seal_ms_p95 | 4.425 | 4.86 | 1.098 | 1.10 | pass |
| anchor-only-unfused-control-churn | false | 100000 | refresh_ms_median | 6.405 | 6.96 | 1.087 | 1.05 | FAIL |
| anchor-only-unfused-control-churn | false | 100000 | rows_per_sec_median | 1.561e+05 | 1.437e+05 | 0.920 | 0.95 | FAIL |
| anchor-only-unfused-control-churn | false | 100000 | refresh_peak_mb_median | 2.2 | 2.2 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control-churn | false | 100000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control-churn | false | 100000 | state_bytes_last | 1.44e+07 | 1.44e+07 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | false | 100000 | meta_bytes_median | 1.696e+05 | 1.697e+05 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control-churn | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | false | 100000 | meta_segs_total | 3000 | 3000 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | false | 100000 | complete_seal_ms | 227.4 | 248.2 | 1.091 | 1.05 | FAIL |
| anchor-only-unfused-control-churn | false | 100000 | restore_ms | 119.1 | 120.8 | 1.015 | 1.05 | pass |
| anchor-only-unfused-control-churn | false | 100000 | first_reseal_ms | 19 | 17.58 | 0.925 | 1.05 | pass |
| anchor-only-unfused-control-churn | false | 100000 | swept_seal_ms_median | 120.3 | 121.2 | 1.008 | 1.05 | pass |
| anchor-only-unfused-control-churn | true | 10000 | seal_ms_median | 4.087 | 4.256 | 1.041 | 1.05 | pass |
| anchor-only-unfused-control-churn | true | 10000 | seal_ms_p95 | 17.48 | 17.77 | 1.017 | 1.10 | pass |
| anchor-only-unfused-control-churn | true | 10000 | refresh_ms_median | 7.912 | 8.173 | 1.033 | 1.05 | pass |
| anchor-only-unfused-control-churn | true | 10000 | rows_per_sec_median | 1.264e+05 | 1.224e+05 | 0.968 | 0.95 | pass |
| anchor-only-unfused-control-churn | true | 10000 | refresh_peak_mb_median | 4.4 | 4.4 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control-churn | true | 10000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control-churn | true | 10000 | state_bytes_last | 1.44e+06 | 1.44e+06 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | true | 10000 | meta_bytes_median | 1.438e+05 | 1.439e+05 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control-churn | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | true | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | true | 10000 | complete_seal_ms | 27.36 | 29.24 | 1.069 | 1.05 | FAIL |
| anchor-only-unfused-control-churn | true | 10000 | restore_ms | 27.45 | 29.39 | 1.071 | 1.05 | FAIL |
| anchor-only-unfused-control-churn | true | 10000 | first_reseal_ms | 8.419 | 8.957 | 1.064 | 1.05 | FAIL |
| anchor-only-unfused-control-churn | true | 10000 | swept_seal_ms_median | 17.51 | 17.95 | 1.025 | 1.05 | pass |
| anchor-only-unfused-control-churn | true | 100000 | seal_ms_median | 3.853 | 4.237 | 1.100 | 1.05 | FAIL |
| anchor-only-unfused-control-churn | true | 100000 | seal_ms_p95 | 4.468 | 4.919 | 1.101 | 1.10 | FAIL |
| anchor-only-unfused-control-churn | true | 100000 | refresh_ms_median | 6.394 | 6.88 | 1.076 | 1.05 | FAIL |
| anchor-only-unfused-control-churn | true | 100000 | rows_per_sec_median | 1.564e+05 | 1.454e+05 | 0.929 | 0.95 | FAIL |
| anchor-only-unfused-control-churn | true | 100000 | refresh_peak_mb_median | 2.2 | 2.2 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control-churn | true | 100000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| anchor-only-unfused-control-churn | true | 100000 | state_bytes_last | 1.44e+07 | 1.44e+07 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | true | 100000 | meta_bytes_median | 1.696e+05 | 1.697e+05 | 1.001 | 1.00 | FAIL |
| anchor-only-unfused-control-churn | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | true | 100000 | meta_segs_total | 3000 | 3000 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| anchor-only-unfused-control-churn | true | 100000 | complete_seal_ms | 231.2 | 218.4 | 0.945 | 1.05 | pass |
| anchor-only-unfused-control-churn | true | 100000 | restore_ms | 116 | 117.4 | 1.012 | 1.05 | pass |
| anchor-only-unfused-control-churn | true | 100000 | first_reseal_ms | 17.58 | 17.57 | 0.999 | 1.05 | pass |
| anchor-only-unfused-control-churn | true | 100000 | swept_seal_ms_median | 121.2 | 123.4 | 1.018 | 1.05 | pass |
| anchor-only-unfused-control-cold-restore | false | 10000 | restore_ms | 32.53 | 33.3 | 1.024 | 1.05 | pass |
| anchor-only-unfused-control-cold-restore | false | 10000 | first_reseal_ms | 16.52 | 7.283 | 0.441 | 1.05 | pass |
| anchor-only-unfused-control-cold-restore | false | 100000 | restore_ms | 105.3 | 103.3 | 0.981 | 1.05 | pass |
| anchor-only-unfused-control-cold-restore | false | 100000 | first_reseal_ms | 22.26 | 5.159 | 0.232 | 1.05 | pass |
| anchor-only-unfused-control-cold-restore | true | 10000 | restore_ms | 33.69 | 34.39 | 1.021 | 1.05 | pass |
| anchor-only-unfused-control-cold-restore | true | 10000 | first_reseal_ms | 16.26 | 7.433 | 0.457 | 1.05 | pass |
| anchor-only-unfused-control-cold-restore | true | 100000 | restore_ms | 105.5 | 101.3 | 0.960 | 1.05 | pass |
| anchor-only-unfused-control-cold-restore | true | 100000 | first_reseal_ms | 18.91 | 5.206 | 0.275 | 1.05 | pass |
| narrow-count-star-key-churn | false | 10000 | seal_ms_median | 3.849 | 2.246 | 0.584 | 1.05 | pass |
| narrow-count-star-key-churn | false | 10000 | seal_ms_p95 | 18.64 | 6.1 | 0.327 | 1.10 | pass |
| narrow-count-star-key-churn | false | 10000 | refresh_ms_median | 7.723 | 6.255 | 0.810 | 1.05 | pass |
| narrow-count-star-key-churn | false | 10000 | rows_per_sec_median | 1.295e+05 | 1.599e+05 | 1.235 | 0.95 | pass |
| narrow-count-star-key-churn | false | 10000 | refresh_peak_mb_median | 4.5 | 4.5 | 1.000 | 1.05 | pass |
| narrow-count-star-key-churn | false | 10000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| narrow-count-star-key-churn | false | 10000 | state_bytes_last | 1.881e+06 | 7.41e+05 | 0.394 | 1.00 | pass |
| narrow-count-star-key-churn | false | 10000 | meta_bytes_median | 1.424e+05 | 7.621e+04 | 0.535 | 1.00 | pass |
| narrow-count-star-key-churn | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | false | 10000 | meta_segs_total | 600 | 400 | 0.667 | 1.00 | pass |
| narrow-count-star-key-churn | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | false | 10000 | complete_seal_ms | 38.23 | 15.82 | 0.414 | 1.05 | pass |
| narrow-count-star-key-churn | false | 10000 | restore_ms | 34.47 | 24.01 | 0.697 | 1.05 | pass |
| narrow-count-star-key-churn | false | 10000 | first_reseal_ms | 9.126 | 5.012 | 0.549 | 1.05 | pass |
| narrow-count-star-key-churn | false | 10000 | swept_seal_ms_median | 18.83 | 6.197 | 0.329 | 1.05 | pass |
| narrow-count-star-key-churn | false | 100000 | seal_ms_median | 4.86 | 2.196 | 0.452 | 1.05 | pass |
| narrow-count-star-key-churn | false | 100000 | seal_ms_p95 | 5.408 | 2.593 | 0.479 | 1.10 | pass |
| narrow-count-star-key-churn | false | 100000 | refresh_ms_median | 7.316 | 4.889 | 0.668 | 1.05 | pass |
| narrow-count-star-key-churn | false | 100000 | rows_per_sec_median | 1.367e+05 | 2.045e+05 | 1.497 | 0.95 | pass |
| narrow-count-star-key-churn | false | 100000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| narrow-count-star-key-churn | false | 100000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| narrow-count-star-key-churn | false | 100000 | state_bytes_last | 1.881e+07 | 7.41e+06 | 0.394 | 1.00 | pass |
| narrow-count-star-key-churn | false | 100000 | meta_bytes_median | 2.111e+05 | 9.286e+04 | 0.440 | 1.00 | pass |
| narrow-count-star-key-churn | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | false | 100000 | meta_segs_total | 3600 | 2400 | 0.667 | 1.00 | pass |
| narrow-count-star-key-churn | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | false | 100000 | complete_seal_ms | 302.3 | 129.8 | 0.429 | 1.05 | pass |
| narrow-count-star-key-churn | false | 100000 | restore_ms | 154.8 | 85.73 | 0.554 | 1.05 | pass |
| narrow-count-star-key-churn | false | 100000 | first_reseal_ms | 19.63 | 10.21 | 0.520 | 1.05 | pass |
| narrow-count-star-key-churn | false | 100000 | swept_seal_ms_median | 174.1 | 44.46 | 0.255 | 1.05 | pass |
| narrow-count-star-key-churn | true | 10000 | seal_ms_median | 2.184 | 2.237 | 1.024 | 1.05 | pass |
| narrow-count-star-key-churn | true | 10000 | seal_ms_p95 | 6.151 | 6.253 | 1.017 | 1.10 | pass |
| narrow-count-star-key-churn | true | 10000 | refresh_ms_median | 5.725 | 5.72 | 0.999 | 1.05 | pass |
| narrow-count-star-key-churn | true | 10000 | rows_per_sec_median | 1.747e+05 | 1.748e+05 | 1.001 | 0.95 | pass |
| narrow-count-star-key-churn | true | 10000 | refresh_peak_mb_median | 4.3 | 4.3 | 1.000 | 1.05 | pass |
| narrow-count-star-key-churn | true | 10000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| narrow-count-star-key-churn | true | 10000 | state_bytes_last | 7.41e+05 | 7.41e+05 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | true | 10000 | meta_bytes_median | 7.621e+04 | 7.621e+04 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | true | 10000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | true | 10000 | complete_seal_ms | 15.67 | 15.86 | 1.012 | 1.05 | pass |
| narrow-count-star-key-churn | true | 10000 | restore_ms | 22.93 | 21.87 | 0.954 | 1.05 | pass |
| narrow-count-star-key-churn | true | 10000 | first_reseal_ms | 5.223 | 4.962 | 0.950 | 1.05 | pass |
| narrow-count-star-key-churn | true | 10000 | swept_seal_ms_median | 6.159 | 6.265 | 1.017 | 1.05 | pass |
| narrow-count-star-key-churn | true | 100000 | seal_ms_median | 2.147 | 2.174 | 1.013 | 1.05 | pass |
| narrow-count-star-key-churn | true | 100000 | seal_ms_p95 | 2.53 | 2.588 | 1.023 | 1.10 | pass |
| narrow-count-star-key-churn | true | 100000 | refresh_ms_median | 4.306 | 4.455 | 1.035 | 1.05 | pass |
| narrow-count-star-key-churn | true | 100000 | rows_per_sec_median | 2.322e+05 | 2.245e+05 | 0.966 | 0.95 | pass |
| narrow-count-star-key-churn | true | 100000 | refresh_peak_mb_median | 2 | 2 | 1.000 | 1.05 | pass |
| narrow-count-star-key-churn | true | 100000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| narrow-count-star-key-churn | true | 100000 | state_bytes_last | 7.41e+06 | 7.41e+06 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | true | 100000 | meta_bytes_median | 9.286e+04 | 9.286e+04 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | true | 100000 | meta_segs_total | 2400 | 2400 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-count-star-key-churn | true | 100000 | complete_seal_ms | 117.3 | 121.3 | 1.034 | 1.05 | pass |
| narrow-count-star-key-churn | true | 100000 | restore_ms | 68.76 | 71.8 | 1.044 | 1.05 | pass |
| narrow-count-star-key-churn | true | 100000 | first_reseal_ms | 10.45 | 10.24 | 0.981 | 1.05 | pass |
| narrow-count-star-key-churn | true | 100000 | swept_seal_ms_median | 45.58 | 45.52 | 0.999 | 1.05 | pass |
| narrow-count-star-key-cold-restore | false | 10000 | restore_ms | 40.52 | 30.83 | 0.761 | 1.05 | pass |
| narrow-count-star-key-cold-restore | false | 10000 | first_reseal_ms | 18.45 | 5.392 | 0.292 | 1.05 | pass |
| narrow-count-star-key-cold-restore | false | 100000 | restore_ms | 150 | 82.94 | 0.553 | 1.05 | pass |
| narrow-count-star-key-cold-restore | false | 100000 | first_reseal_ms | 20.34 | 4.233 | 0.208 | 1.05 | pass |
| narrow-count-star-key-cold-restore | true | 10000 | restore_ms | 28.77 | 29.5 | 1.025 | 1.05 | pass |
| narrow-count-star-key-cold-restore | true | 10000 | first_reseal_ms | 5.23 | 5.341 | 1.021 | 1.05 | pass |
| narrow-count-star-key-cold-restore | true | 100000 | restore_ms | 70.98 | 69.81 | 0.983 | 1.05 | pass |
| narrow-count-star-key-cold-restore | true | 100000 | first_reseal_ms | 4.219 | 4.204 | 0.996 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 10000 | seal_ms_median | 8.053 | 2.314 | 0.287 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 10000 | seal_ms_p95 | 41.96 | 6.368 | 0.152 | 1.10 | pass |
| narrow-sum-avg-count-churn | false | 10000 | refresh_ms_median | 12.42 | 6.749 | 0.543 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 10000 | rows_per_sec_median | 8.049e+04 | 1.482e+05 | 1.841 | 0.95 | pass |
| narrow-sum-avg-count-churn | false | 10000 | refresh_peak_mb_median | 4.6 | 4.6 | 1.000 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 10000 | alloc_mb_median | 0.19 | 0.18 | 0.947 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 10000 | state_bytes_last | 2.88e+06 | 9e+05 | 0.312 | 1.00 | pass |
| narrow-sum-avg-count-churn | false | 10000 | meta_bytes_median | 2.642e+05 | 8.65e+04 | 0.327 | 1.00 | pass |
| narrow-sum-avg-count-churn | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | false | 10000 | meta_segs_total | 700 | 400 | 0.571 | 1.00 | pass |
| narrow-sum-avg-count-churn | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | false | 10000 | complete_seal_ms | 50.11 | 17.63 | 0.352 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 10000 | restore_ms | 39.36 | 28.02 | 0.712 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 10000 | first_reseal_ms | 13.59 | 4.744 | 0.349 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 10000 | swept_seal_ms_median | 42.37 | 6.389 | 0.151 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 100000 | seal_ms_median | 7.257 | 2.272 | 0.313 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 100000 | seal_ms_p95 | 7.941 | 2.681 | 0.338 | 1.10 | pass |
| narrow-sum-avg-count-churn | false | 100000 | refresh_ms_median | 10.53 | 5.353 | 0.508 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 100000 | rows_per_sec_median | 9.498e+04 | 1.868e+05 | 1.967 | 0.95 | pass |
| narrow-sum-avg-count-churn | false | 100000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 100000 | alloc_mb_median | 0.19 | 0.18 | 0.947 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 100000 | state_bytes_last | 2.88e+07 | 9e+06 | 0.312 | 1.00 | pass |
| narrow-sum-avg-count-churn | false | 100000 | meta_bytes_median | 3.024e+05 | 1.029e+05 | 0.340 | 1.00 | pass |
| narrow-sum-avg-count-churn | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | false | 100000 | meta_segs_total | 4200 | 2400 | 0.571 | 1.00 | pass |
| narrow-sum-avg-count-churn | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | false | 100000 | complete_seal_ms | 408.7 | 111.9 | 0.274 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 100000 | restore_ms | 227.2 | 115.1 | 0.507 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 100000 | first_reseal_ms | 26.74 | 10.28 | 0.384 | 1.05 | pass |
| narrow-sum-avg-count-churn | false | 100000 | swept_seal_ms_median | 260.4 | 46.36 | 0.178 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 10000 | seal_ms_median | 2.288 | 2.289 | 1.000 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 10000 | seal_ms_p95 | 6.207 | 6.171 | 0.994 | 1.10 | pass |
| narrow-sum-avg-count-churn | true | 10000 | refresh_ms_median | 6.097 | 6.03 | 0.989 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 10000 | rows_per_sec_median | 1.64e+05 | 1.658e+05 | 1.011 | 0.95 | pass |
| narrow-sum-avg-count-churn | true | 10000 | refresh_peak_mb_median | 4.6 | 4.6 | 1.000 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 10000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 10000 | state_bytes_last | 9e+05 | 9e+05 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | true | 10000 | meta_bytes_median | 8.65e+04 | 8.65e+04 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | true | 10000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | true | 10000 | complete_seal_ms | 16.89 | 16.74 | 0.991 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 10000 | restore_ms | 23.79 | 22.87 | 0.961 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 10000 | first_reseal_ms | 4.826 | 4.802 | 0.995 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 10000 | swept_seal_ms_median | 6.228 | 6.26 | 1.005 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 100000 | seal_ms_median | 2.215 | 2.226 | 1.005 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 100000 | seal_ms_p95 | 2.614 | 2.664 | 1.019 | 1.10 | pass |
| narrow-sum-avg-count-churn | true | 100000 | refresh_ms_median | 4.55 | 4.588 | 1.008 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 100000 | rows_per_sec_median | 2.198e+05 | 2.18e+05 | 0.992 | 0.95 | pass |
| narrow-sum-avg-count-churn | true | 100000 | refresh_peak_mb_median | 2.3 | 2.3 | 1.000 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 100000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 100000 | state_bytes_last | 9e+06 | 9e+06 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | true | 100000 | meta_bytes_median | 1.029e+05 | 1.029e+05 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | true | 100000 | meta_segs_total | 2400 | 2400 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-avg-count-churn | true | 100000 | complete_seal_ms | 130 | 133.7 | 1.029 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 100000 | restore_ms | 73.59 | 75.63 | 1.028 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 100000 | first_reseal_ms | 10.44 | 10.6 | 1.015 | 1.05 | pass |
| narrow-sum-avg-count-churn | true | 100000 | swept_seal_ms_median | 46.84 | 48.83 | 1.042 | 1.05 | pass |
| narrow-sum-avg-count-cold-restore | false | 10000 | restore_ms | 49.67 | 33.69 | 0.678 | 1.05 | pass |
| narrow-sum-avg-count-cold-restore | false | 10000 | first_reseal_ms | 23.43 | 5.296 | 0.226 | 1.05 | pass |
| narrow-sum-avg-count-cold-restore | false | 100000 | restore_ms | 209.5 | 100.8 | 0.481 | 1.05 | pass |
| narrow-sum-avg-count-cold-restore | false | 100000 | first_reseal_ms | 21.53 | 4.367 | 0.203 | 1.05 | pass |
| narrow-sum-avg-count-cold-restore | true | 10000 | restore_ms | 30.42 | 29.59 | 0.973 | 1.05 | pass |
| narrow-sum-avg-count-cold-restore | true | 10000 | first_reseal_ms | 5.296 | 5.265 | 0.994 | 1.05 | pass |
| narrow-sum-avg-count-cold-restore | true | 100000 | restore_ms | 74.64 | 76.77 | 1.029 | 1.05 | pass |
| narrow-sum-avg-count-cold-restore | true | 100000 | first_reseal_ms | 4.246 | 4.306 | 1.014 | 1.05 | pass |
| narrow-sum-churn | false | 10000 | seal_ms_median | 4.601 | 2.309 | 0.502 | 1.05 | pass |
| narrow-sum-churn | false | 10000 | seal_ms_p95 | 20.17 | 6.274 | 0.311 | 1.10 | pass |
| narrow-sum-churn | false | 10000 | refresh_ms_median | 8.275 | 6.076 | 0.734 | 1.05 | pass |
| narrow-sum-churn | false | 10000 | rows_per_sec_median | 1.208e+05 | 1.646e+05 | 1.362 | 0.95 | pass |
| narrow-sum-churn | false | 10000 | refresh_peak_mb_median | 4.4 | 4.4 | 1.000 | 1.05 | pass |
| narrow-sum-churn | false | 10000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| narrow-sum-churn | false | 10000 | state_bytes_last | 1.44e+06 | 9e+05 | 0.625 | 1.00 | pass |
| narrow-sum-churn | false | 10000 | meta_bytes_median | 1.438e+05 | 8.65e+04 | 0.602 | 1.00 | pass |
| narrow-sum-churn | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-churn | false | 10000 | meta_segs_total | 500 | 400 | 0.800 | 1.00 | pass |
| narrow-sum-churn | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-churn | false | 10000 | complete_seal_ms | 26.98 | 17.18 | 0.637 | 1.05 | pass |
| narrow-sum-churn | false | 10000 | restore_ms | 27.86 | 22.37 | 0.803 | 1.05 | pass |
| narrow-sum-churn | false | 10000 | first_reseal_ms | 9.05 | 4.948 | 0.547 | 1.05 | pass |
| narrow-sum-churn | false | 10000 | swept_seal_ms_median | 20.23 | 6.368 | 0.315 | 1.05 | pass |
| narrow-sum-churn | false | 100000 | seal_ms_median | 3.886 | 2.289 | 0.589 | 1.05 | pass |
| narrow-sum-churn | false | 100000 | seal_ms_p95 | 4.36 | 2.831 | 0.649 | 1.10 | pass |
| narrow-sum-churn | false | 100000 | refresh_ms_median | 6.428 | 4.883 | 0.760 | 1.05 | pass |
| narrow-sum-churn | false | 100000 | rows_per_sec_median | 1.556e+05 | 2.048e+05 | 1.316 | 0.95 | pass |
| narrow-sum-churn | false | 100000 | refresh_peak_mb_median | 2.2 | 2.2 | 1.000 | 1.05 | pass |
| narrow-sum-churn | false | 100000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| narrow-sum-churn | false | 100000 | state_bytes_last | 1.44e+07 | 9e+06 | 0.625 | 1.00 | pass |
| narrow-sum-churn | false | 100000 | meta_bytes_median | 1.696e+05 | 1.029e+05 | 0.607 | 1.00 | pass |
| narrow-sum-churn | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-churn | false | 100000 | meta_segs_total | 3000 | 2400 | 0.800 | 1.00 | pass |
| narrow-sum-churn | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-churn | false | 100000 | complete_seal_ms | 226.8 | 143.4 | 0.632 | 1.05 | pass |
| narrow-sum-churn | false | 100000 | restore_ms | 119 | 85.02 | 0.714 | 1.05 | pass |
| narrow-sum-churn | false | 100000 | first_reseal_ms | 17.51 | 10.26 | 0.586 | 1.05 | pass |
| narrow-sum-churn | false | 100000 | swept_seal_ms_median | 122.8 | 46.06 | 0.375 | 1.05 | pass |
| narrow-sum-churn | true | 10000 | seal_ms_median | 2.282 | 2.284 | 1.001 | 1.05 | pass |
| narrow-sum-churn | true | 10000 | seal_ms_p95 | 6.245 | 6.239 | 0.999 | 1.10 | pass |
| narrow-sum-churn | true | 10000 | refresh_ms_median | 5.913 | 5.928 | 1.002 | 1.05 | pass |
| narrow-sum-churn | true | 10000 | rows_per_sec_median | 1.691e+05 | 1.687e+05 | 0.998 | 0.95 | pass |
| narrow-sum-churn | true | 10000 | refresh_peak_mb_median | 4.4 | 4.3 | 0.977 | 1.05 | pass |
| narrow-sum-churn | true | 10000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| narrow-sum-churn | true | 10000 | state_bytes_last | 9e+05 | 9e+05 | 1.000 | 1.00 | pass |
| narrow-sum-churn | true | 10000 | meta_bytes_median | 8.65e+04 | 8.65e+04 | 1.000 | 1.00 | pass |
| narrow-sum-churn | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-churn | true | 10000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| narrow-sum-churn | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-churn | true | 10000 | complete_seal_ms | 16.83 | 17.5 | 1.040 | 1.05 | pass |
| narrow-sum-churn | true | 10000 | restore_ms | 22.34 | 22.59 | 1.011 | 1.05 | pass |
| narrow-sum-churn | true | 10000 | first_reseal_ms | 4.904 | 4.852 | 0.989 | 1.05 | pass |
| narrow-sum-churn | true | 10000 | swept_seal_ms_median | 6.271 | 6.269 | 1.000 | 1.05 | pass |
| narrow-sum-churn | true | 100000 | seal_ms_median | 2.223 | 2.213 | 0.996 | 1.05 | pass |
| narrow-sum-churn | true | 100000 | seal_ms_p95 | 2.555 | 2.632 | 1.030 | 1.10 | pass |
| narrow-sum-churn | true | 100000 | refresh_ms_median | 4.401 | 4.482 | 1.018 | 1.05 | pass |
| narrow-sum-churn | true | 100000 | rows_per_sec_median | 2.273e+05 | 2.232e+05 | 0.982 | 0.95 | pass |
| narrow-sum-churn | true | 100000 | refresh_peak_mb_median | 2.2 | 2.2 | 1.000 | 1.05 | pass |
| narrow-sum-churn | true | 100000 | alloc_mb_median | 0.18 | 0.18 | 1.000 | 1.05 | pass |
| narrow-sum-churn | true | 100000 | state_bytes_last | 9e+06 | 9e+06 | 1.000 | 1.00 | pass |
| narrow-sum-churn | true | 100000 | meta_bytes_median | 1.029e+05 | 1.029e+05 | 1.000 | 1.00 | pass |
| narrow-sum-churn | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-churn | true | 100000 | meta_segs_total | 2400 | 2400 | 1.000 | 1.00 | pass |
| narrow-sum-churn | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| narrow-sum-churn | true | 100000 | complete_seal_ms | 133.6 | 136.1 | 1.019 | 1.05 | pass |
| narrow-sum-churn | true | 100000 | restore_ms | 73.97 | 73.98 | 1.000 | 1.05 | pass |
| narrow-sum-churn | true | 100000 | first_reseal_ms | 10.35 | 10.68 | 1.032 | 1.05 | pass |
| narrow-sum-churn | true | 100000 | swept_seal_ms_median | 49.02 | 45.8 | 0.934 | 1.05 | pass |
| narrow-sum-cold-restore | false | 10000 | restore_ms | 33.75 | 28.17 | 0.835 | 1.05 | pass |
| narrow-sum-cold-restore | false | 10000 | first_reseal_ms | 16.18 | 5.376 | 0.332 | 1.05 | pass |
| narrow-sum-cold-restore | false | 100000 | restore_ms | 105.7 | 80.58 | 0.763 | 1.05 | pass |
| narrow-sum-cold-restore | false | 100000 | first_reseal_ms | 18.69 | 4.393 | 0.235 | 1.05 | pass |
| narrow-sum-cold-restore | true | 10000 | restore_ms | 28.91 | 28.46 | 0.984 | 1.05 | pass |
| narrow-sum-cold-restore | true | 10000 | first_reseal_ms | 5.567 | 5.479 | 0.984 | 1.05 | pass |
| narrow-sum-cold-restore | true | 100000 | restore_ms | 72.27 | 71.61 | 0.991 | 1.05 | pass |
| narrow-sum-cold-restore | true | 100000 | first_reseal_ms | 4.421 | 4.275 | 0.967 | 1.05 | pass |
| residual-heavy-churn | false | 10000 | seal_ms_median | 167.9 | 172.3 | 1.026 | 1.05 | pass |
| residual-heavy-churn | false | 10000 | seal_ms_p95 | 257.7 | 270.1 | 1.048 | 1.10 | pass |
| residual-heavy-churn | false | 10000 | refresh_ms_median | 174.1 | 177.9 | 1.022 | 1.05 | pass |
| residual-heavy-churn | false | 10000 | rows_per_sec_median | 5744 | 5620 | 0.978 | 0.95 | pass |
| residual-heavy-churn | false | 10000 | refresh_peak_mb_median | 4.6 | 4.6 | 1.000 | 1.05 | pass |
| residual-heavy-churn | false | 10000 | alloc_mb_median | 23.59 | 23.59 | 1.000 | 1.05 | pass |
| residual-heavy-churn | false | 10000 | state_bytes_last | 4.692e+07 | 4.638e+07 | 0.988 | 1.00 | pass |
| residual-heavy-churn | false | 10000 | meta_bytes_median | 7.634e+06 | 7.577e+06 | 0.993 | 1.00 | pass |
| residual-heavy-churn | false | 10000 | data_bytes_median | 1.212e+06 | 1.212e+06 | 1.000 | 1.00 | pass |
| residual-heavy-churn | false | 10000 | meta_segs_total | 800 | 700 | 0.875 | 1.00 | pass |
| residual-heavy-churn | false | 10000 | data_segs_total | 100 | 100 | 1.000 | 1.00 | pass |
| residual-heavy-churn | false | 10000 | complete_seal_ms | 71.45 | 63.52 | 0.889 | 1.05 | pass |
| residual-heavy-churn | false | 10000 | restore_ms | 168 | 167.3 | 0.996 | 1.05 | pass |
| residual-heavy-churn | false | 10000 | first_reseal_ms | 318 | 332.2 | 1.045 | 1.05 | pass |
| residual-heavy-churn | false | 10000 | swept_seal_ms_median | 179.6 | 177.1 | 0.986 | 1.05 | pass |
| residual-heavy-churn | false | 100000 | seal_ms_median | 797.8 | 798 | 1.000 | 1.05 | pass |
| residual-heavy-churn | false | 100000 | seal_ms_p95 | 1033 | 1041 | 1.008 | 1.10 | pass |
| residual-heavy-churn | false | 100000 | refresh_ms_median | 801.3 | 801.5 | 1.000 | 1.05 | pass |
| residual-heavy-churn | false | 100000 | rows_per_sec_median | 1248 | 1248 | 1.000 | 0.95 | pass |
| residual-heavy-churn | false | 100000 | refresh_peak_mb_median | 2.4 | 2.4 | 1.000 | 1.05 | pass |
| residual-heavy-churn | false | 100000 | alloc_mb_median | 100.5 | 100.5 | 1.000 | 1.05 | pass |
| residual-heavy-churn | false | 100000 | state_bytes_last | 1.774e+08 | 1.72e+08 | 0.970 | 1.00 | pass |
| residual-heavy-churn | false | 100000 | meta_bytes_median | 3.155e+07 | 3.149e+07 | 0.998 | 1.00 | pass |
| residual-heavy-churn | false | 100000 | data_bytes_median | 3.452e+06 | 3.452e+06 | 1.000 | 1.00 | pass |
| residual-heavy-churn | false | 100000 | meta_segs_total | 1600 | 1400 | 0.875 | 1.00 | pass |
| residual-heavy-churn | false | 100000 | data_segs_total | 200 | 200 | 1.000 | 1.00 | pass |
| residual-heavy-churn | false | 100000 | complete_seal_ms | 713.1 | 677.7 | 0.950 | 1.05 | pass |
| residual-heavy-churn | false | 100000 | restore_ms | 569.9 | 495.2 | 0.869 | 1.05 | pass |
| residual-heavy-churn | false | 100000 | first_reseal_ms | 1082 | 1146 | 1.059 | 1.05 | FAIL |
| residual-heavy-churn | false | 100000 | swept_seal_ms_median | 1167 | 1050 | 0.900 | 1.05 | pass |
| residual-heavy-churn | true | 10000 | seal_ms_median | 171.8 | 170.4 | 0.992 | 1.05 | pass |
| residual-heavy-churn | true | 10000 | seal_ms_p95 | 260.9 | 270.7 | 1.037 | 1.10 | pass |
| residual-heavy-churn | true | 10000 | refresh_ms_median | 176.6 | 174.9 | 0.990 | 1.05 | pass |
| residual-heavy-churn | true | 10000 | rows_per_sec_median | 5661 | 5718 | 1.010 | 0.95 | pass |
| residual-heavy-churn | true | 10000 | refresh_peak_mb_median | 4.6 | 4.6 | 1.000 | 1.05 | pass |
| residual-heavy-churn | true | 10000 | alloc_mb_median | 23.59 | 23.59 | 1.000 | 1.05 | pass |
| residual-heavy-churn | true | 10000 | state_bytes_last | 4.638e+07 | 4.638e+07 | 1.000 | 1.00 | pass |
| residual-heavy-churn | true | 10000 | meta_bytes_median | 7.577e+06 | 7.577e+06 | 1.000 | 1.00 | pass |
| residual-heavy-churn | true | 10000 | data_bytes_median | 1.212e+06 | 1.212e+06 | 1.000 | 1.00 | pass |
| residual-heavy-churn | true | 10000 | meta_segs_total | 700 | 700 | 1.000 | 1.00 | pass |
| residual-heavy-churn | true | 10000 | data_segs_total | 100 | 100 | 1.000 | 1.00 | pass |
| residual-heavy-churn | true | 10000 | complete_seal_ms | 63.08 | 60.87 | 0.965 | 1.05 | pass |
| residual-heavy-churn | true | 10000 | restore_ms | 161.8 | 159.9 | 0.988 | 1.05 | pass |
| residual-heavy-churn | true | 10000 | first_reseal_ms | 334.3 | 323 | 0.966 | 1.05 | pass |
| residual-heavy-churn | true | 10000 | swept_seal_ms_median | 172.6 | 175.1 | 1.014 | 1.05 | pass |
| residual-heavy-churn | true | 100000 | seal_ms_median | 805.4 | 805.9 | 1.001 | 1.05 | pass |
| residual-heavy-churn | true | 100000 | seal_ms_p95 | 1041 | 1044 | 1.003 | 1.10 | pass |
| residual-heavy-churn | true | 100000 | refresh_ms_median | 809.5 | 809.2 | 1.000 | 1.05 | pass |
| residual-heavy-churn | true | 100000 | rows_per_sec_median | 1235 | 1236 | 1.001 | 0.95 | pass |
| residual-heavy-churn | true | 100000 | refresh_peak_mb_median | 2.4 | 2.4 | 1.000 | 1.05 | pass |
| residual-heavy-churn | true | 100000 | alloc_mb_median | 100.5 | 100.5 | 1.000 | 1.05 | pass |
| residual-heavy-churn | true | 100000 | state_bytes_last | 1.72e+08 | 1.72e+08 | 1.000 | 1.00 | pass |
| residual-heavy-churn | true | 100000 | meta_bytes_median | 3.149e+07 | 3.149e+07 | 1.000 | 1.00 | pass |
| residual-heavy-churn | true | 100000 | data_bytes_median | 3.452e+06 | 3.452e+06 | 1.000 | 1.00 | pass |
| residual-heavy-churn | true | 100000 | meta_segs_total | 1400 | 1400 | 1.000 | 1.00 | pass |
| residual-heavy-churn | true | 100000 | data_segs_total | 200 | 200 | 1.000 | 1.00 | pass |
| residual-heavy-churn | true | 100000 | complete_seal_ms | 638.6 | 671.2 | 1.051 | 1.05 | FAIL |
| residual-heavy-churn | true | 100000 | restore_ms | 518 | 508.7 | 0.982 | 1.05 | pass |
| residual-heavy-churn | true | 100000 | first_reseal_ms | 1117 | 1106 | 0.991 | 1.05 | pass |
| residual-heavy-churn | true | 100000 | swept_seal_ms_median | 1047 | 1061 | 1.013 | 1.05 | pass |
| residual-heavy-cold-restore | false | 10000 | restore_ms | 102.8 | 91.17 | 0.887 | 1.05 | pass |
| residual-heavy-cold-restore | false | 10000 | first_reseal_ms | 96.24 | 88.72 | 0.922 | 1.05 | pass |
| residual-heavy-cold-restore | false | 100000 | restore_ms | 431.4 | 388.2 | 0.900 | 1.05 | pass |
| residual-heavy-cold-restore | false | 100000 | first_reseal_ms | 444.2 | 416 | 0.937 | 1.05 | pass |
| residual-heavy-cold-restore | true | 10000 | restore_ms | 95.36 | 92.85 | 0.974 | 1.05 | pass |
| residual-heavy-cold-restore | true | 10000 | first_reseal_ms | 90.77 | 90.46 | 0.997 | 1.05 | pass |
| residual-heavy-cold-restore | true | 100000 | restore_ms | 390.9 | 386.6 | 0.989 | 1.05 | pass |
| residual-heavy-cold-restore | true | 100000 | first_reseal_ms | 417.6 | 425.7 | 1.019 | 1.05 | pass |
| wide-above-budget-churn | false | 10000 | seal_ms_median | 24.7 | 7.439 | 0.301 | 1.05 | pass |
| wide-above-budget-churn | false | 10000 | seal_ms_p95 | 132.9 | 25.66 | 0.193 | 1.10 | pass |
| wide-above-budget-churn | false | 10000 | refresh_ms_median | 34.89 | 15.85 | 0.454 | 1.05 | pass |
| wide-above-budget-churn | false | 10000 | rows_per_sec_median | 2.867e+04 | 6.308e+04 | 2.200 | 0.95 | pass |
| wide-above-budget-churn | false | 10000 | refresh_peak_mb_median | 5.4 | 5.4 | 1.000 | 1.05 | pass |
| wide-above-budget-churn | false | 10000 | alloc_mb_median | 0.22 | 0.21 | 0.955 | 1.05 | pass |
| wide-above-budget-churn | false | 10000 | state_bytes_last | 1.314e+07 | 5.04e+06 | 0.384 | 1.00 | pass |
| wide-above-budget-churn | false | 10000 | meta_bytes_median | 1.259e+06 | 3.954e+05 | 0.314 | 1.00 | pass |
| wide-above-budget-churn | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | false | 10000 | meta_segs_total | 2000 | 500 | 0.250 | 1.00 | pass |
| wide-above-budget-churn | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | false | 10000 | complete_seal_ms | 159.2 | 51.62 | 0.324 | 1.05 | pass |
| wide-above-budget-churn | false | 10000 | restore_ms | 116.3 | 64.85 | 0.557 | 1.05 | pass |
| wide-above-budget-churn | false | 10000 | first_reseal_ms | 47.76 | 13.59 | 0.284 | 1.05 | pass |
| wide-above-budget-churn | false | 10000 | swept_seal_ms_median | 133 | 25.83 | 0.194 | 1.05 | pass |
| wide-above-budget-churn | false | 100000 | seal_ms_median | 31.19 | 7.23 | 0.232 | 1.05 | pass |
| wide-above-budget-churn | false | 100000 | seal_ms_p95 | 38.36 | 8.397 | 0.219 | 1.10 | pass |
| wide-above-budget-churn | false | 100000 | refresh_ms_median | 42.35 | 14.77 | 0.349 | 1.05 | pass |
| wide-above-budget-churn | false | 100000 | rows_per_sec_median | 2.361e+04 | 6.769e+04 | 2.867 | 0.95 | pass |
| wide-above-budget-churn | false | 100000 | refresh_peak_mb_median | 3.1 | 3.1 | 1.000 | 1.05 | pass |
| wide-above-budget-churn | false | 100000 | alloc_mb_median | 0.22 | 0.21 | 0.955 | 1.05 | pass |
| wide-above-budget-churn | false | 100000 | state_bytes_last | 1.314e+08 | 5.04e+07 | 0.384 | 1.00 | pass |
| wide-above-budget-churn | false | 100000 | meta_bytes_median | 1.412e+06 | 4.186e+05 | 0.297 | 1.00 | pass |
| wide-above-budget-churn | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | false | 100000 | meta_segs_total | 1.2e+04 | 3000 | 0.250 | 1.00 | pass |
| wide-above-budget-churn | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | false | 100000 | complete_seal_ms | 1776 | 582.3 | 0.328 | 1.05 | pass |
| wide-above-budget-churn | false | 100000 | restore_ms | 903.5 | 442.4 | 0.490 | 1.05 | pass |
| wide-above-budget-churn | false | 100000 | first_reseal_ms | 163.1 | 20.8 | 0.128 | 1.05 | pass |
| wide-above-budget-churn | false | 100000 | swept_seal_ms_median | 1235 | 138.2 | 0.112 | 1.05 | pass |
| wide-above-budget-churn | true | 10000 | seal_ms_median | 6.09 | 6.12 | 1.005 | 1.05 | pass |
| wide-above-budget-churn | true | 10000 | seal_ms_p95 | 22.73 | 22.61 | 0.995 | 1.10 | pass |
| wide-above-budget-churn | true | 10000 | refresh_ms_median | 10.52 | 10.4 | 0.989 | 1.05 | pass |
| wide-above-budget-churn | true | 10000 | rows_per_sec_median | 9.506e+04 | 9.616e+04 | 1.012 | 0.95 | pass |
| wide-above-budget-churn | true | 10000 | refresh_peak_mb_median | 5.4 | 5.4 | 1.000 | 1.05 | pass |
| wide-above-budget-churn | true | 10000 | alloc_mb_median | 0.21 | 0.21 | 1.000 | 1.05 | pass |
| wide-above-budget-churn | true | 10000 | state_bytes_last | 5.04e+06 | 5.04e+06 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | true | 10000 | meta_bytes_median | 3.954e+05 | 3.954e+05 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | true | 10000 | meta_segs_total | 500 | 500 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | true | 10000 | complete_seal_ms | 38.29 | 38.91 | 1.016 | 1.05 | pass |
| wide-above-budget-churn | true | 10000 | restore_ms | 42.72 | 41.2 | 0.964 | 1.05 | pass |
| wide-above-budget-churn | true | 10000 | first_reseal_ms | 11.34 | 10.99 | 0.969 | 1.05 | pass |
| wide-above-budget-churn | true | 10000 | swept_seal_ms_median | 22.8 | 22.78 | 0.999 | 1.05 | pass |
| wide-above-budget-churn | true | 100000 | seal_ms_median | 5.223 | 5.164 | 0.989 | 1.05 | pass |
| wide-above-budget-churn | true | 100000 | seal_ms_p95 | 6.325 | 6.347 | 1.003 | 1.10 | pass |
| wide-above-budget-churn | true | 100000 | refresh_ms_median | 8.486 | 8.617 | 1.015 | 1.05 | pass |
| wide-above-budget-churn | true | 100000 | rows_per_sec_median | 1.178e+05 | 1.16e+05 | 0.985 | 0.95 | pass |
| wide-above-budget-churn | true | 100000 | refresh_peak_mb_median | 3.1 | 3.1 | 1.000 | 1.05 | pass |
| wide-above-budget-churn | true | 100000 | alloc_mb_median | 0.21 | 0.21 | 1.000 | 1.05 | pass |
| wide-above-budget-churn | true | 100000 | state_bytes_last | 5.04e+07 | 5.04e+07 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | true | 100000 | meta_bytes_median | 4.186e+05 | 4.186e+05 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | true | 100000 | meta_segs_total | 3000 | 3000 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-above-budget-churn | true | 100000 | complete_seal_ms | 362.8 | 356.8 | 0.983 | 1.05 | pass |
| wide-above-budget-churn | true | 100000 | restore_ms | 213.4 | 206.7 | 0.968 | 1.05 | pass |
| wide-above-budget-churn | true | 100000 | first_reseal_ms | 19.27 | 18.98 | 0.985 | 1.05 | pass |
| wide-above-budget-churn | true | 100000 | swept_seal_ms_median | 138.3 | 138.5 | 1.001 | 1.05 | pass |
| wide-above-budget-cold-restore | false | 10000 | restore_ms | 129.9 | 68.25 | 0.526 | 1.05 | pass |
| wide-above-budget-cold-restore | false | 10000 | first_reseal_ms | 57.16 | 12.39 | 0.217 | 1.05 | pass |
| wide-above-budget-cold-restore | false | 100000 | restore_ms | 778 | 340.8 | 0.438 | 1.05 | pass |
| wide-above-budget-cold-restore | false | 100000 | first_reseal_ms | 50.22 | 10.62 | 0.212 | 1.05 | pass |
| wide-above-budget-cold-restore | true | 10000 | restore_ms | 55.11 | 54.23 | 0.984 | 1.05 | pass |
| wide-above-budget-cold-restore | true | 10000 | first_reseal_ms | 10.6 | 10.67 | 1.007 | 1.05 | pass |
| wide-above-budget-cold-restore | true | 100000 | restore_ms | 212 | 202.9 | 0.957 | 1.05 | pass |
| wide-above-budget-cold-restore | true | 100000 | first_reseal_ms | 8.402 | 8.16 | 0.971 | 1.05 | pass |
| wide-at-budget-churn | false | 10000 | seal_ms_median | 24.23 | 5.152 | 0.213 | 1.05 | pass |
| wide-at-budget-churn | false | 10000 | seal_ms_p95 | 131.7 | 11.61 | 0.088 | 1.10 | pass |
| wide-at-budget-churn | false | 10000 | refresh_ms_median | 33.97 | 13.56 | 0.399 | 1.05 | pass |
| wide-at-budget-churn | false | 10000 | rows_per_sec_median | 2.944e+04 | 7.374e+04 | 2.505 | 0.95 | pass |
| wide-at-budget-churn | false | 10000 | refresh_peak_mb_median | 5.4 | 5.4 | 1.000 | 1.05 | pass |
| wide-at-budget-churn | false | 10000 | alloc_mb_median | 0.22 | 0.21 | 0.955 | 1.05 | pass |
| wide-at-budget-churn | false | 10000 | state_bytes_last | 1.302e+07 | 4.38e+06 | 0.336 | 1.00 | pass |
| wide-at-budget-churn | false | 10000 | meta_bytes_median | 1.24e+06 | 3.295e+05 | 0.266 | 1.00 | pass |
| wide-at-budget-churn | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | false | 10000 | meta_segs_total | 2000 | 400 | 0.200 | 1.00 | pass |
| wide-at-budget-churn | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | false | 10000 | complete_seal_ms | 158.5 | 37.67 | 0.238 | 1.05 | pass |
| wide-at-budget-churn | false | 10000 | restore_ms | 123.3 | 60.05 | 0.487 | 1.05 | pass |
| wide-at-budget-churn | false | 10000 | first_reseal_ms | 45.33 | 8.774 | 0.194 | 1.05 | pass |
| wide-at-budget-churn | false | 10000 | swept_seal_ms_median | 132.1 | 11.63 | 0.088 | 1.05 | pass |
| wide-at-budget-churn | false | 100000 | seal_ms_median | 31.34 | 5.392 | 0.172 | 1.05 | pass |
| wide-at-budget-churn | false | 100000 | seal_ms_p95 | 37.42 | 6.312 | 0.169 | 1.10 | pass |
| wide-at-budget-churn | false | 100000 | refresh_ms_median | 42.02 | 12.86 | 0.306 | 1.05 | pass |
| wide-at-budget-churn | false | 100000 | rows_per_sec_median | 2.38e+04 | 7.778e+04 | 3.268 | 0.95 | pass |
| wide-at-budget-churn | false | 100000 | refresh_peak_mb_median | 3.1 | 3.1 | 1.000 | 1.05 | pass |
| wide-at-budget-churn | false | 100000 | alloc_mb_median | 0.22 | 0.21 | 0.955 | 1.05 | pass |
| wide-at-budget-churn | false | 100000 | state_bytes_last | 1.302e+08 | 4.38e+07 | 0.336 | 1.00 | pass |
| wide-at-budget-churn | false | 100000 | meta_bytes_median | 1.403e+06 | 3.432e+05 | 0.245 | 1.00 | pass |
| wide-at-budget-churn | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | false | 100000 | meta_segs_total | 1.2e+04 | 2400 | 0.200 | 1.00 | pass |
| wide-at-budget-churn | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | false | 100000 | complete_seal_ms | 1724 | 489.9 | 0.284 | 1.05 | pass |
| wide-at-budget-churn | false | 100000 | restore_ms | 861.7 | 439.2 | 0.510 | 1.05 | pass |
| wide-at-budget-churn | false | 100000 | first_reseal_ms | 93.44 | 14.03 | 0.150 | 1.05 | pass |
| wide-at-budget-churn | false | 100000 | swept_seal_ms_median | 1230 | 72.94 | 0.059 | 1.05 | pass |
| wide-at-budget-churn | true | 10000 | seal_ms_median | 3.492 | 3.474 | 0.995 | 1.05 | pass |
| wide-at-budget-churn | true | 10000 | seal_ms_p95 | 9.673 | 9.733 | 1.006 | 1.10 | pass |
| wide-at-budget-churn | true | 10000 | refresh_ms_median | 8.094 | 8.025 | 0.992 | 1.05 | pass |
| wide-at-budget-churn | true | 10000 | rows_per_sec_median | 1.236e+05 | 1.246e+05 | 1.009 | 0.95 | pass |
| wide-at-budget-churn | true | 10000 | refresh_peak_mb_median | 5.4 | 5.4 | 1.000 | 1.05 | pass |
| wide-at-budget-churn | true | 10000 | alloc_mb_median | 0.21 | 0.21 | 1.000 | 1.05 | pass |
| wide-at-budget-churn | true | 10000 | state_bytes_last | 4.38e+06 | 4.38e+06 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | true | 10000 | meta_bytes_median | 3.295e+05 | 3.295e+05 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | true | 10000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | true | 10000 | complete_seal_ms | 25.31 | 24.27 | 0.959 | 1.05 | pass |
| wide-at-budget-churn | true | 10000 | restore_ms | 34 | 35.42 | 1.042 | 1.05 | pass |
| wide-at-budget-churn | true | 10000 | first_reseal_ms | 6.558 | 6.357 | 0.969 | 1.05 | pass |
| wide-at-budget-churn | true | 10000 | swept_seal_ms_median | 9.767 | 9.753 | 0.999 | 1.05 | pass |
| wide-at-budget-churn | true | 100000 | seal_ms_median | 3.341 | 3.304 | 0.989 | 1.05 | pass |
| wide-at-budget-churn | true | 100000 | seal_ms_p95 | 4.037 | 4.009 | 0.993 | 1.10 | pass |
| wide-at-budget-churn | true | 100000 | refresh_ms_median | 6.345 | 6.455 | 1.017 | 1.05 | pass |
| wide-at-budget-churn | true | 100000 | rows_per_sec_median | 1.576e+05 | 1.549e+05 | 0.983 | 0.95 | pass |
| wide-at-budget-churn | true | 100000 | refresh_peak_mb_median | 3.1 | 3.1 | 1.000 | 1.05 | pass |
| wide-at-budget-churn | true | 100000 | alloc_mb_median | 0.21 | 0.21 | 1.000 | 1.05 | pass |
| wide-at-budget-churn | true | 100000 | state_bytes_last | 4.38e+07 | 4.38e+07 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | true | 100000 | meta_bytes_median | 3.432e+05 | 3.432e+05 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | true | 100000 | meta_segs_total | 2400 | 2400 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-at-budget-churn | true | 100000 | complete_seal_ms | 252.7 | 248.4 | 0.983 | 1.05 | pass |
| wide-at-budget-churn | true | 100000 | restore_ms | 160.8 | 158.3 | 0.985 | 1.05 | pass |
| wide-at-budget-churn | true | 100000 | first_reseal_ms | 12.08 | 11.77 | 0.975 | 1.05 | pass |
| wide-at-budget-churn | true | 100000 | swept_seal_ms_median | 70.99 | 70.45 | 0.992 | 1.05 | pass |
| wide-at-budget-cold-restore | false | 10000 | restore_ms | 143.2 | 65.08 | 0.454 | 1.05 | pass |
| wide-at-budget-cold-restore | false | 10000 | first_reseal_ms | 58.33 | 8.853 | 0.152 | 1.05 | pass |
| wide-at-budget-cold-restore | false | 100000 | restore_ms | 756.2 | 304.2 | 0.402 | 1.05 | pass |
| wide-at-budget-cold-restore | false | 100000 | first_reseal_ms | 50.85 | 7.996 | 0.157 | 1.05 | pass |
| wide-at-budget-cold-restore | true | 10000 | restore_ms | 47.4 | 47.2 | 0.996 | 1.05 | pass |
| wide-at-budget-cold-restore | true | 10000 | first_reseal_ms | 7.513 | 7.373 | 0.981 | 1.05 | pass |
| wide-at-budget-cold-restore | true | 100000 | restore_ms | 162.1 | 159.3 | 0.983 | 1.05 | pass |
| wide-at-budget-cold-restore | true | 100000 | first_reseal_ms | 6.216 | 6.204 | 0.998 | 1.05 | pass |
| wide-below-budget-churn | false | 10000 | seal_ms_median | 23.57 | 4.775 | 0.203 | 1.05 | pass |
| wide-below-budget-churn | false | 10000 | seal_ms_p95 | 124.7 | 11.07 | 0.089 | 1.10 | pass |
| wide-below-budget-churn | false | 10000 | refresh_ms_median | 33.02 | 12.9 | 0.391 | 1.05 | pass |
| wide-below-budget-churn | false | 10000 | rows_per_sec_median | 3.029e+04 | 7.751e+04 | 2.559 | 0.95 | pass |
| wide-below-budget-churn | false | 10000 | refresh_peak_mb_median | 5.3 | 5.3 | 1.000 | 1.05 | pass |
| wide-below-budget-churn | false | 10000 | alloc_mb_median | 0.22 | 0.21 | 0.955 | 1.05 | pass |
| wide-below-budget-churn | false | 10000 | state_bytes_last | 1.236e+07 | 4.26e+06 | 0.345 | 1.00 | pass |
| wide-below-budget-churn | false | 10000 | meta_bytes_median | 1.185e+06 | 3.211e+05 | 0.271 | 1.00 | pass |
| wide-below-budget-churn | false | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | false | 10000 | meta_segs_total | 1900 | 400 | 0.211 | 1.00 | pass |
| wide-below-budget-churn | false | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | false | 10000 | complete_seal_ms | 150.5 | 34.51 | 0.229 | 1.05 | pass |
| wide-below-budget-churn | false | 10000 | restore_ms | 109.1 | 55.21 | 0.506 | 1.05 | pass |
| wide-below-budget-churn | false | 10000 | first_reseal_ms | 45.34 | 7.913 | 0.175 | 1.05 | pass |
| wide-below-budget-churn | false | 10000 | swept_seal_ms_median | 125 | 11.1 | 0.089 | 1.05 | pass |
| wide-below-budget-churn | false | 100000 | seal_ms_median | 29.14 | 5.192 | 0.178 | 1.05 | pass |
| wide-below-budget-churn | false | 100000 | seal_ms_p95 | 36.26 | 6.215 | 0.171 | 1.10 | pass |
| wide-below-budget-churn | false | 100000 | refresh_ms_median | 39.81 | 12.42 | 0.312 | 1.05 | pass |
| wide-below-budget-churn | false | 100000 | rows_per_sec_median | 2.512e+04 | 8.051e+04 | 3.205 | 0.95 | pass |
| wide-below-budget-churn | false | 100000 | refresh_peak_mb_median | 3.1 | 3.1 | 1.000 | 1.05 | pass |
| wide-below-budget-churn | false | 100000 | alloc_mb_median | 0.22 | 0.21 | 0.955 | 1.05 | pass |
| wide-below-budget-churn | false | 100000 | state_bytes_last | 1.236e+08 | 4.26e+07 | 0.345 | 1.00 | pass |
| wide-below-budget-churn | false | 100000 | meta_bytes_median | 1.329e+06 | 3.349e+05 | 0.252 | 1.00 | pass |
| wide-below-budget-churn | false | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | false | 100000 | meta_segs_total | 1.14e+04 | 2400 | 0.211 | 1.00 | pass |
| wide-below-budget-churn | false | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | false | 100000 | complete_seal_ms | 1658 | 459.7 | 0.277 | 1.05 | pass |
| wide-below-budget-churn | false | 100000 | restore_ms | 841.1 | 412 | 0.490 | 1.05 | pass |
| wide-below-budget-churn | false | 100000 | first_reseal_ms | 152.4 | 13.83 | 0.091 | 1.05 | pass |
| wide-below-budget-churn | false | 100000 | swept_seal_ms_median | 1139 | 65.16 | 0.057 | 1.05 | pass |
| wide-below-budget-churn | true | 10000 | seal_ms_median | 3.409 | 3.429 | 1.006 | 1.05 | pass |
| wide-below-budget-churn | true | 10000 | seal_ms_p95 | 9.441 | 9.184 | 0.973 | 1.10 | pass |
| wide-below-budget-churn | true | 10000 | refresh_ms_median | 7.871 | 7.732 | 0.982 | 1.05 | pass |
| wide-below-budget-churn | true | 10000 | rows_per_sec_median | 1.271e+05 | 1.293e+05 | 1.018 | 0.95 | pass |
| wide-below-budget-churn | true | 10000 | refresh_peak_mb_median | 5.3 | 5.3 | 1.000 | 1.05 | pass |
| wide-below-budget-churn | true | 10000 | alloc_mb_median | 0.21 | 0.21 | 1.000 | 1.05 | pass |
| wide-below-budget-churn | true | 10000 | state_bytes_last | 4.26e+06 | 4.26e+06 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | true | 10000 | meta_bytes_median | 3.211e+05 | 3.211e+05 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | true | 10000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | true | 10000 | meta_segs_total | 400 | 400 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | true | 10000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | true | 10000 | complete_seal_ms | 24.59 | 23.58 | 0.959 | 1.05 | pass |
| wide-below-budget-churn | true | 10000 | restore_ms | 34.01 | 33.86 | 0.996 | 1.05 | pass |
| wide-below-budget-churn | true | 10000 | first_reseal_ms | 6.227 | 6.306 | 1.013 | 1.05 | pass |
| wide-below-budget-churn | true | 10000 | swept_seal_ms_median | 9.529 | 9.431 | 0.990 | 1.05 | pass |
| wide-below-budget-churn | true | 100000 | seal_ms_median | 3.224 | 3.231 | 1.002 | 1.05 | pass |
| wide-below-budget-churn | true | 100000 | seal_ms_p95 | 3.885 | 3.92 | 1.009 | 1.10 | pass |
| wide-below-budget-churn | true | 100000 | refresh_ms_median | 6.063 | 6.14 | 1.013 | 1.05 | pass |
| wide-below-budget-churn | true | 100000 | rows_per_sec_median | 1.649e+05 | 1.629e+05 | 0.987 | 0.95 | pass |
| wide-below-budget-churn | true | 100000 | refresh_peak_mb_median | 3.1 | 3.1 | 1.000 | 1.05 | pass |
| wide-below-budget-churn | true | 100000 | alloc_mb_median | 0.21 | 0.21 | 1.000 | 1.05 | pass |
| wide-below-budget-churn | true | 100000 | state_bytes_last | 4.26e+07 | 4.26e+07 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | true | 100000 | meta_bytes_median | 3.349e+05 | 3.349e+05 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | true | 100000 | data_bytes_median | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | true | 100000 | meta_segs_total | 2400 | 2400 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | true | 100000 | data_segs_total | 0 | 0 | 1.000 | 1.00 | pass |
| wide-below-budget-churn | true | 100000 | complete_seal_ms | 235.4 | 241.3 | 1.025 | 1.05 | pass |
| wide-below-budget-churn | true | 100000 | restore_ms | 151.6 | 149.9 | 0.989 | 1.05 | pass |
| wide-below-budget-churn | true | 100000 | first_reseal_ms | 11.9 | 11.87 | 0.998 | 1.05 | pass |
| wide-below-budget-churn | true | 100000 | swept_seal_ms_median | 62.23 | 62.38 | 1.002 | 1.05 | pass |
| wide-below-budget-cold-restore | false | 10000 | restore_ms | 128.5 | 54.24 | 0.422 | 1.05 | pass |
| wide-below-budget-cold-restore | false | 10000 | first_reseal_ms | 57.87 | 8.28 | 0.143 | 1.05 | pass |
| wide-below-budget-cold-restore | false | 100000 | restore_ms | 745.6 | 280.1 | 0.376 | 1.05 | pass |
| wide-below-budget-cold-restore | false | 100000 | first_reseal_ms | 52.37 | 7.352 | 0.140 | 1.05 | pass |
| wide-below-budget-cold-restore | true | 10000 | restore_ms | 46.42 | 46.4 | 1.000 | 1.05 | pass |
| wide-below-budget-cold-restore | true | 10000 | first_reseal_ms | 7.183 | 7.369 | 1.026 | 1.05 | pass |
| wide-below-budget-cold-restore | true | 100000 | restore_ms | 151.8 | 150.5 | 0.991 | 1.05 | pass |
| wide-below-budget-cold-restore | true | 100000 | first_reseal_ms | 6.04 | 5.962 | 0.987 | 1.05 | pass |

Diagnostics (reported, not gated):
  anchor-only-decimal-churn fusion=false keys=10000 sweep_ms_median: baseline=2.535 candidate=2.526
  anchor-only-decimal-churn fusion=false keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  anchor-only-decimal-churn fusion=false keys=10000 sweeps_total: baseline=10 candidate=10
  anchor-only-decimal-churn fusion=false keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  anchor-only-decimal-churn fusion=false keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  anchor-only-decimal-churn fusion=false keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  anchor-only-decimal-churn fusion=false keys=10000 restore_plus_reseal_ms: baseline=36.97 candidate=37.28 ratio=1.008
  anchor-only-decimal-churn fusion=false keys=100000 sweep_ms_median: baseline=19.54 candidate=22.55
  anchor-only-decimal-churn fusion=false keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  anchor-only-decimal-churn fusion=false keys=100000 sweeps_total: baseline=5 candidate=5
  anchor-only-decimal-churn fusion=false keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  anchor-only-decimal-churn fusion=false keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  anchor-only-decimal-churn fusion=false keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  anchor-only-decimal-churn fusion=false keys=100000 restore_plus_reseal_ms: baseline=152.7 candidate=146.3 ratio=0.958
  anchor-only-decimal-churn fusion=true keys=10000 sweep_ms_median: baseline=2.509 candidate=2.614
  anchor-only-decimal-churn fusion=true keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  anchor-only-decimal-churn fusion=true keys=10000 sweeps_total: baseline=10 candidate=10
  anchor-only-decimal-churn fusion=true keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  anchor-only-decimal-churn fusion=true keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  anchor-only-decimal-churn fusion=true keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  anchor-only-decimal-churn fusion=true keys=10000 restore_plus_reseal_ms: baseline=37.27 candidate=36.97 ratio=0.992
  anchor-only-decimal-churn fusion=true keys=100000 sweep_ms_median: baseline=21.56 candidate=19.69
  anchor-only-decimal-churn fusion=true keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  anchor-only-decimal-churn fusion=true keys=100000 sweeps_total: baseline=5 candidate=5
  anchor-only-decimal-churn fusion=true keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  anchor-only-decimal-churn fusion=true keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  anchor-only-decimal-churn fusion=true keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  anchor-only-decimal-churn fusion=true keys=100000 restore_plus_reseal_ms: baseline=144.1 candidate=145.9 ratio=1.013
  anchor-only-decimal-cold-restore fusion=false keys=10000 restore_plus_reseal_ms: baseline=51.74 candidate=45.4 ratio=0.877
  anchor-only-decimal-cold-restore fusion=false keys=10000 evicted_mb: baseline=40.3 candidate=40.4
  anchor-only-decimal-cold-restore fusion=false keys=100000 restore_plus_reseal_ms: baseline=131.3 candidate=113 ratio=0.860
  anchor-only-decimal-cold-restore fusion=false keys=100000 evicted_mb: baseline=106.8 candidate=106.8
  anchor-only-decimal-cold-restore fusion=true keys=10000 restore_plus_reseal_ms: baseline=50.89 candidate=42.34 ratio=0.832
  anchor-only-decimal-cold-restore fusion=true keys=10000 evicted_mb: baseline=40.3 candidate=40.4
  anchor-only-decimal-cold-restore fusion=true keys=100000 restore_plus_reseal_ms: baseline=135.7 candidate=115.4 ratio=0.850
  anchor-only-decimal-cold-restore fusion=true keys=100000 evicted_mb: baseline=106.8 candidate=106.8
  anchor-only-unfused-control-churn fusion=false keys=10000 sweep_ms_median: baseline=2.529 candidate=2.513
  anchor-only-unfused-control-churn fusion=false keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  anchor-only-unfused-control-churn fusion=false keys=10000 sweeps_total: baseline=10 candidate=10
  anchor-only-unfused-control-churn fusion=false keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  anchor-only-unfused-control-churn fusion=false keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  anchor-only-unfused-control-churn fusion=false keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  anchor-only-unfused-control-churn fusion=false keys=10000 restore_plus_reseal_ms: baseline=35.93 candidate=37.24 ratio=1.036
  anchor-only-unfused-control-churn fusion=false keys=100000 sweep_ms_median: baseline=21.23 candidate=21.29
  anchor-only-unfused-control-churn fusion=false keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  anchor-only-unfused-control-churn fusion=false keys=100000 sweeps_total: baseline=5 candidate=5
  anchor-only-unfused-control-churn fusion=false keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  anchor-only-unfused-control-churn fusion=false keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  anchor-only-unfused-control-churn fusion=false keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  anchor-only-unfused-control-churn fusion=false keys=100000 restore_plus_reseal_ms: baseline=138.1 candidate=138.3 ratio=1.002
  anchor-only-unfused-control-churn fusion=true keys=10000 sweep_ms_median: baseline=2.532 candidate=2.49
  anchor-only-unfused-control-churn fusion=true keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  anchor-only-unfused-control-churn fusion=true keys=10000 sweeps_total: baseline=10 candidate=10
  anchor-only-unfused-control-churn fusion=true keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  anchor-only-unfused-control-churn fusion=true keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  anchor-only-unfused-control-churn fusion=true keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  anchor-only-unfused-control-churn fusion=true keys=10000 restore_plus_reseal_ms: baseline=36.78 candidate=38.97 ratio=1.060
  anchor-only-unfused-control-churn fusion=true keys=100000 sweep_ms_median: baseline=21.7 candidate=23.81
  anchor-only-unfused-control-churn fusion=true keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  anchor-only-unfused-control-churn fusion=true keys=100000 sweeps_total: baseline=5 candidate=5
  anchor-only-unfused-control-churn fusion=true keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  anchor-only-unfused-control-churn fusion=true keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  anchor-only-unfused-control-churn fusion=true keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  anchor-only-unfused-control-churn fusion=true keys=100000 restore_plus_reseal_ms: baseline=134.5 candidate=134.9 ratio=1.003
  anchor-only-unfused-control-cold-restore fusion=false keys=10000 restore_plus_reseal_ms: baseline=49.07 candidate=40.59 ratio=0.827
  anchor-only-unfused-control-cold-restore fusion=false keys=10000 evicted_mb: baseline=28.8 candidate=28.8
  anchor-only-unfused-control-cold-restore fusion=false keys=100000 restore_plus_reseal_ms: baseline=129.5 candidate=108.4 ratio=0.837
  anchor-only-unfused-control-cold-restore fusion=false keys=100000 evicted_mb: baseline=86.7 candidate=86.7
  anchor-only-unfused-control-cold-restore fusion=true keys=10000 restore_plus_reseal_ms: baseline=49.58 candidate=41.87 ratio=0.845
  anchor-only-unfused-control-cold-restore fusion=true keys=10000 evicted_mb: baseline=28.8 candidate=28.8
  anchor-only-unfused-control-cold-restore fusion=true keys=100000 restore_plus_reseal_ms: baseline=128.9 candidate=106.5 ratio=0.826
  anchor-only-unfused-control-cold-restore fusion=true keys=100000 evicted_mb: baseline=86.7 candidate=86.7
  narrow-count-star-key-churn fusion=false keys=10000 sweep_ms_median: baseline=2.617 candidate=2.469
  narrow-count-star-key-churn fusion=false keys=10000 evicted_per_sweep_median: baseline=4750 candidate=4750
  narrow-count-star-key-churn fusion=false keys=10000 sweeps_total: baseline=10 candidate=10
  narrow-count-star-key-churn fusion=false keys=10000 swept_win_visited_median: baseline=-1 candidate=5701
  narrow-count-star-key-churn fusion=false keys=10000 swept_win_removed_median: baseline=-1 candidate=4750
  narrow-count-star-key-churn fusion=false keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  narrow-count-star-key-churn fusion=false keys=10000 restore_plus_reseal_ms: baseline=43.36 candidate=29.02 ratio=0.669
  narrow-count-star-key-churn fusion=false keys=100000 sweep_ms_median: baseline=29.88 candidate=29.13
  narrow-count-star-key-churn fusion=false keys=100000 evicted_per_sweep_median: baseline=4.75e+04 candidate=4.75e+04
  narrow-count-star-key-churn fusion=false keys=100000 sweeps_total: baseline=5 candidate=5
  narrow-count-star-key-churn fusion=false keys=100000 swept_win_visited_median: baseline=-1 candidate=4.845e+04
  narrow-count-star-key-churn fusion=false keys=100000 swept_win_removed_median: baseline=-1 candidate=4.75e+04
  narrow-count-star-key-churn fusion=false keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  narrow-count-star-key-churn fusion=false keys=100000 restore_plus_reseal_ms: baseline=174.4 candidate=95.94 ratio=0.550
  narrow-count-star-key-churn fusion=true keys=10000 sweep_ms_median: baseline=0.447 candidate=0.414
  narrow-count-star-key-churn fusion=true keys=10000 evicted_per_sweep_median: baseline=4750 candidate=4750
  narrow-count-star-key-churn fusion=true keys=10000 sweeps_total: baseline=10 candidate=10
  narrow-count-star-key-churn fusion=true keys=10000 swept_win_visited_median: baseline=-1 candidate=5701
  narrow-count-star-key-churn fusion=true keys=10000 swept_win_removed_median: baseline=-1 candidate=4750
  narrow-count-star-key-churn fusion=true keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  narrow-count-star-key-churn fusion=true keys=10000 restore_plus_reseal_ms: baseline=28.15 candidate=26.84 ratio=0.953
  narrow-count-star-key-churn fusion=true keys=100000 sweep_ms_median: baseline=6.411 candidate=7.216
  narrow-count-star-key-churn fusion=true keys=100000 evicted_per_sweep_median: baseline=4.75e+04 candidate=4.75e+04
  narrow-count-star-key-churn fusion=true keys=100000 sweeps_total: baseline=5 candidate=5
  narrow-count-star-key-churn fusion=true keys=100000 swept_win_visited_median: baseline=-1 candidate=4.845e+04
  narrow-count-star-key-churn fusion=true keys=100000 swept_win_removed_median: baseline=-1 candidate=4.75e+04
  narrow-count-star-key-churn fusion=true keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  narrow-count-star-key-churn fusion=true keys=100000 restore_plus_reseal_ms: baseline=79.27 candidate=82.19 ratio=1.037
  narrow-count-star-key-cold-restore fusion=false keys=10000 restore_plus_reseal_ms: baseline=59.1 candidate=36.21 ratio=0.613
  narrow-count-star-key-cold-restore fusion=false keys=10000 evicted_mb: baseline=34.6 candidate=29.9
  narrow-count-star-key-cold-restore fusion=false keys=100000 restore_plus_reseal_ms: baseline=167.9 candidate=87.29 ratio=0.520
  narrow-count-star-key-cold-restore fusion=false keys=100000 evicted_mb: baseline=97.1 candidate=85.0
  narrow-count-star-key-cold-restore fusion=true keys=10000 restore_plus_reseal_ms: baseline=34.42 candidate=34.85 ratio=1.012
  narrow-count-star-key-cold-restore fusion=true keys=10000 evicted_mb: baseline=29.9 candidate=29.9
  narrow-count-star-key-cold-restore fusion=true keys=100000 restore_plus_reseal_ms: baseline=74.69 candidate=74.16 ratio=0.993
  narrow-count-star-key-cold-restore fusion=true keys=100000 evicted_mb: baseline=85.0 candidate=85.0
  narrow-sum-avg-count-churn fusion=false keys=10000 sweep_ms_median: baseline=3.396 candidate=3.481
  narrow-sum-avg-count-churn fusion=false keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  narrow-sum-avg-count-churn fusion=false keys=10000 sweeps_total: baseline=10 candidate=10
  narrow-sum-avg-count-churn fusion=false keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  narrow-sum-avg-count-churn fusion=false keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  narrow-sum-avg-count-churn fusion=false keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  narrow-sum-avg-count-churn fusion=false keys=10000 restore_plus_reseal_ms: baseline=52.95 candidate=32.82 ratio=0.620
  narrow-sum-avg-count-churn fusion=false keys=100000 sweep_ms_median: baseline=42.43 candidate=39.84
  narrow-sum-avg-count-churn fusion=false keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  narrow-sum-avg-count-churn fusion=false keys=100000 sweeps_total: baseline=5 candidate=5
  narrow-sum-avg-count-churn fusion=false keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  narrow-sum-avg-count-churn fusion=false keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  narrow-sum-avg-count-churn fusion=false keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  narrow-sum-avg-count-churn fusion=false keys=100000 restore_plus_reseal_ms: baseline=253.4 candidate=126.5 ratio=0.499
  narrow-sum-avg-count-churn fusion=true keys=10000 sweep_ms_median: baseline=0.533 candidate=0.525
  narrow-sum-avg-count-churn fusion=true keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  narrow-sum-avg-count-churn fusion=true keys=10000 sweeps_total: baseline=10 candidate=10
  narrow-sum-avg-count-churn fusion=true keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  narrow-sum-avg-count-churn fusion=true keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  narrow-sum-avg-count-churn fusion=true keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  narrow-sum-avg-count-churn fusion=true keys=10000 restore_plus_reseal_ms: baseline=28.35 candidate=27.62 ratio=0.974
  narrow-sum-avg-count-churn fusion=true keys=100000 sweep_ms_median: baseline=9.813 candidate=11.03
  narrow-sum-avg-count-churn fusion=true keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  narrow-sum-avg-count-churn fusion=true keys=100000 sweeps_total: baseline=5 candidate=5
  narrow-sum-avg-count-churn fusion=true keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  narrow-sum-avg-count-churn fusion=true keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  narrow-sum-avg-count-churn fusion=true keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  narrow-sum-avg-count-churn fusion=true keys=100000 restore_plus_reseal_ms: baseline=83.99 candidate=86.47 ratio=1.030
  narrow-sum-avg-count-cold-restore fusion=false keys=10000 restore_plus_reseal_ms: baseline=73.3 candidate=38.93 ratio=0.531
  narrow-sum-avg-count-cold-restore fusion=false keys=10000 evicted_mb: baseline=44.4 candidate=33.1
  narrow-sum-avg-count-cold-restore fusion=false keys=100000 restore_plus_reseal_ms: baseline=232.7 candidate=105.1 ratio=0.452
  narrow-sum-avg-count-cold-restore fusion=false keys=100000 evicted_mb: baseline=116.3 candidate=92.2
  narrow-sum-avg-count-cold-restore fusion=true keys=10000 restore_plus_reseal_ms: baseline=35.84 candidate=34.98 ratio=0.976
  narrow-sum-avg-count-cold-restore fusion=true keys=10000 evicted_mb: baseline=33.1 candidate=33.1
  narrow-sum-avg-count-cold-restore fusion=true keys=100000 restore_plus_reseal_ms: baseline=78.74 candidate=81.05 ratio=1.029
  narrow-sum-avg-count-cold-restore fusion=true keys=100000 evicted_mb: baseline=92.2 candidate=92.2
  narrow-sum-churn fusion=false keys=10000 sweep_ms_median: baseline=2.487 candidate=2.556
  narrow-sum-churn fusion=false keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  narrow-sum-churn fusion=false keys=10000 sweeps_total: baseline=10 candidate=10
  narrow-sum-churn fusion=false keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  narrow-sum-churn fusion=false keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  narrow-sum-churn fusion=false keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  narrow-sum-churn fusion=false keys=10000 restore_plus_reseal_ms: baseline=37.16 candidate=27.02 ratio=0.727
  narrow-sum-churn fusion=false keys=100000 sweep_ms_median: baseline=21.81 candidate=19.29
  narrow-sum-churn fusion=false keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  narrow-sum-churn fusion=false keys=100000 sweeps_total: baseline=5 candidate=5
  narrow-sum-churn fusion=false keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  narrow-sum-churn fusion=false keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  narrow-sum-churn fusion=false keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  narrow-sum-churn fusion=false keys=100000 restore_plus_reseal_ms: baseline=136.3 candidate=95.26 ratio=0.699
  narrow-sum-churn fusion=true keys=10000 sweep_ms_median: baseline=0.5 candidate=0.503
  narrow-sum-churn fusion=true keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  narrow-sum-churn fusion=true keys=10000 sweeps_total: baseline=10 candidate=10
  narrow-sum-churn fusion=true keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  narrow-sum-churn fusion=true keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  narrow-sum-churn fusion=true keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  narrow-sum-churn fusion=true keys=10000 restore_plus_reseal_ms: baseline=27.94 candidate=28.04 ratio=1.004
  narrow-sum-churn fusion=true keys=100000 sweep_ms_median: baseline=8.376 candidate=8.574
  narrow-sum-churn fusion=true keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  narrow-sum-churn fusion=true keys=100000 sweeps_total: baseline=5 candidate=5
  narrow-sum-churn fusion=true keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  narrow-sum-churn fusion=true keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  narrow-sum-churn fusion=true keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  narrow-sum-churn fusion=true keys=100000 restore_plus_reseal_ms: baseline=84.3 candidate=85.27 ratio=1.011
  narrow-sum-cold-restore fusion=false keys=10000 restore_plus_reseal_ms: baseline=49.1 candidate=33.37 ratio=0.680
  narrow-sum-cold-restore fusion=false keys=10000 evicted_mb: baseline=28.8 candidate=29.2
  narrow-sum-cold-restore fusion=false keys=100000 restore_plus_reseal_ms: baseline=124.8 candidate=85.07 ratio=0.681
  narrow-sum-cold-restore fusion=false keys=100000 evicted_mb: baseline=86.7 candidate=83.9
  narrow-sum-cold-restore fusion=true keys=10000 restore_plus_reseal_ms: baseline=34.48 candidate=33.85 ratio=0.982
  narrow-sum-cold-restore fusion=true keys=10000 evicted_mb: baseline=29.2 candidate=29.2
  narrow-sum-cold-restore fusion=true keys=100000 restore_plus_reseal_ms: baseline=76.83 candidate=76.04 ratio=0.990
  narrow-sum-cold-restore fusion=true keys=100000 evicted_mb: baseline=83.9 candidate=83.9
  residual-heavy-churn fusion=false keys=10000 sweep_ms_median: baseline=2.372 candidate=2.437
  residual-heavy-churn fusion=false keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  residual-heavy-churn fusion=false keys=10000 sweeps_total: baseline=10 candidate=10
  residual-heavy-churn fusion=false keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  residual-heavy-churn fusion=false keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  residual-heavy-churn fusion=false keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  residual-heavy-churn fusion=false keys=10000 restore_plus_reseal_ms: baseline=482.9 candidate=509.7 ratio=1.055
  residual-heavy-churn fusion=false keys=100000 sweep_ms_median: baseline=69.55 candidate=74.94
  residual-heavy-churn fusion=false keys=100000 evicted_per_sweep_median: baseline=8.275e+04 candidate=8.275e+04
  residual-heavy-churn fusion=false keys=100000 sweeps_total: baseline=1 candidate=1
  residual-heavy-churn fusion=false keys=100000 swept_win_visited_median: baseline=-1 candidate=8.375e+04
  residual-heavy-churn fusion=false keys=100000 swept_win_removed_median: baseline=-1 candidate=8.275e+04
  residual-heavy-churn fusion=false keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  residual-heavy-churn fusion=false keys=100000 restore_plus_reseal_ms: baseline=1655 candidate=1638 ratio=0.990
  residual-heavy-churn fusion=true keys=10000 sweep_ms_median: baseline=2.454 candidate=2.446
  residual-heavy-churn fusion=true keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  residual-heavy-churn fusion=true keys=10000 sweeps_total: baseline=10 candidate=10
  residual-heavy-churn fusion=true keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  residual-heavy-churn fusion=true keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  residual-heavy-churn fusion=true keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  residual-heavy-churn fusion=true keys=10000 restore_plus_reseal_ms: baseline=495.4 candidate=486.8 ratio=0.983
  residual-heavy-churn fusion=true keys=100000 sweep_ms_median: baseline=57.76 candidate=57.22
  residual-heavy-churn fusion=true keys=100000 evicted_per_sweep_median: baseline=8.275e+04 candidate=8.275e+04
  residual-heavy-churn fusion=true keys=100000 sweeps_total: baseline=1 candidate=1
  residual-heavy-churn fusion=true keys=100000 swept_win_visited_median: baseline=-1 candidate=8.375e+04
  residual-heavy-churn fusion=true keys=100000 swept_win_removed_median: baseline=-1 candidate=8.275e+04
  residual-heavy-churn fusion=true keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  residual-heavy-churn fusion=true keys=100000 restore_plus_reseal_ms: baseline=1612 candidate=1626 ratio=1.009
  residual-heavy-cold-restore fusion=false keys=10000 restore_plus_reseal_ms: baseline=199.1 candidate=181.1 ratio=0.910
  residual-heavy-cold-restore fusion=false keys=10000 evicted_mb: baseline=337.7 candidate=338.1
  residual-heavy-cold-restore fusion=false keys=100000 restore_plus_reseal_ms: baseline=867 candidate=808.3 ratio=0.932
  residual-heavy-cold-restore fusion=false keys=100000 evicted_mb: baseline=2094.6 candidate=2091.8
  residual-heavy-cold-restore fusion=true keys=10000 restore_plus_reseal_ms: baseline=188 candidate=184.6 ratio=0.982
  residual-heavy-cold-restore fusion=true keys=10000 evicted_mb: baseline=338.1 candidate=338.1
  residual-heavy-cold-restore fusion=true keys=100000 restore_plus_reseal_ms: baseline=808.7 candidate=807.7 ratio=0.999
  residual-heavy-cold-restore fusion=true keys=100000 evicted_mb: baseline=2091.8 candidate=2091.8
  wide-above-budget-churn fusion=false keys=10000 sweep_ms_median: baseline=17.45 candidate=17.72
  wide-above-budget-churn fusion=false keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  wide-above-budget-churn fusion=false keys=10000 sweeps_total: baseline=10 candidate=10
  wide-above-budget-churn fusion=false keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  wide-above-budget-churn fusion=false keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  wide-above-budget-churn fusion=false keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  wide-above-budget-churn fusion=false keys=10000 restore_plus_reseal_ms: baseline=164.1 candidate=77.75 ratio=0.474
  wide-above-budget-churn fusion=false keys=100000 sweep_ms_median: baseline=199 candidate=198.8
  wide-above-budget-churn fusion=false keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  wide-above-budget-churn fusion=false keys=100000 sweeps_total: baseline=5 candidate=5
  wide-above-budget-churn fusion=false keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  wide-above-budget-churn fusion=false keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  wide-above-budget-churn fusion=false keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  wide-above-budget-churn fusion=false keys=100000 restore_plus_reseal_ms: baseline=1067 candidate=462.9 ratio=0.434
  wide-above-budget-churn fusion=true keys=10000 sweep_ms_median: baseline=0.6965 candidate=0.694
  wide-above-budget-churn fusion=true keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  wide-above-budget-churn fusion=true keys=10000 sweeps_total: baseline=10 candidate=10
  wide-above-budget-churn fusion=true keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  wide-above-budget-churn fusion=true keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  wide-above-budget-churn fusion=true keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  wide-above-budget-churn fusion=true keys=10000 restore_plus_reseal_ms: baseline=54.53 candidate=52.07 ratio=0.955
  wide-above-budget-churn fusion=true keys=100000 sweep_ms_median: baseline=12.68 candidate=12.35
  wide-above-budget-churn fusion=true keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  wide-above-budget-churn fusion=true keys=100000 sweeps_total: baseline=5 candidate=5
  wide-above-budget-churn fusion=true keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  wide-above-budget-churn fusion=true keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  wide-above-budget-churn fusion=true keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  wide-above-budget-churn fusion=true keys=100000 restore_plus_reseal_ms: baseline=233 candidate=225.8 ratio=0.969
  wide-above-budget-cold-restore fusion=false keys=10000 restore_plus_reseal_ms: baseline=186.7 candidate=80.85 ratio=0.433
  wide-above-budget-cold-restore fusion=false keys=10000 evicted_mb: baseline=182.4 candidate=119.6
  wide-above-budget-cold-restore fusion=false keys=100000 restore_plus_reseal_ms: baseline=828.2 candidate=350.6 ratio=0.423
  wide-above-budget-cold-restore fusion=false keys=100000 evicted_mb: baseline=383.3 candidate=265.0
  wide-above-budget-cold-restore fusion=true keys=10000 restore_plus_reseal_ms: baseline=66.04 candidate=65.17 ratio=0.987
  wide-above-budget-cold-restore fusion=true keys=10000 evicted_mb: baseline=119.6 candidate=119.6
  wide-above-budget-cold-restore fusion=true keys=100000 restore_plus_reseal_ms: baseline=222 candidate=210.8 ratio=0.950
  wide-above-budget-cold-restore fusion=true keys=100000 evicted_mb: baseline=265.0 candidate=265.0
  wide-at-budget-churn fusion=false keys=10000 sweep_ms_median: baseline=17.74 candidate=17.74
  wide-at-budget-churn fusion=false keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  wide-at-budget-churn fusion=false keys=10000 sweeps_total: baseline=10 candidate=10
  wide-at-budget-churn fusion=false keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  wide-at-budget-churn fusion=false keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  wide-at-budget-churn fusion=false keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  wide-at-budget-churn fusion=false keys=10000 restore_plus_reseal_ms: baseline=171.7 candidate=68.96 ratio=0.402
  wide-at-budget-churn fusion=false keys=100000 sweep_ms_median: baseline=197 candidate=196.4
  wide-at-budget-churn fusion=false keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  wide-at-budget-churn fusion=false keys=100000 sweeps_total: baseline=5 candidate=5
  wide-at-budget-churn fusion=false keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  wide-at-budget-churn fusion=false keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  wide-at-budget-churn fusion=false keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  wide-at-budget-churn fusion=false keys=100000 restore_plus_reseal_ms: baseline=955.1 candidate=453.7 ratio=0.475
  wide-at-budget-churn fusion=true keys=10000 sweep_ms_median: baseline=0.7795 candidate=0.7725
  wide-at-budget-churn fusion=true keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  wide-at-budget-churn fusion=true keys=10000 sweeps_total: baseline=10 candidate=10
  wide-at-budget-churn fusion=true keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  wide-at-budget-churn fusion=true keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  wide-at-budget-churn fusion=true keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  wide-at-budget-churn fusion=true keys=10000 restore_plus_reseal_ms: baseline=40.88 candidate=42.32 ratio=1.035
  wide-at-budget-churn fusion=true keys=100000 sweep_ms_median: baseline=12.29 candidate=12.32
  wide-at-budget-churn fusion=true keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  wide-at-budget-churn fusion=true keys=100000 sweeps_total: baseline=5 candidate=5
  wide-at-budget-churn fusion=true keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  wide-at-budget-churn fusion=true keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  wide-at-budget-churn fusion=true keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  wide-at-budget-churn fusion=true keys=100000 restore_plus_reseal_ms: baseline=172.8 candidate=169.6 ratio=0.981
  wide-at-budget-cold-restore fusion=false keys=10000 restore_plus_reseal_ms: baseline=200.4 candidate=73.93 ratio=0.369
  wide-at-budget-cold-restore fusion=false keys=10000 evicted_mb: baseline=181.5 candidate=114.2
  wide-at-budget-cold-restore fusion=false keys=100000 restore_plus_reseal_ms: baseline=803.3 candidate=312.3 ratio=0.389
  wide-at-budget-cold-restore fusion=false keys=100000 evicted_mb: baseline=381.7 candidate=255.2
  wide-at-budget-cold-restore fusion=true keys=10000 restore_plus_reseal_ms: baseline=55.47 candidate=54.48 ratio=0.982
  wide-at-budget-cold-restore fusion=true keys=10000 evicted_mb: baseline=114.2 candidate=114.2
  wide-at-budget-cold-restore fusion=true keys=100000 restore_plus_reseal_ms: baseline=168.4 candidate=165.4 ratio=0.983
  wide-at-budget-cold-restore fusion=true keys=100000 evicted_mb: baseline=255.2 candidate=255.2
  wide-below-budget-churn fusion=false keys=10000 sweep_ms_median: baseline=16.14 candidate=16.33
  wide-below-budget-churn fusion=false keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  wide-below-budget-churn fusion=false keys=10000 sweeps_total: baseline=10 candidate=10
  wide-below-budget-churn fusion=false keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  wide-below-budget-churn fusion=false keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  wide-below-budget-churn fusion=false keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  wide-below-budget-churn fusion=false keys=10000 restore_plus_reseal_ms: baseline=154.1 candidate=63.12 ratio=0.410
  wide-below-budget-churn fusion=false keys=100000 sweep_ms_median: baseline=183.3 candidate=188.6
  wide-below-budget-churn fusion=false keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  wide-below-budget-churn fusion=false keys=100000 sweeps_total: baseline=5 candidate=5
  wide-below-budget-churn fusion=false keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  wide-below-budget-churn fusion=false keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  wide-below-budget-churn fusion=false keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  wide-below-budget-churn fusion=false keys=100000 restore_plus_reseal_ms: baseline=993.5 candidate=425.8 ratio=0.429
  wide-below-budget-churn fusion=true keys=10000 sweep_ms_median: baseline=0.743 candidate=0.758
  wide-below-budget-churn fusion=true keys=10000 evicted_per_sweep_median: baseline=5000 candidate=5000
  wide-below-budget-churn fusion=true keys=10000 sweeps_total: baseline=10 candidate=10
  wide-below-budget-churn fusion=true keys=10000 swept_win_visited_median: baseline=-1 candidate=6000
  wide-below-budget-churn fusion=true keys=10000 swept_win_removed_median: baseline=-1 candidate=5000
  wide-below-budget-churn fusion=true keys=10000 swept_win_inc_median: baseline=-1 candidate=1
  wide-below-budget-churn fusion=true keys=10000 restore_plus_reseal_ms: baseline=40.13 candidate=40.25 ratio=1.003
  wide-below-budget-churn fusion=true keys=100000 sweep_ms_median: baseline=12.2 candidate=11.91
  wide-below-budget-churn fusion=true keys=100000 evicted_per_sweep_median: baseline=5e+04 candidate=5e+04
  wide-below-budget-churn fusion=true keys=100000 sweeps_total: baseline=5 candidate=5
  wide-below-budget-churn fusion=true keys=100000 swept_win_visited_median: baseline=-1 candidate=5.1e+04
  wide-below-budget-churn fusion=true keys=100000 swept_win_removed_median: baseline=-1 candidate=5e+04
  wide-below-budget-churn fusion=true keys=100000 swept_win_inc_median: baseline=-1 candidate=1
  wide-below-budget-churn fusion=true keys=100000 restore_plus_reseal_ms: baseline=163.4 candidate=161.7 ratio=0.989
  wide-below-budget-cold-restore fusion=false keys=10000 restore_plus_reseal_ms: baseline=193.7 candidate=64.35 ratio=0.332
  wide-below-budget-cold-restore fusion=false keys=10000 evicted_mb: baseline=172.2 candidate=109.4
  wide-below-budget-cold-restore fusion=false keys=100000 restore_plus_reseal_ms: baseline=798.4 candidate=287.7 ratio=0.360
  wide-below-budget-cold-restore fusion=false keys=100000 evicted_mb: baseline=363.5 candidate=245.3
  wide-below-budget-cold-restore fusion=true keys=10000 restore_plus_reseal_ms: baseline=53.61 candidate=54.39 ratio=1.015
  wide-below-budget-cold-restore fusion=true keys=10000 evicted_mb: baseline=109.4 candidate=109.4
  wide-below-budget-cold-restore fusion=true keys=100000 restore_plus_reseal_ms: baseline=160 candidate=156.4 ratio=0.978
  wide-below-budget-cold-restore fusion=true keys=100000 evicted_mb: baseline=245.3 candidate=245.3

38 failed or incomplete gate(s)
```
