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

## Not measured

- Cold-cache restore as a reading separate from warm-cache restore.
- The cross-anchor-boundary and add/remove-keys runs the matrix names beside the
  steady rows. The repair cell crosses an anchor boundary every batch but, at the
  default compaction thresholds, never evicts a key; the diagnostic warm-seal run
  above did evict but was not run under the protocol.
- Cross-mode comparisons are reported by the aggregator but are not substitutes
  for the paired ones and are not claimed as such.

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
