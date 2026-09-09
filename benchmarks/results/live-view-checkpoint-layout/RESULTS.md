# Performance acceptance matrix: measured run

Baseline `6a2c65602856f0e3d9699c167f3c9515ae4419d1` against the candidate at
`4056b2515c` on `puzpuzpuz_live_view`. Both revisions ran on the same machine,
filesystem, JVM, heap, worker count, input and maintenance settings, through
`run-matrix.sh`, which fixes every one of those but the machine. 360 runs, none
failed.

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

## Not measured

- The 1,000,000-key scaling run for the anchor-only and single-SUM shapes.
- The closed-segment repair cell: a one-key correction over ten closed-segment
  checkpoints with an independent result oracle.
- Cold-cache restore as a reading separate from warm-cache restore.

Both remaining runs are producible from the committed driver without further
code. Nothing here extrapolates to unmeasured supported queries.

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
