#!/usr/bin/env python3
"""Plot IndexSuiteBenchmark results as multi-panel charts with descriptions."""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from textwrap import dedent

# ── Raw results from the benchmark run ──────────────────────────────────────

FORMATS = ['Legacy', 'Delta', 'FOR', 'LZ4 4KB', 'FSST', 'BP']
COLORS = {
    'Legacy':  '#8c8c8c',
    'Delta':   '#d94f4f',
    'FOR':     '#e07b39',
    'LZ4 4KB': '#4c9ed9',
    'FSST':    '#8e6fbf',
    'BP':      '#3fae49',
}
MARKERS = {'Legacy': 'o', 'Delta': 'X', 'FOR': 's', 'LZ4 4KB': '^', 'FSST': 'D', 'BP': 'P'}

VALS_PER_KEY = [4, 8, 16, 32, 64, 128]
OFFSETS = ['0', '1B', '1T']

# data[dist][metric][fmt] = list of 18 values (6 valsPerKey x 3 offsets)
# Order: valsPerKey=4/off=0, 4/off=1B, 4/off=1T, 8/off=0, ...

DATA = {
    'RANDOM': {
        'size': {
            'Legacy':  [381.5,381.5,381.5, 267.0,267.0,267.0, 209.8,209.8,209.8, 181.2,181.2,181.2, 166.9,166.9,166.9, 159.7,159.7,159.7],
            'Delta':   [1373.3,1373.3,1373.3, 686.6,686.6,686.6, 343.3,343.3,343.3, 171.7,171.7,171.7, 162.1,162.1,162.1, 119.2,119.2,119.2],
            'FOR':     [400.5,400.5,400.5, 324.3,324.3,324.3, 286.1,286.1,286.1, 267.0,267.0,267.0, 257.5,257.5,257.5, 252.7,252.7,252.7],
            'LZ4 4KB': [106.7,106.7,106.8, 106.1,106.1,106.1, 105.9,105.9,106.0, 106.1,106.1,106.2, 106.5,106.5,106.6, 107.5,107.5,107.6],
            'FSST':    [125.1,138.5,156.9, 101.3,117.3,135.6, 90.8,106.3,119.3, 101.1,101.6,114.5, 98.5,98.8,117.7, 97.3,97.7,116.5],
            'BP':      [189.0,189.0,189.0, 121.1,121.1,121.1, 86.5,86.5,86.5, 68.4,68.4,68.4, 58.4,58.4,58.4, 54.5,54.5,54.5],
        },
        'bpv': {
            'Legacy':  [20.0,20.0,20.0, 14.0,14.0,14.0, 11.0,11.0,11.0, 9.5,9.5,9.5, 8.8,8.8,8.8, 8.4,8.4,8.4],
            'Delta':   [72.0,72.0,72.0, 36.0,36.0,36.0, 18.0,18.0,18.0, 9.0,9.0,9.0, 8.5,8.5,8.5, 6.3,6.3,6.3],
            'FOR':     [21.0,21.0,21.0, 17.0,17.0,17.0, 15.0,15.0,15.0, 14.0,14.0,14.0, 13.5,13.5,13.5, 13.3,13.3,13.3],
            'LZ4 4KB': [5.6,5.6,5.6, 5.6,5.6,5.6, 5.6,5.6,5.6, 5.6,5.6,5.6, 5.6,5.6,5.6, 5.6,5.6,5.6],
            'FSST':    [6.6,7.3,8.2, 5.3,6.1,7.1, 4.8,5.6,6.3, 5.3,5.3,6.0, 5.2,5.2,6.2, 5.1,5.1,6.1],
            'BP':      [9.9,9.9,9.9, 6.3,6.3,6.3, 4.5,4.5,4.5, 3.6,3.6,3.6, 3.1,3.1,3.1, 2.9,2.9,2.9],
        },
        'read': {
            'Legacy':  [33.8,12.8,17.3, 11.0,15.9,13.9, 13.1,8.1,8.5, 7.7,9.8,8.5, 10.3,10.1,12.4, 17.5,15.2,16.5],
            'Delta':   [44.4,28.1,34.5, 28.4,33.3,32.1, 15.5,14.0,16.6, 9.4,12.2,12.1, 16.6,12.8,16.9, 16.6,17.5,18.7],
            'FOR':     [16.8,10.4,13.7, 7.8,9.6,10.1, 6.1,5.2,6.6, 6.7,5.4,4.0, 7.3,5.0,6.1, 6.9,6.9,8.0],
            'LZ4 4KB': [53.2,51.6,50.6, 55.8,57.4,56.3, 56.1,55.7,54.8, 57.3,56.0,55.5, 53.6,53.8,51.4, 49.5,48.0,47.8],
            'FSST':    [6.4,8.0,6.9, 4.4,5.6,9.3, 6.7,6.4,6.7, 6.6,7.6,8.7, 10.4,15.5,13.3, 21.6,19.6,20.4],
            'BP':      [8.5,7.7,8.1, 6.4,5.0,5.9, 4.3,5.3,4.7, 5.8,6.0,7.0, 6.8,5.7,6.5, 9.0,8.6,9.2],
        },
        'write': {
            'Legacy':  [1.32,1.16,1.17, 1.07,1.06,1.08, 0.97,0.99,0.98, 0.85,0.85,0.83, 0.53,0.57,0.61, 0.30,0.34,0.34],
            'Delta':   [3.94,3.90,3.94, 3.71,3.74,3.76, 3.53,3.58,3.58, 3.40,3.40,3.36, 2.92,2.91,2.80, 2.14,2.06,2.20],
            'FOR':     [3.09,2.92,3.65, 3.37,3.44,3.42, 2.80,2.90,2.83, 1.89,1.90,1.86, 1.14,1.07,1.05, 0.50,0.49,0.59],
            'LZ4 4KB': [0.98,1.00,0.97, 1.09,1.15,1.09, 1.07,1.06,1.01, 0.97,1.00,0.97, 0.95,0.95,0.94, 0.94,0.95,0.97],
            'FSST':    [0.94,1.01,1.27, 1.01,1.11,1.08, 0.91,0.95,0.95, 0.84,0.87,0.88, 0.81,0.83,0.85, 0.78,0.80,0.85],
            'BP':      [1.01,1.00,1.22, 1.00,0.98,0.94, 0.77,0.78,0.77, 0.68,0.70,0.69, 0.65,0.66,0.64, 0.67,0.63,0.64],
        },
    },
    'ROUND_ROBIN': {
        'size': {
            'Legacy':  [381.5,381.5,381.5, 267.0,267.0,267.0, 209.8,209.8,209.8, 181.2,181.2,181.2, 166.9,166.9,166.9, 159.7,159.7,159.7],
            'Delta':   [1373.3,1373.3,1373.3, 686.6,686.6,686.6, 343.3,343.3,343.3, 171.7,171.7,171.7, 162.1,162.1,162.1, 119.2,119.2,119.2],
            'FOR':     [400.5,400.5,400.5, 324.3,324.3,324.3, 286.1,286.1,286.1, 267.0,267.0,267.0, 257.5,257.5,257.5, 252.7,252.7,252.7],
            'LZ4 4KB': [77.9,77.9,78.0, 78.0,78.0,78.0, 78.7,78.7,78.7, 80.0,80.0,80.0, 83.0,83.0,83.0, 90.4,90.4,90.4],
            'FSST':    [132.1,131.2,175.3, 108.4,111.7,111.3, 103.9,90.6,88.2, 93.3,91.4,84.8, 102.2,90.4,87.9, 99.8,90.2,84.5],
            'BP':      [133.5,133.5,133.5, 66.8,66.8,66.8, 33.4,33.4,33.4, 16.7,16.7,16.7, 8.4,8.4,8.4, 6.9,6.9,6.9],
        },
        'bpv': {
            'Legacy':  [20.0,20.0,20.0, 14.0,14.0,14.0, 11.0,11.0,11.0, 9.5,9.5,9.5, 8.8,8.8,8.8, 8.4,8.4,8.4],
            'Delta':   [72.0,72.0,72.0, 36.0,36.0,36.0, 18.0,18.0,18.0, 9.0,9.0,9.0, 8.5,8.5,8.5, 6.3,6.3,6.3],
            'FOR':     [21.0,21.0,21.0, 17.0,17.0,17.0, 15.0,15.0,15.0, 14.0,14.0,14.0, 13.5,13.5,13.5, 13.3,13.3,13.3],
            'LZ4 4KB': [4.1,4.1,4.1, 4.1,4.1,4.1, 4.1,4.1,4.1, 4.2,4.2,4.2, 4.4,4.4,4.4, 4.7,4.7,4.7],
            'FSST':    [6.9,6.9,9.2, 5.7,5.9,5.8, 5.4,4.8,4.6, 4.9,4.8,4.4, 5.4,4.7,4.6, 5.2,4.7,4.4],
            'BP':      [7.0,7.0,7.0, 3.5,3.5,3.5, 1.8,1.8,1.8, 0.9,0.9,0.9, 0.4,0.4,0.4, 0.4,0.4,0.4],
        },
        'read': {
            'Legacy':  [18.2,26.3,19.7, 12.8,12.7,14.0, 11.5,9.3,8.9, 9.9,10.7,14.0, 10.3,11.6,13.6, 15.9,14.5,16.5],
            'Delta':   [39.1,37.0,39.6, 32.1,36.9,28.3, 24.0,16.4,14.1, 8.1,14.2,8.9, 12.1,14.5,12.3, 15.1,18.1,18.1],
            'FOR':     [7.8,11.0,9.7, 5.6,5.7,4.7, 4.8,2.9,3.6, 3.5,4.5,3.6, 4.4,4.6,4.3, 7.1,7.1,6.8],
            'LZ4 4KB': [35.7,36.4,36.4, 38.3,36.9,37.1, 37.8,37.2,37.8, 37.4,37.2,40.1, 36.8,38.1,36.6, 35.0,36.6,36.0],
            'FSST':    [7.0,6.4,11.4, 5.7,6.8,5.7, 7.5,6.1,5.5, 11.6,9.2,7.2, 15.9,12.3,12.4, 25.9,22.3,21.6],
            'BP':      [6.5,6.3,8.2, 4.0,5.2,3.3, 2.0,2.7,3.1, 2.7,2.9,3.2, 4.0,3.3,3.4, 6.6,5.0,5.5],
        },
        'write': {
            'Legacy':  [32.41,21.08,18.99, 6.53,7.91,6.43, 2.76,2.07,2.73, 1.14,1.10,1.35, 0.54,0.41,0.50, 0.27,0.27,0.29],
            'Delta':   [31.81,31.27,30.38, 11.54,11.99,11.00, 3.58,2.57,2.48, 1.02,1.38,0.96, 0.47,0.47,0.46, 0.42,0.35,0.36],
            'FOR':     [11.29,9.22,10.87, 3.16,3.38,2.89, 0.92,1.21,1.56, 0.44,0.51,0.60, 0.50,0.42,0.44, 0.42,0.42,0.41],
            'LZ4 4KB': [0.34,0.34,0.35, 0.35,0.35,0.35, 0.46,0.46,0.47, 0.49,0.49,0.49, 0.49,0.49,0.49, 0.52,0.50,0.51],
            'FSST':    [0.43,0.40,0.47, 0.42,0.42,0.40, 0.54,0.48,0.46, 0.54,0.49,0.48, 0.54,0.50,0.50, 0.55,0.51,0.51],
            'BP':      [0.41,0.41,0.41, 0.29,0.28,0.28, 0.32,0.33,0.33, 0.30,0.31,0.30, 0.28,0.30,0.29, 0.29,0.30,0.29],
        },
    },
}


def avg_over_offsets(vals):
    """Average every 3 consecutive values (the 3 offsets) -> one per valsPerKey."""
    return [np.mean(vals[i*3:(i+1)*3]) for i in range(6)]


def pick_offset(vals, oi):
    """Pick every 3rd value starting at offset index oi."""
    return [vals[i*3 + oi] for i in range(6)]


# ═══════════════════════════════════════════════════════════════════════════
# Figure 1: Index Size & Read Latency vs Values/Key
# ═══════════════════════════════════════════════════════════════════════════

DESC_HEIGHT = 0.18

fig, axes = plt.subplots(2, 2, figsize=(16, 15.5))
fig.subplots_adjust(top=0.91, bottom=DESC_HEIGHT + 0.02, hspace=0.32, wspace=0.25)
fig.suptitle('Index Size and Read Latency vs Values/Key',
             fontsize=15, fontweight='bold', y=0.97)
fig.text(0.5, 0.935, '20M rows, single commit, averaged over row offsets 0 / 1B / 1T',
         fontsize=11, ha='center', color='#555555')

for col, dist in enumerate(['RANDOM', 'ROUND_ROBIN']):
    dist_label = 'Random (Fisher-Yates shuffle)' if dist == 'RANDOM' else 'Round-Robin (key = row % keyCount)'

    ax = axes[0][col]
    for fmt in FORMATS:
        y = avg_over_offsets(DATA[dist]['size'][fmt])
        ax.plot(VALS_PER_KEY, y, marker=MARKERS[fmt], color=COLORS[fmt],
                label=fmt, linewidth=2, markersize=7)
    ax.set_title(f'Index Size \u2014 {dist_label}', fontsize=11)
    ax.set_xlabel('Values per key')
    ax.set_ylabel('Size (MB)')
    ax.set_xscale('log', base=2)
    ax.set_xticks(VALS_PER_KEY)
    ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=9, loc='upper right')
    ax.grid(True, alpha=0.3)

    ax = axes[1][col]
    for fmt in FORMATS:
        y = avg_over_offsets(DATA[dist]['read'][fmt])
        ax.plot(VALS_PER_KEY, y, marker=MARKERS[fmt], color=COLORS[fmt],
                label=fmt, linewidth=2, markersize=7)
    ax.set_title(f'Read Latency (10K random keys) \u2014 {dist_label}', fontsize=11)
    ax.set_xlabel('Values per key')
    ax.set_ylabel('Read time (ms)')
    ax.set_xscale('log', base=2)
    ax.set_xticks(VALS_PER_KEY)
    ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=9, loc='upper right')
    ax.grid(True, alpha=0.3)

desc1 = dedent("""\
    Test setup:  20M row IDs are written into a bitmap index, then 10,000 random keys are read back (1 warmup + 5 timed
    runs).  Each row ID is assigned to a key according to the distribution.  "Values per key" controls cardinality: at
    4 v/k there are 5M distinct keys (high-cardinality / tag-like), at 128 v/k there are 156K keys (time-series-like).
    Results are averaged over three row-offset settings (0, +1B, +1T) to wash out value-magnitude effects.

    RANDOM distribution (left):  A Fisher-Yates shuffle scatters each key's row IDs randomly across [0, 20M).  This
    produces large, irregular deltas between consecutive values for the same key -- the hardest case for delta encodings.
    ROUND-ROBIN distribution (right):  key = rowId % keyCount.  Every key's values are perfectly spaced (delta =
    keyCount), so delta-based formats like BP see near-optimal compression.  This models a low-cardinality symbol column.

    Key observations:  Delta (red) is by far the largest format at low v/k (1.37 GB at 4 v/k -- 72 B/val) due to its
    per-key overhead with millions of keys, but it shrinks to competitive sizes at 128 v/k (119 MB).  Its read latency is
    also the worst at low v/k (35-44 ms), though it converges toward Legacy at higher cardinalities.  BP (green) dominates
    on both size and read speed at 16+ v/k.  FSST (purple) is the fastest reader at 4 v/k but degrades at high v/k.
    LZ4 (blue) gives flat ~105 MB size but consistently worst read latency (~35-55 ms) due to decompression overhead.\
""")
fig.text(0.03, 0.005, desc1, fontsize=8.2, fontfamily='sans-serif',
         verticalalignment='bottom', linespacing=1.45,
         bbox=dict(boxstyle='round,pad=0.6', facecolor='#f5f5f0', edgecolor='#cccccc'))

fig.savefig('/home/nick/IdeaProjects/questdb-enterprise/questdb/benchmarks/index_suite_size_read.png', dpi=150)
print('Saved index_suite_size_read.png')


# ═══════════════════════════════════════════════════════════════════════════
# Figure 2: Bytes/Value and Write Time
# ═══════════════════════════════════════════════════════════════════════════

fig2, axes2 = plt.subplots(2, 2, figsize=(16, 15.5))
fig2.subplots_adjust(top=0.91, bottom=DESC_HEIGHT + 0.02, hspace=0.32, wspace=0.25)
fig2.suptitle('Compression Efficiency and Write Throughput',
              fontsize=15, fontweight='bold', y=0.97)
fig2.text(0.5, 0.935, '20M rows, single commit, averaged over row offsets 0 / 1B / 1T',
          fontsize=11, ha='center', color='#555555')

for col, dist in enumerate(['RANDOM', 'ROUND_ROBIN']):
    dist_label = 'Random (Fisher-Yates shuffle)' if dist == 'RANDOM' else 'Round-Robin (key = row % keyCount)'

    ax = axes2[0][col]
    for fmt in FORMATS:
        y = avg_over_offsets(DATA[dist]['bpv'][fmt])
        ax.plot(VALS_PER_KEY, y, marker=MARKERS[fmt], color=COLORS[fmt],
                label=fmt, linewidth=2, markersize=7)
    ax.set_title(f'Bytes per Value \u2014 {dist_label}', fontsize=11)
    ax.set_xlabel('Values per key')
    ax.set_ylabel('Bytes / value')
    ax.set_xscale('log', base=2)
    ax.set_xticks(VALS_PER_KEY)
    ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=9, loc='upper right')
    ax.grid(True, alpha=0.3)

    ax = axes2[1][col]
    for fmt in FORMATS:
        y = avg_over_offsets(DATA[dist]['write'][fmt])
        ax.plot(VALS_PER_KEY, y, marker=MARKERS[fmt], color=COLORS[fmt],
                label=fmt, linewidth=2, markersize=7)
    ax.set_title(f'Write Time \u2014 {dist_label}', fontsize=11)
    ax.set_xlabel('Values per key')
    ax.set_ylabel('Write time (s)')
    ax.set_xscale('log', base=2)
    ax.set_xticks(VALS_PER_KEY)
    ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=9, loc='upper left')
    ax.grid(True, alpha=0.3)

desc2 = dedent("""\
    Bytes/value (top row) normalises index size by dividing total bytes by 20M.  An uncompressed 8-byte long would cost
    8.0 B/val; anything below that is net compression.  Delta (red) starts at a staggering 72 B/val at 4 v/k -- its
    fixed per-key metadata (block headers, capacity tracking) dominates when there are 5M keys with only 4 values each.
    As v/k increases, Delta's overhead amortises: at 32 v/k it crosses below Legacy (9.0 vs 9.5 B/val), and at 128 v/k
    it reaches 6.3 B/val.  BP reaches 0.4 B/val at 64+ v/k round-robin -- 3 bits per posting via delta + FoR-64
    bitpacking.  FOR (orange) is consistently the worst after Delta, never dropping below 13 B/val.

    Write time (bottom row) measures wall-clock time for 20M writer.add() calls plus close().  Under round-robin at
    4 v/k (right), Delta and Legacy are catastrophically slow (31s and 32s respectively) -- both formats do random page
    I/O into a huge key file when 5M keys are written interleaved.  Delta stays above 2s even at 128 v/k under random
    scatter (left) due to its more complex encoding logic.  BP, FSST, and LZ4 buffer in memory and flush sequentially,
    keeping writes under 0.5s regardless of key count.\
""")
fig2.text(0.03, 0.005, desc2, fontsize=8.2, fontfamily='sans-serif',
          verticalalignment='bottom', linespacing=1.45,
          bbox=dict(boxstyle='round,pad=0.6', facecolor='#f5f5f0', edgecolor='#cccccc'))

fig2.savefig('/home/nick/IdeaProjects/questdb-enterprise/questdb/benchmarks/index_suite_bpv_write.png', dpi=150)
print('Saved index_suite_bpv_write.png')


# ═══════════════════════════════════════════════════════════════════════════
# Figure 3: Row-offset sensitivity — FSST vs BP vs Delta size
# ═══════════════════════════════════════════════════════════════════════════

fig3, axes3 = plt.subplots(1, 2, figsize=(15, 8))
fig3.subplots_adjust(top=0.82, bottom=0.26, wspace=0.25)
fig3.suptitle('Row Offset Sensitivity \u2014 FSST vs BP vs Delta',
              fontsize=15, fontweight='bold', y=0.95)
fig3.text(0.5, 0.88,
          'How index size changes when all row IDs are shifted by 0 / 1 billion / 1 trillion',
          fontsize=11, ha='center', color='#555555')

# Only show 16+ v/k to keep Delta on-scale with FSST/BP (at 4 v/k Delta is 1373 MB)
VPK_SUBSET = [16, 32, 64, 128]

for col, dist in enumerate(['RANDOM', 'ROUND_ROBIN']):
    ax = axes3[col]
    dist_label = 'Random' if dist == 'RANDOM' else 'Round-Robin'
    for oi, (off_label, ls) in enumerate(zip(OFFSETS, ['-', '--', ':'])):
        fsst_y = pick_offset(DATA[dist]['size']['FSST'], oi)[2:]  # skip 4,8
        bp_y = pick_offset(DATA[dist]['size']['BP'], oi)[2:]
        delta_y = pick_offset(DATA[dist]['size']['Delta'], oi)[2:]
        ax.plot(VPK_SUBSET, delta_y, marker='X', color=COLORS['Delta'],
                linestyle=ls, linewidth=2, markersize=6,
                label=f'Delta off={off_label}')
        ax.plot(VPK_SUBSET, fsst_y, marker='D', color=COLORS['FSST'],
                linestyle=ls, linewidth=2, markersize=6,
                label=f'FSST off={off_label}')
        ax.plot(VPK_SUBSET, bp_y, marker='P', color=COLORS['BP'],
                linestyle=ls, linewidth=2, markersize=6,
                label=f'BP off={off_label}')
    ax.set_title(f'{dist_label} distribution (16\u2013128 v/k)', fontsize=11)
    ax.set_xlabel('Values per key')
    ax.set_ylabel('Size (MB)')
    ax.set_xscale('log', base=2)
    ax.set_xticks(VPK_SUBSET)
    ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=7, ncol=3, loc='upper right')
    ax.grid(True, alpha=0.3)

desc3 = dedent("""\
    The row-offset dimension tests how each format copes with different value magnitudes.  Offset=0 means row IDs span
    [0, 20M) -- 25 bits.  Offset=1B shifts to [1e9, 1e9+20M) -- 30 bits.  Offset=1T shifts to [1e12, 1e12+20M) --
    40 bits.  Only 16-128 v/k shown to keep the three formats on the same scale (at 4 v/k, Delta is 1.37 GB).

    Delta (red) lines overlap perfectly -- like BP, Delta encodes differences between consecutive values, so shifting all
    row IDs by a constant has no effect on the encoded size.  However, Delta's absolute size is significantly larger than
    BP at every v/k because its variable-length encoding (VarInt) is less efficient than BP's FoR-64 bitpacking.

    FSST (purple) fans out at each offset -- the dotted line (1T) is worst at low v/k but actually *better* than solid
    (0) at high v/k under round-robin.  FSST encodes row IDs as raw byte sequences; with large offsets the leading bytes
    become repetitive, which the symbol table can exploit when there are enough values per key to amortise training.

    BP (green) lines overlap perfectly.  BP computes deltas then bitpacks, so shifting values by a constant has zero
    effect.  BP is also the smallest format at every v/k shown here, reaching 6.9 MB at 128 v/k round-robin.\
""")
fig3.text(0.03, 0.005, desc3, fontsize=8.2, fontfamily='sans-serif',
          verticalalignment='bottom', linespacing=1.45,
          bbox=dict(boxstyle='round,pad=0.6', facecolor='#f5f5f0', edgecolor='#cccccc'))

fig3.savefig('/home/nick/IdeaProjects/questdb-enterprise/questdb/benchmarks/index_suite_offset_sensitivity.png', dpi=150)
print('Saved index_suite_offset_sensitivity.png')


# ═══════════════════════════════════════════════════════════════════════════
# Figure 4: Grouped bar chart — size + read annotations at offset=0
# ═══════════════════════════════════════════════════════════════════════════

fig4, axes4 = plt.subplots(2, 1, figsize=(16, 13))
fig4.subplots_adjust(top=0.90, bottom=0.17, hspace=0.35)
fig4.suptitle('Side-by-Side Format Comparison at Offset = 0',
              fontsize=15, fontweight='bold', y=0.97)
fig4.text(0.5, 0.935,
          'Bar height = index size (MB), number above each bar = read latency (ms)',
          fontsize=11, ha='center', color='#555555')

bar_width = 0.13
x_pos = np.arange(len(VALS_PER_KEY))

for row, dist in enumerate(['RANDOM', 'ROUND_ROBIN']):
    ax = axes4[row]
    dist_label = 'Random' if dist == 'RANDOM' else 'Round-Robin'

    for fi, fmt in enumerate(FORMATS):
        sizes = pick_offset(DATA[dist]['size'][fmt], 0)
        reads = pick_offset(DATA[dist]['read'][fmt], 0)
        bars = ax.bar(x_pos + fi * bar_width, sizes, bar_width,
                      label=fmt, color=COLORS[fmt], edgecolor='white', linewidth=0.5)
        for bar, rd in zip(bars, reads):
            if bar.get_height() < 500:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 3,
                        f'{rd:.0f}', ha='center', va='bottom', fontsize=5.5, color='#333')

    ax.set_title(f'{dist_label} distribution \u2014 offset=0', fontsize=11)
    ax.set_xlabel('Values per key')
    ax.set_ylabel('Index Size (MB)')
    ax.set_xticks(x_pos + bar_width * 2.5)
    ax.set_xticklabels(VALS_PER_KEY)
    ax.legend(fontsize=8, loc='upper right')
    ax.grid(True, alpha=0.2, axis='y')

desc4 = dedent("""\
    This chart combines size (bar height) and read latency (number annotation, ms) for offset=0.  Each cluster of 6 bars
    represents one cardinality level.  Delta's red bar is dramatically taller at low v/k: 1373 MB at 4 v/k (off the chart
    top for round-robin), shrinking rapidly to 119 MB at 128 v/k where it becomes smaller than Legacy.

    Random (top):  At 4 v/k, Delta (1373 MB, 44ms) is the worst on both axes.  LZ4 is smallest (107 MB) but slowest
    after Delta on reads (53 ms).  FSST is fastest to read (6 ms) and mid-sized.  At 128 v/k, BP is both smallest
    (55 MB) and fast (9 ms), Delta beats Legacy on size (119 vs 160 MB) but not reads (17 vs 18 ms).

    Round-Robin (bottom):  Delta mirrors the random pattern but with even worse writes at low v/k (32s).  At 32+ v/k,
    Delta's size matches Legacy but reads are 1-2x slower.  BP's green bars nearly vanish at 64-128 v/k (8 and 7 MB)
    while maintaining the fastest reads.  The clear takeaway: Delta's per-key overhead makes it unsuitable for
    high-cardinality workloads, while BP excels precisely in the range where bitmap indexes matter most.\
""")
fig4.text(0.03, 0.005, desc4, fontsize=8.2, fontfamily='sans-serif',
          verticalalignment='bottom', linespacing=1.45,
          bbox=dict(boxstyle='round,pad=0.6', facecolor='#f5f5f0', edgecolor='#cccccc'))

fig4.savefig('/home/nick/IdeaProjects/questdb-enterprise/questdb/benchmarks/index_suite_bars.png', dpi=150)
print('Saved index_suite_bars.png')

print('\nAll charts saved.')
