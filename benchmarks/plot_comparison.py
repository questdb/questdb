#!/usr/bin/env python3
"""Plot IndexComparisonBenchmark results."""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

# --- Raw data from benchmark run ---
scenarios = ['S1: 5M keys\n4 v/k', 'S2: 512 keys\n7K v/k', 'S3: Streaming\n50K keys', 'S4: 8K keys\n10K v/k', 'S5: Zipfian\n1K keys']
short_scenarios = ['S1', 'S2', 'S3', 'S4', 'S5']
formats = ['Legacy', 'FSST', 'Posting']
colors = {'Legacy': '#8c8c8c', 'FSST': '#4c9ed9', 'Posting': '#d94f4f'}

size_sealed = {
    'Legacy':  [534.1, 32.0, 54.0, 1000.4, 171.8],
    'FSST':    [125.1, 16.9, 21.9,  357.3,  47.9],
    'Posting': [195.0,  1.0, 13.9,   21.7,  14.7],
}
size_unsealed = {
    'Legacy':  [ 534.1,  32.0,  54.0, 1000.4, 171.8],
    'FSST':    [ 126.0,  19.0, 218.0,  358.0,  59.0],
    'Posting': [2556.0,   2.0,  23.0,  626.0,  79.0],
}
bval = {
    'Legacy':  [28.0,  9.2, 11.3, 13.1, 18.0],
    'FSST':    [ 6.6,  4.8,  4.6,  4.7,  5.0],
    'Posting': [10.2,  0.3,  2.9,  0.3,  1.5],
}
write_ms = {
    'Legacy':  [1274,  38, 116,  840, 114],
    'FSST':    [ 956,  86, 251, 1270, 210],
    'Posting': [1627,  36, 256,  480, 134],
}
point_sealed = {
    'Legacy':  [ 24.6, 13.0,  9.4, 310.1, 37.2],
    'FSST':    [  6.0, 34.8, 14.6, 820.0, 99.9],
    'Posting': [ 23.7, 13.5,  5.8, 291.6, 43.6],
}
point_unsealed = {
    'Legacy':  [ 24.6,  13.0,   9.4, 310.1,  37.2],
    'FSST':    [  3.8,  43.2, 219.3, 823.7, 129.6],
    'Posting': [ 30.1,  14.0,  12.9, 291.6,  43.5],
}
scan_sealed = {
    'Legacy':  [952.7, 13.5, 30.5, 323.0,  38.4],
    'FSST':    [315.9, 37.5, 66.6, 815.5, 101.8],
    'Posting': [199.0, 13.4, 24.8, 288.1,  44.0],
}
range_sealed = {
    'Legacy':  [ 3.1,  3.3,  1.0, 23.5, 20.2],
    'FSST':    [ 1.5, 22.9,  1.3, 72.7, 68.7],
    'Posting': [16.9,  7.6,  0.6, 21.4, 26.5],
}

geo = lambda vals: np.exp(np.mean(np.log(vals)))

# ============================================================
# Figure 1: Size + B/val (the money chart)
# ============================================================
fig, axes = plt.subplots(1, 3, figsize=(16, 5))

# 1a: Size sealed (log scale)
ax = axes[0]
x = np.arange(len(scenarios))
w = 0.25
for i, fmt in enumerate(formats):
    bars = ax.bar(x + (i - 1) * w, size_sealed[fmt], w, label=fmt, color=colors[fmt])
    for bar, val in zip(bars, size_sealed[fmt]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:.0f}',
                ha='center', va='bottom', fontsize=6)
ax.set_yscale('log')
ax.set_ylabel('Size (MB, log scale)')
ax.set_title('Storage Size (sealed)')
ax.set_xticks(x)
ax.set_xticklabels(scenarios, fontsize=8)
ax.legend(fontsize=8)
ax.grid(axis='y', alpha=0.3)

# 1b: Bytes/value
ax = axes[1]
for i, fmt in enumerate(formats):
    bars = ax.bar(x + (i - 1) * w, bval[fmt], w, label=fmt, color=colors[fmt])
    for bar, val in zip(bars, bval[fmt]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:.1f}',
                ha='center', va='bottom', fontsize=6)
ax.set_ylabel('Bytes / Value')
ax.set_title('Storage Efficiency (sealed)')
ax.set_xticks(x)
ax.set_xticklabels(scenarios, fontsize=8)
ax.legend(fontsize=8)
ax.grid(axis='y', alpha=0.3)

# 1c: Unsealed vs sealed size (Posting only, showing compaction benefit)
ax = axes[2]
posting_u = size_unsealed['Posting']
posting_s = size_sealed['Posting']
bars_u = ax.bar(x - 0.15, posting_u, 0.3, label='Unsealed', color='#e8a0a0')
bars_s = ax.bar(x + 0.15, posting_s, 0.3, label='Sealed', color='#d94f4f')
for bar, val in zip(bars_u, posting_u):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:.0f}',
            ha='center', va='bottom', fontsize=7)
for bar, val in zip(bars_s, posting_s):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:.0f}',
            ha='center', va='bottom', fontsize=7)
ax.set_yscale('log')
ax.set_ylabel('Size (MB, log scale)')
ax.set_title('Posting: Unsealed vs Sealed')
ax.set_xticks(x)
ax.set_xticklabels(scenarios, fontsize=8)
ax.legend(fontsize=8)
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('index_comparison_size.png', dpi=150, bbox_inches='tight')
print('Saved index_comparison_size.png')

# ============================================================
# Figure 2: Read latency (point, scan, range) — sealed
# ============================================================
fig, axes = plt.subplots(1, 3, figsize=(16, 5))

for ax, data, title in [
    (axes[0], point_sealed, 'Point Queries (sealed)'),
    (axes[1], scan_sealed, 'Full Scan (sealed)'),
    (axes[2], range_sealed, 'Range Scan (sealed)'),
]:
    for i, fmt in enumerate(formats):
        bars = ax.bar(x + (i - 1) * w, data[fmt], w, label=fmt, color=colors[fmt])
        for bar, val in zip(bars, data[fmt]):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                    f'{val:.1f}', ha='center', va='bottom', fontsize=5.5)
    ax.set_yscale('log')
    ax.set_ylabel('Latency (ms, log scale)')
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(scenarios, fontsize=8)
    ax.legend(fontsize=7)
    ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('index_comparison_read.png', dpi=150, bbox_inches='tight')
print('Saved index_comparison_read.png')

# ============================================================
# Figure 3: Geomean summary (normalized to Legacy = 1.0)
# ============================================================
metrics = [
    ('Size\n(sealed)', size_sealed),
    ('B/val', bval),
    ('Write', write_ms),
    ('Point\nQuery', point_sealed),
    ('Full\nScan', scan_sealed),
    ('Range\nScan', range_sealed),
]

fig, ax = plt.subplots(figsize=(10, 5))
x_geo = np.arange(len(metrics))
w_geo = 0.25

for i, fmt in enumerate(formats):
    geos = []
    for _, data in metrics:
        geos.append(geo(data[fmt]))
    # Normalize to Legacy
    legacy_geos = [geo(data[fmt_name]) for _, data in metrics for fmt_name in ['Legacy']]
    legacy_geos = [geo(data['Legacy']) for _, data in metrics]
    normed = [g / l for g, l in zip(geos, legacy_geos)]

    bars = ax.bar(x_geo + (i - 1) * w_geo, normed, w_geo, label=fmt, color=colors[fmt])
    for bar, val in zip(bars, normed):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                f'{val:.2f}x', ha='center', va='bottom', fontsize=7, fontweight='bold')

ax.axhline(y=1.0, color='black', linewidth=0.8, linestyle='--', alpha=0.5)
ax.set_ylabel('Relative to Legacy (lower = better)')
ax.set_title('Geomean Across All Scenarios (normalized to Legacy = 1.0)')
ax.set_xticks(x_geo)
ax.set_xticklabels([m[0] for m in metrics], fontsize=9)
ax.legend(fontsize=9)
ax.grid(axis='y', alpha=0.3)
ax.set_ylim(0, max(2.0, ax.get_ylim()[1]))

plt.tight_layout()
plt.savefig('index_comparison_geomean.png', dpi=150, bbox_inches='tight')
print('Saved index_comparison_geomean.png')

# ============================================================
# Figure 4: Unsealed read penalty (ratio of unsealed/sealed latency)
# ============================================================
fig, ax = plt.subplots(figsize=(8, 5))

# Only FSST and Posting have unsealed != sealed
for i, fmt in enumerate(['FSST', 'Posting']):
    ratios = [u / s if s > 0 else 1 for u, s in zip(point_unsealed[fmt], point_sealed[fmt])]
    offset = (i - 0.5) * 0.35
    bars = ax.bar(x + offset, ratios, 0.3, label=fmt, color=colors[fmt])
    for bar, val in zip(bars, ratios):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                f'{val:.1f}x', ha='center', va='bottom', fontsize=8)

ax.axhline(y=1.0, color='black', linewidth=0.8, linestyle='--', alpha=0.5)
ax.set_ylabel('Unsealed / Sealed latency ratio')
ax.set_title('Seal Benefit: Point Query Latency (1.0 = no change)')
ax.set_xticks(x)
ax.set_xticklabels(scenarios, fontsize=8)
ax.legend(fontsize=9)
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('index_comparison_seal_benefit.png', dpi=150, bbox_inches='tight')
print('Saved index_comparison_seal_benefit.png')

print('All plots saved.')
