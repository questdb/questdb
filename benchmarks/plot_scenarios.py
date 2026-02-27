#!/usr/bin/env python3
# /// script
# dependencies = ["matplotlib", "numpy"]
# ///
"""Plot DeltaBitmapIndexBenchmark results across all 3 scenarios."""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

OUT_DIR = '/home/nick/IdeaProjects/questdb-enterprise'

# Color palette
C = {
    'Legacy':           '#8c8c8c',
    'Delta':            '#d94f4f',
    'FOR':              '#e07b39',
    'LZ4':              '#4c9ed9',
    'FSST':             '#8e6fbf',
    'FSST (sealed)':    '#6b4fa0',
    'BP':               '#3fae49',
    'BP (sealed)':      '#1d7a2d',
    'BP (pre-seal)':    '#3fae49',
}

# ════════════════════════════════════════════════════════════════════
# Figure 1: Scenario 1 — High Cardinality (5M keys, 4 vals/key)
# Now with adaptive stride (packed mode) — the big win
# ════════════════════════════════════════════════════════════════════

fig1, axes1 = plt.subplots(1, 3, figsize=(18, 6))
fig1.suptitle('Scenario 1: High Cardinality — 5M keys, 4 values/key, 20M rows',
              fontsize=14, fontweight='bold', y=1.02)

# Storage
labels_s1s = ['Legacy', 'Delta', 'FOR', 'LZ4\n64KB', 'FSST', 'BP']
sizes_s1 = [534.1, 1373.3, 400.5, 97.5, 125.1, 78.9]
colors_s1 = [C['Legacy'], C['Delta'], C['FOR'], C['LZ4'], C['FSST'], C['BP (sealed)']]
bars = axes1[0].bar(labels_s1s, sizes_s1, color=colors_s1, edgecolor='white', linewidth=0.5)
for bar, val in zip(bars, sizes_s1):
    axes1[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 10,
                  f'{val:.0f}', ha='center', va='bottom', fontsize=8, fontweight='bold')
axes1[0].set_ylabel('Size (MB)')
axes1[0].set_title('Storage', fontsize=11)
axes1[0].set_ylim(0, 1600)
axes1[0].grid(True, alpha=0.2, axis='y')

# Point read (10K random keys)
labels_s1r = ['Legacy', 'Delta', 'FOR', 'LZ4\n4KB', 'FSST', 'BP']
reads_s1 = [30.1, 50.8, 9.9, 51.7, 6.6, 5.1]
bars = axes1[1].bar(labels_s1r, reads_s1, color=colors_s1, edgecolor='white', linewidth=0.5)
for bar, val in zip(bars, reads_s1):
    axes1[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                  f'{val:.1f}', ha='center', va='bottom', fontsize=8, fontweight='bold')
axes1[1].set_ylabel('Read time (ms)')
axes1[1].set_title('Point Read (10K random keys)', fontsize=11)
axes1[1].set_ylim(0, 65)
axes1[1].grid(True, alpha=0.2, axis='y')

# Full scan (5M keys, 20M rows)
labels_s1f = ['Legacy', 'Delta', 'FOR', 'LZ4\n64KB', 'FSST', 'BP']
scans_s1 = [975.5, 1136.4, 988.9, 207.4, 292.4, 136.9]
bars = axes1[2].bar(labels_s1f, scans_s1, color=colors_s1, edgecolor='white', linewidth=0.5)
for bar, val in zip(bars, scans_s1):
    axes1[2].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 8,
                  f'{val:.0f}', ha='center', va='bottom', fontsize=8, fontweight='bold')
axes1[2].set_ylabel('Scan time (ms)')
axes1[2].set_title('Full Scan (5M keys, 20M rows)', fontsize=11)
axes1[2].set_ylim(0, 1300)
axes1[2].grid(True, alpha=0.2, axis='y')

fig1.tight_layout()
fig1.savefig(f'{OUT_DIR}/s1_storage.png', dpi=150, bbox_inches='tight')
print('Saved s1_storage.png')


# ════════════════════════════════════════════════════════════════════
# Figure 1b: S1 Read Latency — zoomed to show BP vs FSST vs FOR
# ════════════════════════════════════════════════════════════════════

fig1b, ax1b = plt.subplots(figsize=(10, 6))
fig1b.suptitle('S1: Point Read — 10K random keys from 5M (lower = better)',
               fontsize=13, fontweight='bold', y=1.02)

labels_s1rz = ['FOR', 'FSST', 'BP', 'Legacy', 'Delta', 'LZ4 4KB']
reads_s1z = [9.9, 6.6, 5.1, 30.1, 50.8, 51.7]
colors_s1z = [C['FOR'], C['FSST'], C['BP (sealed)'], C['Legacy'], C['Delta'], C['LZ4']]
bars = ax1b.barh(labels_s1rz, reads_s1z, color=colors_s1z, edgecolor='white', linewidth=0.5)
for bar, val in zip(bars, reads_s1z):
    ax1b.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
              f'{val:.1f} ms', ha='left', va='center', fontsize=10, fontweight='bold')
ax1b.set_xlabel('Read time (ms)')
ax1b.set_xlim(0, 60)
ax1b.grid(True, alpha=0.2, axis='x')
ax1b.invert_yaxis()

fig1b.tight_layout()
fig1b.savefig(f'{OUT_DIR}/s1_read_latency.png', dpi=150, bbox_inches='tight')
print('Saved s1_read_latency.png')


# ════════════════════════════════════════════════════════════════════
# Figure 2: Scenario 2 — Market Data (512 keys, 7168 vals/key)
# ════════════════════════════════════════════════════════════════════

fig2, (ax_s2_storage, ax_s2_read, ax_s2_scan) = plt.subplots(1, 3, figsize=(18, 6))
fig2.suptitle('Scenario 2: Market Data — 512 keys, 7168 values/key, 56 commits',
              fontsize=14, fontweight='bold', y=1.02)

# Storage
labels_s2 = ['Legacy', 'LZ4\n4KB', 'LZ4\n64KB', 'FSST\n56 gens', 'FSST\nsealed', 'BP\n56 gens', 'BP\nsealed']
sizes_s2 = [28.2, 15.4, 14.1, 19.0, 16.9, 2.0, 1.0]
colors_s2 = [C['Legacy'], C['LZ4'], C['LZ4'], C['FSST'], C['FSST (sealed)'], C['BP'], C['BP (sealed)']]
bars = ax_s2_storage.bar(labels_s2, sizes_s2, color=colors_s2, edgecolor='white', linewidth=0.5)
for bar, val in zip(bars, sizes_s2):
    ax_s2_storage.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                       f'{val:.1f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
ax_s2_storage.set_ylabel('Size (MB)')
ax_s2_storage.set_title('Storage', fontsize=11)
ax_s2_storage.set_ylim(0, 35)
ax_s2_storage.grid(True, alpha=0.2, axis='y')

# Read latency (all 512 keys)
labels_s2r = ['Legacy', 'LZ4 4KB\n56 gens', 'FSST\n56 gens', 'FSST\nsealed', 'BP\n56 gens', 'BP\nsealed']
reads_s2 = [10.0, 71.4, 34.6, 32.2, 12.1, 9.6]
colors_s2r = [C['Legacy'], C['LZ4'], C['FSST'], C['FSST (sealed)'], C['BP'], C['BP (sealed)']]
bars = ax_s2_read.bar(labels_s2r, reads_s2, color=colors_s2r, edgecolor='white', linewidth=0.5)
for bar, val in zip(bars, reads_s2):
    ax_s2_read.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f'{val:.1f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
ax_s2_read.set_ylabel('Read time (ms)')
ax_s2_read.set_title('Read Latency (all 512 keys)', fontsize=11)
ax_s2_read.set_ylim(0, 85)
ax_s2_read.grid(True, alpha=0.2, axis='y')

# Full scan
labels_s2f = ['Legacy', 'LZ4 64KB', 'FSST\nsealed', 'BP\nsealed']
scans_s2 = [11.3, 1278.9, 32.2, 9.6]
colors_s2f = [C['Legacy'], C['LZ4'], C['FSST (sealed)'], C['BP (sealed)']]
bars = ax_s2_scan.bar(labels_s2f, scans_s2, color=colors_s2f, edgecolor='white', linewidth=0.5)
for bar, val in zip(bars, scans_s2):
    ax_s2_scan.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 10,
                    f'{val:.1f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
ax_s2_scan.set_ylabel('Scan time (ms)')
ax_s2_scan.set_title('Full Scan (512 keys, 3.7M rows)', fontsize=11)
ax_s2_scan.set_ylim(0, 1500)
ax_s2_scan.grid(True, alpha=0.2, axis='y')

fig2.tight_layout()
fig2.savefig(f'{OUT_DIR}/s2_storage.png', dpi=150, bbox_inches='tight')
print('Saved s2_storage.png')


# ════════════════════════════════════════════════════════════════════
# Figure 2b: S2 Read Latency — zoomed
# ════════════════════════════════════════════════════════════════════

fig2b, ax2b = plt.subplots(figsize=(10, 6))
fig2b.suptitle('S2: Read Latency — all 512 keys (lower = better)',
               fontsize=13, fontweight='bold', y=1.02)

labels_s2rz = ['BP sealed', 'Legacy', 'BP 56 gens', 'FSST sealed', 'FSST 56 gens', 'LZ4 4KB']
reads_s2z = [9.6, 10.0, 12.1, 32.2, 34.6, 71.4]
colors_s2z = [C['BP (sealed)'], C['Legacy'], C['BP'], C['FSST (sealed)'], C['FSST'], C['LZ4']]
bars = ax2b.barh(labels_s2rz, reads_s2z, color=colors_s2z, edgecolor='white', linewidth=0.5)
for bar, val in zip(bars, reads_s2z):
    ax2b.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
              f'{val:.1f} ms', ha='left', va='center', fontsize=10, fontweight='bold')
ax2b.set_xlabel('Read time (ms)')
ax2b.set_xlim(0, 85)
ax2b.grid(True, alpha=0.2, axis='x')
ax2b.invert_yaxis()

fig2b.tight_layout()
fig2b.savefig(f'{OUT_DIR}/s2_read_latency.png', dpi=150, bbox_inches='tight')
print('Saved s2_read_latency.png')


# ════════════════════════════════════════════════════════════════════
# Figure 3: Scenario 3 — Streaming (50K keys, sparse commits)
# ════════════════════════════════════════════════════════════════════

fig3, axes3 = plt.subplots(1, 3, figsize=(18, 6))
fig3.suptitle('Scenario 3: Streaming — 50K keys, 500 commits, 2% key activity per commit',
              fontsize=14, fontweight='bold', y=1.02)

# Storage
labels_s3s = ['Legacy', 'BP\npre-seal', 'BP\nsealed']
sizes_s3 = [54.0, 15.0, 13.9]
colors_s3s = [C['Legacy'], C['BP (pre-seal)'], C['BP (sealed)']]
bars = axes3[0].bar(labels_s3s, sizes_s3, color=colors_s3s, edgecolor='white', linewidth=0.5)
for bar, val in zip(bars, sizes_s3):
    axes3[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                  f'{val:.1f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
axes3[0].set_ylabel('Size (MB)')
axes3[0].set_title('Storage', fontsize=11)
axes3[0].set_ylim(0, 65)
axes3[0].grid(True, alpha=0.2, axis='y')

axes3[0].annotate('3.9x smaller', xy=(2, 13.9), xytext=(1.4, 40),
                  fontsize=9, color=C['BP (sealed)'], fontweight='bold',
                  arrowprops=dict(arrowstyle='->', color=C['BP (sealed)'], lw=1.5))

# Read latency (200 random keys)
labels_s3r = ['Legacy', 'BP\npre-seal', 'BP\nsealed']
reads_s3 = [0.62, 1.59, 0.35]
colors_s3r = [C['Legacy'], C['BP (pre-seal)'], C['BP (sealed)']]
bars = axes3[1].bar(labels_s3r, reads_s3, color=colors_s3r, edgecolor='white', linewidth=0.5)
for bar, val in zip(bars, reads_s3):
    axes3[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02,
                  f'{val:.2f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
axes3[1].set_ylabel('Read time (ms)')
axes3[1].set_title('Read Latency (200 random keys)', fontsize=11)
axes3[1].set_ylim(0, 2.0)
axes3[1].grid(True, alpha=0.2, axis='y')

# Full scan (50K keys)
labels_s3f = ['Legacy', 'BP\npre-seal', 'BP\nsealed']
scans_s3 = [23.5, 41.5, 28.1]
colors_s3f = [C['Legacy'], C['BP (pre-seal)'], C['BP (sealed)']]
bars = axes3[2].bar(labels_s3f, scans_s3, color=colors_s3f, edgecolor='white', linewidth=0.5)
for bar, val in zip(bars, scans_s3):
    axes3[2].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                  f'{val:.1f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
axes3[2].set_ylabel('Scan time (ms)')
axes3[2].set_title('Full Scan (50K keys)', fontsize=11)
axes3[2].set_ylim(0, 50)
axes3[2].grid(True, alpha=0.2, axis='y')

fig3.tight_layout()
fig3.savefig(f'{OUT_DIR}/s3_storage.png', dpi=150, bbox_inches='tight')
print('Saved s3_storage.png')


# ════════════════════════════════════════════════════════════════════
# Figure 3b: S3 Read Latency
# ════════════════════════════════════════════════════════════════════

fig3b, ax3b = plt.subplots(figsize=(10, 5))
fig3b.suptitle('S3: Read Latency — 200 random keys (lower = better)',
               fontsize=13, fontweight='bold', y=1.02)

labels_s3rz = ['BP sealed', 'Legacy', 'BP 500 gens']
reads_s3z = [0.35, 0.62, 1.59]
colors_s3z = [C['BP (sealed)'], C['Legacy'], C['BP']]
bars = ax3b.barh(labels_s3rz, reads_s3z, color=colors_s3z, edgecolor='white', linewidth=0.5)
for bar, val in zip(bars, reads_s3z):
    ax3b.text(bar.get_width() + 0.02, bar.get_y() + bar.get_height()/2,
              f'{val:.2f} ms', ha='left', va='center', fontsize=10, fontweight='bold')
ax3b.set_xlabel('Read time (ms)')
ax3b.set_xlim(0, 2.0)
ax3b.grid(True, alpha=0.2, axis='x')
ax3b.invert_yaxis()

fig3b.tight_layout()
fig3b.savefig(f'{OUT_DIR}/s3_read_latency.png', dpi=150, bbox_inches='tight')
print('Saved s3_read_latency.png')


# ════════════════════════════════════════════════════════════════════
# Figure 4: S3 Header Overhead
# ════════════════════════════════════════════════════════════════════

fig4, ax4 = plt.subplots(figsize=(10, 6))
fig4.suptitle('Scenario 3: Generation Header Overhead — Dense vs Sparse',
              fontsize=14, fontweight='bold', y=1.02)

categories = ['Dense headers\n(50K keys x 8B x 500 gens)', 'Sparse headers\n(1K keys x 12B x 500 gens)',
              'Gen directory\n(24B x 500)', 'Encoded data\n(5M rows)']
dense_bytes = [200_000_000, 0, 12_000, 10_000_000]
sparse_bytes = [0, 6_000_000, 12_000, 10_000_000]

x = np.arange(len(categories))
width = 0.35
bars1 = ax4.bar(x - width/2, [b/1e6 for b in dense_bytes], width,
                label='Dense format (old)', color='#d94f4f', edgecolor='white')
bars2 = ax4.bar(x + width/2, [b/1e6 for b in sparse_bytes], width,
                label='Sparse format (new)', color=C['BP'], edgecolor='white')

for bar, val in zip(bars1, dense_bytes):
    if val > 0:
        ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                 f'{val/1e6:.0f} MB', ha='center', va='bottom', fontsize=9, fontweight='bold')
for bar, val in zip(bars2, sparse_bytes):
    if val > 0:
        ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                 f'{val/1e6:.1f} MB', ha='center', va='bottom', fontsize=9, fontweight='bold',
                 color=C['BP (sealed)'])

ax4.set_ylabel('Size (MB)')
ax4.set_xticks(x)
ax4.set_xticklabels(categories, fontsize=9)
ax4.legend(fontsize=10)
ax4.grid(True, alpha=0.2, axis='y')

ax4.text(0.98, 0.95, 'Dense total: 201 MB\nSparse total: 15 MB\nSavings: 33x',
         transform=ax4.transAxes, fontsize=11, va='top', ha='right',
         bbox=dict(boxstyle='round,pad=0.5', facecolor='#e8f5e9', edgecolor=C['BP']))

fig4.tight_layout()
fig4.savefig(f'{OUT_DIR}/s3_header_overhead.png', dpi=150, bbox_inches='tight')
print('Saved s3_header_overhead.png')


# ════════════════════════════════════════════════════════════════════
# Figure 5: Cross-Scenario Summary — BP vs Legacy (all sealed)
# ════════════════════════════════════════════════════════════════════

fig5, axes5 = plt.subplots(1, 3, figsize=(18, 6.5))
fig5.suptitle('BP vs Legacy — Cross-Scenario Summary (sealed)',
              fontsize=14, fontweight='bold', y=1.02)

scenarios = ['S1: High Card\n5M keys, 4 v/k', 'S2: Market Data\n512 keys, 7K v/k', 'S3: Streaming\n50K keys, sparse']

# Storage comparison
legacy_storage = [534.1, 28.2, 54.0]
bp_storage = [78.9, 1.0, 13.9]

x = np.arange(len(scenarios))
width = 0.3
bars1 = axes5[0].bar(x - width/2, legacy_storage, width, label='Legacy', color=C['Legacy'])
bars2 = axes5[0].bar(x + width/2, bp_storage, width, label='BP (sealed)', color=C['BP (sealed)'])
for bar, val in zip(bars1, legacy_storage):
    axes5[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                  f'{val:.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
for bar, val in zip(bars2, bp_storage):
    axes5[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                  f'{val:.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold', color=C['BP (sealed)'])
axes5[0].set_ylabel('Size (MB)')
axes5[0].set_title('Storage', fontsize=11)
axes5[0].set_xticks(x)
axes5[0].set_xticklabels(scenarios, fontsize=8)
axes5[0].legend(fontsize=9)
axes5[0].grid(True, alpha=0.2, axis='y')

# Compression ratio annotation
for i, (l, b) in enumerate(zip(legacy_storage, bp_storage)):
    ratio = l / b
    axes5[0].annotate(f'{ratio:.0f}x', xy=(i + width/2, b),
                      xytext=(i + width/2 + 0.15, b + (l-b)*0.3),
                      fontsize=8, color=C['BP (sealed)'], fontweight='bold',
                      arrowprops=dict(arrowstyle='->', color=C['BP (sealed)'], lw=1))

# Full scan comparison
legacy_scan = [975.5, 11.3, 23.5]
bp_scan = [136.9, 9.6, 28.1]

bars1 = axes5[1].bar(x - width/2, legacy_scan, width, label='Legacy', color=C['Legacy'])
bars2 = axes5[1].bar(x + width/2, bp_scan, width, label='BP (sealed)', color=C['BP (sealed)'])
for bar, val in zip(bars1, legacy_scan):
    axes5[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 8,
                  f'{val:.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
for bar, val in zip(bars2, bp_scan):
    axes5[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 8,
                  f'{val:.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold', color=C['BP (sealed)'])
axes5[1].set_ylabel('Scan time (ms)')
axes5[1].set_title('Full Scan', fontsize=11)
axes5[1].set_xticks(x)
axes5[1].set_xticklabels(scenarios, fontsize=8)
axes5[1].legend(fontsize=9)
axes5[1].grid(True, alpha=0.2, axis='y')

# Point read comparison
legacy_read = [30.1, 10.0, 0.62]
bp_read_sealed = [5.1, 9.6, 0.35]

bars1 = axes5[2].bar(x - width/2, legacy_read, width, label='Legacy', color=C['Legacy'])
bars2 = axes5[2].bar(x + width/2, bp_read_sealed, width, label='BP (sealed)', color=C['BP (sealed)'])
for bar, val in zip(bars1, legacy_read):
    axes5[2].text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(legacy_read)*0.02,
                  f'{val:.1f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
for bar, val in zip(bars2, bp_read_sealed):
    axes5[2].text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(legacy_read)*0.02,
                  f'{val:.1f}', ha='center', va='bottom', fontsize=9, fontweight='bold', color=C['BP (sealed)'])
axes5[2].set_ylabel('Read time (ms)')
axes5[2].set_title('Point Read', fontsize=11)
axes5[2].set_xticks(x)
axes5[2].set_xticklabels(scenarios, fontsize=8)
axes5[2].legend(fontsize=9)
axes5[2].grid(True, alpha=0.2, axis='y')

fig5.tight_layout()
fig5.savefig(f'{OUT_DIR}/all_scenarios_storage.png', dpi=150, bbox_inches='tight')
print('Saved all_scenarios_storage.png')


# ════════════════════════════════════════════════════════════════════
# Figure 6: S3 Full Scan
# ════════════════════════════════════════════════════════════════════

fig6, ax6 = plt.subplots(figsize=(8, 5))
fig6.suptitle('S3: Full Scan — 50K keys (lower = better)',
              fontsize=13, fontweight='bold', y=1.02)

labels_s3fz = ['Legacy', 'BP sealed', 'BP 500 gens']
scans_s3z = [23.5, 28.1, 41.5]
colors_s3fz = [C['Legacy'], C['BP (sealed)'], C['BP']]
bars = ax6.barh(labels_s3fz, scans_s3z, color=colors_s3fz, edgecolor='white', linewidth=0.5)
for bar, val in zip(bars, scans_s3z):
    ax6.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
             f'{val:.1f} ms', ha='left', va='center', fontsize=10, fontweight='bold')
ax6.set_xlabel('Scan time (ms)')
ax6.set_xlim(0, 50)
ax6.grid(True, alpha=0.2, axis='x')
ax6.invert_yaxis()

fig6.tight_layout()
fig6.savefig(f'{OUT_DIR}/s3_full_scan.png', dpi=150, bbox_inches='tight')
print('Saved s3_full_scan.png')


print('\nAll charts saved to', OUT_DIR)
