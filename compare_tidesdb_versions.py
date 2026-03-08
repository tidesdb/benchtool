#!/usr/bin/env python3
"""
TidesDB Version Comparison Tool
================================
Compares benchmark results between TidesDB versions to identify
regressions and performance gains.

Colors: 
  - Newer Version = Blue (#1565C0)
  - Older Version = Grey (#9E9E9E)
  - Improvement = Green (#4CAF50)
  - Regression = Red (#EF5350)
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import sys
import os
import re
from datetime import datetime

NEW_VER = '#1565C0'      # Blue for newer version
OLD_VER = '#9E9E9E'      # Grey for older version
IMPROVE = '#4CAF50'      # Green for improvement
REGRESS = '#EF5350'      # Red for regression
NEW_VER_L = '#64B5F6'
OLD_VER_L = '#E0E0E0'
OUT_DIR = 'version_comparison_plots'


def setup_style():
    plt.rcParams.update({
        'figure.facecolor': 'white', 'axes.facecolor': '#FAFAFA',
        'axes.grid': True, 'grid.alpha': 0.25, 'grid.linestyle': '--',
        'font.family': 'sans-serif', 'font.size': 10,
        'axes.titlesize': 13, 'axes.titleweight': 'bold', 'axes.labelsize': 11,
        'figure.titlesize': 15, 'figure.titleweight': 'bold',
        'legend.fontsize': 10, 'xtick.labelsize': 9, 'ytick.labelsize': 9,
    })


def extract_date_from_filename(filepath):
    """Extract date from filename like tidesdb_rocksdb_benchmark_results_20260217_113922.csv"""
    basename = os.path.basename(filepath)
    match = re.search(r'(\d{8})_(\d{6})', basename)
    if match:
        date_str = match.group(1)
        time_str = match.group(2)
        return datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
    return None


def load_data(csv_path):
    """Load and filter benchmark data for TidesDB only."""
    df = pd.read_csv(csv_path)
    df = df[df['engine'] == 'tidesdb']
    main = df[~df['test_name'].str.contains('_populate', na=False)]
    main = main[main['operation'] != 'ITER'].copy()
    return main


def val(df, test_name, operation, column):
    """Get value from dataframe for a specific test/operation/column."""
    row = df[(df['test_name'] == test_name) & (df['operation'] == operation)]
    if row.empty:
        return 0
    v = row.iloc[0][column]
    return 0 if pd.isna(v) else float(v)


def fmt_v(v):
    """Format value for display."""
    if v >= 1_000_000:
        return f'{v/1e6:.2f}M'
    if v >= 1_000:
        return f'{v/1e3:.1f}K'
    if v >= 10:
        return f'{v:.0f}'
    return f'{v:.2f}'


def fmt_pct(pct):
    """Format percentage change."""
    if pct >= 0:
        return f'+{pct:.1f}%'
    return f'{pct:.1f}%'


def calc_change(new_val, old_val, higher_is_better=True):
    """Calculate percentage change. Returns (pct_change, is_improvement)."""
    if old_val == 0:
        return (0, True) if new_val == 0 else (float('inf'), higher_is_better)
    pct = ((new_val - old_val) / old_val) * 100
    is_improvement = (pct > 0) if higher_is_better else (pct < 0)
    return pct, is_improvement


def paired_bars(ax, labels, new_vals, old_vals, ylabel, title, new_label, old_label, 
                decimal=False, rotation=25, higher_is_better=True):
    """Draw paired bar chart comparing two versions."""
    x = np.arange(len(labels))
    w = 0.35
    b1 = ax.bar(x - w/2, new_vals, w, label=new_label, color=NEW_VER, edgecolor='white', lw=.5, zorder=3)
    b2 = ax.bar(x + w/2, old_vals, w, label=old_label, color=OLD_VER, edgecolor='white', lw=.5, zorder=3)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=rotation, ha='right')
    ax.legend(loc='best')
    ax.set_axisbelow(True)

    for bars, c in [(b1, NEW_VER), (b2, '#616161')]:
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                txt = f'{h:.2f}' if decimal else fmt_v(h)
                ax.annotate(txt, xy=(bar.get_x() + bar.get_width()/2, h),
                            xytext=(0, 4), textcoords='offset points',
                            ha='center', va='bottom', fontsize=7, color=c, fontweight='bold')
    
    for i, (nv, ov) in enumerate(zip(new_vals, old_vals)):
        if nv > 0 and ov > 0:
            pct, is_imp = calc_change(nv, ov, higher_is_better)
            if abs(pct) < 1000:  # Don't show crazy percentages
                color = IMPROVE if is_imp else REGRESS
                ax.annotate(fmt_pct(pct), xy=(x[i], max(nv, ov)),
                            xytext=(0, 20), textcoords='offset points',
                            ha='center', va='bottom', fontsize=8, color=color, fontweight='bold')


def save(fig, name):
    fig.savefig(f'{OUT_DIR}/{name}', dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f'  + {name}')


def plot_change_summary(df_new, df_old, new_label, old_label):
    """Create a horizontal bar chart showing % change for key benchmarks."""
    tests = [
        ('write_seq_10M_t8_b1000', 'PUT', 'Seq Write (10M, 8t)', True),
        ('write_random_10M_t8_b1000', 'PUT', 'Random Write (10M, 8t)', True),
        ('write_zipfian_5M_t8_b1000', 'PUT', 'Zipfian Write (5M, 8t)', True),
        ('mixed_random_5M_t8_b1000', 'PUT', 'Mixed Write (5M, 8t)', True),
        ('mixed_random_5M_t8_b1000', 'GET', 'Mixed Read (5M, 8t)', True),
        ('delete_random_5M_t8_b1000', 'DELETE', 'Delete (5M, 8t)', True),
        ('seek_random_5M_t8', 'SEEK', 'Random Seek (5M, 8t)', True),
        ('seek_seq_5M_t8', 'SEEK', 'Seq Seek (5M, 8t)', True),
        ('range_random_100_1M_t8', 'RANGE', 'Range 100 (1M, 8t)', True),
        ('range_random_1000_500K_t8', 'RANGE', 'Range 1000 (500K, 8t)', True),
        ('write_large_values_1M_k256_v4096_t8_b1000', 'PUT', 'Large Val Write (1M, 8t)', True),
        ('write_small_values_50M_k16_v64_t8_b1000', 'PUT', 'Small Val Write (50M, 8t)', True),
        ('batch_1_10M_t8', 'PUT', 'Batch 1 (10M, 8t)', True),
        ('batch_100_10M_t8', 'PUT', 'Batch 100 (10M, 8t)', True),
        ('batch_1000_10M_t8', 'PUT', 'Batch 1000 (10M, 8t)', True),
        ('batch_10000_10M_t8', 'PUT', 'Batch 10000 (10M, 8t)', True),
    ]
    
    labels, changes, colors = [], [], []
    for tn, op, lbl, higher_better in tests:
        new_v = val(df_new, tn, op, 'ops_per_sec')
        old_v = val(df_old, tn, op, 'ops_per_sec')
        if new_v > 0 and old_v > 0:
            pct, is_imp = calc_change(new_v, old_v, higher_better)
            if abs(pct) < 1000:  
                labels.append(lbl)
                changes.append(pct)
                colors.append(IMPROVE if is_imp else REGRESS)
    
    if not labels:
        print('  - 00_change_summary.png (no data, skipped)')
        return
    
    fig, ax = plt.subplots(figsize=(14, max(8, len(labels) * 0.4)))
    fig.suptitle(f'TidesDB Performance Change: {old_label} → {new_label}\n(Throughput % Change)')
    
    y = np.arange(len(labels))
    bars = ax.barh(y, changes, color=colors, edgecolor='white', lw=.5, height=.7, zorder=3)
    ax.axvline(x=0, color='#424242', ls='--', lw=1.5, zorder=2)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel('% Change (Positive = Improvement)')
    ax.invert_yaxis()
    ax.set_axisbelow(True)
    
    for bar, change in zip(bars, changes):
        x_pos = bar.get_width()
        offset = 5 if x_pos >= 0 else -5
        ha = 'left' if x_pos >= 0 else 'right'
        ax.text(x_pos + offset * 0.01 * max(abs(c) for c in changes), 
                bar.get_y() + bar.get_height()/2,
                fmt_pct(change), va='center', ha=ha, fontsize=9, fontweight='bold',
                color=IMPROVE if change >= 0 else REGRESS)
    
    ax.text(0.98, 0.02, '▲ Green = Improvement | ▼ Red = Regression',
            transform=ax.transAxes, ha='right', va='bottom', fontsize=9,
            style='italic', color='#757575',
            bbox=dict(boxstyle='round,pad=0.3', fc='white', ec='#E0E0E0'))
    
    save(fig, '00_change_summary.png')


def plot_write_comparison(df_new, df_old, new_label, old_label):
    tests = [
        ('write_seq_10M_t8_b1000', 'Seq\n10M'),
        ('write_random_10M_t8_b1000', 'Random\n10M'),
        ('write_zipfian_5M_t8_b1000', 'Zipfian\n5M'),
    ]
    
    avail = [(t[0], t[1]) for t in tests 
             if val(df_new, t[0], 'PUT', 'ops_per_sec') > 0 or val(df_old, t[0], 'PUT', 'ops_per_sec') > 0]
    
    if not avail:
        print('  - 01_write_comparison.png (no data, skipped)')
        return
    
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.suptitle('TidesDB Write Throughput Comparison')
    
    lbl = [t[1] for t in avail]
    new_vals = [val(df_new, t[0], 'PUT', 'ops_per_sec') for t in avail]
    old_vals = [val(df_old, t[0], 'PUT', 'ops_per_sec') for t in avail]
    
    paired_bars(ax, lbl, new_vals, old_vals, 'ops/sec', 'Write Throughput', 
                new_label, old_label, higher_is_better=True)
    
    fig.tight_layout(rect=[0, 0, 1, .93])
    save(fig, '01_write_comparison.png')



def plot_mixed_comparison(df_new, df_old, new_label, old_label):
    put_tests = [
        ('mixed_random_5M_t8_b1000', 'PUT', 'Random\n5M'),
        ('mixed_zipfian_5M_t8_b1000', 'PUT', 'Zipfian\n5M'),
    ]
    get_tests = [
        ('mixed_random_5M_t8_b1000', 'GET', 'Random\n5M'),
        ('mixed_zipfian_5M_t8_b1000', 'GET', 'Zipfian\n5M'),
    ]
    
    put_avail = [t for t in put_tests 
                 if val(df_new, t[0], t[1], 'ops_per_sec') > 0 or val(df_old, t[0], t[1], 'ops_per_sec') > 0]
    get_avail = [t for t in get_tests 
                 if val(df_new, t[0], t[1], 'ops_per_sec') > 0 or val(df_old, t[0], t[1], 'ops_per_sec') > 0]
    
    if not put_avail and not get_avail:
        print('  - 02_mixed_comparison.png (no data, skipped)')
        return
    
    panels = []
    if put_avail:
        panels.append((put_avail, 'Mixed — Write Side'))
    if get_avail:
        panels.append((get_avail, 'Mixed — Read Side'))
    
    fig, axes = plt.subplots(1, len(panels), figsize=(8 * len(panels), 6))
    if len(panels) == 1:
        axes = [axes]
    fig.suptitle('TidesDB Mixed Workload Comparison')
    
    for ax, (tests, title) in zip(axes, panels):
        lbl = [t[2] for t in tests]
        new_vals = [val(df_new, t[0], t[1], 'ops_per_sec') for t in tests]
        old_vals = [val(df_old, t[0], t[1], 'ops_per_sec') for t in tests]
        paired_bars(ax, lbl, new_vals, old_vals, 'ops/sec', title, new_label, old_label)
    
    fig.tight_layout(rect=[0, 0, 1, .93])
    save(fig, '02_mixed_comparison.png')



def plot_delete_comparison(df_new, df_old, new_label, old_label):
    tests = [
        ('delete_batch_1_5M_t8', 'Batch 1'),
        ('delete_batch_100_5M_t8', 'Batch 100'),
        ('delete_batch_1000_5M_t8', 'Batch 1000'),
        ('delete_random_5M_t8_b1000', 'Random b1000'),
    ]
    
    avail = [t for t in tests 
             if val(df_new, t[0], 'DELETE', 'ops_per_sec') > 0 or val(df_old, t[0], 'DELETE', 'ops_per_sec') > 0]
    
    if not avail:
        print('  - 03_delete_comparison.png (no data, skipped)')
        return
    
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.suptitle('TidesDB Delete Throughput Comparison')
    
    lbl = [t[1] for t in avail]
    new_vals = [val(df_new, t[0], 'DELETE', 'ops_per_sec') for t in avail]
    old_vals = [val(df_old, t[0], 'DELETE', 'ops_per_sec') for t in avail]
    
    paired_bars(ax, lbl, new_vals, old_vals, 'ops/sec', 'Delete Throughput', 
                new_label, old_label, higher_is_better=True)
    
    fig.tight_layout(rect=[0, 0, 1, .93])
    save(fig, '03_delete_comparison.png')



def plot_seek_comparison(df_new, df_old, new_label, old_label):
    tests = [
        ('seek_random_5M_t8', 'Random'),
        ('seek_seq_5M_t8', 'Sequential'),
        ('seek_zipfian_5M_t8', 'Zipfian'),
    ]
    
    avail = [t for t in tests 
             if val(df_new, t[0], 'SEEK', 'ops_per_sec') > 0 or val(df_old, t[0], 'SEEK', 'ops_per_sec') > 0]
    
    if not avail:
        print('  - 04_seek_comparison.png (no data, skipped)')
        return
    
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.suptitle('TidesDB Seek Throughput Comparison')
    
    lbl = [t[1] for t in avail]
    new_vals = [val(df_new, t[0], 'SEEK', 'ops_per_sec') for t in avail]
    old_vals = [val(df_old, t[0], 'SEEK', 'ops_per_sec') for t in avail]
    
    paired_bars(ax, lbl, new_vals, old_vals, 'ops/sec', 'Seek Throughput', 
                new_label, old_label, higher_is_better=True)
    
    fig.tight_layout(rect=[0, 0, 1, .93])
    save(fig, '04_seek_comparison.png')


def plot_range_comparison(df_new, df_old, new_label, old_label):
    tests = [
        ('range_random_100_1M_t8', 'Rand 100\n1M'),
        ('range_random_1000_500K_t8', 'Rand 1000\n500K'),
        ('range_seq_100_1M_t8', 'Seq 100\n1M'),
    ]
    
    avail = [t for t in tests 
             if val(df_new, t[0], 'RANGE', 'ops_per_sec') > 0 or val(df_old, t[0], 'RANGE', 'ops_per_sec') > 0]
    
    if not avail:
        print('  - 05_range_comparison.png (no data, skipped)')
        return
    
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.suptitle('TidesDB Range Scan Throughput Comparison')
    
    lbl = [t[1] for t in avail]
    new_vals = [val(df_new, t[0], 'RANGE', 'ops_per_sec') for t in avail]
    old_vals = [val(df_old, t[0], 'RANGE', 'ops_per_sec') for t in avail]
    
    paired_bars(ax, lbl, new_vals, old_vals, 'ops/sec', 'Range Scan Throughput', 
                new_label, old_label, higher_is_better=True)
    
    fig.tight_layout(rect=[0, 0, 1, .93])
    save(fig, '05_range_comparison.png')


def plot_batch_comparison(df_new, df_old, new_label, old_label):
    batches = [1, 10, 100, 1000, 10000]
    names = ['batch_1_10M_t8', 'batch_10_10M_t8', 'batch_100_10M_t8',
             'batch_1000_10M_t8', 'batch_10000_10M_t8']
    
    avail = [(b, n) for b, n in zip(batches, names) 
             if val(df_new, n, 'PUT', 'ops_per_sec') > 0 or val(df_old, n, 'PUT', 'ops_per_sec') > 0]
    
    if not avail:
        print('  - 06_batch_comparison.png (no data, skipped)')
        return
    
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.suptitle('TidesDB Batch Size Scaling Comparison')
    
    batch_sizes = [b for b, n in avail]
    new_vals = [val(df_new, n, 'PUT', 'ops_per_sec') for b, n in avail]
    old_vals = [val(df_old, n, 'PUT', 'ops_per_sec') for b, n in avail]
    
    ax.plot(batch_sizes, new_vals, 'o-', color=NEW_VER, lw=2.5, ms=8, label=new_label, zorder=3)
    ax.plot(batch_sizes, old_vals, 's--', color=OLD_VER, lw=2.5, ms=8, label=old_label, zorder=3)
    ax.set_xscale('log')
    ax.set_xlabel('Batch Size')
    ax.set_ylabel('ops/sec')
    ax.set_title('Batch Size Scaling — Write Throughput')
    ax.legend()
    ax.set_axisbelow(True)
    
    for b, nv, ov in zip(batch_sizes, new_vals, old_vals):
        if nv > 0:
            ax.annotate(fmt_v(nv), (b, nv), textcoords='offset points',
                        xytext=(0, 10), ha='center', fontsize=7, color=NEW_VER, fontweight='bold')
        if ov > 0:
            ax.annotate(fmt_v(ov), (b, ov), textcoords='offset points',
                        xytext=(0, -14), ha='center', fontsize=7, color='#616161', fontweight='bold')
        if nv > 0 and ov > 0:
            pct, is_imp = calc_change(nv, ov, True)
            if abs(pct) < 1000:
                color = IMPROVE if is_imp else REGRESS
                ax.annotate(fmt_pct(pct), (b, max(nv, ov)), textcoords='offset points',
                            xytext=(0, 25), ha='center', fontsize=8, color=color, fontweight='bold')
    
    fig.tight_layout(rect=[0, 0, 1, .93])
    save(fig, '06_batch_comparison.png')


def plot_value_size_comparison(df_new, df_old, new_label, old_label):
    tests = [
        ('write_small_values_50M_k16_v64_t8_b1000', '64B val\n50M'),
        ('write_random_10M_t8_b1000', '100B val\n10M'),
        ('write_large_values_1M_k256_v4096_t8_b1000', '4KB val\n1M'),
    ]
    
    avail = [t for t in tests 
             if val(df_new, t[0], 'PUT', 'ops_per_sec') > 0 or val(df_old, t[0], 'PUT', 'ops_per_sec') > 0]
    
    if not avail:
        print('  - 07_value_size_comparison.png (no data, skipped)')
        return
    
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.suptitle('TidesDB Value Size Impact Comparison')
    
    lbl = [t[1] for t in avail]
    new_vals = [val(df_new, t[0], 'PUT', 'ops_per_sec') for t in avail]
    old_vals = [val(df_old, t[0], 'PUT', 'ops_per_sec') for t in avail]
    
    paired_bars(ax, lbl, new_vals, old_vals, 'ops/sec', 'Value Size Impact on Write Throughput', 
                new_label, old_label, higher_is_better=True)
    
    fig.tight_layout(rect=[0, 0, 1, .93])
    save(fig, '07_value_size_comparison.png')


def plot_latency_comparison(df_new, df_old, new_label, old_label):
    tests = [
        ('write_seq_10M_t8_b1000', 'PUT', 'Seq Write'),
        ('write_random_10M_t8_b1000', 'PUT', 'Rand Write'),
        ('mixed_random_5M_t8_b1000', 'GET', 'Mixed Read'),
        ('seek_random_5M_t8', 'SEEK', 'Seek'),
        ('range_random_100_1M_t8', 'RANGE', 'Range'),
        ('delete_random_5M_t8_b1000', 'DELETE', 'Delete'),
    ]
    
    avail = [t for t in tests 
             if val(df_new, t[0], t[1], 'avg_latency_us') > 0 or val(df_old, t[0], t[1], 'avg_latency_us') > 0]
    
    if not avail:
        print('  - 08_latency_comparison.png (no data, skipped)')
        return
    
    fig, ax = plt.subplots(figsize=(14, 6))
    fig.suptitle('TidesDB Average Latency Comparison (Lower is Better)')
    
    lbl = [t[2] for t in avail]
    new_vals = [val(df_new, t[0], t[1], 'avg_latency_us') for t in avail]
    old_vals = [val(df_old, t[0], t[1], 'avg_latency_us') for t in avail]
    
    paired_bars(ax, lbl, new_vals, old_vals, 'Avg Latency (us)', 'Average Latency', 
                new_label, old_label, higher_is_better=False)
    
    fig.tight_layout(rect=[0, 0, 1, .93])
    save(fig, '08_latency_comparison.png')


def plot_latency_percentiles_comparison(df_new, df_old, new_label, old_label):
    workloads = [
        ('write_random_10M_t8_b1000', 'PUT', 'Random Write'),
        ('seek_random_5M_t8', 'SEEK', 'Random Seek'),
        ('delete_random_5M_t8_b1000', 'DELETE', 'Delete'),
    ]
    
    avail = [w for w in workloads 
             if val(df_new, w[0], w[1], 'p50_us') > 0 or val(df_old, w[0], w[1], 'p50_us') > 0]
    
    if not avail:
        print('  - 09_latency_percentiles_comparison.png (no data, skipped)')
        return
    
    fig, axes = plt.subplots(1, len(avail), figsize=(6 * len(avail), 6))
    if len(avail) == 1:
        axes = [axes]
    fig.suptitle('TidesDB Latency Percentiles Comparison (p50/p95/p99)')
    
    for ax, (tn, op, title) in zip(axes, avail):
        pcts = ['p50_us', 'p95_us', 'p99_us']
        new_vals = [val(df_new, tn, op, p) for p in pcts]
        old_vals = [val(df_old, tn, op, p) for p in pcts]
        paired_bars(ax, ['p50', 'p95', 'p99'], new_vals, old_vals, 'Latency (us)', title, 
                    new_label, old_label, rotation=0, higher_is_better=False)
    
    fig.tight_layout(rect=[0, 0, 1, .93])
    save(fig, '09_latency_percentiles_comparison.png')


def plot_write_amp_comparison(df_new, df_old, new_label, old_label):
    tests = [
        ('write_seq_10M_t8_b1000', 'PUT', 'Seq'),
        ('write_random_10M_t8_b1000', 'PUT', 'Random'),
        ('write_zipfian_5M_t8_b1000', 'PUT', 'Zipfian'),
        ('write_small_values_50M_k16_v64_t8_b1000', 'PUT', 'Small'),
        ('write_large_values_1M_k256_v4096_t8_b1000', 'PUT', 'Large'),
    ]
    
    avail = [t for t in tests 
             if val(df_new, t[0], t[1], 'write_amp') > 0 or val(df_old, t[0], t[1], 'write_amp') > 0]
    
    if not avail:
        print('  - 10_write_amp_comparison.png (no data, skipped)')
        return
    
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.suptitle('TidesDB Write Amplification Comparison (Lower is Better)')
    
    lbl = [t[2] for t in avail]
    new_vals = [val(df_new, t[0], t[1], 'write_amp') for t in avail]
    old_vals = [val(df_old, t[0], t[1], 'write_amp') for t in avail]
    
    paired_bars(ax, lbl, new_vals, old_vals, 'Write Amplification', 'Write Amplification', 
                new_label, old_label, decimal=True, higher_is_better=False)
    
    fig.tight_layout(rect=[0, 0, 1, .93])
    save(fig, '10_write_amp_comparison.png')


def plot_resource_comparison(df_new, df_old, new_label, old_label):
    tests = [
        ('write_seq_10M_t8_b1000', 'PUT', 'Seq Write'),
        ('write_random_10M_t8_b1000', 'PUT', 'Rand Write'),
        ('write_small_values_50M_k16_v64_t8_b1000', 'PUT', 'Small Val'),
        ('write_large_values_1M_k256_v4096_t8_b1000', 'PUT', 'Large Val'),
    ]
    
    avail = [t for t in tests 
             if val(df_new, t[0], t[1], 'peak_rss_mb') > 0 or val(df_old, t[0], t[1], 'peak_rss_mb') > 0]
    
    if not avail:
        print('  - 11_resource_comparison.png (no data, skipped)')
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('TidesDB Resource Usage Comparison')
    
    lbl = [t[2] for t in avail]
    metrics = [
        (axes[0, 0], 'peak_rss_mb', 'Peak RSS (MB)', 'Memory Usage', False),
        (axes[0, 1], 'disk_write_mb', 'Disk Write (MB)', 'Disk Write Volume', False),
        (axes[1, 0], 'cpu_percent', 'CPU %', 'CPU Utilization', False),
        (axes[1, 1], 'db_size_mb', 'DB Size (MB)', 'Database Size', False),
    ]
    
    for ax, col, ylabel, title, higher_better in metrics:
        new_vals = [val(df_new, t[0], t[1], col) for t in avail]
        old_vals = [val(df_old, t[0], t[1], col) for t in avail]
        paired_bars(ax, lbl, new_vals, old_vals, ylabel, title, 
                    new_label, old_label, higher_is_better=higher_better)
    
    fig.tight_layout(rect=[0, 0, 1, .95])
    save(fig, '11_resource_comparison.png')


def generate_text_report(df_new, df_old, new_label, old_label, new_csv, old_csv):
    """Generate a text summary of regressions and improvements."""
    tests = [
        ('write_seq_10M_t8_b1000', 'PUT', 'Sequential Write (10M, 8t)'),
        ('write_random_10M_t8_b1000', 'PUT', 'Random Write (10M, 8t)'),
        ('write_zipfian_5M_t8_b1000', 'PUT', 'Zipfian Write (5M, 8t)'),
        ('mixed_random_5M_t8_b1000', 'PUT', 'Mixed Write (5M, 8t)'),
        ('mixed_random_5M_t8_b1000', 'GET', 'Mixed Read (5M, 8t)'),
        ('mixed_zipfian_5M_t8_b1000', 'PUT', 'Mixed Zipfian Write (5M, 8t)'),
        ('mixed_zipfian_5M_t8_b1000', 'GET', 'Mixed Zipfian Read (5M, 8t)'),
        ('delete_random_5M_t8_b1000', 'DELETE', 'Delete (5M, 8t)'),
        ('delete_batch_1_5M_t8', 'DELETE', 'Delete Batch 1 (5M, 8t)'),
        ('delete_batch_100_5M_t8', 'DELETE', 'Delete Batch 100 (5M, 8t)'),
        ('delete_batch_1000_5M_t8', 'DELETE', 'Delete Batch 1000 (5M, 8t)'),
        ('seek_random_5M_t8', 'SEEK', 'Random Seek (5M, 8t)'),
        ('seek_seq_5M_t8', 'SEEK', 'Sequential Seek (5M, 8t)'),
        ('seek_zipfian_5M_t8', 'SEEK', 'Zipfian Seek (5M, 8t)'),
        ('range_random_100_1M_t8', 'RANGE', 'Range 100 (1M, 8t)'),
        ('range_random_1000_500K_t8', 'RANGE', 'Range 1000 (500K, 8t)'),
        ('range_seq_100_1M_t8', 'RANGE', 'Range Seq 100 (1M, 8t)'),
        ('write_large_values_1M_k256_v4096_t8_b1000', 'PUT', 'Large Value Write (1M, 8t)'),
        ('write_small_values_50M_k16_v64_t8_b1000', 'PUT', 'Small Value Write (50M, 8t)'),
        ('batch_1_10M_t8', 'PUT', 'Batch 1 (10M, 8t)'),
        ('batch_10_10M_t8', 'PUT', 'Batch 10 (10M, 8t)'),
        ('batch_100_10M_t8', 'PUT', 'Batch 100 (10M, 8t)'),
        ('batch_1000_10M_t8', 'PUT', 'Batch 1000 (10M, 8t)'),
        ('batch_10000_10M_t8', 'PUT', 'Batch 10000 (10M, 8t)'),
    ]
    
    improvements = []
    regressions = []
    unchanged = []
    
    for tn, op, lbl in tests:
        new_v = val(df_new, tn, op, 'ops_per_sec')
        old_v = val(df_old, tn, op, 'ops_per_sec')
        if new_v > 0 and old_v > 0:
            pct, is_imp = calc_change(new_v, old_v, True)
            if abs(pct) < 1000:
                entry = {
                    'name': lbl,
                    'old': old_v,
                    'new': new_v,
                    'pct': pct,
                }
                if abs(pct) < 1:
                    unchanged.append(entry)
                elif is_imp:
                    improvements.append(entry)
                else:
                    regressions.append(entry)
    
    improvements.sort(key=lambda x: x['pct'], reverse=True)
    regressions.sort(key=lambda x: x['pct'])
    
    report = []
    report.append("=" * 80)
    report.append("TidesDB VERSION COMPARISON REPORT")
    report.append("=" * 80)
    report.append(f"\nOlder Version: {old_label}")
    report.append(f"  File: {old_csv}")
    report.append(f"\nNewer Version: {new_label}")
    report.append(f"  File: {new_csv}")
    report.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("\n" + "=" * 80)
    
    report.append(f"\n{'SUMMARY':^80}")
    report.append("-" * 80)
    report.append(f"  Improvements: {len(improvements)}")
    report.append(f"  Regressions:  {len(regressions)}")
    report.append(f"  Unchanged:    {len(unchanged)}")
    
    if improvements:
        report.append("\n" + "=" * 80)
        report.append(f"{'IMPROVEMENTS (Higher throughput in newer version)':^80}")
        report.append("=" * 80)
        report.append(f"{'Benchmark':<45} {'Old (ops/s)':<15} {'New (ops/s)':<15} {'Change':>10}")
        report.append("-" * 80)
        for entry in improvements:
            report.append(f"{entry['name']:<45} {fmt_v(entry['old']):<15} {fmt_v(entry['new']):<15} {fmt_pct(entry['pct']):>10}")
    
    if regressions:
        report.append("\n" + "=" * 80)
        report.append(f"{'REGRESSIONS (Lower throughput in newer version)':^80}")
        report.append("=" * 80)
        report.append(f"{'Benchmark':<45} {'Old (ops/s)':<15} {'New (ops/s)':<15} {'Change':>10}")
        report.append("-" * 80)
        for entry in regressions:
            report.append(f"{entry['name']:<45} {fmt_v(entry['old']):<15} {fmt_v(entry['new']):<15} {fmt_pct(entry['pct']):>10}")
    
    if unchanged:
        report.append("\n" + "=" * 80)
        report.append(f"{'UNCHANGED (< 1% change)':^80}")
        report.append("=" * 80)
        for entry in unchanged:
            report.append(f"  {entry['name']}")
    
    report.append("\n" + "=" * 80)
    
    report_text = "\n".join(report)
    
    report_path = f'{OUT_DIR}/version_comparison_report.txt'
    with open(report_path, 'w') as f:
        f.write(report_text)
    print(f'  + version_comparison_report.txt')
    
    print("\n" + report_text)


def main():
    if len(sys.argv) < 3:
        print("Usage: python compare_tidesdb_versions.py <newer_csv> <older_csv>")
        print("\nExample:")
        print("  python compare_tidesdb_versions.py \\")
        print("    tidesdb_rocksdb_benchmark_results_20260217_113922.csv \\")
        print("    tidesdb_rocksdb_benchmark_results_20260216_061038.csv")
        sys.exit(1)
    
    new_csv = sys.argv[1]
    old_csv = sys.argv[2]
    
    if not os.path.exists(new_csv):
        print(f"Error: File not found: {new_csv}")
        sys.exit(1)
    if not os.path.exists(old_csv):
        print(f"Error: File not found: {old_csv}")
        sys.exit(1)
    
    new_date = extract_date_from_filename(new_csv)
    old_date = extract_date_from_filename(old_csv)
    
    if new_date and old_date:
        new_label = new_date.strftime('%Y-%m-%d %H:%M')
        old_label = old_date.strftime('%Y-%m-%d %H:%M')
    else:
        new_label = os.path.basename(new_csv)
        old_label = os.path.basename(old_csv)
    
    print(f"\nTidesDB Version Comparison")
    print(f"=" * 50)
    print(f"Newer: {new_label}")
    print(f"Older: {old_label}")
    print(f"=" * 50)
    
    print("\nLoading data...")
    df_new = load_data(new_csv)
    df_old = load_data(old_csv)
    
    print(f"  Newer version: {len(df_new)} TidesDB benchmark entries")
    print(f"  Older version: {len(df_old)} TidesDB benchmark entries")
    
    # Create output directory
    os.makedirs(OUT_DIR, exist_ok=True)
    
    # Setup plotting style
    setup_style()
    
    # Generate plots
    print(f"\nGenerating comparison plots in '{OUT_DIR}/'...")
    
    plot_change_summary(df_new, df_old, new_label, old_label)
    plot_write_comparison(df_new, df_old, new_label, old_label)
    plot_mixed_comparison(df_new, df_old, new_label, old_label)
    plot_delete_comparison(df_new, df_old, new_label, old_label)
    plot_seek_comparison(df_new, df_old, new_label, old_label)
    plot_range_comparison(df_new, df_old, new_label, old_label)
    plot_batch_comparison(df_new, df_old, new_label, old_label)
    plot_value_size_comparison(df_new, df_old, new_label, old_label)
    plot_latency_comparison(df_new, df_old, new_label, old_label)
    plot_latency_percentiles_comparison(df_new, df_old, new_label, old_label)
    plot_write_amp_comparison(df_new, df_old, new_label, old_label)
    plot_resource_comparison(df_new, df_old, new_label, old_label)
    
    # Generate text report
    print("\nGenerating text report...")
    generate_text_report(df_new, df_old, new_label, old_label, new_csv, old_csv)
    
    print(f"\nDone! Output saved to '{OUT_DIR}/'")


if __name__ == '__main__':
    main()
