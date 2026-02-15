#!/usr/bin/env python3
"""
TidesDB vs RocksDB Benchmark Visualization
===========================================
Generates comparison plots from CSV benchmark data.
Supports both tidesdb_rocksdb.sh and tidesdb_rocksdb_old.sh formats.
Colors: TidesDB = Blue, RocksDB = Grey
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import sys
import os
import glob

# ── Colors ──
TIDES = '#1565C0'
ROCKS = '#9E9E9E'
TIDES_L = '#64B5F6'
ROCKS_L = '#E0E0E0'
OUT_DIR = 'benchmark_plots'


def setup_style():
    plt.rcParams.update({
        'figure.facecolor': 'white', 'axes.facecolor': '#FAFAFA',
        'axes.grid': True, 'grid.alpha': 0.25, 'grid.linestyle': '--',
        'font.family': 'sans-serif', 'font.size': 10,
        'axes.titlesize': 13, 'axes.titleweight': 'bold', 'axes.labelsize': 11,
        'figure.titlesize': 15, 'figure.titleweight': 'bold',
        'legend.fontsize': 10, 'xtick.labelsize': 9, 'ytick.labelsize': 9,
    })


def load_data(csv_path):
    df = pd.read_csv(csv_path)
    main = df[~df['test_name'].str.contains('_populate', na=False)]
    main = main[main['operation'] != 'ITER'].copy()
    return main


def has_data(df, tests):
    """Check if any of the specified tests have data for both engines."""
    for item in tests:
        tn = item[0] if isinstance(item, tuple) else item
        op = item[1] if isinstance(item, tuple) and len(item) > 1 else 'PUT'
        t = val(df, 'tidesdb', tn, op, 'ops_per_sec')
        r = val(df, 'rocksdb', tn, op, 'ops_per_sec')
        if t > 0 or r > 0:
            return True
    return False


def filter_available(df, tests, op_idx=1):
    """Filter tests to only those with data available."""
    available = []
    for item in tests:
        tn = item[0]
        op = item[op_idx] if len(item) > op_idx else 'PUT'
        t = val(df, 'tidesdb', tn, op, 'ops_per_sec')
        r = val(df, 'rocksdb', tn, op, 'ops_per_sec')
        if t > 0 or r > 0:
            available.append(item)
    return available


def val(df, engine, test_name, operation, column):
    row = df[(df['engine'] == engine) & (df['test_name'] == test_name) & (df['operation'] == operation)]
    if row.empty:
        return 0
    v = row.iloc[0][column]
    return 0 if pd.isna(v) else float(v)


def fmt_v(v):
    if v >= 1_000_000:
        return f'{v/1e6:.2f}M'
    if v >= 1_000:
        return f'{v/1e3:.1f}K'
    if v >= 10:
        return f'{v:.0f}'
    return f'{v:.2f}'


def paired_bars(ax, labels, tv, rv, ylabel, title, decimal=False, rotation=25):
    x = np.arange(len(labels))
    w = 0.35
    b1 = ax.bar(x - w/2, tv, w, label='TidesDB', color=TIDES, edgecolor='white', lw=.5, zorder=3)
    b2 = ax.bar(x + w/2, rv, w, label='RocksDB', color=ROCKS, edgecolor='white', lw=.5, zorder=3)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=rotation, ha='right')
    ax.legend(loc='best')
    ax.set_axisbelow(True)
    for bars, c in [(b1, TIDES), (b2, '#616161')]:
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                txt = f'{h:.2f}' if decimal else fmt_v(h)
                ax.annotate(txt, xy=(bar.get_x() + bar.get_width()/2, h),
                            xytext=(0, 4), textcoords='offset points',
                            ha='center', va='bottom', fontsize=7, color=c, fontweight='bold')


def save(fig, name):
    fig.savefig(f'{OUT_DIR}/{name}', dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f'  + {name}')


# ═══════════════════════════════════════════════
# Plot 00: Speedup Summary
# ═══════════════════════════════════════════════
def plot_speedup_summary(df):
    tests = [
        ('write_seq_10M_t8_b1000', 'PUT', 'Seq Write (10M, 8t)'),
        ('write_random_10M_t8_b1000', 'PUT', 'Random Write (10M, 8t)'),
        ('write_zipfian_5M_t8_b1000', 'PUT', 'Zipfian Write (5M, 8t)'),
        ('read_random_10M_t8', 'GET', 'Random Read (10M, 8t)'),
        ('mixed_random_5M_t8_b1000', 'PUT', 'Mixed Write (5M, 8t)'),
        ('delete_random_5M_t8_b1000', 'DELETE', 'Delete (5M, 8t)'),
        ('seek_random_5M_t8', 'SEEK', 'Random Seek (5M, 8t)'),
        ('seek_seq_5M_t8', 'SEEK', 'Seq Seek (5M, 8t)'),
        ('range_random_100_1M_t8', 'RANGE', 'Range 100 (1M, 8t)'),
        ('range_random_1000_500K_t8', 'RANGE', 'Range 1000 (500K, 8t)'),
        ('write_large_values_1M_k256_v4096_t8_b1000', 'PUT', 'Large Val Write (1M, 8t)'),
        ('write_small_values_50M_k16_v64_t8_b1000', 'PUT', 'Small Val Write (50M, 8t)'),
        ('write_seq_40M_t16_b1000', 'PUT', 'Seq Write (40M, 16t)'),
        ('write_random_40M_t16_b1000', 'PUT', 'Random Write (40M, 16t)'),
        ('read_random_40M_t16', 'GET', 'Random Read (40M, 16t)'),
        ('seek_random_20M_t16', 'SEEK', 'Random Seek (20M, 16t)'),
        ('seek_seq_20M_t16', 'SEEK', 'Seq Seek (20M, 16t)'),
        ('range_random_100_4M_t16', 'RANGE', 'Range 100 (4M, 16t)'),
        ('range_random_1000_2M_t16', 'RANGE', 'Range 1000 (2M, 16t)'),
        ('sync_write_random_25K_t1_b1000', 'PUT', 'Sync Write (25K, 1t)'),
        ('sync_write_random_50K_t4_b1000', 'PUT', 'Sync Write (50K, 4t)'),
        ('sync_write_random_100K_t8_b1000', 'PUT', 'Sync Write (100K, 8t)'),
        ('sync_write_random_500K_t16_b1000', 'PUT', 'Sync Write (500K, 16t)'),
    ]
    labels, ratios = [], []
    for tn, op, lbl in tests:
        t = val(df, 'tidesdb', tn, op, 'ops_per_sec')
        r = val(df, 'rocksdb', tn, op, 'ops_per_sec')
        if t > 0 and r > 0:
            labels.append(lbl)
            ratios.append(t / r)
    fig, ax = plt.subplots(figsize=(14, 10))
    fig.suptitle('TidesDB Speedup over RocksDB (Throughput Ratio)')
    y = np.arange(len(labels))
    colors = [TIDES if r >= 1.0 else '#EF5350' for r in ratios]
    bars = ax.barh(y, ratios, color=colors, edgecolor='white', lw=.5, height=.7, zorder=3)
    ax.axvline(x=1.0, color='#424242', ls='--', lw=1.5, zorder=2)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel('Speedup Factor (TidesDB / RocksDB)')
    ax.invert_yaxis()
    ax.set_axisbelow(True)
    for bar, ratio in zip(bars, ratios):
        ax.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2,
                f'{ratio:.2f}x', va='center', fontsize=9, fontweight='bold',
                color=TIDES if ratio >= 1.0 else '#C62828')
    ax.text(0.98, 0.02, '> 1.0 = TidesDB faster | < 1.0 = RocksDB faster',
            transform=ax.transAxes, ha='right', va='bottom', fontsize=9,
            style='italic', color='#757575',
            bbox=dict(boxstyle='round,pad=0.3', fc='white', ec='#E0E0E0'))
    save(fig, '00_speedup_summary.png')


# ═══════════════════════════════════════════════
# Plot 01: Write Throughput
# ═══════════════════════════════════════════════
def plot_write_throughput(df):
    std_tests = [('write_seq_10M_t8_b1000','Seq\n10M'),
                 ('write_random_10M_t8_b1000','Random\n10M'),
                 ('write_zipfian_5M_t8_b1000','Zipfian\n5M')]
    large_tests = [('write_seq_40M_t16_b1000','Seq\n40M'),
                   ('write_random_40M_t16_b1000','Random\n40M'),
                   ('write_zipfian_20M_t16_b1000','Zipfian\n20M')]
    
    std_avail = [(t[0], t[1]) for t in std_tests if val(df,'tidesdb',t[0],'PUT','ops_per_sec') > 0 or val(df,'rocksdb',t[0],'PUT','ops_per_sec') > 0]
    large_avail = [(t[0], t[1]) for t in large_tests if val(df,'tidesdb',t[0],'PUT','ops_per_sec') > 0 or val(df,'rocksdb',t[0],'PUT','ops_per_sec') > 0]
    
    if not std_avail and not large_avail:
        print('  - 01_write_throughput.png (no data, skipped)')
        return
    
    # Determine layout based on available data
    if std_avail and large_avail:
        fig, (a1, a2) = plt.subplots(1, 2, figsize=(16, 6))
        axes = [(a1, std_avail, 'Standard (8 threads)'), (a2, large_avail, 'Large Scale (16 threads)')]
    elif std_avail:
        fig, a1 = plt.subplots(1, 1, figsize=(10, 6))
        axes = [(a1, std_avail, 'Standard (8 threads)')]
    else:
        fig, a1 = plt.subplots(1, 1, figsize=(10, 6))
        axes = [(a1, large_avail, 'Large Scale (16 threads)')]
    
    fig.suptitle('Write Throughput (ops/sec)')
    for ax, tests, title in axes:
        lbl = [t[1] for t in tests]
        tv = [val(df,'tidesdb',t[0],'PUT','ops_per_sec') for t in tests]
        rv = [val(df,'rocksdb',t[0],'PUT','ops_per_sec') for t in tests]
        paired_bars(ax, lbl, tv, rv, 'ops/sec', title)
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '01_write_throughput.png')


# ═══════════════════════════════════════════════
# Plot 02: Read & Mixed Throughput
# ═══════════════════════════════════════════════
def plot_read_mixed(df):
    # Filter to available tests
    read_tests = [('read_random_10M_t8','GET','Read\n10M,8t'),('read_random_40M_t16','GET','Read\n40M,16t')]
    mixed_put = [('mixed_random_5M_t8_b1000','PUT','Rand\n5M,8t'),
                 ('mixed_zipfian_5M_t8_b1000','PUT','Zipf\n5M,8t'),
                 ('mixed_random_20M_t16_b1000','PUT','Rand\n20M,16t')]
    mixed_get = [('mixed_random_5M_t8_b1000','GET','Rand\n5M,8t'),
                 ('mixed_zipfian_5M_t8_b1000','GET','Zipf\n5M,8t'),
                 ('mixed_random_20M_t16_b1000','GET','Rand\n20M,16t')]
    
    read_avail = [t for t in read_tests if val(df,'tidesdb',t[0],t[1],'ops_per_sec') > 0 or val(df,'rocksdb',t[0],t[1],'ops_per_sec') > 0]
    put_avail = [t for t in mixed_put if val(df,'tidesdb',t[0],t[1],'ops_per_sec') > 0 or val(df,'rocksdb',t[0],t[1],'ops_per_sec') > 0]
    get_avail = [t for t in mixed_get if val(df,'tidesdb',t[0],t[1],'ops_per_sec') > 0 or val(df,'rocksdb',t[0],t[1],'ops_per_sec') > 0]
    
    panels = []
    if read_avail:
        panels.append((read_avail, 'Read Throughput'))
    if put_avail:
        panels.append((put_avail, 'Mixed — Write Side'))
    if get_avail:
        panels.append((get_avail, 'Mixed — Read Side'))
    
    if not panels:
        print('  - 02_read_mixed_throughput.png (no data, skipped)')
        return
    
    fig, axes = plt.subplots(1, len(panels), figsize=(6*len(panels), 6))
    if len(panels) == 1:
        axes = [axes]
    fig.suptitle('Read & Mixed Workload Throughput')
    
    for ax, (tests, title) in zip(axes, panels):
        paired_bars(ax, [x[2] for x in tests],
                    [val(df,'tidesdb',x[0],x[1],'ops_per_sec') for x in tests],
                    [val(df,'rocksdb',x[0],x[1],'ops_per_sec') for x in tests],
                    'ops/sec', title)
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '02_read_mixed_throughput.png')


# ═══════════════════════════════════════════════
# Plot 03: Delete Throughput
# ═══════════════════════════════════════════════
def plot_delete(df):
    std_tests = [('delete_batch_1_5M_t8','Batch 1'),
                 ('delete_batch_100_5M_t8','Batch 100'),
                 ('delete_batch_1000_5M_t8','Batch 1000'),
                 ('delete_random_5M_t8_b1000','Random b1000')]
    large_tests = [('delete_batch_1_20M_t16','Batch 1'),
                   ('delete_batch_1000_20M_t16','Batch 1000'),
                   ('delete_random_20M_t16_b1000','Main b1000')]
    
    std_avail = [t for t in std_tests if val(df,'tidesdb',t[0],'DELETE','ops_per_sec') > 0 or val(df,'rocksdb',t[0],'DELETE','ops_per_sec') > 0]
    large_avail = [t for t in large_tests if val(df,'tidesdb',t[0],'DELETE','ops_per_sec') > 0 or val(df,'rocksdb',t[0],'DELETE','ops_per_sec') > 0]
    
    if not std_avail and not large_avail:
        print('  - 03_delete_throughput.png (no data, skipped)')
        return
    
    panels = []
    if std_avail:
        panels.append((std_avail, 'Standard (8 threads)'))
    if large_avail:
        panels.append((large_avail, 'Large Scale (16 threads)'))
    
    fig, axes = plt.subplots(1, len(panels), figsize=(8*len(panels), 6))
    if len(panels) == 1:
        axes = [axes]
    fig.suptitle('Delete Throughput')
    
    for ax, (tests, title) in zip(axes, panels):
        lbl = [t[1] for t in tests]
        tv = [val(df,'tidesdb',t[0],'DELETE','ops_per_sec') for t in tests]
        rv = [val(df,'rocksdb',t[0],'DELETE','ops_per_sec') for t in tests]
        paired_bars(ax, lbl, tv, rv, 'ops/sec', title)
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '03_delete_throughput.png')


# ═══════════════════════════════════════════════
# Plot 04: Seek Throughput
# ═══════════════════════════════════════════════
def plot_seek(df):
    std_tests = [('seek_random_5M_t8','Random'),('seek_seq_5M_t8','Sequential'),
                 ('seek_zipfian_5M_t8','Zipfian')]
    large_tests = [('seek_random_20M_t16','Random'),('seek_seq_20M_t16','Sequential'),
                   ('seek_zipfian_20M_t16','Zipfian')]
    
    std_avail = [t for t in std_tests if val(df,'tidesdb',t[0],'SEEK','ops_per_sec') > 0 or val(df,'rocksdb',t[0],'SEEK','ops_per_sec') > 0]
    large_avail = [t for t in large_tests if val(df,'tidesdb',t[0],'SEEK','ops_per_sec') > 0 or val(df,'rocksdb',t[0],'SEEK','ops_per_sec') > 0]
    
    if not std_avail and not large_avail:
        print('  - 04_seek_throughput.png (no data, skipped)')
        return
    
    panels = []
    if std_avail:
        panels.append((std_avail, 'Standard (8 threads)'))
    if large_avail:
        panels.append((large_avail, 'Large Scale (16 threads)'))
    
    fig, axes = plt.subplots(1, len(panels), figsize=(8*len(panels), 6))
    if len(panels) == 1:
        axes = [axes]
    fig.suptitle('Seek Throughput')
    
    for ax, (tests, title) in zip(axes, panels):
        lbl = [t[1] for t in tests]
        tv = [val(df,'tidesdb',t[0],'SEEK','ops_per_sec') for t in tests]
        rv = [val(df,'rocksdb',t[0],'SEEK','ops_per_sec') for t in tests]
        paired_bars(ax, lbl, tv, rv, 'ops/sec', title)
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '04_seek_throughput.png')


# ═══════════════════════════════════════════════
# Plot 05: Range Scan Throughput
# ═══════════════════════════════════════════════
def plot_range(df):
    std_tests = [('range_random_100_1M_t8','Rand 100\n1M'),
                 ('range_random_1000_500K_t8','Rand 1000\n500K'),
                 ('range_seq_100_1M_t8','Seq 100\n1M')]
    large_tests = [('range_random_100_4M_t16','Rand 100\n4M'),
                   ('range_random_1000_2M_t16','Rand 1000\n2M'),
                   ('range_seq_100_4M_t16','Seq 100\n4M')]
    
    std_avail = [t for t in std_tests if val(df,'tidesdb',t[0],'RANGE','ops_per_sec') > 0 or val(df,'rocksdb',t[0],'RANGE','ops_per_sec') > 0]
    large_avail = [t for t in large_tests if val(df,'tidesdb',t[0],'RANGE','ops_per_sec') > 0 or val(df,'rocksdb',t[0],'RANGE','ops_per_sec') > 0]
    
    if not std_avail and not large_avail:
        print('  - 05_range_scan_throughput.png (no data, skipped)')
        return
    
    panels = []
    if std_avail:
        panels.append((std_avail, 'Standard (8 threads)'))
    if large_avail:
        panels.append((large_avail, 'Large Scale (16 threads)'))
    
    fig, axes = plt.subplots(1, len(panels), figsize=(8*len(panels), 6))
    if len(panels) == 1:
        axes = [axes]
    fig.suptitle('Range Scan Throughput')
    
    for ax, (tests, title) in zip(axes, panels):
        lbl = [t[1] for t in tests]
        tv = [val(df,'tidesdb',t[0],'RANGE','ops_per_sec') for t in tests]
        rv = [val(df,'rocksdb',t[0],'RANGE','ops_per_sec') for t in tests]
        paired_bars(ax, lbl, tv, rv, 'ops/sec', title)
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '05_range_scan_throughput.png')


# ═══════════════════════════════════════════════
# Plot 06: Batch Size Scaling
# ═══════════════════════════════════════════════
def plot_batch_scaling(df):
    std_batches = [1,10,100,1000,10000]
    std_names = ['batch_1_10M_t8','batch_10_10M_t8','batch_100_10M_t8',
                 'batch_1000_10M_t8','batch_10000_10M_t8']
    large_batches = [1,100,1000]
    large_names = ['batch_1_40M_t16','batch_100_40M_t16','batch_1000_40M_t16']
    
    # Check which data is available
    std_avail = [(b, n) for b, n in zip(std_batches, std_names) 
                 if val(df,'tidesdb',n,'PUT','ops_per_sec') > 0 or val(df,'rocksdb',n,'PUT','ops_per_sec') > 0]
    large_avail = [(b, n) for b, n in zip(large_batches, large_names)
                   if val(df,'tidesdb',n,'PUT','ops_per_sec') > 0 or val(df,'rocksdb',n,'PUT','ops_per_sec') > 0]
    
    if not std_avail and not large_avail:
        print('  - 06_batch_size_scaling.png (no data, skipped)')
        return
    
    panels = []
    if std_avail:
        panels.append(([b for b,n in std_avail], [n for b,n in std_avail], 'Standard (10M, 8t)'))
    if large_avail:
        panels.append(([b for b,n in large_avail], [n for b,n in large_avail], 'Large Scale (40M, 16t)'))
    
    fig, axes = plt.subplots(1, len(panels), figsize=(8*len(panels), 6))
    if len(panels) == 1:
        axes = [axes]
    fig.suptitle('Batch Size Scaling — Write Throughput')
    
    for ax, (batches, names, title) in zip(axes, panels):
        tv = [val(df,'tidesdb',n,'PUT','ops_per_sec') for n in names]
        rv = [val(df,'rocksdb',n,'PUT','ops_per_sec') for n in names]
        ax.plot(batches, tv, 'o-', color=TIDES, lw=2.5, ms=8, label='TidesDB', zorder=3)
        ax.plot(batches, rv, 's-', color=ROCKS, lw=2.5, ms=8, label='RocksDB', zorder=3)
        ax.set_xscale('log')
        ax.set_xlabel('Batch Size')
        ax.set_ylabel('ops/sec')
        ax.set_title(title)
        ax.legend()
        ax.set_axisbelow(True)
        for b, t, r in zip(batches, tv, rv):
            if t > 0:
                ax.annotate(fmt_v(t), (b, t), textcoords='offset points',
                            xytext=(0, 10), ha='center', fontsize=7, color=TIDES, fontweight='bold')
            if r > 0:
                ax.annotate(fmt_v(r), (b, r), textcoords='offset points',
                            xytext=(0, -14), ha='center', fontsize=7, color='#616161', fontweight='bold')
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '06_batch_size_scaling.png')


# ═══════════════════════════════════════════════
# Plot 07: Value Size Impact
# ═══════════════════════════════════════════════
def plot_value_size(df):
    std_tests = [('write_small_values_50M_k16_v64_t8_b1000','64B val\n50M'),
                 ('write_random_10M_t8_b1000','100B val\n10M'),
                 ('write_large_values_1M_k256_v4096_t8_b1000','4KB val\n1M')]
    large_tests = [('write_small_values_200M_k16_v64_t16_b1000','64B val\n200M'),
                   ('write_random_40M_t16_b1000','100B val\n40M'),
                   ('write_large_values_4M_k256_v4096_t16_b1000','4KB val\n4M')]
    
    std_avail = [t for t in std_tests if val(df,'tidesdb',t[0],'PUT','ops_per_sec') > 0 or val(df,'rocksdb',t[0],'PUT','ops_per_sec') > 0]
    large_avail = [t for t in large_tests if val(df,'tidesdb',t[0],'PUT','ops_per_sec') > 0 or val(df,'rocksdb',t[0],'PUT','ops_per_sec') > 0]
    
    if not std_avail and not large_avail:
        print('  - 07_value_size_impact.png (no data, skipped)')
        return
    
    panels = []
    if std_avail:
        panels.append((std_avail, 'Standard (8 threads)'))
    if large_avail:
        panels.append((large_avail, 'Large Scale (16 threads)'))
    
    fig, axes = plt.subplots(1, len(panels), figsize=(8*len(panels), 6))
    if len(panels) == 1:
        axes = [axes]
    fig.suptitle('Value Size Impact on Write Throughput')
    
    for ax, (tests, title) in zip(axes, panels):
        lbl = [t[1] for t in tests]
        tv = [val(df,'tidesdb',t[0],'PUT','ops_per_sec') for t in tests]
        rv = [val(df,'rocksdb',t[0],'PUT','ops_per_sec') for t in tests]
        paired_bars(ax, lbl, tv, rv, 'ops/sec', title)
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '07_value_size_impact.png')


# ═══════════════════════════════════════════════
# Plot 08: Latency Overview (4-panel)
# ═══════════════════════════════════════════════
def plot_latency_overview(df):
    all_panels = [
        ('Write Latency', [
            ('write_seq_10M_t8_b1000','PUT','Seq\n10M'),
            ('write_random_10M_t8_b1000','PUT','Rand\n10M'),
            ('write_zipfian_5M_t8_b1000','PUT','Zipf\n5M'),
            ('write_seq_40M_t16_b1000','PUT','Seq\n40M'),
            ('write_random_40M_t16_b1000','PUT','Rand\n40M')]),
        ('Read Latency', [
            ('read_random_10M_t8','GET','Read\n10M,8t'),
            ('read_random_40M_t16','GET','Read\n40M,16t'),
            ('mixed_random_5M_t8_b1000','GET','Mix GET\n5M,8t'),
            ('mixed_random_20M_t16_b1000','GET','Mix GET\n20M,16t')]),
        ('Seek Latency', [
            ('seek_random_5M_t8','SEEK','Rand\n5M,8t'),
            ('seek_seq_5M_t8','SEEK','Seq\n5M,8t'),
            ('seek_zipfian_5M_t8','SEEK','Zipf\n5M,8t'),
            ('seek_random_20M_t16','SEEK','Rand\n20M,16t'),
            ('seek_seq_20M_t16','SEEK','Seq\n20M,16t')]),
        ('Range Scan Latency', [
            ('range_random_100_1M_t8','RANGE','R100\n1M,8t'),
            ('range_random_1000_500K_t8','RANGE','R1000\n500K,8t'),
            ('range_seq_100_1M_t8','RANGE','S100\n1M,8t'),
            ('range_random_100_4M_t16','RANGE','R100\n4M,16t'),
            ('range_random_1000_2M_t16','RANGE','R1000\n2M,16t')]),
    ]
    
    # Filter panels to only those with data, and filter tests within panels
    panels = []
    for title, tests in all_panels:
        avail = [t for t in tests if val(df,'tidesdb',t[0],t[1],'avg_latency_us') > 0 or val(df,'rocksdb',t[0],t[1],'avg_latency_us') > 0]
        if avail:
            panels.append((title, avail))
    
    if not panels:
        print('  - 08_latency_overview.png (no data, skipped)')
        return
    
    # Dynamic layout
    n = len(panels)
    if n <= 2:
        fig, axes = plt.subplots(1, n, figsize=(8*n, 6))
        if n == 1:
            axes = [axes]
    else:
        rows = (n + 1) // 2
        fig, axes = plt.subplots(rows, 2, figsize=(18, 6*rows))
        axes = axes.flatten()[:n]
    
    fig.suptitle('Average Latency (us) — Lower is Better')
    for ax, (title, tests) in zip(axes, panels):
        lbl = [t[2] for t in tests]
        tv = [val(df,'tidesdb',t[0],t[1],'avg_latency_us') for t in tests]
        rv = [val(df,'rocksdb',t[0],t[1],'avg_latency_us') for t in tests]
        paired_bars(ax, lbl, tv, rv, 'Avg Latency (us)', title)
    fig.tight_layout(rect=[0,0,1,.95])
    save(fig, '08_latency_overview.png')


# ═══════════════════════════════════════════════
# Plot 09: Latency Percentiles (6-panel)
# ═══════════════════════════════════════════════
def plot_latency_percentiles(df):
    all_wklds = [
        ('write_seq_10M_t8_b1000','PUT','Seq Write (10M)'),
        ('write_random_10M_t8_b1000','PUT','Random Write (10M)'),
        ('read_random_10M_t8','GET','Random Read (10M)'),
        ('seek_random_5M_t8','SEEK','Random Seek (5M)'),
        ('range_random_100_1M_t8','RANGE','Range 100 (1M)'),
        ('delete_random_5M_t8_b1000','DELETE','Delete (5M)'),
    ]
    
    # Filter to available workloads
    wklds = [w for w in all_wklds if val(df,'tidesdb',w[0],w[1],'p50_us') > 0 or val(df,'rocksdb',w[0],w[1],'p50_us') > 0]
    
    if not wklds:
        print('  - 09_latency_percentiles.png (no data, skipped)')
        return
    
    # Dynamic layout
    n = len(wklds)
    if n <= 3:
        fig, axes = plt.subplots(1, n, figsize=(6*n, 6))
        if n == 1:
            axes = [axes]
        else:
            axes = list(axes)
    else:
        rows = (n + 2) // 3
        fig, axes = plt.subplots(rows, 3, figsize=(20, 6*rows))
        axes = axes.flatten()[:n]
    
    fig.suptitle('Latency Percentiles (us) — p50 / p95 / p99')
    for ax, (tn, op, title) in zip(axes, wklds):
        pcts = ['p50_us','p95_us','p99_us']
        tv = [val(df,'tidesdb',tn,op,p) for p in pcts]
        rv = [val(df,'rocksdb',tn,op,p) for p in pcts]
        paired_bars(ax, ['p50','p95','p99'], tv, rv, 'Latency (us)', title, rotation=0)
    fig.tight_layout(rect=[0,0,1,.95])
    save(fig, '09_latency_percentiles.png')


# ═══════════════════════════════════════════════
# Plot 10: Write Amplification
# ═══════════════════════════════════════════════
def plot_write_amp(df):
    std_tests = [('write_seq_10M_t8_b1000','PUT','Seq\n10M'),
                 ('write_random_10M_t8_b1000','PUT','Rand\n10M'),
                 ('write_zipfian_5M_t8_b1000','PUT','Zipf\n5M'),
                 ('write_small_values_50M_k16_v64_t8_b1000','PUT','Small\n50M'),
                 ('write_large_values_1M_k256_v4096_t8_b1000','PUT','Large\n1M')]
    large_tests = [('write_seq_40M_t16_b1000','PUT','Seq\n40M'),
                   ('write_random_40M_t16_b1000','PUT','Rand\n40M'),
                   ('write_zipfian_20M_t16_b1000','PUT','Zipf\n20M'),
                   ('write_small_values_200M_k16_v64_t16_b1000','PUT','Small\n200M'),
                   ('write_large_values_4M_k256_v4096_t16_b1000','PUT','Large\n4M')]
    
    std_avail = [t for t in std_tests if val(df,'tidesdb',t[0],t[1],'write_amp') > 0 or val(df,'rocksdb',t[0],t[1],'write_amp') > 0]
    large_avail = [t for t in large_tests if val(df,'tidesdb',t[0],t[1],'write_amp') > 0 or val(df,'rocksdb',t[0],t[1],'write_amp') > 0]
    
    if not std_avail and not large_avail:
        print('  - 10_write_amplification.png (no data, skipped)')
        return
    
    panels = []
    if std_avail:
        panels.append((std_avail, 'Standard Scale'))
    if large_avail:
        panels.append((large_avail, 'Large Scale'))
    
    fig, axes = plt.subplots(1, len(panels), figsize=(9*len(panels), 6))
    if len(panels) == 1:
        axes = [axes]
    fig.suptitle('Write Amplification — Lower is Better')
    
    for ax, (tests, title) in zip(axes, panels):
        lbl = [t[2] for t in tests]
        tv = [val(df,'tidesdb',t[0],t[1],'write_amp') for t in tests]
        rv = [val(df,'rocksdb',t[0],t[1],'write_amp') for t in tests]
        paired_bars(ax, lbl, tv, rv, 'Write Amplification', title, decimal=True)
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '10_write_amplification.png')


# ═══════════════════════════════════════════════
# Plot 11: Space Efficiency
# ═══════════════════════════════════════════════
def plot_space(df):
    all_tests = [
        ('write_seq_10M_t8_b1000','PUT','Seq 10M'),
        ('write_random_10M_t8_b1000','PUT','Rand 10M'),
        ('write_small_values_50M_k16_v64_t8_b1000','PUT','Small 50M'),
        ('write_large_values_1M_k256_v4096_t8_b1000','PUT','Large 1M'),
        ('write_seq_40M_t16_b1000','PUT','Seq 40M'),
        ('write_random_40M_t16_b1000','PUT','Rand 40M'),
    ]
    
    tests = [t for t in all_tests if val(df,'tidesdb',t[0],t[1],'db_size_mb') > 0 or val(df,'rocksdb',t[0],t[1],'db_size_mb') > 0]
    
    if not tests:
        print('  - 11_space_efficiency.png (no data, skipped)')
        return
    
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(18, 6))
    fig.suptitle('Space Efficiency — DB Size & Amplification')
    
    lbl = [t[2] for t in tests]
    paired_bars(a1, lbl,
                [val(df,'tidesdb',t[0],t[1],'db_size_mb') for t in tests],
                [val(df,'rocksdb',t[0],t[1],'db_size_mb') for t in tests],
                'DB Size (MB)', 'On-Disk Database Size')
    paired_bars(a2, lbl,
                [val(df,'tidesdb',t[0],t[1],'space_amp') for t in tests],
                [val(df,'rocksdb',t[0],t[1],'space_amp') for t in tests],
                'Space Amplification', 'Space Amplification (lower = better)', decimal=True)
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '11_space_efficiency.png')


# ═══════════════════════════════════════════════
# Plot 12: Resource Usage (4-panel)
# ═══════════════════════════════════════════════
def plot_resources(df):
    all_tests = [
        ('write_seq_10M_t8_b1000','PUT','Seq 10M'),
        ('write_random_10M_t8_b1000','PUT','Rand 10M'),
        ('read_random_10M_t8','GET','Read 10M'),
        ('write_seq_40M_t16_b1000','PUT','Seq 40M'),
        ('write_random_40M_t16_b1000','PUT','Rand 40M'),
        ('read_random_40M_t16','GET','Read 40M'),
    ]
    
    tests = [t for t in all_tests if val(df,'tidesdb',t[0],t[1],'peak_rss_mb') > 0 or val(df,'rocksdb',t[0],t[1],'peak_rss_mb') > 0]
    
    if not tests:
        print('  - 12_resource_usage.png (no data, skipped)')
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle('Resource Usage Comparison')
    lbl = [t[2] for t in tests]
    for ax, col, ylabel, title in [
        (axes[0,0], 'peak_rss_mb', 'Peak RSS (MB)', 'Memory Usage (Peak RSS)'),
        (axes[0,1], 'disk_write_mb', 'Disk Write (MB)', 'Disk Write Volume'),
        (axes[1,0], 'cpu_percent', 'CPU %', 'CPU Utilization'),
        (axes[1,1], 'peak_vms_mb', 'Peak VMS (MB)', 'Virtual Memory (Peak VMS)'),
    ]:
        tv = [val(df,'tidesdb',t[0],t[1],col) for t in tests]
        rv = [val(df,'rocksdb',t[0],t[1],col) for t in tests]
        paired_bars(ax, lbl, tv, rv, ylabel, title)
    fig.tight_layout(rect=[0,0,1,.95])
    save(fig, '12_resource_usage.png')


# ═══════════════════════════════════════════════
# Plot 13: Tail Latency (avg vs p99)
# ═══════════════════════════════════════════════
def plot_tail_latency(df):
    std_tests = [('write_seq_10M_t8_b1000','PUT','Seq'),
                 ('write_random_10M_t8_b1000','PUT','Random'),
                 ('write_zipfian_5M_t8_b1000','PUT','Zipfian'),
                 ('write_large_values_1M_k256_v4096_t8_b1000','PUT','LargeVal')]
    large_tests = [('write_seq_40M_t16_b1000','PUT','Seq'),
                   ('write_random_40M_t16_b1000','PUT','Random'),
                   ('write_zipfian_20M_t16_b1000','PUT','Zipfian'),
                   ('write_large_values_4M_k256_v4096_t16_b1000','PUT','LargeVal')]
    
    std_avail = [t for t in std_tests if val(df,'tidesdb',t[0],t[1],'avg_latency_us') > 0 or val(df,'rocksdb',t[0],t[1],'avg_latency_us') > 0]
    large_avail = [t for t in large_tests if val(df,'tidesdb',t[0],t[1],'avg_latency_us') > 0 or val(df,'rocksdb',t[0],t[1],'avg_latency_us') > 0]
    
    if not std_avail and not large_avail:
        print('  - 13_tail_latency.png (no data, skipped)')
        return
    
    panels = []
    if std_avail:
        panels.append((std_avail, 'Standard Scale'))
    if large_avail:
        panels.append((large_avail, 'Large Scale'))
    
    fig, axes = plt.subplots(1, len(panels), figsize=(9*len(panels), 6))
    if len(panels) == 1:
        axes = [axes]
    fig.suptitle('Tail Latency: Average vs p99 (us)')
    
    for ax, (tests, title) in zip(axes, panels):
        x = np.arange(len(tests))
        w = 0.18
        lbl = [t[2] for t in tests]
        t_avg = [val(df,'tidesdb',t[0],t[1],'avg_latency_us') for t in tests]
        t_p99 = [val(df,'tidesdb',t[0],t[1],'p99_us') for t in tests]
        r_avg = [val(df,'rocksdb',t[0],t[1],'avg_latency_us') for t in tests]
        r_p99 = [val(df,'rocksdb',t[0],t[1],'p99_us') for t in tests]
        ax.bar(x-1.5*w, t_avg, w, label='TidesDB avg', color=TIDES, zorder=3)
        ax.bar(x-0.5*w, t_p99, w, label='TidesDB p99', color=TIDES_L, zorder=3)
        ax.bar(x+0.5*w, r_avg, w, label='RocksDB avg', color=ROCKS, zorder=3)
        ax.bar(x+1.5*w, r_p99, w, label='RocksDB p99', color=ROCKS_L, zorder=3)
        ax.set_xticks(x)
        ax.set_xticklabels(lbl)
        ax.set_ylabel('Latency (us)')
        ax.set_title(title)
        ax.legend(fontsize=8)
        ax.set_axisbelow(True)
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '13_tail_latency.png')


# ═══════════════════════════════════════════════
# Plot 14: Duration Comparison
# ═══════════════════════════════════════════════
def plot_duration(df):
    std_tests = [('write_seq_10M_t8_b1000','PUT','Seq Write\n10M'),
                 ('write_random_10M_t8_b1000','PUT','Rand Write\n10M'),
                 ('read_random_10M_t8','GET','Read\n10M'),
                 ('write_small_values_50M_k16_v64_t8_b1000','PUT','Small\n50M'),
                 ('write_large_values_1M_k256_v4096_t8_b1000','PUT','Large\n1M')]
    large_tests = [('write_seq_40M_t16_b1000','PUT','Seq Write\n40M'),
                   ('write_random_40M_t16_b1000','PUT','Rand Write\n40M'),
                   ('read_random_40M_t16','GET','Read\n40M'),
                   ('write_small_values_200M_k16_v64_t16_b1000','PUT','Small\n200M'),
                   ('write_large_values_4M_k256_v4096_t16_b1000','PUT','Large\n4M')]
    
    std_avail = [t for t in std_tests if val(df,'tidesdb',t[0],t[1],'duration_sec') > 0 or val(df,'rocksdb',t[0],t[1],'duration_sec') > 0]
    large_avail = [t for t in large_tests if val(df,'tidesdb',t[0],t[1],'duration_sec') > 0 or val(df,'rocksdb',t[0],t[1],'duration_sec') > 0]
    
    if not std_avail and not large_avail:
        print('  - 14_duration_comparison.png (no data, skipped)')
        return
    
    panels = []
    if std_avail:
        panels.append((std_avail, 'Standard Scale'))
    if large_avail:
        panels.append((large_avail, 'Large Scale'))
    
    fig, axes = plt.subplots(1, len(panels), figsize=(9*len(panels), 6))
    if len(panels) == 1:
        axes = [axes]
    fig.suptitle('Wall-Clock Duration (sec) — Lower is Better')
    
    for ax, (tests, title) in zip(axes, panels):
        lbl = [t[2] for t in tests]
        tv = [val(df,'tidesdb',t[0],t[1],'duration_sec') for t in tests]
        rv = [val(df,'rocksdb',t[0],t[1],'duration_sec') for t in tests]
        paired_bars(ax, lbl, tv, rv, 'Duration (sec)', title)
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '14_duration_comparison.png')


# ═══════════════════════════════════════════════
# Plot 15: Latency Variability (CV%)
# ═══════════════════════════════════════════════
def plot_cv(df):
    write_tests = [('write_seq_10M_t8_b1000','PUT','Seq Write'),
                   ('write_random_10M_t8_b1000','PUT','Rand Write'),
                   ('write_zipfian_5M_t8_b1000','PUT','Zipf Write'),
                   ('write_large_values_1M_k256_v4096_t8_b1000','PUT','Large Val')]
    read_tests = [('read_random_10M_t8','GET','Rand Read'),
                  ('seek_random_5M_t8','SEEK','Rand Seek'),
                  ('seek_seq_5M_t8','SEEK','Seq Seek'),
                  ('range_random_100_1M_t8','RANGE','Range 100')]
    
    write_avail = [t for t in write_tests if val(df,'tidesdb',t[0],t[1],'cv_percent') > 0 or val(df,'rocksdb',t[0],t[1],'cv_percent') > 0]
    read_avail = [t for t in read_tests if val(df,'tidesdb',t[0],t[1],'cv_percent') > 0 or val(df,'rocksdb',t[0],t[1],'cv_percent') > 0]
    
    if not write_avail and not read_avail:
        print('  - 15_latency_variability.png (no data, skipped)')
        return
    
    panels = []
    if write_avail:
        panels.append((write_avail, 'Write Variability'))
    if read_avail:
        panels.append((read_avail, 'Read/Seek Variability'))
    
    fig, axes = plt.subplots(1, len(panels), figsize=(9*len(panels), 6))
    if len(panels) == 1:
        axes = [axes]
    fig.suptitle('Latency Variability (CV%) — Lower is More Consistent')
    
    for ax, (tests, title) in zip(axes, panels):
        lbl = [t[2] for t in tests]
        tv = [val(df,'tidesdb',t[0],t[1],'cv_percent') for t in tests]
        rv = [val(df,'rocksdb',t[0],t[1],'cv_percent') for t in tests]
        paired_bars(ax, lbl, tv, rv, 'CV %', title, decimal=True)
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '15_latency_variability.png')


# ═══════════════════════════════════════════════
# Plot 16: Synced Write Throughput & Latency
# ═══════════════════════════════════════════════
def plot_sync_writes(df):
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Synced (Durable) Write Performance — Scaling')
    tests = [
        ('sync_write_random_25K_t1_b1000', '25K\n1 thread'),
        ('sync_write_random_50K_t4_b1000', '50K\n4 threads'),
        ('sync_write_random_100K_t8_b1000', '100K\n8 threads'),
        ('sync_write_random_500K_t16_b1000', '500K\n16 threads'),
    ]
    lbl = [t[1] for t in tests]
    tv = [val(df, 'tidesdb', t[0], 'PUT', 'ops_per_sec') for t in tests]
    rv = [val(df, 'rocksdb', t[0], 'PUT', 'ops_per_sec') for t in tests]
    if any(v > 0 for v in tv + rv):
        paired_bars(a1, lbl, tv, rv, 'ops/sec', 'Throughput (sync=on)', rotation=0)
        tv = [val(df, 'tidesdb', t[0], 'PUT', 'avg_latency_us') for t in tests]
        rv = [val(df, 'rocksdb', t[0], 'PUT', 'avg_latency_us') for t in tests]
        paired_bars(a2, lbl, tv, rv, 'Avg Latency (us)', 'Latency (sync=on)', rotation=0)
        fig.tight_layout(rect=[0, 0, 1, .93])
        save(fig, '16_sync_write_performance.png')
    else:
        plt.close(fig)
        print('  - 16_sync_write_performance.png (no sync data found, skipped)')


# ═══════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════
def main():
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        csvs = sorted(glob.glob('tidesdb_rocksdb_benchmark_results_*.csv'))
        if not csvs:
            print('Usage: python3 plot_tidesdb_rocksdb.py <csv_file>')
            sys.exit(1)
        csv_path = csvs[-1]

    print(f'Loading: {csv_path}')
    df = load_data(csv_path)
    os.makedirs(OUT_DIR, exist_ok=True)
    setup_style()

    print('Generating plots...')
    plot_speedup_summary(df)
    plot_write_throughput(df)
    plot_read_mixed(df)
    plot_delete(df)
    plot_seek(df)
    plot_range(df)
    plot_batch_scaling(df)
    plot_value_size(df)
    plot_latency_overview(df)
    plot_latency_percentiles(df)
    plot_write_amp(df)
    plot_space(df)
    plot_resources(df)
    plot_tail_latency(df)
    plot_duration(df)
    plot_cv(df)
    plot_sync_writes(df)

    n = len([f for f in os.listdir(OUT_DIR) if f.endswith('.png')])
    print(f'\nDone! {n} plots saved to {OUT_DIR}/')


if __name__ == '__main__':
    main()
