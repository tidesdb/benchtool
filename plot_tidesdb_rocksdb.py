#!/usr/bin/env python3
"""
TidesDB vs RocksDB Benchmark (tidesdb_rocksdb.sh) Visualization
===========================================
Generates comparison plots from CSV benchmark data.
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
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Write Throughput (ops/sec)')
    for ax, tests, title in [
        (a1, [('write_seq_10M_t8_b1000','Seq\n10M'),
               ('write_random_10M_t8_b1000','Random\n10M'),
               ('write_zipfian_5M_t8_b1000','Zipfian\n5M')],
         'Standard (8 threads, 64MB cache)'),
        (a2, [('write_seq_40M_t16_b1000','Seq\n40M'),
               ('write_random_40M_t16_b1000','Random\n40M'),
               ('write_zipfian_20M_t16_b1000','Zipfian\n20M')],
         'Large Scale (16 threads, 6GB cache)'),
    ]:
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
    fig, (a1, a2, a3) = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle('Read & Mixed Workload Throughput')
    # Reads
    t = [('read_random_10M_t8','GET','Read\n10M,8t'),('read_random_40M_t16','GET','Read\n40M,16t')]
    paired_bars(a1, [x[2] for x in t],
                [val(df,'tidesdb',x[0],x[1],'ops_per_sec') for x in t],
                [val(df,'rocksdb',x[0],x[1],'ops_per_sec') for x in t],
                'ops/sec', 'Read Throughput')
    # Mixed PUT
    t = [('mixed_random_5M_t8_b1000','PUT','Rand\n5M,8t'),
         ('mixed_zipfian_5M_t8_b1000','PUT','Zipf\n5M,8t'),
         ('mixed_random_20M_t16_b1000','PUT','Rand\n20M,16t')]
    paired_bars(a2, [x[2] for x in t],
                [val(df,'tidesdb',x[0],x[1],'ops_per_sec') for x in t],
                [val(df,'rocksdb',x[0],x[1],'ops_per_sec') for x in t],
                'ops/sec', 'Mixed — Write Side')
    # Mixed GET
    t = [('mixed_random_5M_t8_b1000','GET','Rand\n5M,8t'),
         ('mixed_zipfian_5M_t8_b1000','GET','Zipf\n5M,8t'),
         ('mixed_random_20M_t16_b1000','GET','Rand\n20M,16t')]
    paired_bars(a3, [x[2] for x in t],
                [val(df,'tidesdb',x[0],x[1],'ops_per_sec') for x in t],
                [val(df,'rocksdb',x[0],x[1],'ops_per_sec') for x in t],
                'ops/sec', 'Mixed — Read Side')
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '02_read_mixed_throughput.png')


# ═══════════════════════════════════════════════
# Plot 03: Delete Throughput
# ═══════════════════════════════════════════════
def plot_delete(df):
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Delete Throughput')
    for ax, tests, title in [
        (a1, [('delete_batch_1_5M_t8','Batch 1'),
               ('delete_batch_100_5M_t8','Batch 100'),
               ('delete_batch_1000_5M_t8','Batch 1000')],
         'Standard (5M, 8t)'),
        (a2, [('delete_batch_1_20M_t16','Batch 1'),
               ('delete_batch_1000_20M_t16','Batch 1000'),
               ('delete_random_20M_t16_b1000','Main b1000')],
         'Large Scale (20M, 16t)'),
    ]:
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
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Seek Throughput')
    for ax, tests, title in [
        (a1, [('seek_random_5M_t8','Random'),('seek_seq_5M_t8','Sequential'),
               ('seek_zipfian_5M_t8','Zipfian')], 'Standard (5M, 8t)'),
        (a2, [('seek_random_20M_t16','Random'),('seek_seq_20M_t16','Sequential'),
               ('seek_zipfian_20M_t16','Zipfian')], 'Large Scale (20M, 16t)'),
    ]:
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
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Range Scan Throughput')
    for ax, tests, title in [
        (a1, [('range_random_100_1M_t8','Rand 100\n1M'),
               ('range_random_1000_500K_t8','Rand 1000\n500K'),
               ('range_seq_100_1M_t8','Seq 100\n1M')], 'Standard (8t)'),
        (a2, [('range_random_100_4M_t16','Rand 100\n4M'),
               ('range_random_1000_2M_t16','Rand 1000\n2M'),
               ('range_seq_100_4M_t16','Seq 100\n4M')], 'Large Scale (16t)'),
    ]:
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
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Batch Size Scaling — Write Throughput')
    for ax, batches, names, title in [
        (a1, [1,10,100,1000,10000],
         ['batch_1_10M_t8','batch_10_10M_t8','batch_100_10M_t8',
          'batch_1000_10M_t8','batch_10000_10M_t8'],
         'Standard (10M, 8t)'),
        (a2, [1,100,1000],
         ['batch_1_40M_t16','batch_100_40M_t16','batch_1000_40M_t16'],
         'Large Scale (40M, 16t)'),
    ]:
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
            ax.annotate(fmt_v(t), (b, t), textcoords='offset points',
                        xytext=(0, 10), ha='center', fontsize=7, color=TIDES, fontweight='bold')
            ax.annotate(fmt_v(r), (b, r), textcoords='offset points',
                        xytext=(0, -14), ha='center', fontsize=7, color='#616161', fontweight='bold')
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '06_batch_size_scaling.png')


# ═══════════════════════════════════════════════
# Plot 07: Value Size Impact
# ═══════════════════════════════════════════════
def plot_value_size(df):
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Value Size Impact on Write Throughput')
    for ax, tests, title in [
        (a1, [('write_small_values_50M_k16_v64_t8_b1000','64B val\n50M'),
               ('write_random_10M_t8_b1000','100B val\n10M'),
               ('write_large_values_1M_k256_v4096_t8_b1000','4KB val\n1M')],
         'Standard (8t)'),
        (a2, [('write_small_values_200M_k16_v64_t16_b1000','64B val\n200M'),
               ('write_random_40M_t16_b1000','100B val\n40M'),
               ('write_large_values_4M_k256_v4096_t16_b1000','4KB val\n4M')],
         'Large Scale (16t)'),
    ]:
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
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle('Average Latency (us) — Lower is Better')
    panels = [
        (axes[0,0], 'Write Latency', [
            ('write_seq_10M_t8_b1000','PUT','Seq\n10M'),
            ('write_random_10M_t8_b1000','PUT','Rand\n10M'),
            ('write_zipfian_5M_t8_b1000','PUT','Zipf\n5M'),
            ('write_seq_40M_t16_b1000','PUT','Seq\n40M'),
            ('write_random_40M_t16_b1000','PUT','Rand\n40M')]),
        (axes[0,1], 'Read Latency', [
            ('read_random_10M_t8','GET','Read\n10M,8t'),
            ('read_random_40M_t16','GET','Read\n40M,16t'),
            ('mixed_random_5M_t8_b1000','GET','Mix GET\n5M,8t'),
            ('mixed_random_20M_t16_b1000','GET','Mix GET\n20M,16t')]),
        (axes[1,0], 'Seek Latency', [
            ('seek_random_5M_t8','SEEK','Rand\n5M,8t'),
            ('seek_seq_5M_t8','SEEK','Seq\n5M,8t'),
            ('seek_zipfian_5M_t8','SEEK','Zipf\n5M,8t'),
            ('seek_random_20M_t16','SEEK','Rand\n20M,16t'),
            ('seek_seq_20M_t16','SEEK','Seq\n20M,16t')]),
        (axes[1,1], 'Range Scan Latency', [
            ('range_random_100_1M_t8','RANGE','R100\n1M,8t'),
            ('range_random_1000_500K_t8','RANGE','R1000\n500K,8t'),
            ('range_seq_100_1M_t8','RANGE','S100\n1M,8t'),
            ('range_random_100_4M_t16','RANGE','R100\n4M,16t'),
            ('range_random_1000_2M_t16','RANGE','R1000\n2M,16t')]),
    ]
    for ax, title, tests in panels:
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
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    fig.suptitle('Latency Percentiles (us) — p50 / p95 / p99')
    wklds = [
        ('write_seq_10M_t8_b1000','PUT','Seq Write (10M)'),
        ('write_random_10M_t8_b1000','PUT','Random Write (10M)'),
        ('read_random_10M_t8','GET','Random Read (10M)'),
        ('seek_random_5M_t8','SEEK','Random Seek (5M)'),
        ('range_random_100_1M_t8','RANGE','Range 100 (1M)'),
        ('delete_random_5M_t8_b1000','DELETE','Delete (5M)'),
    ]
    for idx, (tn, op, title) in enumerate(wklds):
        ax = axes[idx//3][idx%3]
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
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(18, 6))
    fig.suptitle('Write Amplification — Lower is Better')
    for ax, tests, title in [
        (a1, [('write_seq_10M_t8_b1000','PUT','Seq\n10M'),
               ('write_random_10M_t8_b1000','PUT','Rand\n10M'),
               ('write_zipfian_5M_t8_b1000','PUT','Zipf\n5M'),
               ('write_small_values_50M_k16_v64_t8_b1000','PUT','Small\n50M'),
               ('write_large_values_1M_k256_v4096_t8_b1000','PUT','Large\n1M')],
         'Standard Scale'),
        (a2, [('write_seq_40M_t16_b1000','PUT','Seq\n40M'),
               ('write_random_40M_t16_b1000','PUT','Rand\n40M'),
               ('write_zipfian_20M_t16_b1000','PUT','Zipf\n20M'),
               ('write_small_values_200M_k16_v64_t16_b1000','PUT','Small\n200M'),
               ('write_large_values_4M_k256_v4096_t16_b1000','PUT','Large\n4M')],
         'Large Scale'),
    ]:
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
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(18, 6))
    fig.suptitle('Space Efficiency — DB Size & Amplification')
    tests = [
        ('write_seq_10M_t8_b1000','PUT','Seq 10M'),
        ('write_random_10M_t8_b1000','PUT','Rand 10M'),
        ('write_small_values_50M_k16_v64_t8_b1000','PUT','Small 50M'),
        ('write_large_values_1M_k256_v4096_t8_b1000','PUT','Large 1M'),
        ('write_seq_40M_t16_b1000','PUT','Seq 40M'),
        ('write_random_40M_t16_b1000','PUT','Rand 40M'),
    ]
    lbl = [t[2] for t in tests]
    paired_bars(a1, lbl,
                [val(df,'tidesdb',t[0],t[1],'db_size_mb') for t in tests],
                [val(df,'rocksdb',t[0],t[1],'db_size_mb') for t in tests],
                'DB Size (MB)', 'On-Disk Database Size')
    tests2 = [
        ('write_seq_10M_t8_b1000','PUT','Seq 10M'),
        ('write_random_10M_t8_b1000','PUT','Rand 10M'),
        ('write_zipfian_5M_t8_b1000','PUT','Zipf 5M'),
        ('write_seq_40M_t16_b1000','PUT','Seq 40M'),
        ('write_random_40M_t16_b1000','PUT','Rand 40M'),
    ]
    lbl2 = [t[2] for t in tests2]
    paired_bars(a2, lbl2,
                [val(df,'tidesdb',t[0],t[1],'space_amp') for t in tests2],
                [val(df,'rocksdb',t[0],t[1],'space_amp') for t in tests2],
                'Space Amplification', 'Space Amplification (lower = better)', decimal=True)
    fig.tight_layout(rect=[0,0,1,.93])
    save(fig, '11_space_efficiency.png')


# ═══════════════════════════════════════════════
# Plot 12: Resource Usage (4-panel)
# ═══════════════════════════════════════════════
def plot_resources(df):
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle('Resource Usage Comparison')
    tests = [
        ('write_seq_10M_t8_b1000','PUT','Seq 10M'),
        ('write_random_10M_t8_b1000','PUT','Rand 10M'),
        ('read_random_10M_t8','GET','Read 10M'),
        ('write_seq_40M_t16_b1000','PUT','Seq 40M'),
        ('write_random_40M_t16_b1000','PUT','Rand 40M'),
        ('read_random_40M_t16','GET','Read 40M'),
    ]
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
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(18, 6))
    fig.suptitle('Tail Latency: Average vs p99 (us)')
    for ax, tests, title in [
        (a1, [('write_seq_10M_t8_b1000','PUT','Seq'),
               ('write_random_10M_t8_b1000','PUT','Random'),
               ('write_zipfian_5M_t8_b1000','PUT','Zipfian'),
               ('write_large_values_1M_k256_v4096_t8_b1000','PUT','LargeVal')],
         'Standard Scale'),
        (a2, [('write_seq_40M_t16_b1000','PUT','Seq'),
               ('write_random_40M_t16_b1000','PUT','Random'),
               ('write_zipfian_20M_t16_b1000','PUT','Zipfian'),
               ('write_large_values_4M_k256_v4096_t16_b1000','PUT','LargeVal')],
         'Large Scale'),
    ]:
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
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(18, 6))
    fig.suptitle('Wall-Clock Duration (sec) — Lower is Better')
    for ax, tests, title in [
        (a1, [('write_seq_10M_t8_b1000','PUT','Seq Write\n10M'),
               ('write_random_10M_t8_b1000','PUT','Rand Write\n10M'),
               ('read_random_10M_t8','GET','Read\n10M'),
               ('write_small_values_50M_k16_v64_t8_b1000','PUT','Small\n50M'),
               ('write_large_values_1M_k256_v4096_t8_b1000','PUT','Large\n1M')],
         'Standard Scale'),
        (a2, [('write_seq_40M_t16_b1000','PUT','Seq Write\n40M'),
               ('write_random_40M_t16_b1000','PUT','Rand Write\n40M'),
               ('read_random_40M_t16','GET','Read\n40M'),
               ('write_small_values_200M_k16_v64_t16_b1000','PUT','Small\n200M'),
               ('write_large_values_4M_k256_v4096_t16_b1000','PUT','Large\n4M')],
         'Large Scale'),
    ]:
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
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(18, 6))
    fig.suptitle('Latency Variability (CV%) — Lower is More Consistent')
    for ax, tests, title in [
        (a1, [('write_seq_10M_t8_b1000','PUT','Seq Write'),
               ('write_random_10M_t8_b1000','PUT','Rand Write'),
               ('write_zipfian_5M_t8_b1000','PUT','Zipf Write'),
               ('write_large_values_1M_k256_v4096_t8_b1000','PUT','Large Val')],
         'Write Variability'),
        (a2, [('read_random_10M_t8','GET','Rand Read'),
               ('seek_random_5M_t8','SEEK','Rand Seek'),
               ('seek_seq_5M_t8','SEEK','Seq Seek'),
               ('range_random_100_1M_t8','RANGE','Range 100')],
         'Read/Seek Variability'),
    ]:
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
