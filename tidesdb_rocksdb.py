#!/usr/bin/env python3
"""
TidesDB vs RocksDB Benchmark Visualization

This script generates publication-quality benchmark visualizations, featuring:
- Clean, minimalist design with serif fonts
- High information density with proper statistical annotations
- Log-scale axes where appropriate for latency data
- Grouped bar charts with error indicators
- Latency percentile analysis (P50, P95, P99)
- Resource utilization comparisons
- Write/Read/Space amplification analysis

Usage:
    python3 tidesdb_rocksdb.py <csv_file> [output_dir]

CSV Format Expected (from benchtool):
    engine,operation,ops_per_sec,duration_sec,avg_latency_us,stddev_us,cv_percent,
    p50_us,p95_us,p99_us,min_us,max_us,peak_rss_mb,peak_vms_mb,disk_read_mb,
    disk_write_mb,cpu_user_sec,cpu_sys_sec,cpu_percent,db_size_mb,
    write_amp,read_amp,space_amp
"""

import sys
import os
import argparse
from datetime import datetime
from typing import Optional, Tuple, List, Dict
import warnings

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter, LogLocator
from matplotlib.lines import Line2D
import matplotlib.gridspec as gridspec

warnings.filterwarnings('ignore', category=UserWarning)

COLORS = {
    'tidesdb': '#2E86AB',
    'rocksdb': '#A23B72',
    'tidesdb_light': '#7FB3D3',
    'rocksdb_light': '#D4789C',
    'tidesdb_dark': '#1A5276',
    'rocksdb_dark': '#7B2D56',
    'grid': '#CCCCCC',
    'text': '#333333',
    'background': '#FFFFFF',
    'annotation': '#666666',
}


def setup_style():
    plt.rcParams.update({
        'font.family': 'serif',
        'font.serif': ['Times New Roman', 'DejaVu Serif', 'Liberation Serif', 'serif'],
        'font.size': 11,
        'axes.titlesize': 13,
        'axes.labelsize': 12,
        'xtick.labelsize': 10,
        'ytick.labelsize': 10,
        'legend.fontsize': 10,
        'figure.titlesize': 14,
        'axes.spines.top': False,
        'axes.spines.right': False,
        'axes.linewidth': 1.0,
        'axes.grid': True,
        'axes.axisbelow': True,
        'grid.alpha': 0.4,
        'grid.linestyle': '-',
        'grid.linewidth': 0.5,
        'grid.color': COLORS['grid'],
        'figure.facecolor': COLORS['background'],
        'figure.dpi': 150,
        'savefig.dpi': 300,
        'savefig.bbox': 'tight',
        'axes.facecolor': COLORS['background'],
        'axes.edgecolor': COLORS['text'],
        'axes.labelcolor': COLORS['text'],
        'text.color': COLORS['text'],
        'legend.frameon': True,
        'legend.framealpha': 0.95,
        'legend.edgecolor': COLORS['grid'],
        'lines.linewidth': 2.0,
        'lines.markersize': 7,
    })


def format_ops(x, pos=None):
    """Format operations per second."""
    if x >= 1e6:
        return f'{x/1e6:.1f}M'
    elif x >= 1e3:
        return f'{x/1e3:.0f}K'
    return f'{x:.0f}'


def load_benchmark_data(csv_path):
    """Load and preprocess benchmark CSV data."""
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    if 'engine' in df.columns:
        df['engine'] = df['engine'].str.lower().str.strip()
    if 'operation' in df.columns:
        df['operation'] = df['operation'].str.upper().str.strip()
    return df


def calculate_speedup(tidesdb_val, rocksdb_val):
    """Calculate speedup ratio and format string."""
    if rocksdb_val > 0 and tidesdb_val > 0:
        ratio = tidesdb_val / rocksdb_val
        if ratio >= 1:
            return ratio, f'{ratio:.2f}x faster'
        else:
            return ratio, f'{1/ratio:.2f}x slower'
    return 1.0, 'N/A'


def plot_throughput_comparison(df, output_dir):
    """Generate throughput comparison bar chart."""
    fig, ax = plt.subplots(figsize=(14, 7))
    
    operations = df['operation'].unique()
    x = np.arange(len(operations))
    width = 0.35
    
    tidesdb_ops = []
    rocksdb_ops = []
    
    for op in operations:
        op_data = df[df['operation'] == op]
        t_val = op_data[op_data['engine'] == 'tidesdb']['ops_per_sec'].mean()
        r_val = op_data[op_data['engine'] == 'rocksdb']['ops_per_sec'].mean()
        tidesdb_ops.append(t_val if not np.isnan(t_val) else 0)
        rocksdb_ops.append(r_val if not np.isnan(r_val) else 0)
    
    bars1 = ax.bar(x - width/2, tidesdb_ops, width, label='TidesDB',
                   color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'], linewidth=1.5)
    bars2 = ax.bar(x + width/2, rocksdb_ops, width, label='RocksDB',
                   color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'], linewidth=1.5)
    
    # Add speedup annotations
    for i, (t_val, r_val) in enumerate(zip(tidesdb_ops, rocksdb_ops)):
        if t_val > 0 and r_val > 0:
            ratio, label = calculate_speedup(t_val, r_val)
            max_val = max(t_val, r_val)
            color = COLORS['tidesdb_dark'] if ratio >= 1 else COLORS['rocksdb_dark']
            ax.annotate(label, xy=(i, max_val * 1.08), ha='center', va='bottom',
                       fontsize=8, color=color, fontweight='bold')
    
    ax.set_ylabel('Throughput (ops/sec)', fontweight='bold')
    ax.set_xlabel('Operation Type', fontweight='bold')
    ax.set_title('TidesDB vs RocksDB: Throughput Comparison\nHigher is Better',
                 fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(operations, fontweight='medium')
    ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
    ax.legend(loc='upper right')
    ax.set_ylim(bottom=0)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig01_throughput_comparison.png'), dpi=300)
    plt.savefig(os.path.join(output_dir, 'fig01_throughput_comparison.pdf'))
    plt.close()
    print("  [OK] fig01_throughput_comparison.png/pdf")


def plot_latency_percentiles(df, output_dir):
    """Generate latency percentile comparison (P50, P95, P99)."""
    operations = df['operation'].unique()
    n_ops = len(operations)
    
    if n_ops == 0:
        print("  [SKIP] latency percentiles: no data")
        return
    
    n_cols = min(3, n_ops)
    n_rows = (n_ops + n_cols - 1) // n_cols
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(5*n_cols, 4.5*n_rows))
    if n_ops == 1:
        axes = np.array([axes])
    axes = axes.flatten() if n_ops > 1 else [axes]
    
    fig.suptitle('Latency Percentile Analysis (Log Scale)\nLower is Better',
                 fontweight='bold', y=1.02)
    
    percentiles = ['p50_us', 'p95_us', 'p99_us']
    percentile_labels = ['P50', 'P95', 'P99']
    
    for idx, op in enumerate(operations):
        ax = axes[idx]
        op_data = df[df['operation'] == op]
        
        x = np.arange(len(percentiles))
        width = 0.35
        
        tidesdb_data = op_data[op_data['engine'] == 'tidesdb']
        rocksdb_data = op_data[op_data['engine'] == 'rocksdb']
        
        tidesdb_vals = [max(tidesdb_data[p].mean() if p in tidesdb_data.columns else 0.1, 0.1)
                       for p in percentiles]
        rocksdb_vals = [max(rocksdb_data[p].mean() if p in rocksdb_data.columns else 0.1, 0.1)
                       for p in percentiles]
        
        ax.bar(x - width/2, tidesdb_vals, width, label='TidesDB',
               color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'])
        ax.bar(x + width/2, rocksdb_vals, width, label='RocksDB',
               color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'])
        
        ax.set_ylabel('Latency (us)')
        ax.set_title(f'{op}', fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(percentile_labels)
        ax.set_yscale('log')
        ax.legend(loc='upper left', fontsize=8)
    
    for idx in range(n_ops, len(axes)):
        axes[idx].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig02_latency_percentiles.png'), dpi=300)
    plt.savefig(os.path.join(output_dir, 'fig02_latency_percentiles.pdf'))
    plt.close()
    print("  [OK] fig02_latency_percentiles.png/pdf")


def plot_latency_variability(df, output_dir):
    """Generate latency variability (CV%) comparison."""
    if 'cv_percent' not in df.columns:
        print("  [SKIP] latency variability: cv_percent not found")
        return
    
    operations = df['operation'].unique()
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(operations))
    width = 0.35
    
    tidesdb_cv = [df[(df['operation'] == op) & (df['engine'] == 'tidesdb')]['cv_percent'].mean()
                  for op in operations]
    rocksdb_cv = [df[(df['operation'] == op) & (df['engine'] == 'rocksdb')]['cv_percent'].mean()
                  for op in operations]
    
    tidesdb_cv = [v if not np.isnan(v) else 0 for v in tidesdb_cv]
    rocksdb_cv = [v if not np.isnan(v) else 0 for v in rocksdb_cv]
    
    ax.bar(x - width/2, tidesdb_cv, width, label='TidesDB',
           color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'])
    ax.bar(x + width/2, rocksdb_cv, width, label='RocksDB',
           color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'])
    
    ax.set_ylabel('Coefficient of Variation (%)', fontweight='bold')
    ax.set_xlabel('Operation Type', fontweight='bold')
    ax.set_title('Latency Variability (CV%): Lower is More Consistent', fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(operations)
    ax.legend(loc='upper right')
    ax.set_ylim(bottom=0)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig03_latency_variability.png'), dpi=300)
    plt.savefig(os.path.join(output_dir, 'fig03_latency_variability.pdf'))
    plt.close()
    print("  [OK] fig03_latency_variability.png/pdf")


def plot_resource_utilization(df, output_dir):
    """Generate resource utilization comparison."""
    fig = plt.figure(figsize=(16, 10))
    gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.3, wspace=0.25)
    fig.suptitle('Resource Utilization Comparison', fontweight='bold', y=0.98)
    
    operations = df['operation'].unique()
    x = np.arange(len(operations))
    width = 0.35
    
    metrics = [
        ('peak_rss_mb', 'Peak RSS (MB)', 'Memory Usage'),
        ('cpu_percent', 'CPU (%)', 'CPU Utilization'),
        ('disk_write_mb', 'Disk Writes (MB)', 'Disk Write I/O'),
        ('db_size_mb', 'DB Size (MB)', 'Storage Footprint'),
    ]
    
    for i, (col, ylabel, title) in enumerate(metrics):
        ax = fig.add_subplot(gs[i // 2, i % 2])
        
        if col not in df.columns:
            ax.set_visible(False)
            continue
        
        tidesdb_vals = [df[(df['operation'] == op) & (df['engine'] == 'tidesdb')][col].mean()
                        for op in operations]
        rocksdb_vals = [df[(df['operation'] == op) & (df['engine'] == 'rocksdb')][col].mean()
                        for op in operations]
        
        tidesdb_vals = [v if not np.isnan(v) else 0 for v in tidesdb_vals]
        rocksdb_vals = [v if not np.isnan(v) else 0 for v in rocksdb_vals]
        
        ax.bar(x - width/2, tidesdb_vals, width, label='TidesDB',
               color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'])
        ax.bar(x + width/2, rocksdb_vals, width, label='RocksDB',
               color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'])
        
        ax.set_ylabel(ylabel, fontweight='bold')
        ax.set_title(title, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(operations, rotation=45, ha='right')
        ax.legend(loc='upper right', fontsize=9)
    
    plt.savefig(os.path.join(output_dir, 'fig04_resource_utilization.png'), dpi=300)
    plt.savefig(os.path.join(output_dir, 'fig04_resource_utilization.pdf'))
    plt.close()
    print("  [OK] fig04_resource_utilization.png/pdf")


def plot_amplification_factors(df, output_dir):
    """Generate amplification factors comparison."""
    amp_cols = ['write_amp', 'read_amp', 'space_amp']
    available = [c for c in amp_cols if c in df.columns]
    
    if not available:
        print("  [SKIP] amplification factors: no columns found")
        return
    
    df_amp = df[df[available].notna().any(axis=1)]
    df_amp = df_amp[df_amp[available].sum(axis=1) > 0]
    
    if df_amp.empty:
        print("  [SKIP] amplification factors: no valid data")
        return
    
    operations = df_amp['operation'].unique()
    
    fig, axes = plt.subplots(1, len(available), figsize=(5*len(available), 5))
    if len(available) == 1:
        axes = [axes]
    
    fig.suptitle('Amplification Factors (Lower is Better, 1.0x = No Amplification)',
                 fontweight='bold', y=1.02)
    
    labels = {'write_amp': 'Write Amp', 'read_amp': 'Read Amp', 'space_amp': 'Space Amp'}
    
    for ax, col in zip(axes, available):
        x = np.arange(len(operations))
        width = 0.35
        
        tidesdb_vals = [df_amp[(df_amp['operation'] == op) & (df_amp['engine'] == 'tidesdb')][col].mean()
                        for op in operations]
        rocksdb_vals = [df_amp[(df_amp['operation'] == op) & (df_amp['engine'] == 'rocksdb')][col].mean()
                        for op in operations]
        
        tidesdb_vals = [v if not np.isnan(v) else 0 for v in tidesdb_vals]
        rocksdb_vals = [v if not np.isnan(v) else 0 for v in rocksdb_vals]
        
        ax.bar(x - width/2, tidesdb_vals, width, label='TidesDB',
               color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'])
        ax.bar(x + width/2, rocksdb_vals, width, label='RocksDB',
               color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'])
        
        ax.axhline(y=1.0, color='gray', linestyle='--', linewidth=1.5, alpha=0.7)
        ax.set_ylabel(f'{labels[col]} (x)', fontweight='bold')
        ax.set_title(labels[col], fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(operations, rotation=45, ha='right')
        ax.legend(loc='upper right', fontsize=8)
        ax.set_ylim(bottom=0)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig05_amplification_factors.png'), dpi=300)
    plt.savefig(os.path.join(output_dir, 'fig05_amplification_factors.pdf'))
    plt.close()
    print("  [OK] fig05_amplification_factors.png/pdf")


def plot_performance_heatmap(df, output_dir):
    """Generate performance summary heatmap."""
    operations = df['operation'].unique()
    
    if len(operations) == 0:
        print("  [SKIP] performance heatmap: no data")
        return
    
    speedups = []
    for op in operations:
        op_data = df[df['operation'] == op]
        t_ops = op_data[op_data['engine'] == 'tidesdb']['ops_per_sec'].mean()
        r_ops = op_data[op_data['engine'] == 'rocksdb']['ops_per_sec'].mean()
        speedups.append(t_ops / r_ops if r_ops > 0 and t_ops > 0 else 1.0)
    
    sorted_idx = np.argsort(speedups)
    operations = [operations[i] for i in sorted_idx]
    speedups = [speedups[i] for i in sorted_idx]
    
    fig, ax = plt.subplots(figsize=(12, max(6, len(operations) * 0.5)))
    
    colors = [COLORS['tidesdb'] if s >= 1 else COLORS['rocksdb'] for s in speedups]
    edge_colors = [COLORS['tidesdb_dark'] if s >= 1 else COLORS['rocksdb_dark'] for s in speedups]
    
    bars = ax.barh(range(len(operations)), speedups, color=colors, edgecolor=edge_colors, linewidth=1.5)
    ax.axvline(x=1.0, color='gray', linestyle='--', linewidth=2, alpha=0.8)
    
    for i, (bar, val) in enumerate(zip(bars, speedups)):
        label = f'{val:.2f}x'
        color = COLORS['tidesdb_dark'] if val >= 1 else COLORS['rocksdb_dark']
        offset = 5 if val >= 1 else -5
        ha = 'left' if val >= 1 else 'right'
        ax.annotate(label, xy=(val, i), xytext=(offset, 0), textcoords='offset points',
                   ha=ha, va='center', fontsize=9, fontweight='bold', color=color)
    
    ax.set_yticks(range(len(operations)))
    ax.set_yticklabels(operations, fontweight='medium')
    ax.set_xlabel('Speedup (TidesDB / RocksDB)', fontweight='bold')
    ax.set_title('Performance Summary\n(>1.0 = TidesDB Faster, <1.0 = RocksDB Faster)',
                 fontweight='bold', pad=15)
    
    legend_elements = [
        mpatches.Patch(facecolor=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'],
                      label='TidesDB Faster', linewidth=1.5),
        mpatches.Patch(facecolor=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'],
                      label='RocksDB Faster', linewidth=1.5),
    ]
    ax.legend(handles=legend_elements, loc='lower right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig06_performance_heatmap.png'), dpi=300)
    plt.savefig(os.path.join(output_dir, 'fig06_performance_heatmap.pdf'))
    plt.close()
    print("  [OK] fig06_performance_heatmap.png/pdf")


def plot_tail_latency(df, output_dir):
    """Generate tail latency analysis (P99/P50 ratio)."""
    if 'p99_us' not in df.columns or 'p50_us' not in df.columns:
        print("  [SKIP] tail latency: missing percentile columns")
        return
    
    operations = df['operation'].unique()
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(operations))
    width = 0.35
    
    tidesdb_ratio = []
    rocksdb_ratio = []
    
    for op in operations:
        op_data = df[df['operation'] == op]
        t_data = op_data[op_data['engine'] == 'tidesdb']
        r_data = op_data[op_data['engine'] == 'rocksdb']
        
        t_p99 = t_data['p99_us'].mean() if not t_data.empty else 0
        t_p50 = t_data['p50_us'].mean() if not t_data.empty else 1
        r_p99 = r_data['p99_us'].mean() if not r_data.empty else 0
        r_p50 = r_data['p50_us'].mean() if not r_data.empty else 1
        
        tidesdb_ratio.append(t_p99 / max(t_p50, 0.1) if t_p99 > 0 else 0)
        rocksdb_ratio.append(r_p99 / max(r_p50, 0.1) if r_p99 > 0 else 0)
    
    ax.bar(x - width/2, tidesdb_ratio, width, label='TidesDB',
           color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'])
    ax.bar(x + width/2, rocksdb_ratio, width, label='RocksDB',
           color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'])
    
    ax.axhline(y=2.0, color='orange', linestyle='--', linewidth=1.5, alpha=0.7)
    ax.axhline(y=10.0, color='red', linestyle='--', linewidth=1.5, alpha=0.7)
    
    ax.set_ylabel('P99/P50 Ratio', fontweight='bold')
    ax.set_xlabel('Operation Type', fontweight='bold')
    ax.set_title('Tail Latency Analysis (P99/P50)\nLower = More Predictable', fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(operations)
    ax.legend(loc='upper right')
    ax.set_ylim(bottom=0)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig07_tail_latency.png'), dpi=300)
    plt.savefig(os.path.join(output_dir, 'fig07_tail_latency.pdf'))
    plt.close()
    print("  [OK] fig07_tail_latency.png/pdf")


def plot_efficiency(df, output_dir):
    """Generate efficiency analysis (ops per resource)."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle('Efficiency Analysis: Ops per Resource Unit (Higher is Better)',
                 fontweight='bold', y=1.02)
    
    operations = df['operation'].unique()
    x = np.arange(len(operations))
    width = 0.35
    
    # Ops per MB memory
    ax = axes[0]
    if 'peak_rss_mb' in df.columns:
        tidesdb_eff = []
        rocksdb_eff = []
        for op in operations:
            op_data = df[df['operation'] == op]
            t_data = op_data[op_data['engine'] == 'tidesdb']
            r_data = op_data[op_data['engine'] == 'rocksdb']
            
            t_ops = t_data['ops_per_sec'].mean() if not t_data.empty else 0
            t_mem = t_data['peak_rss_mb'].mean() if not t_data.empty else 1
            r_ops = r_data['ops_per_sec'].mean() if not r_data.empty else 0
            r_mem = r_data['peak_rss_mb'].mean() if not r_data.empty else 1
            
            tidesdb_eff.append(t_ops / max(t_mem, 1) if t_ops > 0 else 0)
            rocksdb_eff.append(r_ops / max(r_mem, 1) if r_ops > 0 else 0)
        
        ax.bar(x - width/2, tidesdb_eff, width, label='TidesDB',
               color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'])
        ax.bar(x + width/2, rocksdb_eff, width, label='RocksDB',
               color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'])
        ax.set_ylabel('Ops/sec per MB', fontweight='bold')
        ax.set_title('Memory Efficiency', fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(operations, rotation=45, ha='right')
        ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
        ax.legend(loc='upper right')
    
    # Ops per CPU%
    ax = axes[1]
    if 'cpu_percent' in df.columns:
        tidesdb_eff = []
        rocksdb_eff = []
        for op in operations:
            op_data = df[df['operation'] == op]
            t_data = op_data[op_data['engine'] == 'tidesdb']
            r_data = op_data[op_data['engine'] == 'rocksdb']
            
            t_ops = t_data['ops_per_sec'].mean() if not t_data.empty else 0
            t_cpu = t_data['cpu_percent'].mean() if not t_data.empty else 1
            r_ops = r_data['ops_per_sec'].mean() if not r_data.empty else 0
            r_cpu = r_data['cpu_percent'].mean() if not r_data.empty else 1
            
            tidesdb_eff.append(t_ops / max(t_cpu, 1) if t_ops > 0 else 0)
            rocksdb_eff.append(r_ops / max(r_cpu, 1) if r_ops > 0 else 0)
        
        ax.bar(x - width/2, tidesdb_eff, width, label='TidesDB',
               color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'])
        ax.bar(x + width/2, rocksdb_eff, width, label='RocksDB',
               color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'])
        ax.set_ylabel('Ops/sec per CPU%', fontweight='bold')
        ax.set_title('CPU Efficiency', fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(operations, rotation=45, ha='right')
        ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
        ax.legend(loc='upper right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig08_efficiency.png'), dpi=300)
    plt.savefig(os.path.join(output_dir, 'fig08_efficiency.pdf'))
    plt.close()
    print("  [OK] fig08_efficiency.png/pdf")


def plot_latency_distribution(df, output_dir):
    """Generate latency distribution with min/avg/max."""
    operations = df['operation'].unique()
    
    if len(operations) == 0:
        print("  [SKIP] latency distribution: no data")
        return
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    x = np.arange(len(operations))
    width = 0.35
    
    for i, engine in enumerate(['tidesdb', 'rocksdb']):
        offset = -width/2 if engine == 'tidesdb' else width/2
        color = COLORS[engine]
        dark_color = COLORS[f'{engine}_dark']
        
        avgs, mins, maxs = [], [], []
        
        for op in operations:
            op_data = df[(df['operation'] == op) & (df['engine'] == engine)]
            if not op_data.empty:
                avgs.append(op_data['avg_latency_us'].mean())
                mins.append(op_data['min_us'].mean() if 'min_us' in op_data.columns else 0)
                maxs.append(op_data['max_us'].mean() if 'max_us' in op_data.columns else 0)
            else:
                avgs.append(0)
                mins.append(0)
                maxs.append(0)
        
        avgs = [max(v, 0.1) if not np.isnan(v) else 0.1 for v in avgs]
        
        bars = ax.bar(x + offset, avgs, width, label=f'{engine.capitalize()} (avg)',
                     color=color, edgecolor=dark_color, linewidth=1)
        
        yerr_lower = [max(0, a - m) for a, m in zip(avgs, mins)]
        yerr_upper = [max(0, M - a) for a, M in zip(avgs, maxs)]
        ax.errorbar(x + offset, avgs, yerr=[yerr_lower, yerr_upper],
                   fmt='none', color=dark_color, capsize=3, capthick=1.5)
    
    ax.set_ylabel('Latency (us)', fontweight='bold')
    ax.set_xlabel('Operation Type', fontweight='bold')
    ax.set_title('Latency Distribution: Average with Min/Max Range', fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(operations)
    ax.set_yscale('log')
    ax.legend(loc='upper right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig09_latency_distribution.png'), dpi=300)
    plt.savefig(os.path.join(output_dir, 'fig09_latency_distribution.pdf'))
    plt.close()
    print("  [OK] fig09_latency_distribution.png/pdf")


def generate_summary_table(df, output_dir):
    """Generate summary table in CSV and LaTeX."""
    summary = []
    
    for op in df['operation'].unique():
        op_data = df[df['operation'] == op]
        t_data = op_data[op_data['engine'] == 'tidesdb']
        r_data = op_data[op_data['engine'] == 'rocksdb']
        
        if t_data.empty or r_data.empty:
            continue
        
        t_ops = t_data['ops_per_sec'].mean()
        r_ops = r_data['ops_per_sec'].mean()
        speedup = t_ops / r_ops if r_ops > 0 else 0
        
        t_p99 = t_data['p99_us'].mean() if 'p99_us' in t_data.columns else 0
        r_p99 = r_data['p99_us'].mean() if 'p99_us' in r_data.columns else 0
        
        summary.append({
            'Operation': op,
            'TidesDB (ops/s)': f'{t_ops:,.0f}',
            'RocksDB (ops/s)': f'{r_ops:,.0f}',
            'Speedup': f'{speedup:.2f}x',
            'TidesDB P99 (us)': f'{t_p99:.1f}',
            'RocksDB P99 (us)': f'{r_p99:.1f}',
        })
    
    if not summary:
        print("  [SKIP] summary table: no data")
        return
    
    summary_df = pd.DataFrame(summary)
    summary_df.to_csv(os.path.join(output_dir, 'summary_table.csv'), index=False)
    print("  [OK] summary_table.csv")
    
    latex = summary_df.to_latex(index=False, escape=False)
    with open(os.path.join(output_dir, 'summary_table.tex'), 'w') as f:
        f.write(latex)
    print("  [OK] summary_table.tex")


def main():
    parser = argparse.ArgumentParser(
        description='TidesDB vs RocksDB Benchmark Visualization',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
    python3 tidesdb_rocksdb.py tidesdb_rocksdb.csv
    python3 tidesdb_rocksdb.py tidesdb_rocksdb.csv ./graphs
    python3 tidesdb_rocksdb.py results.csv output/ --no-pdf
        '''
    )
    parser.add_argument('csv_file', help='Path to benchmark CSV file')
    parser.add_argument('output_dir', nargs='?', default=None,
                       help='Output directory (default: same as CSV file)')
    parser.add_argument('--no-pdf', action='store_true',
                       help='Skip PDF generation')
    
    args = parser.parse_args()
    
    csv_path = args.csv_file
    output_dir = args.output_dir or os.path.dirname(csv_path) or '.'
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found: {csv_path}")
        sys.exit(1)
    
    os.makedirs(output_dir, exist_ok=True)
    
    print()
    print("=" * 65)
    print("  TidesDB vs RocksDB Benchmark Visualization")
    print("=" * 65)
    print(f"  Input:  {csv_path}")
    print(f"  Output: {output_dir}")
    print("=" * 65)
    print()
    
    setup_style()
    
    print("Loading data...")
    df = load_benchmark_data(csv_path)
    print(f"  Loaded {len(df)} records, {df['operation'].nunique()} operations")
    print(f"  Engines: {', '.join(df['engine'].unique())}")
    print()
    
    print("Generating figures...")
    plot_throughput_comparison(df, output_dir)
    plot_latency_percentiles(df, output_dir)
    plot_latency_variability(df, output_dir)
    plot_resource_utilization(df, output_dir)
    plot_amplification_factors(df, output_dir)
    plot_performance_heatmap(df, output_dir)
    plot_tail_latency(df, output_dir)
    plot_efficiency(df, output_dir)
    plot_latency_distribution(df, output_dir)
    
    print()
    print("Generating summary tables...")
    generate_summary_table(df, output_dir)
    
    print()
    print("=" * 65)
    print("  Visualization complete!")
    print(f"  All outputs saved to: {output_dir}")
    print("=" * 65)
    print()


if __name__ == '__main__':
    main()
