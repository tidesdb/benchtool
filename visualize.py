#!/usr/bin/env python3
"""
TidesDB vs RocksDB Benchmark Visualization
Generates graphs 

Usage:
    python3 visualize.py <csv_file> [output_dir]
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.ticker import FuncFormatter
from datetime import datetime

COLORS = {
    'tidesdb': '#A8D5BA',      # Soft sage green
    'rocksdb': '#F4B8C5',      # Soft rose pink
    'tidesdb_dark': '#7CB899', # Darker sage for emphasis
    'rocksdb_dark': '#E8919F', # Darker rose for emphasis
    'grid': '#E8E8E8',         # Light gray grid
    'text': '#4A4A4A',         # Dark gray text
    'background': '#FAFAFA',   # Off-white background
}

# Pastel palette for multi-bar charts
PASTEL_PALETTE = [
    '#A8D5BA',  # Sage green
    '#F4B8C5',  # Rose pink
    '#B8D4E8',  # Sky blue
    '#F5D5A8',  # Peach
    '#D4B8E8',  # Lavender
    '#A8E8D5',  # Mint
    '#E8D4B8',  # Sand
    '#C5F4B8',  # Light lime
]

def setup_style():
    """Configure matplotlib for plots."""
    plt.rcParams.update({
        'font.family': 'serif',
        'font.serif': ['Times New Roman', 'DejaVu Serif', 'serif'],
        'font.size': 10,
        'axes.titlesize': 12,
        'axes.labelsize': 11,
        'xtick.labelsize': 9,
        'ytick.labelsize': 9,
        'legend.fontsize': 9,
        'figure.titlesize': 14,
        'axes.spines.top': False,
        'axes.spines.right': False,
        'axes.grid': True,
        'grid.alpha': 0.3,
        'grid.linestyle': '--',
        'figure.facecolor': COLORS['background'],
        'axes.facecolor': 'white',
        'axes.edgecolor': COLORS['text'],
        'text.color': COLORS['text'],
        'axes.labelcolor': COLORS['text'],
        'xtick.color': COLORS['text'],
        'ytick.color': COLORS['text'],
    })

def format_ops(x, pos):
    """Format large numbers for readability."""
    if x >= 1e6:
        return f'{x/1e6:.1f}M'
    elif x >= 1e3:
        return f'{x/1e3:.0f}K'
    return f'{x:.0f}'

def load_data(csv_path):
    """Load and preprocess benchmark data."""
    df = pd.read_csv(csv_path)
    # Clean column names
    df.columns = df.columns.str.strip()
    return df

def plot_throughput_comparison(df, output_dir):
    """
    Throughput comparison bar chart for main operations.
    """
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle('Throughput Comparison: TidesDB vs RocksDB', fontweight='bold', y=0.98)
    
    # Define test categories
    categories = [
        ('Write Operations', ['seq_write_10M', 'random_write_10M', 'zipfian_write_5M']),
        ('Read Operations', ['random_read_10M', 'seq_read_10M']),
        ('Mixed & Delete', ['mixed_random_5M', 'mixed_zipfian_5M', 'random_delete_5M']),
        ('Seek & Range', ['random_seek_5M', 'seq_seek_5M', 'range_100_1M']),
    ]
    
    for ax, (title, tests) in zip(axes.flat, categories):
        test_data = df[df['test_name'].isin(tests)]
        if test_data.empty:
            ax.set_visible(False)
            continue
            
        # Pivot for grouped bar chart
        pivot = test_data.pivot_table(
            index='test_name', 
            columns='engine', 
            values='ops_per_sec',
            aggfunc='mean'
        ).reindex(tests)
        
        x = np.arange(len(pivot.index))
        width = 0.35
        
        bars1 = ax.bar(x - width/2, pivot.get('tidesdb', [0]*len(x)), width, 
                       label='TidesDB', color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'], linewidth=1)
        bars2 = ax.bar(x + width/2, pivot.get('rocksdb', [0]*len(x)), width,
                       label='RocksDB', color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'], linewidth=1)
        
        ax.set_ylabel('Operations/sec')
        ax.set_title(title, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels([t.replace('_', '\n') for t in pivot.index], rotation=0)
        ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
        ax.legend(loc='upper right')
        
        # Add value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.annotate(format_ops(height, None),
                               xy=(bar.get_x() + bar.get_width()/2, height),
                               xytext=(0, 3), textcoords="offset points",
                               ha='center', va='bottom', fontsize=7)
    
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(os.path.join(output_dir, 'fig1_throughput_comparison.png'), dpi=300, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'fig1_throughput_comparison.pdf'), bbox_inches='tight')
    plt.close()
    print("  Generated: fig1_throughput_comparison.png/pdf")

def plot_latency_distribution(df, output_dir):
    """
    Latency percentile comparison (p50, p95, p99).
    """
    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    fig.suptitle('Latency Distribution: TidesDB vs RocksDB', fontweight='bold', y=1.02)
    
    # Select representative tests
    tests = ['random_write_10M', 'random_read_10M', 'mixed_random_5M']
    test_labels = ['Random Write', 'Random Read', 'Mixed Workload']
    
    for ax, test, label in zip(axes, tests, test_labels):
        test_data = df[df['test_name'] == test]
        if test_data.empty:
            ax.set_visible(False)
            continue
        
        percentiles = ['p50_us', 'p95_us', 'p99_us']
        percentile_labels = ['P50', 'P95', 'P99']
        
        x = np.arange(len(percentiles))
        width = 0.35
        
        tidesdb_data = test_data[test_data['engine'] == 'tidesdb']
        rocksdb_data = test_data[test_data['engine'] == 'rocksdb']
        
        tidesdb_vals = [tidesdb_data[p].mean() if not tidesdb_data.empty else 0 for p in percentiles]
        rocksdb_vals = [rocksdb_data[p].mean() if not rocksdb_data.empty else 0 for p in percentiles]
        
        ax.bar(x - width/2, tidesdb_vals, width, label='TidesDB', 
               color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'], linewidth=1)
        ax.bar(x + width/2, rocksdb_vals, width, label='RocksDB',
               color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'], linewidth=1)
        
        ax.set_ylabel('Latency (μs)')
        ax.set_title(label, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(percentile_labels)
        ax.legend(loc='upper left')
        ax.set_yscale('log')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig2_latency_distribution.png'), dpi=300, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'fig2_latency_distribution.pdf'), bbox_inches='tight')
    plt.close()
    print("  Generated: fig2_latency_distribution.png/pdf")

def plot_batch_size_impact(df, output_dir):
    """
    Impact of batch size on throughput.
    """
    batch_tests = df[df['test_name'].str.startswith('batch_')]
    if batch_tests.empty:
        print("  Skipping: No batch size data found")
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Extract batch size from test name
    batch_tests = batch_tests.copy()
    batch_tests['batch_extracted'] = batch_tests['test_name'].str.extract(r'batch_(\d+)').astype(int)
    
    for engine, color, marker in [('tidesdb', COLORS['tidesdb_dark'], 'o'), 
                                   ('rocksdb', COLORS['rocksdb_dark'], 's')]:
        engine_data = batch_tests[batch_tests['engine'] == engine].sort_values('batch_extracted')
        if not engine_data.empty:
            ax.plot(engine_data['batch_extracted'], engine_data['ops_per_sec'], 
                   marker=marker, markersize=8, linewidth=2, color=color,
                   label=engine.capitalize(), markerfacecolor=COLORS[engine])
    
    ax.set_xlabel('Batch Size')
    ax.set_ylabel('Operations/sec')
    ax.set_title('Impact of Batch Size on Write Throughput', fontweight='bold')
    ax.set_xscale('log')
    ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig3_batch_size_impact.png'), dpi=300, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'fig3_batch_size_impact.pdf'), bbox_inches='tight')
    plt.close()
    print("  Generated: fig3_batch_size_impact.png/pdf")

def plot_thread_scaling(df, output_dir):
    """
    Thread scaling analysis.
    """
    thread_tests = df[df['test_name'].str.startswith('threads_')]
    if thread_tests.empty:
        print("  Skipping: No thread scaling data found")
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Extract thread count from test name
    thread_tests = thread_tests.copy()
    thread_tests['thread_count'] = thread_tests['test_name'].str.extract(r'threads_(\d+)').astype(int)
    
    for engine, color, marker in [('tidesdb', COLORS['tidesdb_dark'], 'o'), 
                                   ('rocksdb', COLORS['rocksdb_dark'], 's')]:
        engine_data = thread_tests[thread_tests['engine'] == engine].sort_values('thread_count')
        if not engine_data.empty:
            ax.plot(engine_data['thread_count'], engine_data['ops_per_sec'], 
                   marker=marker, markersize=8, linewidth=2, color=color,
                   label=engine.capitalize(), markerfacecolor=COLORS[engine])
    
    ax.set_xlabel('Number of Threads')
    ax.set_ylabel('Operations/sec')
    ax.set_title('Thread Scaling: Write Throughput', fontweight='bold')
    ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_xticks([1, 4, 8, 16])
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig4_thread_scaling.png'), dpi=300, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'fig4_thread_scaling.pdf'), bbox_inches='tight')
    plt.close()
    print("  Generated: fig4_thread_scaling.png/pdf")

def plot_value_size_impact(df, output_dir):
    """
    Impact of value size on performance.
    """
    value_tests = df[df['test_name'].str.contains('value_write')]
    if value_tests.empty:
        print("  Skipping: No value size data found")
        return
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    
    # Order tests by value size
    test_order = ['small_value_write', 'medium_value_write', 'large_value_write']
    value_labels = ['64B', '1KB', '4KB']
    
    # Throughput
    ax = axes[0]
    pivot = value_tests.pivot_table(
        index='test_name', columns='engine', values='ops_per_sec', aggfunc='mean'
    ).reindex([t for t in test_order if t in value_tests['test_name'].values])
    
    if not pivot.empty:
        x = np.arange(len(pivot.index))
        width = 0.35
        
        ax.bar(x - width/2, pivot.get('tidesdb', [0]*len(x)), width, 
               label='TidesDB', color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'])
        ax.bar(x + width/2, pivot.get('rocksdb', [0]*len(x)), width,
               label='RocksDB', color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'])
        
        ax.set_ylabel('Operations/sec')
        ax.set_title('Throughput by Value Size', fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(value_labels[:len(x)])
        ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
        ax.legend()
    
    # Average latency
    ax = axes[1]
    pivot = value_tests.pivot_table(
        index='test_name', columns='engine', values='avg_latency_us', aggfunc='mean'
    ).reindex([t for t in test_order if t in value_tests['test_name'].values])
    
    if not pivot.empty:
        x = np.arange(len(pivot.index))
        
        ax.bar(x - width/2, pivot.get('tidesdb', [0]*len(x)), width, 
               label='TidesDB', color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'])
        ax.bar(x + width/2, pivot.get('rocksdb', [0]*len(x)), width,
               label='RocksDB', color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'])
        
        ax.set_ylabel('Average Latency (μs)')
        ax.set_title('Latency by Value Size', fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(value_labels[:len(x)])
        ax.legend()
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig5_value_size_impact.png'), dpi=300, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'fig5_value_size_impact.pdf'), bbox_inches='tight')
    plt.close()
    print("  Generated: fig5_value_size_impact.png/pdf")

def plot_resource_usage(df, output_dir):
    """
    Resource usage comparison (memory, disk I/O, CPU).
    """
    # Get unique tests with resource data
    tests = df['test_name'].unique()[:6]  # Limit to first 6 tests
    test_data = df[df['test_name'].isin(tests)]
    
    if test_data.empty:
        print("  Skipping: No resource data found")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle('Resource Usage Comparison', fontweight='bold', y=0.98)
    
    metrics = [
        ('peak_rss_mb', 'Peak RSS Memory (MB)', axes[0, 0]),
        ('disk_write_mb', 'Disk Writes (MB)', axes[0, 1]),
        ('cpu_percent', 'CPU Utilization (%)', axes[1, 0]),
        ('db_size_mb', 'Database Size (MB)', axes[1, 1]),
    ]
    
    for col, ylabel, ax in metrics:
        if col not in test_data.columns:
            ax.set_visible(False)
            continue
            
        pivot = test_data.pivot_table(
            index='test_name', columns='engine', values=col, aggfunc='mean'
        )
        
        if pivot.empty:
            ax.set_visible(False)
            continue
        
        x = np.arange(len(pivot.index))
        width = 0.35
        
        ax.bar(x - width/2, pivot.get('tidesdb', [0]*len(x)), width, 
               label='TidesDB', color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'])
        ax.bar(x + width/2, pivot.get('rocksdb', [0]*len(x)), width,
               label='RocksDB', color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'])
        
        ax.set_ylabel(ylabel)
        ax.set_xticks(x)
        ax.set_xticklabels([t.replace('_', '\n')[:15] for t in pivot.index], rotation=45, ha='right')
        ax.legend(loc='upper right')
    
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(os.path.join(output_dir, 'fig6_resource_usage.png'), dpi=300, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'fig6_resource_usage.pdf'), bbox_inches='tight')
    plt.close()
    print("  Generated: fig6_resource_usage.png/pdf")

def plot_amplification_factors(df, output_dir):
    """
    Write, Read, and Space amplification comparison.
    """
    # Get tests with amplification data
    amp_cols = ['write_amp', 'read_amp', 'space_amp']
    test_data = df[df[amp_cols].notna().any(axis=1)]
    
    if test_data.empty:
        print("  Skipping: No amplification data found")
        return
    
    # Select representative tests
    tests = ['random_write_10M', 'random_read_10M', 'mixed_random_5M']
    test_data = test_data[test_data['test_name'].isin(tests)]
    
    if test_data.empty:
        print("  Skipping: No amplification data for selected tests")
        return
    
    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    fig.suptitle('Amplification Factors', fontweight='bold', y=1.02)
    
    amp_labels = ['Write Amplification', 'Read Amplification', 'Space Amplification']
    
    for ax, col, label in zip(axes, amp_cols, amp_labels):
        pivot = test_data.pivot_table(
            index='test_name', columns='engine', values=col, aggfunc='mean'
        ).reindex([t for t in tests if t in test_data['test_name'].values])
        
        if pivot.empty or pivot.isna().all().all():
            ax.set_visible(False)
            continue
        
        x = np.arange(len(pivot.index))
        width = 0.35
        
        tidesdb_vals = pivot.get('tidesdb', pd.Series([0]*len(x))).fillna(0)
        rocksdb_vals = pivot.get('rocksdb', pd.Series([0]*len(x))).fillna(0)
        
        ax.bar(x - width/2, tidesdb_vals, width, 
               label='TidesDB', color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'])
        ax.bar(x + width/2, rocksdb_vals, width,
               label='RocksDB', color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'])
        
        ax.set_ylabel(f'{label} (×)')
        ax.set_title(label, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels([t.replace('_', '\n') for t in pivot.index], rotation=0)
        ax.legend(loc='upper right')
        ax.axhline(y=1, color='gray', linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig7_amplification_factors.png'), dpi=300, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'fig7_amplification_factors.pdf'), bbox_inches='tight')
    plt.close()
    print("  Generated: fig7_amplification_factors.png/pdf")

def plot_summary_heatmap(df, output_dir):
    """
    Summary heatmap showing relative performance (TidesDB vs RocksDB).
    """
    # Calculate speedup ratios
    pivot = df.pivot_table(
        index='test_name', columns='engine', values='ops_per_sec', aggfunc='mean'
    )
    
    if pivot.empty or 'tidesdb' not in pivot.columns or 'rocksdb' not in pivot.columns:
        print("  Skipping: Insufficient data for heatmap")
        return
    
    # Calculate speedup (TidesDB / RocksDB)
    speedup = (pivot['tidesdb'] / pivot['rocksdb']).dropna()
    
    if speedup.empty:
        print("  Skipping: No valid speedup data")
        return
    
    fig, ax = plt.subplots(figsize=(10, max(6, len(speedup) * 0.4)))
    
    # Sort by speedup
    speedup = speedup.sort_values()
    
    # Color based on speedup (green if TidesDB faster, red if slower)
    colors = [COLORS['tidesdb'] if s >= 1 else COLORS['rocksdb'] for s in speedup]
    
    bars = ax.barh(range(len(speedup)), speedup, color=colors, edgecolor='white', linewidth=0.5)
    
    ax.set_yticks(range(len(speedup)))
    ax.set_yticklabels([t.replace('_', ' ').title() for t in speedup.index])
    ax.set_xlabel('Speedup Ratio (TidesDB / RocksDB)')
    ax.set_title('Performance Comparison Summary\n(>1 = TidesDB faster, <1 = RocksDB faster)', 
                 fontweight='bold')
    ax.axvline(x=1, color='gray', linestyle='--', linewidth=2, alpha=0.7)
    
    # Add value labels
    for i, (bar, val) in enumerate(zip(bars, speedup)):
        ax.annotate(f'{val:.2f}×', 
                   xy=(val, i),
                   xytext=(5 if val >= 1 else -5, 0),
                   textcoords='offset points',
                   ha='left' if val >= 1 else 'right',
                   va='center', fontsize=8)
    
    # Add legend
    legend_elements = [
        mpatches.Patch(facecolor=COLORS['tidesdb'], label='TidesDB Faster'),
        mpatches.Patch(facecolor=COLORS['rocksdb'], label='RocksDB Faster'),
    ]
    ax.legend(handles=legend_elements, loc='lower right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig8_summary_heatmap.png'), dpi=300, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'fig8_summary_heatmap.pdf'), bbox_inches='tight')
    plt.close()
    print("  Generated: fig8_summary_heatmap.png/pdf")

def plot_latency_variability(df, output_dir):
    """
    Latency variability (CV%) comparison.
    """
    tests = ['random_write_10M', 'random_read_10M', 'mixed_random_5M', 'random_seek_5M']
    test_data = df[df['test_name'].isin(tests)]
    
    if test_data.empty or 'cv_percent' not in test_data.columns:
        print("  Skipping: No CV data found")
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    pivot = test_data.pivot_table(
        index='test_name', columns='engine', values='cv_percent', aggfunc='mean'
    ).reindex([t for t in tests if t in test_data['test_name'].values])
    
    if pivot.empty:
        print("  Skipping: No CV data for selected tests")
        return
    
    x = np.arange(len(pivot.index))
    width = 0.35
    
    ax.bar(x - width/2, pivot.get('tidesdb', [0]*len(x)), width, 
           label='TidesDB', color=COLORS['tidesdb'], edgecolor=COLORS['tidesdb_dark'])
    ax.bar(x + width/2, pivot.get('rocksdb', [0]*len(x)), width,
           label='RocksDB', color=COLORS['rocksdb'], edgecolor=COLORS['rocksdb_dark'])
    
    ax.set_ylabel('Coefficient of Variation (%)')
    ax.set_title('Latency Variability (Lower is Better)', fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([t.replace('_', '\n') for t in pivot.index])
    ax.legend()
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'fig9_latency_variability.png'), dpi=300, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'fig9_latency_variability.pdf'), bbox_inches='tight')
    plt.close()
    print("  Generated: fig9_latency_variability.png/pdf")

def generate_summary_table(df, output_dir):
    """
    Generate a summary table in CSV and LaTeX format.
    """
    # Calculate summary statistics
    summary_data = []
    
    for test in df['test_name'].unique():
        test_df = df[df['test_name'] == test]
        
        tidesdb = test_df[test_df['engine'] == 'tidesdb']
        rocksdb = test_df[test_df['engine'] == 'rocksdb']
        
        if tidesdb.empty or rocksdb.empty:
            continue
        
        tidesdb_ops = tidesdb['ops_per_sec'].mean()
        rocksdb_ops = rocksdb['ops_per_sec'].mean()
        speedup = tidesdb_ops / rocksdb_ops if rocksdb_ops > 0 else 0
        
        summary_data.append({
            'Test': test.replace('_', ' ').title(),
            'TidesDB (ops/s)': f'{tidesdb_ops:,.0f}',
            'RocksDB (ops/s)': f'{rocksdb_ops:,.0f}',
            'Speedup': f'{speedup:.2f}×',
            'TidesDB P99 (μs)': f'{tidesdb["p99_us"].mean():.1f}',
            'RocksDB P99 (μs)': f'{rocksdb["p99_us"].mean():.1f}',
        })
    
    if not summary_data:
        print("  Skipping: No summary data to generate")
        return
    
    summary_df = pd.DataFrame(summary_data)
    
    summary_df.to_csv(os.path.join(output_dir, 'summary_table.csv'), index=False)
    print("  Generated: summary_table.csv")
    
    # Save as LaTeX
    latex_table = summary_df.to_latex(index=False, escape=False)
    with open(os.path.join(output_dir, 'summary_table.tex'), 'w') as f:
        f.write(latex_table)
    print("  Generated: summary_table.tex")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 visualize_benchmark.py <csv_file> [output_dir]")
        print("\nExample:")
        print("  python3 visualize_benchmark.py benchmark_results/benchmark_20240101_120000.csv")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else os.path.dirname(csv_path) or '.'
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found: {csv_path}")
        sys.exit(1)
    
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\n{'='*60}")
    print("TidesDB vs RocksDB Benchmark Visualization")
    print(f"{'='*60}")
    print(f"Input:  {csv_path}")
    print(f"Output: {output_dir}")
    print(f"{'='*60}\n")
    
    setup_style()
    
    print("Loading data...")
    df = load_data(csv_path)
    print(f"  Loaded {len(df)} records from {df['test_name'].nunique()} tests\n")
    
    print("Generating figures...")
    plot_throughput_comparison(df, output_dir)
    plot_latency_distribution(df, output_dir)
    plot_batch_size_impact(df, output_dir)
    plot_thread_scaling(df, output_dir)
    plot_value_size_impact(df, output_dir)
    plot_resource_usage(df, output_dir)
    plot_amplification_factors(df, output_dir)
    plot_summary_heatmap(df, output_dir)
    plot_latency_variability(df, output_dir)
    
    print("\nGenerating summary tables...")
    generate_summary_table(df, output_dir)
    
    print(f"\n{'='*60}")
    print("Visualization complete!")
    print(f"All outputs saved to: {output_dir}")
    print(f"{'='*60}\n")

if __name__ == '__main__':
    main()
