#!/usr/bin/env python3
"""
TidesDB vs RocksDB Benchmark Visualization Extensive
Generates graphs with statistical analysis
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.ticker import FuncFormatter
import warnings
warnings.filterwarnings('ignore')

COLORS = {
    'tidesdb': '#7FB3D5', 'rocksdb': '#F5B7B1',
    'tidesdb_dark': '#5499C7', 'rocksdb_dark': '#EC7063',
    'text': '#2C3E50',
}

def setup_style():
    plt.rcParams.update({
        'font.family': 'serif', 'font.size': 10, 'axes.titlesize': 11,
        'axes.labelsize': 10, 'axes.spines.top': False, 'axes.spines.right': False,
        'axes.grid': True, 'grid.alpha': 0.4, 'figure.dpi': 150, 'savefig.dpi': 300,
    })

def format_ops(x, pos):
    if x >= 1e6: return f'{x/1e6:.1f}M'
    elif x >= 1e3: return f'{x/1e3:.0f}K'
    return f'{x:.0f}'

def load_data(csv_path):
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    return df

def calc_stats(df, group_cols, val_col):
    g = df.groupby(group_cols)[val_col].agg(['mean', 'std', 'count'])
    g['ci95'] = g['std'] / np.sqrt(g['count']) * 1.96
    return g.reset_index()

def plot_throughput_overview(df, out):
    fig, axes = plt.subplots(2, 3, figsize=(14, 9))
    fig.suptitle('Throughput Comparison', fontweight='bold')
    cats = [
        ('Write', ['write_seq_5M', 'write_random_5M', 'write_zipfian_5M'], 'PUT'),
        ('Read (Warm)', ['read_random_warm_5M', 'read_seq_warm_5M'], 'GET'),
        ('Read (Cold)', ['read_random_cold_5M', 'read_seq_cold_5M'], 'GET'),
        ('Mixed', ['mixed_random_5M', 'mixed_zipfian_5M'], 'PUT'),
        ('Seek', ['seek_random_5M', 'seek_seq_5M'], 'SEEK'),
        ('Delete', ['delete_random_5M'], 'DELETE'),
    ]
    for ax, (title, tests, op) in zip(axes.flat, cats):
        data = df[(df['test_name'].isin(tests)) & (df['operation'] == op)]
        if data.empty: ax.set_title(title); continue
        stats = calc_stats(data, ['test_name', 'engine'], 'ops_per_sec')
        order = [t for t in tests if t in stats['test_name'].values]
        x, w = np.arange(len(order)), 0.35
        for i, eng in enumerate(['tidesdb', 'rocksdb']):
            ed = stats[stats['engine'] == eng].set_index('test_name')
            m = [ed.loc[t, 'mean'] if t in ed.index else 0 for t in order]
            e = [ed.loc[t, 'ci95'] if t in ed.index else 0 for t in order]
            c = COLORS['tidesdb'] if eng == 'tidesdb' else COLORS['rocksdb']
            ax.bar(x + (-w/2 if i==0 else w/2), m, w, yerr=e, label=eng.upper(), color=c, capsize=3)
        ax.set_ylabel('ops/sec'); ax.set_title(title, fontweight='bold')
        ax.set_xticks(x); ax.set_xticklabels([t.split('_')[1] for t in order], rotation=15)
        ax.yaxis.set_major_formatter(FuncFormatter(format_ops)); ax.legend()
    plt.tight_layout(); plt.savefig(f'{out}/fig01_throughput.png'); plt.savefig(f'{out}/fig01_throughput.pdf'); plt.close()
    print("  fig01_throughput")

def plot_latency(df, out):
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle('Latency Distribution', fontweight='bold')
    wl = [('Write', 'write_random_5M', 'PUT'), ('Read', 'read_random_warm_5M', 'GET'),
          ('Seek', 'seek_random_5M', 'SEEK'), ('Mixed', 'mixed_random_5M', 'PUT')]
    pcts = ['p50_us', 'p95_us', 'p99_us', 'max_us']
    for ax, (title, test, op) in zip(axes.flat, wl):
        data = df[(df['test_name'] == test) & (df['operation'] == op)]
        if data.empty: continue
        x, w = np.arange(4), 0.35
        for i, eng in enumerate(['tidesdb', 'rocksdb']):
            ed = data[data['engine'] == eng]
            m = [ed[p].mean() for p in pcts]
            c = COLORS['tidesdb'] if eng == 'tidesdb' else COLORS['rocksdb']
            ax.bar(x + (-w/2 if i==0 else w/2), m, w, label=eng.upper(), color=c)
        ax.set_ylabel('Latency (μs)'); ax.set_title(title, fontweight='bold')
        ax.set_xticks(x); ax.set_xticklabels(['P50', 'P95', 'P99', 'Max'])
        ax.set_yscale('log'); ax.legend()
    plt.tight_layout(); plt.savefig(f'{out}/fig02_latency.png'); plt.savefig(f'{out}/fig02_latency.pdf'); plt.close()
    print("  fig02_latency")

def plot_cache(df, out):
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))
    fig.suptitle('Cache Impact', fontweight='bold')
    tests = [('read_random_warm_5M', 'Warm'), ('read_random_cold_5M', 'Cold')]
    x, w = np.arange(2), 0.35
    for ax, col, yl in [(axes[0], 'ops_per_sec', 'ops/sec'), (axes[1], 'avg_latency_us', 'Latency (μs)')]:
        for i, eng in enumerate(['tidesdb', 'rocksdb']):
            m = [df[(df['test_name']==t) & (df['engine']==eng) & (df['operation']=='GET')][col].mean() for t,_ in tests]
            c = COLORS['tidesdb'] if eng == 'tidesdb' else COLORS['rocksdb']
            ax.bar(x + (-w/2 if i==0 else w/2), m, w, label=eng.upper(), color=c)
        ax.set_ylabel(yl); ax.set_xticks(x); ax.set_xticklabels([t[1] for t in tests]); ax.legend()
        if col == 'ops_per_sec': ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
    plt.tight_layout(); plt.savefig(f'{out}/fig03_cache.png'); plt.savefig(f'{out}/fig03_cache.pdf'); plt.close()
    print("  fig03_cache")

def plot_threads(df, out):
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))
    fig.suptitle('Thread Scaling', fontweight='bold')
    td = df[df['test_name'].str.startswith('threads_') & (df['operation'] == 'PUT')].copy()
    if td.empty: print("  skip threads"); return
    td['threads'] = td['test_name'].str.extract(r'(\d+)').astype(int)
    for eng in ['tidesdb', 'rocksdb']:
        d = td[td['engine'] == eng].groupby('threads')['ops_per_sec'].mean().reset_index().sort_values('threads')
        c, m = (COLORS['tidesdb_dark'], 'o') if eng == 'tidesdb' else (COLORS['rocksdb_dark'], 's')
        axes[0].plot(d['threads'], d['ops_per_sec'], marker=m, color=c, label=eng.upper(), linewidth=2)
        if len(d) > 0:
            base = d[d['threads']==1]['ops_per_sec'].values
            if len(base): 
                axes[1].plot(d['threads'], d['ops_per_sec']/base[0], marker=m, color=c, label=eng.upper())
                axes[2].plot(d['threads'], (d['ops_per_sec']/base[0])/d['threads']*100, marker=m, color=c, label=eng.upper())
    axes[0].set_ylabel('ops/sec'); axes[0].yaxis.set_major_formatter(FuncFormatter(format_ops))
    axes[1].set_ylabel('Speedup'); axes[1].plot([1,16],[1,16],'k--',alpha=0.5,label='Ideal')
    axes[2].set_ylabel('Efficiency (%)'); axes[2].axhline(100,color='k',linestyle='--',alpha=0.5)
    for ax in axes: ax.set_xlabel('Threads'); ax.legend(); ax.set_xticks([1,2,4,8,16])
    plt.tight_layout(); plt.savefig(f'{out}/fig04_threads.png'); plt.savefig(f'{out}/fig04_threads.pdf'); plt.close()
    print("  fig04_threads")

def plot_batch(df, out):
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))
    fig.suptitle('Batch Size Impact', fontweight='bold')
    bd = df[df['test_name'].str.startswith('batch_') & (df['operation'] == 'PUT')].copy()
    if bd.empty: print("  skip batch"); return
    bd['batch'] = bd['test_name'].str.extract(r'(\d+)').astype(int)
    for eng in ['tidesdb', 'rocksdb']:
        d = bd[bd['engine'] == eng].groupby('batch').agg({'ops_per_sec':'mean','avg_latency_us':'mean','write_amp':'mean'}).reset_index().sort_values('batch')
        c, m = (COLORS['tidesdb_dark'], 'o') if eng == 'tidesdb' else (COLORS['rocksdb_dark'], 's')
        axes[0].plot(d['batch'], d['ops_per_sec'], marker=m, color=c, label=eng.upper())
        axes[1].plot(d['batch'], d['avg_latency_us'], marker=m, color=c, label=eng.upper())
        axes[2].plot(d['batch'], d['write_amp'], marker=m, color=c, label=eng.upper())
    axes[0].set_ylabel('ops/sec'); axes[0].yaxis.set_major_formatter(FuncFormatter(format_ops))
    axes[1].set_ylabel('Latency (μs)'); axes[1].set_yscale('log')
    axes[2].set_ylabel('Write Amp'); axes[2].axhline(1,color='gray',linestyle='--',alpha=0.5)
    for ax in axes: ax.set_xlabel('Batch Size'); ax.set_xscale('log'); ax.legend()
    plt.tight_layout(); plt.savefig(f'{out}/fig05_batch.png'); plt.savefig(f'{out}/fig05_batch.pdf'); plt.close()
    print("  fig05_batch")

def plot_value_size(df, out):
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))
    fig.suptitle('Value Size Impact', fontweight='bold')
    sm = {'write_small_value':64,'write_medium_value':1024,'write_large_value':4096,'write_xlarge_value':16384}
    vd = df[df['test_name'].isin(sm.keys()) & (df['operation'] == 'PUT')].copy()
    if vd.empty: print("  skip value"); return
    vd['vsize'] = vd['test_name'].map(sm)
    for eng in ['tidesdb', 'rocksdb']:
        d = vd[vd['engine'] == eng].groupby('vsize').agg({'ops_per_sec':'mean','space_amp':'mean'}).reset_index().sort_values('vsize')
        d['mb_s'] = d['ops_per_sec'] * d['vsize'] / 1e6
        c, m = (COLORS['tidesdb_dark'], 'o') if eng == 'tidesdb' else (COLORS['rocksdb_dark'], 's')
        axes[0].plot(d['vsize'], d['ops_per_sec'], marker=m, color=c, label=eng.upper())
        axes[1].plot(d['vsize'], d['mb_s'], marker=m, color=c, label=eng.upper())
        axes[2].plot(d['vsize'], d['space_amp'], marker=m, color=c, label=eng.upper())
    axes[0].set_ylabel('ops/sec'); axes[0].yaxis.set_major_formatter(FuncFormatter(format_ops))
    axes[1].set_ylabel('MB/sec'); axes[2].set_ylabel('Space Amp')
    for ax in axes: ax.set_xlabel('Value Size (bytes)'); ax.set_xscale('log'); ax.legend()
    plt.tight_layout(); plt.savefig(f'{out}/fig06_value.png'); plt.savefig(f'{out}/fig06_value.pdf'); plt.close()
    print("  fig06_value")

def plot_resources(df, out):
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle('Resource Usage', fontweight='bold')
    tests = ['write_random_5M', 'read_random_warm_5M', 'mixed_random_5M', 'seek_random_5M']
    labels = ['Write', 'Read', 'Mixed', 'Seek']
    ops = ['PUT', 'GET', 'PUT', 'SEEK']
    metrics = [('peak_rss_mb', 'Memory (MB)'), ('disk_write_mb', 'Disk Write (MB)'), 
               ('cpu_percent', 'CPU (%)'), ('db_size_mb', 'DB Size (MB)')]
    for ax, (col, yl) in zip(axes.flat, metrics):
        x, w = np.arange(len(tests)), 0.35
        for i, eng in enumerate(['tidesdb', 'rocksdb']):
            m = [df[(df['test_name']==t) & (df['engine']==eng) & (df['operation']==o)][col].mean() for t,o in zip(tests,ops)]
            c = COLORS['tidesdb'] if eng == 'tidesdb' else COLORS['rocksdb']
            ax.bar(x + (-w/2 if i==0 else w/2), m, w, label=eng.upper(), color=c)
        ax.set_ylabel(yl); ax.set_xticks(x); ax.set_xticklabels(labels); ax.legend()
    plt.tight_layout(); plt.savefig(f'{out}/fig07_resources.png'); plt.savefig(f'{out}/fig07_resources.pdf'); plt.close()
    print("  fig07_resources")

def plot_amplification(df, out):
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))
    fig.suptitle('Amplification Factors', fontweight='bold')
    tests = ['write_seq_5M', 'write_random_5M', 'write_zipfian_5M']
    labels = ['Seq', 'Random', 'Zipfian']
    x, w = np.arange(3), 0.35
    for ax, col, yl in [(axes[0],'write_amp','Write Amp'), (axes[1],'space_amp','Space Amp')]:
        for i, eng in enumerate(['tidesdb', 'rocksdb']):
            m = [df[(df['test_name']==t) & (df['engine']==eng) & (df['operation']=='PUT')][col].mean() for t in tests]
            c = COLORS['tidesdb'] if eng == 'tidesdb' else COLORS['rocksdb']
            ax.bar(x + (-w/2 if i==0 else w/2), m, w, label=eng.upper(), color=c)
        ax.set_ylabel(yl); ax.set_xticks(x); ax.set_xticklabels(labels); ax.axhline(1,color='gray',linestyle='--'); ax.legend()

    ax = axes[2]
    rd = df[df['test_name'].str.contains('read') & (df['operation']=='GET')]
    if rd['disk_read_mb'].sum() > 0:
        tests2 = ['read_random_warm_5M', 'read_seq_warm_5M']
        for i, eng in enumerate(['tidesdb', 'rocksdb']):
            m = [rd[(rd['test_name']==t) & (rd['engine']==eng)]['disk_read_mb'].mean() for t in tests2]
            c = COLORS['tidesdb'] if eng == 'tidesdb' else COLORS['rocksdb']
            ax.bar(np.arange(2) + (-w/2 if i==0 else w/2), m, w, label=eng.upper(), color=c)
        ax.set_ylabel('Disk Read (MB)'); ax.set_xticks([0,1]); ax.set_xticklabels(['Random','Seq']); ax.legend()
    else:
        ax.text(0.5, 0.5, 'No disk read data', ha='center', va='center', transform=ax.transAxes)
    ax.set_title('Read I/O')
    plt.tight_layout(); plt.savefig(f'{out}/fig08_amplification.png'); plt.savefig(f'{out}/fig08_amplification.pdf'); plt.close()
    print("  fig08_amplification")

def plot_variability(df, out):
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))
    fig.suptitle('Latency Variability', fontweight='bold')
    tests = [('write_random_5M','PUT'), ('read_random_warm_5M','GET'), ('mixed_random_5M','PUT'), ('seek_random_5M','SEEK')]
    labels = ['Write', 'Read', 'Mixed', 'Seek']
    x, w = np.arange(4), 0.35
    for i, eng in enumerate(['tidesdb', 'rocksdb']):
        cv = [df[(df['test_name']==t) & (df['engine']==eng) & (df['operation']==o)]['cv_percent'].mean() for t,o in tests]
        c = COLORS['tidesdb'] if eng == 'tidesdb' else COLORS['rocksdb']
        axes[0].bar(x + (-w/2 if i==0 else w/2), cv, w, label=eng.upper(), color=c)
        ratio = []
        for t,o in tests:
            d = df[(df['test_name']==t) & (df['engine']==eng) & (df['operation']==o)]
            ratio.append(d['p99_us'].mean()/d['p50_us'].mean() if d['p50_us'].mean()>0 else 0)
        axes[1].bar(x + (-w/2 if i==0 else w/2), ratio, w, label=eng.upper(), color=c)
    axes[0].set_ylabel('CV (%)'); axes[1].set_ylabel('P99/P50 Ratio')
    for ax in axes: ax.set_xticks(x); ax.set_xticklabels(labels); ax.legend()
    plt.tight_layout(); plt.savefig(f'{out}/fig09_variability.png'); plt.savefig(f'{out}/fig09_variability.pdf'); plt.close()
    print("  fig09_variability")

def plot_summary(df, out):
    fig, ax = plt.subplots(figsize=(10, 8))
    speedups = []
    for test in df['test_name'].unique():
        for op in df[df['test_name']==test]['operation'].unique():
            t = df[(df['test_name']==test) & (df['engine']=='tidesdb') & (df['operation']==op)]['ops_per_sec'].mean()
            r = df[(df['test_name']==test) & (df['engine']=='rocksdb') & (df['operation']==op)]['ops_per_sec'].mean()
            if r > 0: speedups.append({'test': f"{test}\n({op})", 'speedup': t/r})
    if not speedups: print("  skip summary"); return
    sdf = pd.DataFrame(speedups).sort_values('speedup')
    colors = [COLORS['tidesdb'] if s>=1 else COLORS['rocksdb'] for s in sdf['speedup']]
    ax.barh(range(len(sdf)), sdf['speedup'], color=colors)
    ax.set_yticks(range(len(sdf))); ax.set_yticklabels(sdf['test'], fontsize=7)
    ax.set_xlabel('Speedup (TidesDB/RocksDB)'); ax.axvline(1, color='k', linewidth=1.5)
    ax.set_title('Summary (>1 = TidesDB faster)', fontweight='bold')
    for i, v in enumerate(sdf['speedup']): ax.annotate(f'{v:.2f}×', xy=(v,i), fontsize=6)
    plt.tight_layout(); plt.savefig(f'{out}/fig10_summary.png'); plt.savefig(f'{out}/fig10_summary.pdf'); plt.close()
    print("  fig10_summary")

def plot_range(df, out):
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))
    fig.suptitle('Range Scan', fontweight='bold')
    sm = {'range_10':10, 'range_100':100, 'range_1000':1000}
    rd = df[df['test_name'].isin(sm.keys()) & (df['operation']=='RANGE')].copy()
    if rd.empty: print("  skip range"); return
    rd['rsize'] = rd['test_name'].map(sm)
    for eng in ['tidesdb', 'rocksdb']:
        d = rd[rd['engine']==eng].groupby('rsize').agg({'ops_per_sec':'mean','avg_latency_us':'mean'}).reset_index().sort_values('rsize')
        c, m = (COLORS['tidesdb_dark'], 'o') if eng == 'tidesdb' else (COLORS['rocksdb_dark'], 's')
        axes[0].plot(d['rsize'], d['ops_per_sec'], marker=m, color=c, label=eng.upper())
        axes[1].plot(d['rsize'], d['avg_latency_us'], marker=m, color=c, label=eng.upper())
    axes[0].set_ylabel('scans/sec'); axes[0].yaxis.set_major_formatter(FuncFormatter(format_ops))
    axes[1].set_ylabel('Latency (μs)'); axes[1].set_yscale('log')
    for ax in axes: ax.set_xlabel('Range Size'); ax.set_xscale('log'); ax.legend()
    plt.tight_layout(); plt.savefig(f'{out}/fig11_range.png'); plt.savefig(f'{out}/fig11_range.pdf'); plt.close()
    print("  fig11_range")

def plot_key_size(df, out):
    fig, ax = plt.subplots(figsize=(8, 5))
    fig.suptitle('Key Size Impact', fontweight='bold')
    sm = {'key_8B':8, 'key_32B':32, 'key_64B':64, 'key_128B':128}
    kd = df[df['test_name'].isin(sm.keys()) & (df['operation']=='PUT')].copy()
    if kd.empty: print("  skip key"); return
    kd['ksize'] = kd['test_name'].map(sm)
    for eng in ['tidesdb', 'rocksdb']:
        d = kd[kd['engine']==eng].groupby('ksize')['ops_per_sec'].mean().reset_index().sort_values('ksize')
        c, m = (COLORS['tidesdb_dark'], 'o') if eng == 'tidesdb' else (COLORS['rocksdb_dark'], 's')
        ax.plot(d['ksize'], d['ops_per_sec'], marker=m, color=c, label=eng.upper(), linewidth=2)
    ax.set_xlabel('Key Size (bytes)'); ax.set_ylabel('ops/sec')
    ax.yaxis.set_major_formatter(FuncFormatter(format_ops)); ax.legend()
    plt.tight_layout(); plt.savefig(f'{out}/fig12_keysize.png'); plt.savefig(f'{out}/fig12_keysize.pdf'); plt.close()
    print("  fig12_keysize")

def generate_tables(df, out):
    summary = []
    for test in df['test_name'].unique():
        for op in ['PUT','GET','SEEK','DELETE','RANGE']:
            t = df[(df['test_name']==test) & (df['engine']=='tidesdb') & (df['operation']==op)]
            r = df[(df['test_name']==test) & (df['engine']=='rocksdb') & (df['operation']==op)]
            if t.empty or r.empty: continue
            summary.append({
                'Test': test, 'Op': op,
                'TidesDB ops/s': f"{t['ops_per_sec'].mean():,.0f}",
                'RocksDB ops/s': f"{r['ops_per_sec'].mean():,.0f}",
                'Speedup': f"{t['ops_per_sec'].mean()/r['ops_per_sec'].mean():.2f}x",
                'TidesDB P99': f"{t['p99_us'].mean():.0f}μs",
                'RocksDB P99': f"{r['p99_us'].mean():.0f}μs",
            })
    if summary:
        pd.DataFrame(summary).to_csv(f'{out}/summary_table.csv', index=False)
        print("  summary_table.csv")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 visualize_extensive.py <csv_file> [output_dir]")
        sys.exit(1)
    csv_path = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else os.path.dirname(csv_path) or '.'
    os.makedirs(out, exist_ok=True)
    setup_style()
    print(f"\nLoading {csv_path}...")
    df = load_data(csv_path)
    print(f"Loaded {len(df)} records\n\nGenerating figures...")
    plot_throughput_overview(df, out)
    plot_latency(df, out)
    plot_cache(df, out)
    plot_threads(df, out)
    plot_batch(df, out)
    plot_value_size(df, out)
    plot_resources(df, out)
    plot_amplification(df, out)
    plot_variability(df, out)
    plot_summary(df, out)
    plot_range(df, out)
    plot_key_size(df, out)
    print("\nGenerating tables...")
    generate_tables(df, out)
    print(f"\nDone! Output: {out}")

if __name__ == '__main__':
    main()
