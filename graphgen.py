#!/usr/bin/env python3
"""
Generic graph generator for benchtool CSV outputs.
Creates image-only outputs and adapts to any runner output CSV.
"""

from __future__ import annotations

import argparse
import os
from typing import Iterable, List

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


PALETTE = {
    "tidesdb": "#7FAADC",
    "rocksdb": "#E8A87C",
    "other": "#9DB4C0",
    "accent": "#C5C3C6",
    "ink": "#2D2D2D",
    "paper": "#FFFFFF",
    "grid": "#E0E0E0",
}


def setup_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["Helvetica", "Arial", "DejaVu Sans", "Liberation Sans"],
            "font.size": 9,
            "axes.titlesize": 10,
            "axes.labelsize": 9,
            "axes.titleweight": "normal",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.spines.left": True,
            "axes.spines.bottom": True,
            "axes.linewidth": 0.8,
            "axes.grid": True,
            "axes.grid.axis": "y",
            "axes.axisbelow": True,
            "grid.alpha": 0.4,
            "grid.linestyle": "-",
            "grid.linewidth": 0.5,
            "grid.color": PALETTE["grid"],
            "figure.dpi": 150,
            "figure.facecolor": PALETTE["paper"],
            "axes.facecolor": PALETTE["paper"],
            "savefig.dpi": 300,
            "savefig.facecolor": PALETTE["paper"],
            "savefig.bbox": "tight",
            "savefig.pad_inches": 0.1,
            "legend.frameon": False,
            "legend.fontsize": 8,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
        }
    )


def add_clean_background(fig: plt.Figure) -> None:
    fig.patch.set_facecolor(PALETTE["paper"])


def format_ops(x: float, _pos: int) -> str:
    if x >= 1e6:
        return f"{x / 1e6:.1f}M"
    if x >= 1e3:
        return f"{x / 1e3:.0f}K"
    return f"{x:.0f}"


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    if "test_name" not in df.columns:
        parts = []
        for col in [
            "workload",
            "pattern",
            "operation",
            "num_operations",
            "threads",
            "batch_size",
            "key_size",
            "value_size",
            "range_size",
            "sync_enabled",
        ]:
            if col in df.columns:
                parts.append(df[col].astype(str))
        df["test_name"] = parts[0] if parts else "unknown"
        for part in parts[1:]:
            df["test_name"] = df["test_name"] + "_" + part
    return df


def to_numeric(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def engine_color(engine: str) -> str:
    if engine == "tidesdb":
        return PALETTE["tidesdb"]
    if engine == "rocksdb":
        return PALETTE["rocksdb"]
    return PALETTE["other"]


def top_tests(df: pd.DataFrame, operation: str, max_tests: int = 12) -> List[str]:
    subset = df[df["operation"] == operation].copy()
    if subset.empty:
        return []
    by_test = subset.groupby("test_name")["ops_per_sec"].mean().sort_values(ascending=False)
    return by_test.head(max_tests).index.tolist()


def plot_throughput(df: pd.DataFrame, out_dir: str) -> None:
    ops = sorted([op for op in df["operation"].dropna().unique().tolist() if op != "ITER"])
    if not ops:
        return

    cols = 2
    rows = int(np.ceil(len(ops) / cols))
    fig, axes = plt.subplots(rows, cols, figsize=(12, 4 + rows * 3.2))
    axes = np.array(axes).reshape(-1)

    for ax, op in zip(axes, ops):
        tests = top_tests(df, op)
        data = df[df["test_name"].isin(tests) & (df["operation"] == op)]
        if data.empty:
            ax.set_visible(False)
            continue
        stats = (
            data.groupby(["test_name", "engine"])["ops_per_sec"]
            .mean()
            .reset_index()
        )
        order = tests
        x = np.arange(len(order))
        width = 0.35
        engines = sorted(stats["engine"].unique().tolist())
        for i, eng in enumerate(engines):
            ed = stats[stats["engine"] == eng].set_index("test_name")
            vals = [ed.loc[t, "ops_per_sec"] if t in ed.index else 0 for t in order]
            ax.bar(
                x + (i - (len(engines) - 1) / 2) * width,
                vals,
                width,
                label=eng.upper(),
                color=engine_color(eng),
                alpha=0.9,
            )
        ax.set_title(f"{op} throughput")
        ax.set_ylabel("ops/sec")
        ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
        ax.set_xticks(x)
        ax.set_xticklabels(order, rotation=15, ha="right", fontsize=8)
        ax.legend(frameon=False, fontsize=8)

    for ax in axes[len(ops) :]:
        ax.set_visible(False)

    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "throughput_overview.png"))
    plt.close(fig)


def plot_latency(df: pd.DataFrame, out_dir: str) -> None:
    if "avg_latency_us" not in df.columns:
        return
    ops = sorted([op for op in df["operation"].dropna().unique().tolist() if op != "ITER"])
    rows = int(np.ceil(len(ops) / 2))
    fig, axes = plt.subplots(rows, 2, figsize=(12, 4 + rows * 3))
    axes = np.array(axes).reshape(-1)

    for ax, op in zip(axes, ops):
        data = df[df["operation"] == op].copy()
        if data.empty:
            ax.set_visible(False)
            continue
        stats = data.groupby(["engine"])["avg_latency_us"].mean().reset_index()
        engines = stats["engine"].tolist()
        vals = stats["avg_latency_us"].tolist()
        colors = [engine_color(e) for e in engines]
        ax.bar(engines, vals, color=colors, alpha=0.9)
        ax.set_title(f"{op} avg latency")
        ax.set_ylabel("us")
        ax.set_yscale("log")

    for ax in axes[len(ops) :]:
        ax.set_visible(False)

    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "latency_overview.png"))
    plt.close(fig)


def plot_latency_percentiles(df: pd.DataFrame, out_dir: str) -> None:
    percent_cols = [
        c for c in ["min_us", "p50_us", "p95_us", "p99_us", "max_us"] if c in df.columns
    ]
    if not percent_cols:
        return
    ops = sorted([op for op in df["operation"].dropna().unique().tolist() if op != "ITER"])
    rows = int(np.ceil(len(ops) / 2))
    fig, axes = plt.subplots(rows, 2, figsize=(12, 4 + rows * 3))
    axes = np.array(axes).reshape(-1)

    for ax, op in zip(axes, ops):
        data = df[df["operation"] == op].copy()
        if data.empty:
            ax.set_visible(False)
            continue
        x = np.arange(len(percent_cols))
        width = 0.35
        engines = sorted(data["engine"].unique().tolist())
        for i, eng in enumerate(engines):
            ed = data[data["engine"] == eng]
            vals = [ed[c].mean() for c in percent_cols]
            ax.bar(
                x + (i - (len(engines) - 1) / 2) * width,
                vals,
                width,
                label=eng.upper(),
                color=engine_color(eng),
                alpha=0.9,
            )
        ax.set_title(f"{op} latency percentiles")
        ax.set_xticks(x)
        ax.set_xticklabels([c.replace("_us", "").upper() for c in percent_cols])
        ax.set_ylabel("us")
        ax.set_yscale("log")
        ax.legend(frameon=False, fontsize=8)

    for ax in axes[len(ops) :]:
        ax.set_visible(False)

    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "latency_percentiles.png"))
    plt.close(fig)


def plot_variability(df: pd.DataFrame, out_dir: str) -> None:
    if "cv_percent" not in df.columns:
        return
    ops = sorted([op for op in df["operation"].dropna().unique().tolist() if op != "ITER"])
    rows = int(np.ceil(len(ops) / 2))
    fig, axes = plt.subplots(rows, 2, figsize=(12, 4 + rows * 3))
    axes = np.array(axes).reshape(-1)

    for ax, op in zip(axes, ops):
        data = df[df["operation"] == op].copy()
        if data.empty:
            ax.set_visible(False)
            continue
        stats = data.groupby("engine")["cv_percent"].mean().reset_index()
        engines = stats["engine"].tolist()
        vals = stats["cv_percent"].tolist()
        colors = [engine_color(e) for e in engines]
        ax.bar(engines, vals, color=colors, alpha=0.9)
        ax.set_title(f"{op} variability (CV%)")
        ax.set_ylabel("CV%")

    for ax in axes[len(ops) :]:
        ax.set_visible(False)

    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "variability_cv.png"))
    plt.close(fig)


def plot_latency_stddev(df: pd.DataFrame, out_dir: str) -> None:
    if "stddev_us" not in df.columns:
        return
    ops = sorted([op for op in df["operation"].dropna().unique().tolist() if op != "ITER"])
    rows = int(np.ceil(len(ops) / 2))
    fig, axes = plt.subplots(rows, 2, figsize=(12, 4 + rows * 3))
    axes = np.array(axes).reshape(-1)

    for ax, op in zip(axes, ops):
        data = df[df["operation"] == op].copy()
        if data.empty:
            ax.set_visible(False)
            continue
        stats = data.groupby("engine")["stddev_us"].mean().reset_index()
        engines = stats["engine"].tolist()
        vals = stats["stddev_us"].tolist()
        colors = [engine_color(e) for e in engines]
        ax.bar(engines, vals, color=colors, alpha=0.9)
        ax.set_title(f"{op} latency stddev")
        ax.set_ylabel("us")
        ax.set_yscale("log")

    for ax in axes[len(ops) :]:
        ax.set_visible(False)

    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "latency_stddev.png"))
    plt.close(fig)


def plot_amplification(df: pd.DataFrame, out_dir: str) -> None:
    cols = [c for c in ["write_amp", "read_amp", "space_amp"] if c in df.columns]
    if not cols:
        return
    fig, axes = plt.subplots(1, len(cols), figsize=(5 * len(cols), 4.8))
    if len(cols) == 1:
        axes = [axes]
    for ax, col in zip(axes, cols):
        stats = df.groupby("engine")[col].mean().reset_index()
        engines = stats["engine"].tolist()
        vals = stats[col].tolist()
        colors = [engine_color(e) for e in engines]
        ax.bar(engines, vals, color=colors, alpha=0.9)
        ax.axhline(1, color=PALETTE["accent"], linestyle="--", linewidth=1)
        ax.set_title(col.replace("_", " "))
    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "amplification.png"))
    plt.close(fig)


def plot_resource_overview(df: pd.DataFrame, out_dir: str) -> None:
    cols = [
        c
        for c in [
            "peak_rss_mb",
            "peak_vms_mb",
            "disk_read_mb",
            "disk_write_mb",
            "cpu_user_sec",
            "cpu_sys_sec",
            "cpu_percent",
            "db_size_mb",
            "duration_sec",
        ]
        if c in df.columns
    ]
    if not cols:
        return
    fig, axes = plt.subplots(2, int(np.ceil(len(cols) / 2)), figsize=(14, 7))
    axes = np.array(axes).reshape(-1)
    for ax, col in zip(axes, cols):
        stats = df.groupby("engine")[col].mean().reset_index()
        engines = stats["engine"].tolist()
        vals = stats[col].tolist()
        colors = [engine_color(e) for e in engines]
        ax.bar(engines, vals, color=colors, alpha=0.9)
        ax.set_title(col.replace("_", " "))
    for ax in axes[len(cols) :]:
        ax.set_visible(False)
    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "resource_overview.png"))
    plt.close(fig)


def plot_param_sweep(df: pd.DataFrame, param: str, op_filter: str, out_dir: str) -> None:
    if param not in df.columns:
        return
    subset = df[df["operation"] == op_filter].copy()
    if subset[param].nunique() < 2:
        return

    fig, ax = plt.subplots(figsize=(8, 5.5))
    for engine in sorted(subset["engine"].unique().tolist()):
        eng = subset[subset["engine"] == engine]
        series = (
            eng.groupby(param)["ops_per_sec"]
            .mean()
            .reset_index()
            .sort_values(param)
        )
        ax.plot(
            series[param],
            series["ops_per_sec"],
            marker="o",
            linewidth=2,
            label=engine.upper(),
            color=engine_color(engine),
        )
    ax.set_title(f"{op_filter} throughput vs {param}")
    ax.set_xlabel(param.replace("_", " "))
    ax.set_ylabel("ops/sec")
    ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
    ax.legend(frameon=False)
    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, f"sweep_{op_filter.lower()}_{param}.png"))
    plt.close(fig)


def plot_operation_mix(df: pd.DataFrame, out_dir: str) -> None:
    """Plot throughput by operation type."""
    if "operation" not in df.columns:
        return
    df_filtered = df[df["operation"] != "ITER"]
    if df_filtered.empty:
        return
    stats = df_filtered.groupby(["engine", "operation"])["ops_per_sec"].mean().reset_index()
    stats.columns = ["engine", "operation", "mean"]
    engines = stats["engine"].unique().tolist()
    ops = stats["operation"].unique().tolist()
    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(ops))
    width = 0.35
    for i, eng in enumerate(engines):
        ed = stats[stats["engine"] == eng].set_index("operation")
        vals = [ed.loc[o, "mean"] if o in ed.index else 0 for o in ops]
        ax.bar(
            x + (i - (len(engines) - 1) / 2) * width,
            vals,
            width,
            label=eng.upper(),
            color=engine_color(eng),
            edgecolor="none",
        )
    ax.set_title("Operation Mix Throughput")
    ax.set_xticks(x)
    ax.set_xticklabels(ops)
    ax.set_ylabel("ops/sec")
    ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
    ax.legend(frameon=False)
    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "operation_mix.png"))
    plt.close(fig)


def plot_engine_comparison(df: pd.DataFrame, out_dir: str) -> None:
    if "operation" not in df.columns or "engine" not in df.columns:
        return
    
    ops_order = ["PUT", "GET", "DELETE", "SEEK", "RANGE"]
    available_ops = [op for op in ops_order if op in df["operation"].values]
    if not available_ops:
        return
    
    stats = df[df["operation"].isin(available_ops)].groupby(
        ["engine", "operation"]
    )["ops_per_sec"].mean().reset_index()
    stats.columns = ["engine", "operation", "mean"]
    
    engines = sorted(stats["engine"].unique().tolist())
    if len(engines) < 2:
        return
    
    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(len(available_ops))
    width = 0.35
    
    for i, eng in enumerate(engines):
        ed = stats[stats["engine"] == eng].set_index("operation")
        vals = [ed.loc[o, "mean"] if o in ed.index else 0 for o in available_ops]
        ax.bar(
            x + (i - (len(engines) - 1) / 2) * width,
            vals,
            width,
            label=eng.upper(),
            color=engine_color(eng),
            edgecolor="none",
        )
    
    ax.set_xlabel("Operation")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.set_xticks(x)
    ax.set_xticklabels(available_ops)
    ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
    ax.legend(loc="upper right")
    ax.set_title("Engine Comparison")
    
    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "engine_comparison.png"))
    plt.close(fig)


def plot_speedup_ratios(df: pd.DataFrame, out_dir: str) -> None:
    """Plot speedup ratios between engines (tidesdb vs rocksdb)."""
    if "engine" not in df.columns or "operation" not in df.columns:
        return
    
    engines = df["engine"].unique().tolist()
    if "tidesdb" not in engines or "rocksdb" not in engines:
        return
    
    ops_order = ["PUT", "GET", "DELETE", "SEEK", "RANGE"]
    available_ops = [op for op in ops_order if op in df["operation"].values]
    if not available_ops:
        return
    
    speedups = []
    for op in available_ops:
        tidesdb_ops = df[(df["engine"] == "tidesdb") & (df["operation"] == op)]["ops_per_sec"].mean()
        rocksdb_ops = df[(df["engine"] == "rocksdb") & (df["operation"] == op)]["ops_per_sec"].mean()
        if pd.notna(tidesdb_ops) and pd.notna(rocksdb_ops) and rocksdb_ops > 0:
            speedups.append((op, tidesdb_ops / rocksdb_ops))
    
    if not speedups:
        return
    
    ops, ratios = zip(*speedups)
    
    fig, ax = plt.subplots(figsize=(8, 5))
    colors = [PALETTE["tidesdb"] if r >= 1 else PALETTE["rocksdb"] for r in ratios]
    bars = ax.bar(ops, ratios, color=colors, alpha=0.9, edgecolor="none")
    
    ax.axhline(1, color=PALETTE["accent"], linestyle="--", linewidth=1.5, label="Equal performance")
    
    for bar, ratio in zip(bars, ratios):
        height = bar.get_height()
        label = f"{ratio:.2f}x"
        if ratio >= 1:
            label = f"+{label}" if ratio > 1 else label
        ax.annotate(
            label,
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 3),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold",
        )
    
    ax.set_xlabel("Operation")
    ax.set_ylabel("Speedup Ratio (TidesDB / RocksDB)")
    ax.set_title("TidesDB vs RocksDB Speedup")
    ax.set_ylim(0, max(ratios) * 1.2)
    
    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "speedup_ratios.png"))
    plt.close(fig)


def plot_latency_vs_throughput(df: pd.DataFrame, out_dir: str) -> None:
    """Scatter plot of latency vs throughput to show efficiency."""
    if "avg_latency_us" not in df.columns or "ops_per_sec" not in df.columns:
        return
    
    ops = [op for op in ["PUT", "GET", "DELETE", "SEEK", "RANGE"] if op in df["operation"].values]
    if not ops:
        return
    
    fig, ax = plt.subplots(figsize=(9, 6))
    
    markers = {"PUT": "o", "GET": "s", "DELETE": "^", "SEEK": "D", "RANGE": "v"}
    
    for engine in sorted(df["engine"].unique().tolist()):
        for op in ops:
            subset = df[(df["engine"] == engine) & (df["operation"] == op)]
            if subset.empty:
                continue
            ax.scatter(
                subset["ops_per_sec"],
                subset["avg_latency_us"],
                c=engine_color(engine),
                marker=markers.get(op, "o"),
                s=60,
                alpha=0.7,
                label=f"{engine.upper()} {op}",
                edgecolors="white",
                linewidths=0.5,
            )
    
    ax.set_xlabel("Throughput (ops/sec)")
    ax.set_ylabel("Average Latency (μs)")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.xaxis.set_major_formatter(FuncFormatter(format_ops))
    ax.set_title("Latency vs Throughput (lower-right is better)")
    
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(by_label.values(), by_label.keys(), loc="upper right", fontsize=7, ncol=2)
    
    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "latency_vs_throughput.png"))
    plt.close(fig)


def plot_per_test_comparison(df: pd.DataFrame, out_dir: str) -> None:
    """Side-by-side comparison for each test_name with both engines."""
    if "test_name" not in df.columns or "engine" not in df.columns:
        return
    
    engines = df["engine"].unique().tolist()
    if len(engines) < 2:
        return
    
    ops = [op for op in ["PUT", "GET", "DELETE", "SEEK", "RANGE"] if op in df["operation"].values]
    
    for op in ops:
        op_data = df[df["operation"] == op]
        tests_with_both = (
            op_data.groupby("test_name")["engine"]
            .nunique()
            .reset_index()
        )
        tests_with_both = tests_with_both[tests_with_both["engine"] >= 2]["test_name"].tolist()
        
        if len(tests_with_both) < 1:
            continue
        
        tests_with_both = tests_with_both[:10]
        
        fig, ax = plt.subplots(figsize=(12, 5 + len(tests_with_both) * 0.3))
        
        stats = op_data[op_data["test_name"].isin(tests_with_both)].groupby(
            ["test_name", "engine"]
        )["ops_per_sec"].mean().reset_index()
        
        y = np.arange(len(tests_with_both))
        height = 0.35
        
        for i, eng in enumerate(sorted(engines)):
            ed = stats[stats["engine"] == eng].set_index("test_name")
            vals = [ed.loc[t, "ops_per_sec"] if t in ed.index else 0 for t in tests_with_both]
            ax.barh(
                y + (i - (len(engines) - 1) / 2) * height,
                vals,
                height,
                label=eng.upper(),
                color=engine_color(eng),
                edgecolor="none",
            )
        
        ax.set_xlabel("Throughput (ops/sec)")
        ax.set_ylabel("Test")
        ax.set_yticks(y)
        ax.set_yticklabels(tests_with_both, fontsize=7)
        ax.xaxis.set_major_formatter(FuncFormatter(format_ops))
        ax.legend(loc="lower right")
        ax.set_title(f"{op} Per-Test Comparison")
        
        add_clean_background(fig)
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"per_test_{op.lower()}.png"))
        plt.close(fig)


def plot_summary_table(df: pd.DataFrame, out_dir: str) -> None:
    """Generate a summary statistics table as an image."""
    if "engine" not in df.columns or "operation" not in df.columns:
        return
    
    ops = [op for op in ["PUT", "GET", "DELETE", "SEEK", "RANGE"] if op in df["operation"].values]
    engines = sorted(df["engine"].unique().tolist())
    
    if not ops or not engines:
        return
    
    rows = []
    for op in ops:
        row = {"Operation": op}
        for eng in engines:
            subset = df[(df["engine"] == eng) & (df["operation"] == op)]
            if subset.empty:
                row[f"{eng.upper()} ops/sec"] = "-"
                row[f"{eng.upper()} avg lat (μs)"] = "-"
            else:
                ops_sec = subset["ops_per_sec"].mean()
                lat = subset["avg_latency_us"].mean() if "avg_latency_us" in subset.columns else np.nan
                row[f"{eng.upper()} ops/sec"] = f"{ops_sec:,.0f}"
                row[f"{eng.upper()} avg lat (μs)"] = f"{lat:,.1f}" if pd.notna(lat) else "-"
        
        if len(engines) >= 2 and "tidesdb" in engines and "rocksdb" in engines:
            t_ops = df[(df["engine"] == "tidesdb") & (df["operation"] == op)]["ops_per_sec"].mean()
            r_ops = df[(df["engine"] == "rocksdb") & (df["operation"] == op)]["ops_per_sec"].mean()
            if pd.notna(t_ops) and pd.notna(r_ops) and r_ops > 0:
                ratio = t_ops / r_ops
                row["Speedup"] = f"{ratio:.2f}x"
            else:
                row["Speedup"] = "-"
        
        rows.append(row)
    
    summary_df = pd.DataFrame(rows)
    
    fig, ax = plt.subplots(figsize=(len(summary_df.columns) * 1.5, len(ops) * 0.6 + 1))
    ax.axis("off")
    
    table = ax.table(
        cellText=summary_df.values,
        colLabels=summary_df.columns,
        cellLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.2, 1.5)
    
    for i in range(len(summary_df.columns)):
        table[(0, i)].set_facecolor(PALETTE["grid"])
        table[(0, i)].set_text_props(weight="bold")
    
    ax.set_title("Performance Summary", fontsize=12, fontweight="bold", pad=20)
    
    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "summary_table.png"))
    plt.close(fig)


def plot_iterator_throughput(df: pd.DataFrame, out_dir: str) -> None:
    """Plot ITER (iterator) throughput separately since it's excluded from other charts."""
    if "ITER" not in df["operation"].values:
        return
    
    iter_data = df[df["operation"] == "ITER"]
    if iter_data.empty:
        return
    
    tests = iter_data["test_name"].unique().tolist()[:15]
    
    fig, ax = plt.subplots(figsize=(12, 5 + len(tests) * 0.2))
    
    stats = iter_data[iter_data["test_name"].isin(tests)].groupby(
        ["test_name", "engine"]
    )["ops_per_sec"].mean().reset_index()
    
    engines = sorted(stats["engine"].unique().tolist())
    y = np.arange(len(tests))
    height = 0.35
    
    for i, eng in enumerate(engines):
        ed = stats[stats["engine"] == eng].set_index("test_name")
        vals = [ed.loc[t, "ops_per_sec"] if t in ed.index else 0 for t in tests]
        ax.barh(
            y + (i - (len(engines) - 1) / 2) * height,
            vals,
            height,
            label=eng.upper(),
            color=engine_color(eng),
            edgecolor="none",
        )
    
    ax.set_xlabel("Throughput (ops/sec)")
    ax.set_ylabel("Test")
    ax.set_yticks(y)
    ax.set_yticklabels(tests, fontsize=7)
    ax.xaxis.set_major_formatter(FuncFormatter(format_ops))
    ax.legend(loc="lower right")
    ax.set_title("Iterator (ITER) Throughput")
    
    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "iterator_throughput.png"))
    plt.close(fig)


def plot_pattern_comparison(df: pd.DataFrame, out_dir: str) -> None:
    """Compare performance across different key patterns (seq, random, zipfian)."""
    if "pattern" not in df.columns:
        return
    patterns = df["pattern"].dropna().unique().tolist()
    if len(patterns) < 2:
        return
    
    ops = [op for op in ["PUT", "GET", "DELETE", "SEEK"] if op in df["operation"].values]
    if not ops:
        return
    
    for op in ops:
        subset = df[df["operation"] == op]
        if subset.empty:
            continue
        
        stats = subset.groupby(["engine", "pattern"])["ops_per_sec"].mean().reset_index()
        engines = sorted(stats["engine"].unique().tolist())
        avail_patterns = sorted(stats["pattern"].unique().tolist())
        
        if len(avail_patterns) < 2:
            continue
        
        fig, ax = plt.subplots(figsize=(8, 5))
        x = np.arange(len(avail_patterns))
        width = 0.35
        
        for i, eng in enumerate(engines):
            ed = stats[stats["engine"] == eng].set_index("pattern")
            vals = [ed.loc[p, "ops_per_sec"] if p in ed.index else 0 for p in avail_patterns]
            ax.bar(
                x + (i - (len(engines) - 1) / 2) * width,
                vals,
                width,
                label=eng.upper(),
                color=engine_color(eng),
                edgecolor="none",
            )
        
        ax.set_xlabel("Key Pattern")
        ax.set_ylabel("Throughput (ops/sec)")
        ax.set_xticks(x)
        ax.set_xticklabels(avail_patterns)
        ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
        ax.legend(loc="upper right")
        ax.set_title(f"{op} Performance by Key Pattern")
        
        add_clean_background(fig)
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"pattern_{op.lower()}.png"))
        plt.close(fig)


def plot_workload_comparison(df: pd.DataFrame, out_dir: str) -> None:
    """Compare performance across different workload types."""
    if "workload" not in df.columns:
        return
    workloads = df["workload"].dropna().unique().tolist()
    if len(workloads) < 2:
        return
    
    stats = df.groupby(["engine", "workload"])["ops_per_sec"].mean().reset_index()
    engines = sorted(stats["engine"].unique().tolist())
    avail_workloads = sorted(stats["workload"].unique().tolist())
    
    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(len(avail_workloads))
    width = 0.35
    
    for i, eng in enumerate(engines):
        ed = stats[stats["engine"] == eng].set_index("workload")
        vals = [ed.loc[w, "ops_per_sec"] if w in ed.index else 0 for w in avail_workloads]
        ax.bar(
            x + (i - (len(engines) - 1) / 2) * width,
            vals,
            width,
            label=eng.upper(),
            color=engine_color(eng),
            edgecolor="none",
        )
    
    ax.set_xlabel("Workload Type")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.set_xticks(x)
    ax.set_xticklabels(avail_workloads)
    ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
    ax.legend(loc="upper right")
    ax.set_title("Performance by Workload Type")
    
    add_clean_background(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "workload_comparison.png"))
    plt.close(fig)


def plot_sync_comparison(df: pd.DataFrame, out_dir: str) -> None:
    """Compare sync enabled vs disabled performance."""
    if "sync_enabled" not in df.columns:
        return
    sync_vals = df["sync_enabled"].dropna().unique().tolist()
    if len(sync_vals) < 2:
        return
    
    ops = [op for op in ["PUT", "GET", "DELETE"] if op in df["operation"].values]
    if not ops:
        return
    
    for op in ops:
        subset = df[df["operation"] == op]
        if subset.empty:
            continue
        
        stats = subset.groupby(["engine", "sync_enabled"])["ops_per_sec"].mean().reset_index()
        engines = sorted(stats["engine"].unique().tolist())
        
        fig, ax = plt.subplots(figsize=(6, 5))
        x = np.arange(2)
        width = 0.35
        sync_labels = ["Sync Off", "Sync On"]
        
        for i, eng in enumerate(engines):
            ed = stats[stats["engine"] == eng].set_index("sync_enabled")
            vals = [ed.loc[s, "ops_per_sec"] if s in ed.index else 0 for s in [0, 1]]
            ax.bar(
                x + (i - (len(engines) - 1) / 2) * width,
                vals,
                width,
                label=eng.upper(),
                color=engine_color(eng),
                edgecolor="none",
            )
        
        ax.set_xlabel("Sync Mode")
        ax.set_ylabel("Throughput (ops/sec)")
        ax.set_xticks(x)
        ax.set_xticklabels(sync_labels)
        ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
        ax.legend(loc="upper right")
        ax.set_title(f"{op} Performance: Sync Comparison")
        
        add_clean_background(fig)
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"sync_{op.lower()}.png"))
        plt.close(fig)


def plot_value_size_impact(df: pd.DataFrame, out_dir: str) -> None:
    """Show impact of value size on throughput."""
    if "value_size" not in df.columns:
        return
    if df["value_size"].nunique() < 2:
        return
    
    ops = [op for op in ["PUT", "GET"] if op in df["operation"].values]
    for op in ops:
        subset = df[df["operation"] == op]
        if subset["value_size"].nunique() < 2:
            continue
        
        fig, ax = plt.subplots(figsize=(8, 5))
        for engine in sorted(subset["engine"].unique().tolist()):
            eng_data = subset[subset["engine"] == engine]
            series = eng_data.groupby("value_size")["ops_per_sec"].mean().reset_index().sort_values("value_size")
            ax.plot(
                series["value_size"],
                series["ops_per_sec"],
                marker="o",
                linewidth=2,
                label=engine.upper(),
                color=engine_color(engine),
            )
        
        ax.set_xlabel("Value Size (bytes)")
        ax.set_ylabel("Throughput (ops/sec)")
        ax.set_xscale("log")
        ax.yaxis.set_major_formatter(FuncFormatter(format_ops))
        ax.legend(loc="upper right")
        ax.set_title(f"{op} Throughput vs Value Size")
        
        add_clean_background(fig)
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"value_size_{op.lower()}.png"))
        plt.close(fig)


def generate_graphs(csv_path: str, out_dir: str) -> None:
    setup_style()
    df = pd.read_csv(csv_path)
    df = ensure_columns(df)
    numeric_cols = [
        "ops_per_sec",
        "duration_sec",
        "avg_latency_us",
        "stddev_us",
        "cv_percent",
        "p50_us",
        "p95_us",
        "p99_us",
        "min_us",
        "max_us",
        "peak_rss_mb",
        "peak_vms_mb",
        "disk_read_mb",
        "disk_write_mb",
        "cpu_user_sec",
        "cpu_sys_sec",
        "cpu_percent",
        "db_size_mb",
        "write_amp",
        "read_amp",
        "space_amp",
        "threads",
        "batch_size",
        "key_size",
        "value_size",
        "range_size",
        "sync_enabled",
    ]
    df = to_numeric(df, numeric_cols)
    df = df.dropna(subset=["ops_per_sec", "engine", "operation"])
    os.makedirs(out_dir, exist_ok=True)

    plot_throughput(df, out_dir)
    plot_latency(df, out_dir)
    plot_latency_percentiles(df, out_dir)
    plot_variability(df, out_dir)
    plot_latency_stddev(df, out_dir)
    plot_resource_overview(df, out_dir)
    plot_amplification(df, out_dir)
    plot_operation_mix(df, out_dir)
    plot_engine_comparison(df, out_dir)
    
    plot_pattern_comparison(df, out_dir)
    plot_workload_comparison(df, out_dir)
    plot_sync_comparison(df, out_dir)
    plot_value_size_impact(df, out_dir)
    
    plot_speedup_ratios(df, out_dir)
    plot_latency_vs_throughput(df, out_dir)
    plot_per_test_comparison(df, out_dir)
    plot_summary_table(df, out_dir)
    plot_iterator_throughput(df, out_dir)
    
    available_ops = df["operation"].unique().tolist()
    sweep_params = ["threads", "batch_size", "value_size", "key_size"]
    
    for op in available_ops:
        if op == "ITER": 
            continue
        for param in sweep_params:
            if param in df.columns and df[df["operation"] == op][param].nunique() >= 2:
                plot_param_sweep(df, param, op, out_dir)
    
    if "RANGE" in available_ops and "range_size" in df.columns:
        if df[df["operation"] == "RANGE"]["range_size"].nunique() >= 2:
            plot_param_sweep(df, "range_size", "RANGE", out_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate graphs from benchtool CSV.")
    parser.add_argument("csv", help="Path to benchtool CSV file")
    parser.add_argument(
        "out_dir",
        nargs="?",
        default=None,
        help="Output directory for images (default: <csv_dir>/graphs)",
    )
    args = parser.parse_args()
    csv_path = args.csv
    out_dir = args.out_dir or os.path.join(os.path.dirname(csv_path) or ".", "graphs")
    generate_graphs(csv_path, out_dir)


if __name__ == "__main__":
    main()
