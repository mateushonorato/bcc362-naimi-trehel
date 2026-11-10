#!/usr/bin/env python3
"""
Gera gráficos científicos de alta resolução a partir dos dados experimentais
consolidados em results/metrics_results_local.csv e results/metrics_results_swarm.csv.
"""

import csv
import math
import os
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

# Configuração de estilo visual acadêmico e moderno
plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "axes.labelsize": 11,
    "axes.labelweight": "bold",
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 10,
    "figure.titlesize": 14,
    "figure.dpi": 300,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "axes.grid": True,
    "grid.alpha": 0.25,
    "grid.linestyle": "--"
})

BASE_DIR = Path(__file__).resolve().parent
CHARTS_DIR = BASE_DIR / "charts"
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

LOCAL_CSV = BASE_DIR / "metrics_results_local.csv"
SWARM_CSV = BASE_DIR / "metrics_results_swarm.csv"

def load_averages(csv_file):
    with open(csv_file, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    stats = {}
    for r in rows:
        key = (r["experiment"], int(r["nodes_count"]))
        if key not in stats:
            stats[key] = {"time": [], "mpe": [], "wait": [], "over": [], "mc": []}
        stats[key]["time"].append(float(r["wall_time_s"]))
        stats[key]["mpe"].append(float(r["msgs_per_entry"]))
        stats[key]["wait"].append(float(r["overall_median_wait_ms"]))
        stats[key]["over"].append(int(r["overlap_events"]))
        stats[key]["mc"].append(int(r["max_concurrent"]))

    res = {}
    for k, v in stats.items():
        res[k] = {
            "avg_time": sum(v["time"]) / len(v["time"]),
            "avg_mpe": sum(v["mpe"]) / len(v["mpe"]),
            "avg_wait": sum(v["wait"]) / len(v["wait"]),
            "tot_over": sum(v["over"]),
            "max_mc": max(v["mc"])
        }
    return res

local_data = load_averages(LOCAL_CSV)
swarm_data = load_averages(SWARM_CSV)

# ==============================================================================
# GRÁFICO 1: Conformidade com a Complexidade Teórica O(log N) de Naimi-Tréhel
# ==============================================================================
def plot_complexity():
    fig, ax = plt.subplots(figsize=(9, 5.5))

    # Curva teórica Naimi-Tréhel: H_{N-1} + 1
    nodes_range = np.arange(2, 13)
    harmonic_upper = [sum(1.0 / i for i in range(1, n)) + 1.0 for n in nodes_range]

    # Modelo linear (ex: Token Ring ~ N/2)
    linear_ring = [n / 2.0 for n in nodes_range]

    # Plota curvas de referência
    ax.plot(nodes_range, harmonic_upper, color="#1f4e79", lw=2.5, marker="o", markersize=6,
            label=r"Cota Superior Harmônica de Naimi–Tréhel: $\bar{M}(N) = H_{N-1} + 1$", zorder=3)
    ax.plot(nodes_range, linear_ring, color="#b22222", lw=2.0, ls="--",
            label=r"Algoritmo Linear em Anel (Token Ring): $\approx N/2$", zorder=2)

    # Área sombreada delimitando a região sub-harmônica
    ax.fill_between(nodes_range, 0, harmonic_upper, color="#1f4e79", alpha=0.08,
                    label="Região Teórica Sub-Harmônica")

    # Pontos empíricos medidos
    # N = 5
    mpe_5_norm_loc = local_data[("normal_contention", 5)]["avg_mpe"]
    mpe_5_norm_swm = swarm_data[("normal_contention", 5)]["avg_mpe"]
    mpe_5_high_swm = swarm_data[("high_contention", 5)]["avg_mpe"]
    mpe_5_uneq_swm = swarm_data[("unequal_load", 5)]["avg_mpe"]

    # N = 10
    mpe_10_norm_loc = local_data[("normal_contention", 10)]["avg_mpe"]
    mpe_10_norm_swm = swarm_data[("normal_contention", 10)]["avg_mpe"]
    mpe_10_high_swm = swarm_data[("high_contention", 10)]["avg_mpe"]
    mpe_10_uneq_swm = swarm_data[("unequal_load", 10)]["avg_mpe"]

    ax.scatter([5, 10], [mpe_5_norm_loc, mpe_10_norm_loc], color="#2e7d32", s=90, marker="s",
               label=f"Empírico Local (Carga Normal): 5={mpe_5_norm_loc:.2f}, 10={mpe_10_norm_loc:.2f}", zorder=4)
    ax.scatter([5, 10], [mpe_5_norm_swm, mpe_10_norm_swm], color="#d95f02", s=110, marker="^",
               label=f"Empírico Nuvem OCI (Carga Normal): 5={mpe_5_norm_swm:.2f}, 10={mpe_10_norm_swm:.2f}", zorder=5)
    ax.scatter([5, 10], [mpe_5_high_swm, mpe_10_high_swm], color="#7570b3", s=80, marker="D",
               label=f"Empírico Nuvem OCI (Alta Contenção): 5={mpe_5_high_swm:.2f}, 10={mpe_10_high_swm:.2f}", zorder=4)
    ax.scatter([5, 10], [mpe_5_uneq_swm, mpe_10_uneq_swm], color="#008080", s=80, marker="v",
               label=f"Empírico Nuvem OCI (Carga Desigual): 5={mpe_5_uneq_swm:.2f}, 10={mpe_10_uneq_swm:.2f}", zorder=4)

    # Anotações nos pontos principais
    ax.annotate(r"$H_4 + 1 = 3{,}08$" + "\n" + f"Nuvem: {mpe_5_norm_swm:.2f}",
                xy=(5, mpe_5_norm_swm), xytext=(5.3, 2.2),
                arrowprops=dict(arrowstyle="->", color="#333", lw=1.2),
                bbox=dict(boxstyle="round,pad=0.3", fc="#fff", ec="#bbb", alpha=0.9))

    ax.annotate(r"$H_9 + 1 = 3{,}83$" + "\n" + f"Nuvem: {mpe_10_norm_swm:.2f}",
                xy=(10, mpe_10_norm_swm), xytext=(8.6, 4.4),
                arrowprops=dict(arrowstyle="->", color="#333", lw=1.2),
                bbox=dict(boxstyle="round,pad=0.3", fc="#fff", ec="#bbb", alpha=0.9))

    ax.set_title("Conformidade com a Complexidade Logarítmica de Mensagens — O(log N)", pad=12)
    ax.set_xlabel("Número de Processos Participantes (N)")
    ax.set_ylabel("Média de Mensagens por Entrada na Seção Crítica")
    ax.set_xticks(range(2, 13))
    ax.set_xlim(1.5, 12.5)
    ax.set_ylim(0, 6.5)
    ax.legend(loc="upper left", framealpha=0.95, edgecolor="#ddd")

    out_file = CHARTS_DIR / "01_complexidade_mensagens.png"
    plt.savefig(out_file)
    plt.close()
    print(f"[OK] Gerado: {out_file.name}")

# ==============================================================================
# GRÁFICO 2: Tempo de Espera Mediano em Alta Contenção (Fila FIFO Determinística)
# ==============================================================================
def plot_wait_fifo():
    fig, ax = plt.subplots(figsize=(8, 5))

    # Valores teóricos para Alta Contenção (mu_CS = 0.6s = 600ms):
    # Fila cheia com N-1 predecessores -> (N-1) * 600 ms
    theory_5 = (5 - 1) * 600.0   # 2400 ms
    theory_10 = (10 - 1) * 600.0 # 5400 ms

    loc_5 = local_data[("high_contention", 5)]["avg_wait"]
    loc_10 = local_data[("high_contention", 10)]["avg_wait"]

    swm_5 = swarm_data[("high_contention", 5)]["avg_wait"]
    swm_10 = swarm_data[("high_contention", 10)]["avg_wait"]

    labels = ["5 Nós", "10 Nós"]
    x = np.arange(len(labels))
    width = 0.25

    rects1 = ax.bar(x - width, [theory_5, theory_10], width, label=r"Modelo Analítico FIFO: $(N-1) \times \mu_{CS}$",
                    color="#90a4ae", edgecolor="#546e7a", hatch="//")
    rects2 = ax.bar(x, [loc_5, loc_10], width, label="Empírico Local (Docker Compose)",
                    color="#2b5c8f", edgecolor="#1a365d")
    rects3 = ax.bar(x + width, [swm_5, swm_10], width, label="Empírico Nuvem (OCI Swarm - 3 VMs)",
                    color="#d95f02", edgecolor="#a03b00")

    # Valores nas barras
    for rects in [rects1, rects2, rects3]:
        for r in rects:
            h = r.get_height()
            ax.annotate(f"{h:.0f} ms",
                        xy=(r.get_x() + r.get_width() / 2, h),
                        xytext=(0, 4), textcoords="offset points",
                        ha="center", va="bottom", fontsize=9, fontweight="bold")

    # Destaque do overhead de rede no caso de 10 nós
    delta = swm_10 - loc_10
    ax.annotate(f"Overhead VXLAN/OCI:\n+{delta:.1f} ms",
                xy=(x[1] + width, swm_10), xytext=(x[1] + width + 0.18, swm_10 - 400),
                arrowprops=dict(arrowstyle="->", color="#d95f02", lw=1.2),
                bbox=dict(boxstyle="round,pad=0.3", fc="#fff3e0", ec="#ffb74d", alpha=0.9),
                fontsize=8.5, fontweight="bold")

    ax.set_title("Tempo de Espera Mediano em Alta Contenção: Fila FIFO vs. Medições", pad=12)
    ax.set_ylabel("Tempo de Espera Mediano (ms)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontweight="bold")
    ax.set_ylim(0, 6800)
    ax.legend(loc="upper left", framealpha=0.95, edgecolor="#ddd")

    out_file = CHARTS_DIR / "02_espera_fila_fifo.png"
    plt.savefig(out_file)
    plt.close()
    print(f"[OK] Gerado: {out_file.name}")

# ==============================================================================
# GRÁFICO 3: Efeito de Localidade do Token (Carga Normal vs. Carga Desigual)
# ==============================================================================
def plot_locality_effect():
    fig, ax = plt.subplots(figsize=(8, 5))

    # Comparativo: Carga Normal vs Carga Desigual
    # 5 nós Local
    norm_loc_5 = local_data[("normal_contention", 5)]["avg_mpe"]
    uneq_loc_5 = local_data[("unequal_load", 5)]["avg_mpe"]

    # 5 nós Nuvem
    norm_swm_5 = swarm_data[("normal_contention", 5)]["avg_mpe"]
    uneq_swm_5 = swarm_data[("unequal_load", 5)]["avg_mpe"]

    # 10 nós Local
    norm_loc_10 = local_data[("normal_contention", 10)]["avg_mpe"]
    uneq_loc_10 = local_data[("unequal_load", 10)]["avg_mpe"]

    # 10 nós Nuvem
    norm_swm_10 = swarm_data[("normal_contention", 10)]["avg_mpe"]
    uneq_swm_10 = swarm_data[("unequal_load", 10)]["avg_mpe"]

    categories = ["5 Nós (Local)", "5 Nós (Nuvem OCI)", "10 Nós (Local)", "10 Nós (Nuvem OCI)"]
    normal_vals = [norm_loc_5, norm_swm_5, norm_loc_10, norm_swm_10]
    unequal_vals = [uneq_loc_5, uneq_swm_5, uneq_loc_10, uneq_swm_10]

    x = np.arange(len(categories))
    width = 0.35

    rects1 = ax.bar(x - width/2, normal_vals, width, label="Carga Normal (Distribuição Equilibrada)",
                    color="#4a7bb0", edgecolor="#244d7a")
    rects2 = ax.bar(x + width/2, unequal_vals, width, label="Carga Desigual (Client 1 com Rajada Rápida)",
                    color="#2ca25f", edgecolor="#006d2c")

    # Valores e percentual de redução
    for i in range(len(categories)):
        v1 = normal_vals[i]
        v2 = unequal_vals[i]
        diff_pct = ((v2 - v1) / v1) * 100.0

        ax.annotate(f"{v1:.2f}",
                    xy=(rects1[i].get_x() + rects1[i].get_width() / 2, v1),
                    xytext=(0, 3), textcoords="offset points",
                    ha="center", va="bottom", fontsize=9, fontweight="bold")

        ax.annotate(f"{v2:.2f}",
                    xy=(rects2[i].get_x() + rects2[i].get_width() / 2, v2),
                    xytext=(0, 3), textcoords="offset points",
                    ha="center", va="bottom", fontsize=9, fontweight="bold")

        if diff_pct < 0:
            badge_color = "#e8f5e9"
            border_color = "#2e7d32"
            text_color = "#1b5e20"
            label_text = f"Economia:\n{diff_pct:.1f}%"
        else:
            badge_color = "#fff3e0"
            border_color = "#e65100"
            text_color = "#b71c1c"
            label_text = f"{diff_pct:+.1f}%"

        ax.annotate(label_text,
                    xy=(x[i], max(v1, v2)), xytext=(0, 18), textcoords="offset points",
                    ha="center", va="bottom", fontsize=8, fontweight="bold",
                    color=text_color,
                    bbox=dict(boxstyle="round,pad=0.2", fc=badge_color, ec=border_color, alpha=0.9))

    ax.set_title("Efeito de Localidade: Otimização de Mensagens por Reversão de Caminhos", pad=14)
    ax.set_ylabel("Média de Mensagens por Entrada")
    ax.set_xticks(x)
    ax.set_xticklabels(categories, fontweight="bold")
    ax.set_ylim(0, 4.5)
    ax.legend(loc="upper left", framealpha=0.95, edgecolor="#ddd")

    out_file = CHARTS_DIR / "03_efeito_localidade.png"
    plt.savefig(out_file)
    plt.close()
    print(f"[OK] Gerado: {out_file.name}")

# ==============================================================================
# GRÁFICO 4: Comparativo Cruzado de Latência de Espera (Local vs. Nuvem OCI)
# ==============================================================================
def plot_cross_comparison():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.8), sharey=False)

    scenarios = ["Carga Normal", "Alta Contenção", "Carga Desigual"]

    # 5 Nós
    loc_5 = [
        local_data[("normal_contention", 5)]["avg_wait"],
        local_data[("high_contention", 5)]["avg_wait"],
        local_data[("unequal_load", 5)]["avg_wait"]
    ]
    swm_5 = [
        swarm_data[("normal_contention", 5)]["avg_wait"],
        swarm_data[("high_contention", 5)]["avg_wait"],
        swarm_data[("unequal_load", 5)]["avg_wait"]
    ]

    # 10 Nós
    loc_10 = [
        local_data[("normal_contention", 10)]["avg_wait"],
        local_data[("high_contention", 10)]["avg_wait"],
        local_data[("unequal_load", 10)]["avg_wait"]
    ]
    swm_10 = [
        swarm_data[("normal_contention", 10)]["avg_wait"],
        swarm_data[("high_contention", 10)]["avg_wait"],
        swarm_data[("unequal_load", 10)]["avg_wait"]
    ]

    x = np.arange(len(scenarios))
    width = 0.35

    # Subplot 1: 5 Nós
    rects1 = ax1.bar(x - width/2, loc_5, width, label="Local (Docker Compose)", color="#386cb0", edgecolor="#1a3b68")
    rects2 = ax1.bar(x + width/2, swm_5, width, label="Nuvem (OCI Swarm - 3 VMs)", color="#f28e2b", edgecolor="#b85d00")

    for rects in [rects1, rects2]:
        for r in rects:
            h = r.get_height()
            ax1.annotate(f"{h:.1f}",
                         xy=(r.get_x() + r.get_width() / 2, h),
                         xytext=(0, 3), textcoords="offset points",
                         ha="center", va="bottom", fontsize=8.5, fontweight="bold")

    ax1.set_title("Topologia com 5 Nós", fontsize=12, pad=10)
    ax1.set_ylabel("Tempo de Espera Mediano (ms)")
    ax1.set_xticks(x)
    ax1.set_xticklabels(scenarios, fontsize=9.5)
    ax1.set_ylim(0, 3100)
    ax1.legend(loc="upper left", fontsize=9)

    # Subplot 2: 10 Nós
    rects3 = ax2.bar(x - width/2, loc_10, width, label="Local (Docker Compose)", color="#386cb0", edgecolor="#1a3b68")
    rects4 = ax2.bar(x + width/2, swm_10, width, label="Nuvem (OCI Swarm - 3 VMs)", color="#f28e2b", edgecolor="#b85d00")

    for rects in [rects3, rects4]:
        for r in rects:
            h = r.get_height()
            ax2.annotate(f"{h:.1f}",
                         xy=(r.get_x() + r.get_width() / 2, h),
                         xytext=(0, 3), textcoords="offset points",
                         ha="center", va="bottom", fontsize=8.5, fontweight="bold")

    ax2.set_title("Topologia com 10 Nós", fontsize=12, pad=10)
    ax2.set_ylabel("Tempo de Espera Mediano (ms)")
    ax2.set_xticks(x)
    ax2.set_xticklabels(scenarios, fontsize=9.5)
    ax2.set_ylim(0, 6800)
    ax2.legend(loc="upper left", fontsize=9)

    fig.suptitle("Comparativo Cruzado de Latência: Impacto da Infraestrutura (Local vs. Nuvem OCI)", fontsize=13, y=1.02, fontweight="bold")
    plt.tight_layout()

    out_file = CHARTS_DIR / "04_overhead_rede_local_vs_nuvem.png"
    plt.savefig(out_file)
    plt.close()
    print(f"[OK] Gerado: {out_file.name}")

if __name__ == "__main__":
    print("[*] Gerando gráficos a partir dos dados consolidados em results/...")
    plot_complexity()
    plot_wait_fifo()
    plot_locality_effect()
    plot_cross_comparison()
    print("[+] Todos os 4 gráficos foram gerados com sucesso em results/charts/!")
