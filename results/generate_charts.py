#!/usr/bin/env python3
"""
Gera gráficos científicos de alta resolução a partir dos dados experimentais
consolidados em results/metrics_results_local.csv e results/metrics_results_swarm.csv.

Gráficos Teóricos e Empíricos:
  - 01_complexidade_mensagens.png: Cota Harmônica Teórica O(log N) vs. Testes Locais.
  - 02_espera_fila_fifo.png: Previsão Determinística FIFO (N-1)*mu_CS vs. Medição Local.
  - 03_efeito_localidade.png: Efeito de Localidade e Reuso do Token (Carga Normal vs. Desigual Local).
  - 04_overhead_rede_local_vs_nuvem.png: Comparativo Cruzado de Latência (Local Bridge vs. Nuvem OCI Swarm).
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
    fig, ax = plt.subplots(figsize=(8.5, 5.2))

    # Curva teórica Naimi-Tréhel: H_{N-1} + 1
    nodes_range = np.arange(2, 13)
    harmonic_upper = [sum(1.0 / i for i in range(1, n)) + 1.0 for n in nodes_range]

    # Plota curva teórica contínua
    ax.plot(nodes_range, harmonic_upper, color="#1f4e79", lw=2.6, marker="o", markersize=5.5,
            label=r"Cota Superior Harmônica Teórica: $\bar{M}(N) = H_{N-1} + 1$", zorder=3)

    # Área sombreada delimitando a região sub-harmônica válida
    ax.fill_between(nodes_range, 0, harmonic_upper, color="#1f4e79", alpha=0.08,
                    label="Região Teórica Sub-Harmônica (Naimi–Tréhel)")

    # Dados medidos locais (Médias das 3 repetições)
    # 5 nós
    mpe_5_norm = local_data[("normal_contention", 5)]["avg_mpe"]
    mpe_5_high = local_data[("high_contention", 5)]["avg_mpe"]
    mpe_5_uneq = local_data[("unequal_load", 5)]["avg_mpe"]

    # 10 nós
    mpe_10_norm = local_data[("normal_contention", 10)]["avg_mpe"]
    mpe_10_high = local_data[("high_contention", 10)]["avg_mpe"]
    mpe_10_uneq = local_data[("unequal_load", 10)]["avg_mpe"]

    # Scatter points para cada cenário com marcadores e cores acadêmicas distintas
    ax.scatter([5, 10], [mpe_5_norm, mpe_10_norm], color="#2e7d32", s=90, marker="s",
               label=f"Carga Normal (5: {mpe_5_norm:.2f} | 10: {mpe_10_norm:.2f})", zorder=5)
    ax.scatter([5, 10], [mpe_5_high, mpe_10_high], color="#d95f02", s=100, marker="D",
               label=f"Alta Contenção (5: {mpe_5_high:.2f} | 10: {mpe_10_high:.2f})", zorder=5)
    ax.scatter([5, 10], [mpe_5_uneq, mpe_10_uneq], color="#7570b3", s=90, marker="^",
               label=f"Carga Desigual (5: {mpe_5_uneq:.2f} | 10: {mpe_10_uneq:.2f})", zorder=5)

    # Anotações nas cotas teóricas de 5 e 10 nós
    h4 = sum(1.0 / i for i in range(1, 5)) + 1.0  # 3.083
    h9 = sum(1.0 / i for i in range(1, 10)) + 1.0 # 3.829

    ax.annotate(f"Cota $H_4 + 1 \\approx {h4:.2f}$\n(Máx. medido: {max(mpe_5_norm, mpe_5_high, mpe_5_uneq):.2f})",
                xy=(5, h4), xytext=(3.4, 3.75),
                arrowprops=dict(arrowstyle="->", color="#1f4e79", lw=1.2),
                bbox=dict(boxstyle="round,pad=0.35", fc="#eef4fa", ec="#1f4e79", alpha=0.95),
                fontsize=9, fontweight="bold")

    ax.annotate(f"Cota $H_9 + 1 \\approx {h9:.2f}$\n(Máx. medido: {max(mpe_10_norm, mpe_10_high, mpe_10_uneq):.2f})",
                xy=(10, h9), xytext=(7.8, 4.3),
                arrowprops=dict(arrowstyle="->", color="#1f4e79", lw=1.2),
                bbox=dict(boxstyle="round,pad=0.35", fc="#eef4fa", ec="#1f4e79", alpha=0.95),
                fontsize=9, fontweight="bold")

    # Destaque da taxa de crescimento logarítmica (ln(2) ≈ 0.69)
    ax.annotate(r"Crescimento $\approx \ln(2) \approx +0{,}69$ msgs ao dobrar $N$",
                xy=(7.5, 3.4), xytext=(6.2, 2.3),
                arrowprops=dict(arrowstyle="->", color="#555", lw=1.0, ls=":"),
                bbox=dict(boxstyle="round,pad=0.3", fc="#fff9c4", ec="#fbc02d", alpha=0.9),
                fontsize=8.5, fontweight="bold")

    ax.set_title("Complexidade de Mensagens de Naimi–Tréhel vs. Cota Teórica Harmônica", pad=12)
    ax.set_xlabel("Número de Processos Participantes (N)")
    ax.set_ylabel("Mensagens Médias por Entrada na Seção Crítica")
    ax.set_xticks(range(2, 13))
    ax.set_xlim(1.5, 11.5)
    ax.set_ylim(1.8, 4.8)
    ax.legend(loc="upper left", framealpha=0.95, edgecolor="#ddd")

    out_file = CHARTS_DIR / "01_complexidade_mensagens.png"
    plt.savefig(out_file)
    plt.close()
    print(f"[OK] Gerado: {out_file.name}")

# ==============================================================================
# GRÁFICO 2: Tempo de Espera Mediano em Alta Contenção (Fila FIFO Determinística)
# ==============================================================================
def plot_wait_fifo():
    fig, ax = plt.subplots(figsize=(7.5, 4.8))

    # Valores teóricos para Alta Contenção (mu_CS = 0.6s = 600ms):
    # Fila cheia com N-1 predecessores -> (N-1) * 600 ms
    theory_5 = (5 - 1) * 600.0   # 2400 ms
    theory_10 = (10 - 1) * 600.0 # 5400 ms

    # Valores locais medidos
    loc_5 = local_data[("high_contention", 5)]["avg_wait"]   # 2426.46 ms
    loc_10 = local_data[("high_contention", 10)]["avg_wait"] # 5317.99 ms

    labels = ["5 Nós", "10 Nós"]
    x = np.arange(len(labels))
    width = 0.30

    rects1 = ax.bar(x - width/2, [theory_5, theory_10], width,
                    label=r"Modelo Analítico FIFO: $(N-1) \times \mu_{CS}$ ($\mu_{CS}=600$ ms)",
                    color="#90a4ae", edgecolor="#37474f", hatch="//")
    rects2 = ax.bar(x + width/2, [loc_5, loc_10], width,
                    label="Mediana Medida (Alta Contenção)",
                    color="#1f4e79", edgecolor="#0d233a")

    # Valores acima das barras
    for rects in [rects1, rects2]:
        for r in rects:
            h = r.get_height()
            ax.annotate(f"{h:.1f} ms",
                        xy=(r.get_x() + r.get_width() / 2, h),
                        xytext=(0, 4), textcoords="offset points",
                        ha="center", va="bottom", fontsize=9.5, fontweight="bold")

    # Badges de aderência teórica / erro relativo
    err_5 = ((loc_5 - theory_5) / theory_5) * 100.0
    err_10 = ((loc_10 - theory_10) / theory_10) * 100.0

    ax.annotate(f"Desvio: {err_5:+.1f}%\n(+{loc_5 - theory_5:.1f} ms)",
                xy=(x[0] + width/2, loc_5), xytext=(x[0] + width + 0.08, loc_5 - 450),
                arrowprops=dict(arrowstyle="->", color="#1b5e20", lw=1.2),
                bbox=dict(boxstyle="round,pad=0.3", fc="#e8f5e9", ec="#2e7d32", alpha=0.95),
                fontsize=8.5, fontweight="bold", color="#1b5e20")

    ax.annotate(f"Desvio: {err_10:+.1f}%\n({loc_10 - theory_10:.1f} ms)",
                xy=(x[1] + width/2, loc_10), xytext=(x[1] + width + 0.08, loc_10 - 750),
                arrowprops=dict(arrowstyle="->", color="#1b5e20", lw=1.2),
                bbox=dict(boxstyle="round,pad=0.3", fc="#e8f5e9", ec="#2e7d32", alpha=0.95),
                fontsize=8.5, fontweight="bold", color="#1b5e20")

    ax.set_title("Tempo de Espera Mediano sob Alta Contenção: Previsão Teórica FIFO vs. Medição", pad=12)
    ax.set_ylabel("Tempo de Espera Mediano (ms)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontweight="bold", fontsize=11)
    ax.set_ylim(0, 6600)
    ax.legend(loc="upper left", framealpha=0.95, edgecolor="#ddd")

    out_file = CHARTS_DIR / "02_espera_fila_fifo.png"
    plt.savefig(out_file)
    plt.close()
    print(f"[OK] Gerado: {out_file.name}")

# ==============================================================================
# GRÁFICO 3: Efeito de Localidade do Token (Carga Normal vs. Carga Desigual)
# ==============================================================================
def plot_locality_effect():
    fig, ax = plt.subplots(figsize=(7.5, 4.8))

    # Comparativo: Carga Normal vs Carga Desigual nos testes Locais
    norm_5 = local_data[("normal_contention", 5)]["avg_mpe"]
    uneq_5 = local_data[("unequal_load", 5)]["avg_mpe"]

    norm_10 = local_data[("normal_contention", 10)]["avg_mpe"]
    uneq_10 = local_data[("unequal_load", 10)]["avg_mpe"]

    categories = ["Topologia 5 Nós", "Topologia 10 Nós"]
    normal_vals = [norm_5, norm_10]
    unequal_vals = [uneq_5, uneq_10]

    x = np.arange(len(categories))
    width = 0.28

    rects1 = ax.bar(x - width/2, normal_vals, width, label="Carga Normal (Distribuição Equilibrada)",
                    color="#4a7bb0", edgecolor="#244d7a")
    rects2 = ax.bar(x + width/2, unequal_vals, width, label="Carga Desigual (Rajada Rápida no Nó 1)",
                    color="#2ca25f", edgecolor="#006d2c")

    # Valores e percentuais de redução
    for i in range(len(categories)):
        v1 = normal_vals[i]
        v2 = unequal_vals[i]
        diff_pct = ((v2 - v1) / v1) * 100.0
        diff_abs = v2 - v1

        ax.annotate(f"{v1:.2f}",
                    xy=(rects1[i].get_x() + rects1[i].get_width() / 2, v1),
                    xytext=(0, 3), textcoords="offset points",
                    ha="center", va="bottom", fontsize=10, fontweight="bold")

        ax.annotate(f"{v2:.2f}",
                    xy=(rects2[i].get_x() + rects2[i].get_width() / 2, v2),
                    xytext=(0, 3), textcoords="offset points",
                    ha="center", va="bottom", fontsize=10, fontweight="bold")

        label_text = f"Economia de Mensagens:\n{diff_pct:.1f}% ({diff_abs:.2f} msgs/entrada)"
        ax.annotate(label_text,
                    xy=(x[i], max(v1, v2)), xytext=(0, 20), textcoords="offset points",
                    ha="center", va="bottom", fontsize=8.5, fontweight="bold",
                    color="#1b5e20",
                    bbox=dict(boxstyle="round,pad=0.3", fc="#e8f5e9", ec="#2e7d32", alpha=0.95))

    # Box explicativo teórico
    ax.text(0.5, 0.08,
            "Reuso Local do Token: nó detentor satisfaz rajadas com 0 mensagens na rede (has_token == True).",
            transform=ax.transAxes, ha="center", fontsize=8.5, style="italic",
            bbox=dict(boxstyle="square,pad=0.4", fc="#f5f5f5", ec="#ccc", alpha=0.9))

    ax.set_title("Efeito de Localidade: Otimização de Mensagens sob Carga Desigual", pad=12)
    ax.set_ylabel("Mensagens Médias por Entrada")
    ax.set_xticks(x)
    ax.set_xticklabels(categories, fontweight="bold", fontsize=11)
    ax.set_ylim(0, 4.4)
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
    rects2 = ax1.bar(x + width/2, swm_5, width, label="Nuvem OCI (Docker Swarm - 3 VMs)", color="#f28e2b", edgecolor="#b85d00")

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
    ax1.set_xticklabels(scenarios, fontsize=9.5, fontweight="bold")
    ax1.set_ylim(0, 3100)
    ax1.legend(loc="upper left", fontsize=9)

    # Subplot 2: 10 Nós
    rects3 = ax2.bar(x - width/2, loc_10, width, label="Local (Docker Compose)", color="#386cb0", edgecolor="#1a3b68")
    rects4 = ax2.bar(x + width/2, swm_10, width, label="Nuvem OCI (Docker Swarm - 3 VMs)", color="#f28e2b", edgecolor="#b85d00")

    for rects in [rects3, rects4]:
        for r in rects:
            h = r.get_height()
            ax2.annotate(f"{h:.1f}",
                         xy=(r.get_x() + r.get_width() / 2, h),
                         xytext=(0, 3), textcoords="offset points",
                         ha="center", va="bottom", fontsize=8.5, fontweight="bold")

    # Destaque do Overhead VXLAN na Alta Contenção de 10 nós
    delta_vxlan = swm_10[1] - loc_10[1]
    ax2.annotate(f"+{delta_vxlan:.1f} ms\n(Overhead VXLAN/RTT)",
                 xy=(x[1] + width/2, swm_10[1]), xytext=(x[1] + width/2 + 0.15, swm_10[1] - 800),
                 arrowprops=dict(arrowstyle="->", color="#b85d00", lw=1.2),
                 bbox=dict(boxstyle="round,pad=0.3", fc="#fff3e0", ec="#ffb74d", alpha=0.95),
                 fontsize=8.5, fontweight="bold", color="#a03b00")

    ax2.set_title("Topologia com 10 Nós", fontsize=12, pad=10)
    ax2.set_ylabel("Tempo de Espera Mediano (ms)")
    ax2.set_xticks(x)
    ax2.set_xticklabels(scenarios, fontsize=9.5, fontweight="bold")
    ax2.set_ylim(0, 6800)
    ax2.legend(loc="upper left", fontsize=9)

    fig.suptitle("Comparativo Cruzado de Latência: Efeito do RTT e Encapsulamento VXLAN", fontsize=13, y=1.02, fontweight="bold")
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
