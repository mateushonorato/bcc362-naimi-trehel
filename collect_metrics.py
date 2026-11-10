#!/usr/bin/env python3
"""
collect_metrics.py — Automação de Repetições e Coleta de Métricas para TP2 (BCC362)
Executa as baterias de testes exigidas pela Seção 7 do enunciado do TP2:
- Pelo menos 3 repetições para Carga Normal e Alta Contenção
- Suporte a 5 e 10 nós (experimento de escala)
- Suporte a execução Local (Docker Compose) e Nuvem (Docker Swarm)
- Coleta métricas de latência (mediana em ms), tempo total, mensagens por entrada e colisões
- Exporta dados completos para JSON e CSV
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import time
import urllib.request

OBSERVER_URL = "http://localhost:9000"

def run_cmd(cmd, env=None, check=True):
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    res = subprocess.run(cmd, shell=True, env=full_env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if check and res.returncode != 0:
        raise RuntimeError(f"Comando falhou (code {res.returncode}): {cmd}\nStderr: {res.stderr}")
    return res

def wait_for_http(url, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.5)
    return False

def get_json(url, timeout=5):
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"[WARN] Erro ao consultar {url}: {e}")
        return None

def get_compose_args(mode, nodes, exp_name):
    if mode == "local":
        args = ["-f", "docker-compose.yml"]
        if nodes == 10:
            args.extend(["-f", "docker-compose-10nodes.yml"])
        if "unequal" in exp_name:
            args.extend(["-f", "docker-compose-unequal.yml"])
        return " ".join(args)
    else:
        args = ["-c", "docker-stack.yml"]
        if nodes == 10:
            args.extend(["-c", "docker-stack-10nodes.yml"])
        if "unequal" in exp_name:
            args.extend(["-c", "docker-stack-unequal.yml"])
        return " ".join(args)

def start_cluster(mode, nodes, exp_name, env_vars):
    if mode == "local":
        file_args = get_compose_args("local", nodes, exp_name)
        print(f"[*] [Local] Subindo cluster de {nodes} nós ({file_args})...")
        run_cmd(f"docker compose {file_args} down", check=False)
        run_cmd(f"docker compose {file_args} up -d", env=env_vars)
    elif mode == "swarm":
        file_args = get_compose_args("swarm", nodes, exp_name)
        print(f"[*] [Swarm] Fazendo deploy da stack com {file_args}...")
        run_cmd("docker stack rm clustersync", check=False)
        time.sleep(10)  # aguarda limpeza de redes e serviços no Swarm
        run_cmd(f"docker stack deploy {file_args} clustersync", env=env_vars)

def stop_cluster(mode, nodes, exp_name="normal"):
    if mode == "local":
        file_args = get_compose_args("local", nodes, exp_name)
        print(f"[*] [Local] Finalizando cluster ({file_args})...")
        run_cmd(f"docker compose {file_args} down", check=False)
    elif mode == "swarm":
        print("[*] [Swarm] Removendo stack clustersync...")
        run_cmd("docker stack rm clustersync", check=False)
        time.sleep(5)

def wait_completion(expected_total, timeout=300):
    print(f"[*] Aguardando conclusão de {expected_total} pedidos no Observador (timeout={timeout}s)...")
    start = time.time()
    last_completed = -1
    while time.time() - start < timeout:
        stats = get_json(f"{OBSERVER_URL}/stats")
        if stats:
            completed = stats.get("total_completed", 0)
            if completed != last_completed:
                print(f"    -> Progresso: {completed}/{expected_total} pedidos concluídos (ativos={len(stats.get('active_now', []))})")
                last_completed = completed
            if completed >= expected_total:
                return stats, time.time() - start
        time.sleep(1.0)
    raise TimeoutError(f"Tempo limite excedido ({timeout}s) esperando {expected_total} pedidos. Último concluído: {last_completed}")

def collect_node_metrics(nodes):
    node_data = {}
    total_protocol_msgs = 0
    total_acquisitions = 0
    node_medians = []

    for i in range(1, nodes + 1):
        port = 8000 + i
        url = f"http://localhost:{port}/status"
        st = get_json(url)
        if st:
            node_id = st.get("node_id", f"node{i}")
            msgs = st.get("protocol_msgs_sent", 0)
            acqs = st.get("acquisitions", 0)
            med = st.get("median_wait_ms", 0.0)
            node_data[node_id] = {
                "protocol_msgs_sent": msgs,
                "acquisitions": acqs,
                "median_wait_ms": med,
                "has_token": st.get("has_token", False)
            }
            total_protocol_msgs += msgs
            total_acquisitions += acqs
            node_medians.append(med)
        else:
            print(f"[WARN] Nó na porta {port} não respondeu.")

    overall_median = sorted(node_medians)[len(node_medians) // 2] if node_medians else 0.0
    msgs_per_entry = round(total_protocol_msgs / total_acquisitions, 3) if total_acquisitions > 0 else 0.0

    return {
        "nodes": node_data,
        "total_protocol_msgs": total_protocol_msgs,
        "total_acquisitions": total_acquisitions,
        "overall_median_wait_ms": overall_median,
        "msgs_per_entry": msgs_per_entry
    }

def run_single_experiment(mode, nodes, exp_name, rep_idx, num_requests, cs_min, cs_max, client_min, client_max, client1_min=None, client1_max=None, sync_enabled=True):
    expected_total = nodes * num_requests
    seed = 42 + rep_idx * 10

    env_vars = {
        "SYNC_ENABLED": "true" if sync_enabled else "false",
        "NUM_REQUESTS": str(num_requests),
        "CS_MIN_SLEEP": str(cs_min),
        "CS_MAX_SLEEP": str(cs_max),
        "CLIENT_MIN_SLEEP": str(client_min),
        "CLIENT_MAX_SLEEP": str(client_max),
        "RANDOM_SEED": str(seed),
        "CLIENT_TIMEOUT": "300.0"
    }

    print("\n" + "="*80)
    print(f"EXPOSIÇÃO: {exp_name.upper()} | NÓS: {nodes} | REPETIÇÃO: {rep_idx} | SEMENTE: {seed} | SYNC: {sync_enabled}")
    if exp_name == "unequal":
        print(f"Configuração: {num_requests} req/cliente | Total esperado: {expected_total} | CS: [{cs_min}s, {cs_max}s] | Sleep client1: [0.1s, 0.3s] (override) | Sleep outros: [{client_min}s, {client_max}s]")
    else:
        print(f"Configuração: {num_requests} req/cliente | Total esperado: {expected_total} | CS: [{cs_min}s, {cs_max}s] | Sleep cliente: [{client_min}s, {client_max}s]")
    print("="*80)

    try:
        start_cluster(mode, nodes, exp_name, env_vars)
        if not wait_for_http(f"{OBSERVER_URL}/stats", timeout=30):
            raise RuntimeError("Observador não respondeu na porta 9000.")

        wall_start = time.time()
        obs_stats, wait_duration = wait_completion(expected_total, timeout=360)
        wall_time = round(time.time() - wall_start, 2)

        # Coleta telemetria de cada nó
        metrics = collect_node_metrics(nodes)

        result = {
            "experiment": exp_name,
            "mode": mode,
            "nodes_count": nodes,
            "repetition": rep_idx,
            "seed": seed,
            "num_requests_per_client": num_requests,
            "expected_total": expected_total,
            "total_completed": obs_stats.get("total_completed", 0),
            "overlap_events": obs_stats.get("overlap_events", 0),
            "max_concurrent": obs_stats.get("max_concurrent", 0),
            "wall_time_s": wall_time,
            "cs_duration_range": [cs_min, cs_max],
            "client_sleep_range": [client_min, client_max],
            "total_protocol_msgs": metrics["total_protocol_msgs"],
            "total_acquisitions": metrics["total_acquisitions"],
            "msgs_per_entry": metrics["msgs_per_entry"],
            "overall_median_wait_ms": metrics["overall_median_wait_ms"],
            "nodes_detail": metrics["nodes"]
        }

        print("\n[OK] Resultado da repetição:")
        print(f"     Concluídos: {result['total_completed']}/{expected_total} | Sobreposições: {result['overlap_events']} (max={result['max_concurrent']})")
        print(f"     Tempo total: {wall_time}s | Espera mediana: {result['overall_median_wait_ms']}ms | Msgs/entrada: {result['msgs_per_entry']}")

        return result

    finally:
        stop_cluster(mode, nodes, exp_name)

def export_results(all_results, json_path, csv_path):
    # Exporta JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    print(f"\n[+] Dados completos salvos em JSON: {json_path}")

    # Exporta CSV
    if all_results:
        fields = [
            "experiment", "mode", "nodes_count", "repetition", "seed",
            "num_requests_per_client", "expected_total", "total_completed",
            "overlap_events", "max_concurrent", "wall_time_s",
            "total_protocol_msgs", "total_acquisitions", "msgs_per_entry",
            "overall_median_wait_ms"
        ]
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for r in all_results:
                writer.writerow(r)
        print(f"[+] Tabela comparativa salva em CSV: {csv_path}")

def print_summary_table(all_results):
    print("\n" + "#"*90)
    print("RESUMO CONSOLIDADO DAS MÉTRICAS — BCC362 (TP2)")
    print("#"*90)
    header = f"{'Experimento':<18} | {'Nós':<4} | {'Rep':<3} | {'Concl.':<7} | {'Overlaps':<8} | {'Tempo (s)':<9} | {'Msgs/Entrada':<12} | {'Espera Med (ms)':<15}"
    print(header)
    print("-" * len(header))
    for r in all_results:
        exp = r["experiment"]
        n = r["nodes_count"]
        rep = r["repetition"]
        conc = f"{r['total_completed']}/{r['expected_total']}"
        over = f"{r['overlap_events']} (max {r['max_concurrent']})"
        t = f"{r['wall_time_s']:.2f}"
        mpe = f"{r['msgs_per_entry']:.3f}"
        med = f"{r['overall_median_wait_ms']:.2f}"
        print(f"{exp:<18} | {n:<4} | {rep:<3} | {conc:<7} | {over:<8} | {t:<9} | {mpe:<12} | {med:<15}")
    print("#"*90 + "\n")

def main():
    parser = argparse.ArgumentParser(description="Automação de Testes de Métricas — TP2 Sistemas Distribuídos")
    parser.add_argument("--mode", choices=["local", "swarm"], default="local", help="Ambiente de execução (default: local)")
    parser.add_argument("--nodes", choices=["5", "10", "both"], default="both", help="Quantidade de nós do cluster (default: both)")
    parser.add_argument("--experiments", nargs="+", choices=["disabled", "normal", "high", "unequal", "all"], default=["all"], help="Experimentos a executar (default: all)")
    parser.add_argument("--repetitions", type=int, default=3, help="Número de repetições por experimento (default: 3)")
    parser.add_argument("--requests", type=int, default=10, help="Quantidade de requisições por cliente (default: 10)")
    parser.add_argument("--append", action="store_true", help="Acrescenta aos resultados existentes em vez de sobrescrever")
    parser.add_argument("--json-out", default="metrics_results.json", help="Arquivo JSON de saída (default: metrics_results.json)")
    parser.add_argument("--csv-out", default="metrics_results.csv", help="Arquivo CSV de saída (default: metrics_results.csv)")

    args = parser.parse_args()

    # Define quais nós rodar
    node_list = [5, 10] if args.nodes == "both" else [int(args.nodes)]

    # Define quais experimentos rodar
    if "all" in args.experiments:
        exp_list = ["disabled", "normal", "high", "unequal"]
    else:
        exp_list = args.experiments

    results = []
    if args.append and os.path.exists(args.json_out):
        try:
            with open(args.json_out, "r", encoding="utf-8") as f:
                results = json.load(f)
        except Exception:
            results = []

    for nodes in node_list:
        for exp in exp_list:
            c1_min, c1_max = None, None
            if exp == "disabled":
                c_min, c_max = 0.0, 0.0
                sync = False
                exp_name = "disabled_sync"
                num_reps = 1 if args.repetitions > 1 else 1
            elif exp == "normal":
                c_min, c_max = 1.0, 5.0
                sync = True
                exp_name = "normal_contention"
                num_reps = args.repetitions
            elif exp == "unequal":
                c_min, c_max = 1.0, 5.0
                c1_min, c1_max = 0.1, 0.3
                sync = True
                exp_name = "unequal_load"
                num_reps = args.repetitions
            else:  # high contention
                c_min, c_max = 0.0, 0.0
                sync = True
                exp_name = "high_contention"
                num_reps = args.repetitions

            for rep in range(1, num_reps + 1):
                res = run_single_experiment(
                    mode=args.mode,
                    nodes=nodes,
                    exp_name=exp_name,
                    rep_idx=rep,
                    num_requests=args.requests,
                    cs_min=0.2,
                    cs_max=1.0,
                    client_min=c_min,
                    client_max=c_max,
                    client1_min=c1_min,
                    client1_max=c1_max,
                    sync_enabled=sync
                )
                results.append(res)
                # Salva incrementalmente para não perder dados se for interrompido
                export_results(results, args.json_out, args.csv_out)

    print_summary_table(results)

if __name__ == "__main__":
    main()
