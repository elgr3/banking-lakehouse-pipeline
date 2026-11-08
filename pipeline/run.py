"""End-to-end orchestration: generate -> dbt build (initial load) -> dbt build (incremental) -> metrics.

Usage:
    python -m pipeline run                       # full volume (~500k transactions)
    python -m pipeline run --n-transactions 20000 --n-customers 2000 --n-merchants 300   # CI sample

Every number written to reports/metrics.json is measured from the run itself
(DuckDB counts and dbt's run_results.json); nothing is hard-coded.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from generator.generate import GenConfig, generate

ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE = ROOT / "warehouse" / "lakehouse.duckdb"
REPORTS = ROOT / "reports"


def log(msg: str) -> None:
    print(f"[pipeline {datetime.now():%H:%M:%S}] {msg}", flush=True)


def dbt_executable() -> str:
    scripts = Path(sys.executable).parent
    for name in ("dbt.exe", "dbt"):
        if (scripts / name).exists():
            return str(scripts / name)
    found = shutil.which("dbt")
    if not found:
        raise SystemExit("dbt executable not found - install requirements.txt in the active environment")
    return found


def run_dbt(args: list[str], phase: str) -> dict:
    """Run dbt in a subprocess (so the DuckDB file lock is released afterwards)."""
    cmd = [dbt_executable(), *args, "--profiles-dir", str(ROOT), "--project-dir", str(ROOT)]
    log(f"{phase}: {' '.join(args)}")
    t0 = time.perf_counter()
    env = {**os.environ, "DUCKDB_PATH": str(WAREHOUSE)}
    rr = ROOT / "target" / "run_results.json"
    rr.unlink(missing_ok=True)
    proc = subprocess.run(cmd, cwd=ROOT, env=env)
    elapsed = time.perf_counter() - t0
    if not rr.exists():
        raise SystemExit(f"dbt {phase} failed before producing run_results.json (exit code {proc.returncode})")
    results = json.loads(rr.read_text(encoding="utf-8"))
    summary = summarize_run_results(results)
    summary["seconds"] = round(elapsed, 2)  # wall clock incl. dbt process start-up and parsing
    summary["dbt_reported_elapsed_seconds"] = round(results.get("elapsed_time", 0.0), 2)  # execution only
    summary["exit_code"] = proc.returncode
    # keep a copy of each phase's run_results for traceability
    shutil.copy(ROOT / "target" / "run_results.json", REPORTS / f"run_results_{phase}.json")
    return summary


def summarize_run_results(results: dict) -> dict:
    tests = [r for r in results["results"] if r["unique_id"].startswith("test.")]
    models = [r for r in results["results"] if r["unique_id"].startswith("model.")]
    seeds = [r for r in results["results"] if r["unique_id"].startswith("seed.")]

    def count(rows, status):
        return sum(1 for r in rows if r["status"] == status)

    return {
        "models_total": len(models),
        "models_success": count(models, "success"),
        "seeds_total": len(seeds),
        "tests_total": len(tests),
        "tests_passed": count(tests, "pass"),
        "tests_warned": count(tests, "warn"),
        "tests_failed": count(tests, "fail"),
        "tests_errored": count(tests, "error"),
        "tests_skipped": count(tests, "skipped"),
        "failed_tests": [r["unique_id"] for r in tests if r["status"] in ("fail", "error")],
    }


def scalar(con, sql: str):
    return con.execute(sql).fetchone()[0]


def collect_data_metrics(labels_path: Path) -> dict:
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    try:
        raw = {t: scalar(con, f"select count(*) from bronze.bronze_{t}")
               for t in ("customers", "accounts", "merchants", "fx_rates", "transactions")}
        clean = {
            "customers": scalar(con, "select count(*) from silver.silver_customers"),
            "accounts": scalar(con, "select count(*) from silver.silver_accounts"),
            "merchants": scalar(con, "select count(*) from silver.silver_merchants"),
            "fx_rates": scalar(con, "select count(*) from silver.silver_fx_rates"),
            "transactions": scalar(con, "select count(*) from silver.silver_transactions"),
        }
        rejected = scalar(con, "select count(*) from silver.silver_transactions_rejected")
        valid_rows = scalar(con, "select count(*) from silver.int_transactions_validated where rejection_reason is null")
        by_reason = dict(con.execute(
            "select rejection_reason, count(*) from silver.silver_transactions_rejected group by 1 order by 2 desc"
        ).fetchall())
        raw_tx = raw["transactions"]
        clean_tx = clean["transactions"]
        tx = {
            "raw_rows": raw_tx,
            "rows_after_cleaning": clean_tx,
            "rejected_rows_quarantined": rejected,
            "duplicate_rows_removed": valid_rows - clean_tx,
            "pct_rows_rejected": round(100 * rejected / raw_tx, 3),
            "pct_rows_removed_total": round(100 * (raw_tx - clean_tx) / raw_tx, 3),
            "rejections_by_reason": by_reason,
            "late_arrivals_kept": scalar(con, "select count(*) from silver.silver_transactions where is_late_arrival"),
            "non_eur_converted": scalar(con, "select count(*) from silver.silver_transactions where currency_original <> 'EUR'"),
            "approved_amount_eur_total": float(scalar(con,
                "select sum(amount_eur) from silver.silver_transactions where status = 'APPROVED'")),
        }
        gold = {
            "daily_category_kpi_rows": scalar(con, "select count(*) from gold.gold_daily_category_kpis"),
            "customer_360_rows": scalar(con, "select count(*) from gold.gold_customer_360"),
            "fraud_alerts": scalar(con, "select count(*) from gold.gold_fraud_signals"),
            "fraud_alerts_high": scalar(con, "select count(*) from gold.gold_fraud_signals where risk_level = 'HIGH'"),
        }
        # rule evaluation against the generator's ground-truth (synthetic labels of injected bursts)
        if labels_path.exists():
            lp = labels_path.as_posix()
            injected_clean = scalar(con, f"""select count(*) from silver.silver_transactions t
                join read_csv('{lp}', all_varchar=true) l using (transaction_id) where t.status = 'APPROVED'""")
            injected_flagged = scalar(con, f"""select count(*) from gold.gold_fraud_signals f
                join read_csv('{lp}', all_varchar=true) l using (transaction_id)""")
            gold["fraud_rules_eval_on_injected_bursts"] = {
                "injected_burst_rows_in_silver": injected_clean,
                "injected_burst_rows_flagged": injected_flagged,
                "recall_pct": round(100 * injected_flagged / injected_clean, 2) if injected_clean else None,
                "share_of_alerts_from_injected_bursts_pct":
                    round(100 * injected_flagged / gold["fraud_alerts"], 2) if gold["fraud_alerts"] else None,
            }
        return {"raw_rows": raw, "clean_rows": clean, "transactions": tx, "gold": gold}
    finally:
        con.close()


def silver_tx_count() -> int:
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    try:
        return scalar(con, "select count(*) from silver.silver_transactions")
    finally:
        con.close()


def cmd_run(a: argparse.Namespace) -> int:
    t_start = time.perf_counter()
    REPORTS.mkdir(exist_ok=True)
    raw_dir = ROOT / "data" / "raw"

    # 1. generate synthetic raw data
    t0 = time.perf_counter()
    if a.skip_generate:
        stats = json.loads((raw_dir / "_generation_stats.json").read_text(encoding="utf-8"))
        log("skip generation (re-using data/raw)")
    else:
        log(f"generating synthetic data: {a.n_transactions} transactions, seed={a.seed}")
        stats = generate(GenConfig(a.n_transactions, a.n_customers, a.n_merchants, a.seed, raw_dir))
    gen_seconds = time.perf_counter() - t0

    # 2. fresh warehouse
    WAREHOUSE.parent.mkdir(exist_ok=True)
    for p in WAREHOUSE.parent.glob("lakehouse.duckdb*"):
        p.unlink()

    # 3. initial load: every batch except the last one
    last_batch = stats["last_batch_date"]
    vars_initial = json.dumps({"ingest_until": f"{last_batch} 00:00:00"})
    initial = run_dbt(["build", "--vars", vars_initial, "--threads", str(a.threads)], "initial_load")
    rows_initial = silver_tx_count()

    # 4. incremental run: the last batch (contains late-arriving records and replays)
    incremental = run_dbt(["build", "--threads", str(a.threads)], "incremental")
    rows_final = silver_tx_count()

    docs_seconds = None
    if a.docs:
        t0 = time.perf_counter()
        subprocess.run([dbt_executable(), "docs", "generate", "--profiles-dir", str(ROOT), "--project-dir", str(ROOT)],
                       cwd=ROOT, env={**os.environ, "DUCKDB_PATH": str(WAREHOUSE)}, check=False)
        docs_seconds = round(time.perf_counter() - t0, 2)

    data = collect_data_metrics(ROOT / "data" / "labels" / "injected_fraud.csv")
    total = time.perf_counter() - t_start

    metrics = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "data_is_synthetic": True,
        "parameters": {"n_transactions_requested": a.n_transactions, "n_customers": a.n_customers,
                       "n_merchants": a.n_merchants, "seed": a.seed},
        "generator": {"files_rows": {k: stats[k] for k in ("customers", "accounts", "merchants", "fx_rates", "transactions")},
                      "transaction_batches": stats["transaction_batches"],
                      "injected_quality_issues": stats["injected_quality_issues"]},
        **data,
        "incremental": {
            "silver_rows_after_initial_load": rows_initial,
            "silver_rows_after_incremental_run": rows_final,
            "rows_added_by_incremental_run": rows_final - rows_initial,
            "last_batch_date": last_batch,
        },
        "dbt": {"initial_load": initial, "incremental": incremental},
        "runtime_seconds": {
            "generate": round(gen_seconds, 2),
            "dbt_build_initial_load": initial["seconds"],
            "dbt_build_incremental": incremental["seconds"],
            "dbt_docs_generate": docs_seconds,
            "total": round(total, 2),
        },
        "environment": {"python": platform.python_version(), "duckdb": duckdb.__version__,
                        "platform": platform.platform(terse=True)},
    }
    try:
        from importlib.metadata import version
        metrics["environment"]["dbt_core"] = version("dbt-core")
        metrics["environment"]["dbt_duckdb"] = version("dbt-duckdb")
    except Exception:  # pragma: no cover
        pass

    out = REPORTS / "metrics.json"
    out.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")
    log(f"metrics written to {out.relative_to(ROOT)}")
    final = incremental
    log(f"transactions raw={data['transactions']['raw_rows']} clean={data['transactions']['rows_after_cleaning']} "
        f"rejected={data['transactions']['pct_rows_rejected']}% | tests {final['tests_passed']}/{final['tests_total']} passed "
        f"| total {metrics['runtime_seconds']['total']}s")
    failed = initial["exit_code"] or incremental["exit_code"]
    return 1 if failed else 0


def main() -> None:
    p = argparse.ArgumentParser(prog="python -m pipeline", description="Banking lakehouse ELT pipeline")
    sub = p.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run", help="generate data, run dbt build (initial + incremental), write reports/metrics.json")
    r.add_argument("--n-transactions", type=int, default=500_000)
    r.add_argument("--n-customers", type=int, default=20_000)
    r.add_argument("--n-merchants", type=int, default=2_000)
    r.add_argument("--seed", type=int, default=42)
    r.add_argument("--threads", type=int, default=4)
    r.add_argument("--skip-generate", action="store_true", help="re-use existing data/raw")
    r.add_argument("--docs", action="store_true", help="also run dbt docs generate")
    a = p.parse_args()
    if a.command == "run":
        sys.exit(cmd_run(a))


if __name__ == "__main__":
    main()
