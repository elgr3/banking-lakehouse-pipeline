"""Seeded generator of SYNTHETIC raw banking data.

Every name, identifier, amount and rate produced here is fictitious. The data
is shaped to look like a card-payment feed and deliberately contains quality
problems so the downstream dbt layers have something real to clean:

* exact duplicates and replayed transactions (same id, later ingestion)
* null keys / null amounts / null e-mails
* malformed or heterogeneous date formats
* unknown or badly formatted currency codes
* French decimal commas in amounts
* late-arriving records (ingested weeks after the event)
* orphan merchant references
* injected fraud-like patterns (velocity bursts, night e-commerce, outliers)

Output (in ``data/raw/``):
    customers.csv, accounts.csv, merchants.json, fx_rates.csv,
    transactions/transactions_<ingestion_date>.csv  (one file per weekly batch)
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np

START_DATE = date(2025, 1, 1)
N_DAYS = 181  # 2025-01-01 -> 2025-06-30

FIRST_NAMES = [
    "Camille", "Lucas", "Léa", "Hugo", "Chloé", "Louis", "Manon", "Gabriel", "Inès", "Jules",
    "Sarah", "Adam", "Emma", "Nathan", "Jade", "Yanis", "Aïcha", "Moussa", "Fatou", "Rayan",
    "Clara", "Mathis", "Lina", "Théo", "Zoé", "Ibrahim", "Awa", "Paul", "Alice", "Karim",
]
LAST_NAMES = [
    "Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand", "Leroy",
    "Moreau", "Simon", "Laurent", "Lefebvre", "Michel", "Garcia", "Diallo", "Traoré", "Ndiaye",
    "Benali", "Rousseau", "Fontaine", "Chevalier", "Mercier", "Blanc", "Guerin", "Fournier",
]
COUNTRIES = ["FR", "BE", "DE", "ES", "IT", "GB", "CH", "US", "MA", "SN", "CA", "JP"]
CUSTOMER_COUNTRY_W = [0.78, 0.04, 0.03, 0.03, 0.03, 0.02, 0.02, 0.01, 0.02, 0.01, 0.005, 0.005]
COUNTRY_CCY = {
    "FR": "EUR", "BE": "EUR", "DE": "EUR", "ES": "EUR", "IT": "EUR", "GB": "GBP",
    "CH": "CHF", "US": "USD", "MA": "MAD", "SN": "XOF", "CA": "CAD", "JP": "JPY",
}
# approximate value of 1 unit of currency in EUR (fictitious base for random walk)
FX_BASE = {"USD": 0.92, "GBP": 1.17, "CHF": 1.04, "JPY": 0.0062, "CAD": 0.68, "MAD": 0.092, "XOF": 0.001524}
SEGMENTS = ["MASS_MARKET", "PREMIUM", "PRIVATE", "YOUNG"]
SEGMENT_W = [0.62, 0.22, 0.04, 0.12]

# category -> (mcc, lognormal mu, sigma)
CATEGORIES = {
    "GROCERIES": (5411, 3.4, 0.6),
    "RESTAURANTS": (5812, 3.2, 0.5),
    "FUEL": (5541, 3.9, 0.3),
    "TRAVEL": (4511, 5.6, 0.7),
    "ELECTRONICS": (5732, 5.0, 0.9),
    "FASHION": (5651, 4.2, 0.7),
    "HEALTH": (5912, 3.0, 0.6),
    "ENTERTAINMENT": (7832, 3.0, 0.5),
    "UTILITIES": (4900, 4.3, 0.4),
    "CASH_WITHDRAWAL": (6011, 4.4, 0.6),
}
CATEGORY_W = [0.27, 0.17, 0.08, 0.03, 0.05, 0.09, 0.07, 0.07, 0.05, 0.12]
MERCHANT_PREFIX = {
    "GROCERIES": ["Marché", "Epicerie", "Super", "Primeur"],
    "RESTAURANTS": ["Bistrot", "Brasserie", "Café", "Pizzeria"],
    "FUEL": ["Station", "Relais", "Carbu"],
    "TRAVEL": ["Air", "Voyages", "Rail", "Hotel"],
    "ELECTRONICS": ["Tech", "Digital", "Ordi"],
    "FASHION": ["Mode", "Boutique", "Atelier"],
    "HEALTH": ["Pharmacie", "Optique", "Santé"],
    "ENTERTAINMENT": ["Cinéma", "Théâtre", "Jeux"],
    "UTILITIES": ["Energie", "Télécom", "Eau"],
    "CASH_WITHDRAWAL": ["DAB"],
}
MERCHANT_SUFFIX = ["du Centre", "Lumière", "Horizon", "Express", "Plus", "& Co", "Saint-Michel", "Atlas"]


@dataclass
class GenConfig:
    n_transactions: int = 500_000
    n_customers: int = 20_000
    n_merchants: int = 2_000
    seed: int = 42
    out_dir: Path = Path("data/raw")
    stats: dict = field(default_factory=dict)


def _write_csv(path: Path, header: list[str], rows) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for r in rows:
            w.writerow(r)
            n += 1
    return n


def _fmt_ts(dt: datetime, style: int) -> str:
    if style == 0:
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    if style == 1:
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return dt.strftime("%d/%m/%Y %H:%M")


# --------------------------------------------------------------------------- customers
def gen_customers(cfg: GenConfig, rng: np.random.Generator):
    n = cfg.n_customers
    ids = np.array([f"C{i:06d}" for i in range(1, n + 1)])
    countries = rng.choice(COUNTRIES, size=n, p=np.array(CUSTOMER_COUNTRY_W) / sum(CUSTOMER_COUNTRY_W))
    segments = rng.choice(SEGMENTS, size=n, p=SEGMENT_W)
    birth_offsets = rng.integers(18 * 365, 85 * 365, size=n)
    created_offsets = rng.integers(0, 15 * 365, size=n)
    rows = []
    for i in range(n):
        fn = FIRST_NAMES[rng.integers(len(FIRST_NAMES))]
        ln = LAST_NAMES[rng.integers(len(LAST_NAMES))]
        bdate = date(2025, 1, 1) - timedelta(days=int(birth_offsets[i]))
        r = rng.random()
        if r < 0.03:
            birth = bdate.strftime("%d/%m/%Y")  # alternative but parseable format
        elif r < 0.035:
            birth = rng.choice(["unknown", "31/02/1990", "0000-00-00", ""])  # garbage
        else:
            birth = bdate.isoformat()
        email = f"{fn.lower()}.{ln.lower()}{i}@example.com"
        if rng.random() < 0.01:
            email = ""
        created = datetime(2024, 12, 31) - timedelta(days=int(created_offsets[i]))
        rows.append([ids[i], fn, ln, email, birth, countries[i], segments[i], created.isoformat(), created.isoformat()])
    # SCD-like updates: 2% customers re-sent with a newer updated_at and changed segment
    n_upd = int(n * 0.02)
    upd_idx = rng.choice(n, size=n_upd, replace=False)
    for j in upd_idx:
        r = list(rows[j])
        r[6] = rng.choice(SEGMENTS)
        r[8] = (datetime(2025, 1, 15) + timedelta(days=int(rng.integers(0, 150)))).isoformat()
        rows.append(r)
    # exact duplicates: 0.5%
    for j in rng.choice(n, size=int(n * 0.005), replace=False):
        rows.append(list(rows[j]))
    rng.shuffle(rows)
    header = ["customer_id", "first_name", "last_name", "email", "birth_date", "country", "segment", "created_at", "updated_at"]
    cfg.stats["customers"] = _write_csv(cfg.out_dir / "customers.csv", header, rows)
    return ids, countries


# --------------------------------------------------------------------------- accounts
def gen_accounts(cfg: GenConfig, rng: np.random.Generator, cust_ids, cust_countries):
    rows = []
    card_accounts = []  # (account_id, customer_country)
    k = 0
    n_cust = len(cust_ids)
    n_acc_arr = rng.choice([1, 2, 3], size=n_cust, p=[0.55, 0.35, 0.10])
    total = int(n_acc_arr.sum())
    extra_types = rng.choice(["SAVINGS", "CREDIT_CARD"], size=total)
    ccy_u = rng.random(total)
    opened_off = rng.integers(0, 15 * 365, size=total)
    status_u = rng.random(total)
    for cid, ctry, n_acc in zip(cust_ids, cust_countries, n_acc_arr):
        types = ["CHECKING"] + [str(extra_types[k + j]) for j in range(1, n_acc)]
        for t in types:
            aid = f"A{k + 1:07d}"
            ccy = COUNTRY_CCY[ctry] if ccy_u[k] < 0.7 else "EUR"
            opened = date(2010, 1, 1) + timedelta(days=int(opened_off[k]))
            status = "ACTIVE" if status_u[k] < 0.96 else "CLOSED"
            k += 1
            rows.append([aid, cid, t, ccy, opened.isoformat(), status])
            if t in ("CHECKING", "CREDIT_CARD") and status == "ACTIVE":
                card_accounts.append((aid, ctry))
    header = ["account_id", "customer_id", "account_type", "currency", "opened_at", "status"]
    cfg.stats["accounts"] = _write_csv(cfg.out_dir / "accounts.csv", header, rows)
    return card_accounts


# --------------------------------------------------------------------------- merchants
def gen_merchants(cfg: GenConfig, rng: np.random.Generator):
    cats = list(CATEGORIES)
    merch = []
    for i in range(1, cfg.n_merchants + 1):
        cat = cats[rng.choice(len(cats), p=CATEGORY_W)]
        country = "FR" if rng.random() < 0.72 else rng.choice(COUNTRIES)
        name = f"{rng.choice(MERCHANT_PREFIX[cat])} {rng.choice(MERCHANT_SUFFIX)} {i}"
        rec = {
            "merchant_id": f"M{i:05d}",
            "merchant_name": name,
            "mcc": str(CATEGORIES[cat][0]),
            "category": cat if rng.random() > 0.01 else None,  # 1% missing category
            "country": country,
            "onboarded_at": (date(2015, 1, 1) + timedelta(days=int(rng.integers(0, 3600)))).isoformat(),
        }
        merch.append(rec)
    path = cfg.out_dir / "merchants.json"
    path.write_text(json.dumps(merch, ensure_ascii=False, indent=1), encoding="utf-8")
    cfg.stats["merchants"] = len(merch)
    return merch


# --------------------------------------------------------------------------- fx rates
def gen_fx(cfg: GenConfig, rng: np.random.Generator):
    rows = []
    for ccy, base in FX_BASE.items():
        level = base
        for d in range(N_DAYS + 60):  # cover late-arrival window
            day = START_DATE + timedelta(days=d)
            level *= float(np.exp(rng.normal(0, 0.004)))
            if day.weekday() >= 5:  # no ECB-like publication on weekends
                continue
            rows.append([day.isoformat(), ccy, f"{level:.8f}"])
    # one duplicated publication (same day/currency re-sent)
    rows.append(list(rows[10]))
    header = ["rate_date", "currency", "rate_to_eur"]
    cfg.stats["fx_rates"] = _write_csv(cfg.out_dir / "fx_rates.csv", header, rows)


# --------------------------------------------------------------------------- transactions
def _next_monday(day_num: np.ndarray) -> np.ndarray:
    """day_num = days since 1970-01-01 (a Thursday). Return the Monday on/after."""
    weekday = (day_num + 3) % 7  # Monday = 0
    return day_num + (7 - weekday) % 7


def gen_transactions(cfg: GenConfig, rng: np.random.Generator, card_accounts, merchants):
    n = cfg.n_transactions
    cats = list(CATEGORIES)
    m_ids = np.array([m["merchant_id"] for m in merchants])
    m_cat = np.array([next(i for i, c in enumerate(cats) if str(CATEGORIES[c][0]) == m["mcc"]) for m in merchants])
    m_ccy = np.array([COUNTRY_CCY[m["country"]] for m in merchants])
    m_country = np.array([m["country"] for m in merchants])
    fx_base = {**FX_BASE, "EUR": 1.0}
    m_fx = np.array([fx_base[c] for c in m_ccy])
    acc_ids = np.array([a for a, _ in card_accounts])
    mus = np.array([CATEGORIES[c][1] for c in cats])
    sigmas = np.array([CATEGORIES[c][2] for c in cats])

    hour_w = np.array([1, 0.6, 0.4, 0.3, 0.3, 0.5, 1.5, 3, 4, 4.5, 5, 6, 7.5, 6.5, 5, 5, 5.5, 6.5, 7, 6.5, 5, 4, 3, 2])
    hour_w = hour_w / hour_w.sum()

    # ---- normal behaviour
    cat_idx = rng.choice(len(cats), size=n, p=CATEGORY_W)
    merch_idx = np.empty(n, dtype=np.int64)
    for c in range(len(cats)):
        mask = cat_idx == c
        pool = np.flatnonzero(m_cat == c)
        merch_idx[mask] = pool[rng.integers(0, len(pool), size=mask.sum())]
    acc = acc_ids[rng.integers(0, len(acc_ids), size=n)]
    amount_eur = rng.lognormal(mus[cat_idx], sigmas[cat_idx])
    amount = amount_eur / m_fx[merch_idx]
    start = np.datetime64(START_DATE.isoformat(), "s")
    ts = (start + rng.integers(0, N_DAYS, size=n) * np.timedelta64(86400, "s")
          + rng.choice(24, size=n, p=hour_w) * np.timedelta64(3600, "s")
          + rng.integers(0, 3600, size=n) * np.timedelta64(1, "s"))
    u = rng.random(n)
    channel = np.where(u < 0.40, "POS", np.where(u < 0.75, "CONTACTLESS", "ECOM")).astype("<U11")
    channel[cat_idx == cats.index("CASH_WITHDRAWAL")] = "ATM"
    travel_or_elec = np.isin(cat_idx, [cats.index("TRAVEL"), cats.index("ELECTRONICS")])
    channel[travel_or_elec & (u < 0.6)] = "ECOM"
    status = np.where(rng.random(n) < 0.962, "APPROVED", "DECLINED")

    # ---- injected fraud-like patterns: velocity bursts at night on foreign e-commerce
    n_bursts = max(5, n // 3000)
    foreign_pool = np.flatnonzero(~np.isin(m_country, ["FR", "BE"]) & np.isin(m_cat, [cats.index(c) for c in ("ELECTRONICS", "FASHION", "TRAVEL")]))
    b_acc, b_merch, b_ts, b_amt = [], [], [], []
    for _ in range(n_bursts):
        a = acc_ids[rng.integers(len(acc_ids))]
        t0 = start + np.timedelta64(int(rng.integers(0, N_DAYS)) * 86400 + int(rng.integers(0, 5)) * 3600 + int(rng.integers(0, 3600)), "s")
        k = int(rng.integers(6, 11))
        mi = foreign_pool[rng.integers(0, len(foreign_pool), size=k)]
        b_acc += [a] * k
        b_merch += list(mi)
        b_ts += [t0 + np.timedelta64(int(s), "s") for s in np.cumsum(rng.integers(30, 90, size=k))]
        b_amt += list(rng.uniform(300, 2500, size=k) / m_fx[mi])
    nb = len(b_acc)
    acc = np.concatenate([acc, np.array(b_acc)])
    merch_idx = np.concatenate([merch_idx, np.array(b_merch, dtype=np.int64)])
    ts = np.concatenate([ts, np.array(b_ts, dtype="datetime64[s]")])
    amount = np.concatenate([amount, np.array(b_amt)])
    channel = np.concatenate([channel, np.full(nb, "ECOM")])
    status = np.concatenate([status, np.full(nb, "APPROVED")])
    # ---- isolated outliers (amount x15-x40)
    n_out = max(10, n // 2000)
    oi = rng.integers(0, n, size=n_out)
    acc = np.concatenate([acc, acc[oi]])
    merch_idx = np.concatenate([merch_idx, merch_idx[oi]])
    ts = np.concatenate([ts, ts[oi] + np.timedelta64(600, "s")])
    amount = np.concatenate([amount, amount[oi] * rng.uniform(15, 40, size=n_out)])
    channel = np.concatenate([channel, channel[oi]])
    status = np.concatenate([status, status[oi]])
    total = len(acc)

    # ---- ingestion batches: weekly, the Monday after event date (+1 day lag)
    day_num = ts.astype("datetime64[D]").astype(np.int64)
    end_num = np.datetime64((START_DATE + timedelta(days=N_DAYS)).isoformat(), "D").astype(np.int64)
    last_batch = int(_next_monday(np.array([end_num]))[0])
    batch = _next_monday(day_num + 1)
    late = rng.random(total) < 0.02
    batch[late] += rng.choice([7, 14, 21, 28], size=late.sum())
    batch = np.minimum(batch, last_batch)

    # ---- quality problems on the raw feed
    dq = {}
    ts_str = np.datetime_as_string(ts, unit="s").astype(object)
    r = rng.random(total)
    s1 = r < 0.02
    ts_str[s1] = [x.replace("T", " ") for x in ts_str[s1]]
    s2 = (r >= 0.02) & (r < 0.025)
    ts_str[s2] = [f"{x[8:10]}/{x[5:7]}/{x[0:4]} {x[11:16]}" for x in ts_str[s2]]
    s3 = (r >= 0.025) & (r < 0.029)
    ts_str[s3] = rng.choice(["", "N/A", "2025-13-45T99:00:00", "31/02/2025 25:61"], size=s3.sum())
    dq["alt_date_format"] = int(s1.sum() + s2.sum())
    dq["bad_date"] = int(s3.sum())

    amt_str = np.array([f"{x:.2f}" for x in amount], dtype=object)
    r = rng.random(total)
    a1 = r < 0.003
    amt_str[a1] = ""
    a2 = (r >= 0.003) & (r < 0.03)
    amt_str[a2] = [x.replace(".", ",") for x in amt_str[a2]]
    dq["null_amount"] = int(a1.sum())
    dq["comma_decimal"] = int(a2.sum())

    ccy = m_ccy[merch_idx].astype(object)
    r = rng.random(total)
    c1 = r < 0.003
    ccy[c1] = rng.choice(["XXX", "BTC", "ZZZ", "E U R"], size=c1.sum())
    c2 = (r >= 0.003) & (r < 0.015)
    ccy[c2] = [f" {x.lower()} " for x in ccy[c2]]
    dq["unknown_ccy"] = int(c1.sum())
    dq["messy_ccy"] = int(c2.sum())

    acc_str = acc.astype(object)
    n1 = rng.random(total) < 0.004
    acc_str[n1] = ""
    dq["null_account"] = int(n1.sum())
    mid_str = m_ids[merch_idx].astype(object)
    o1 = rng.random(total) < 0.002
    mid_str[o1] = [f"M9{v}" for v in rng.integers(1000, 9999, size=o1.sum())]
    dq["orphan_merchant"] = int(o1.sum())
    dq["late_arriving"] = int(late.sum())

    batch_dt = batch.astype("datetime64[D]").astype("datetime64[s]")
    ingested = np.datetime_as_string(batch_dt + np.timedelta64(7200, "s") + rng.integers(0, 3600, size=total) * np.timedelta64(1, "s"), unit="s")
    tx_id = np.array([f"T{i:09d}" for i in range(1, total + 1)], dtype=object)
    rows = list(zip(tx_id, acc_str, mid_str, ts_str, amt_str, ccy, channel, status, ingested, batch))

    # exact duplicates in the same batch (1%) and replays in the following batch (0.3%)
    dups = [rows[j] for j in rng.choice(total, size=int(total * 0.01), replace=False)]
    replays = []
    for j in rng.choice(total, size=int(total * 0.003), replace=False):
        r_ = list(rows[j])
        nb_ = min(int(r_[9]) + 7, last_batch)
        r_[9] = nb_
        r_[8] = str(np.datetime64(nb_, "D")) + "T03:00:00"
        replays.append(tuple(r_))
    rows.extend(dups)
    rows.extend(replays)
    dq["exact_duplicates"] = len(dups)
    dq["replayed"] = len(replays)
    dq["fraud_burst_rows"] = nb
    dq["amount_outliers"] = n_out

    tx_dir = cfg.out_dir / "transactions"
    by_batch: dict[int, list] = {}
    for r_ in rows:
        by_batch.setdefault(int(r_[9]), []).append(r_[:9])
    header = ["transaction_id", "account_id", "merchant_id", "transaction_ts", "amount", "currency", "channel", "status", "ingested_at"]
    written = 0
    for b in sorted(by_batch):
        batch_rows = by_batch[b]
        order = rng.permutation(len(batch_rows))
        written += _write_csv(tx_dir / f"transactions_{np.datetime64(b, 'D')}.csv", header, (batch_rows[k] for k in order))
    # ground-truth labels of injected fraud bursts (NOT ingested: used only to evaluate the rules)
    labels = cfg.out_dir.parent / "labels" / "injected_fraud.csv"
    _write_csv(labels, ["transaction_id"], ([tx_id[i]] for i in range(n, n + nb)))
    cfg.stats["transactions"] = written
    cfg.stats["transaction_batches"] = len(by_batch)
    cfg.stats["last_batch_date"] = str(np.datetime64(max(by_batch), "D"))
    cfg.stats["injected_quality_issues"] = dq


def generate(cfg: GenConfig) -> dict:
    rng = np.random.default_rng(cfg.seed)
    if cfg.out_dir.exists():
        shutil.rmtree(cfg.out_dir)
    cfg.out_dir.mkdir(parents=True)
    cust_ids, cust_countries = gen_customers(cfg, rng)
    card_accounts = gen_accounts(cfg, rng, cust_ids, cust_countries)
    merchants = gen_merchants(cfg, rng)
    gen_fx(cfg, rng)
    # base volume excludes the injected duplicates / fraud rows added on top
    gen_transactions(cfg, rng, card_accounts, merchants)
    (cfg.out_dir / "_generation_stats.json").write_text(json.dumps(cfg.stats, indent=2, default=str), encoding="utf-8")
    return cfg.stats


def main() -> None:
    p = argparse.ArgumentParser(description="Generate synthetic raw banking data")
    p.add_argument("--n-transactions", type=int, default=500_000)
    p.add_argument("--n-customers", type=int, default=20_000)
    p.add_argument("--n-merchants", type=int, default=2_000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out-dir", type=Path, default=Path("data/raw"))
    a = p.parse_args()
    stats = generate(GenConfig(a.n_transactions, a.n_customers, a.n_merchants, a.seed, a.out_dir))
    print(json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    main()
