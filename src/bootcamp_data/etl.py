from dataclasses import dataclass,asdict
from pathlib import Path
import pandas as pd
import json
import logging

from bootcamp_data.io import write_parquet,read_users_csv,read_orders_csv
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.transforms import apply_mapping, parse_datetime, add_time_parts, winsorize, add_outlier_flag,enforce_schema, add_missing_flags,normalize_text
from bootcamp_data.joins import safe_left_join



@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path


def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users

def transform(orders_raw, users):

    require_columns(orders_raw,  ["order_id","user_id","amount","quantity","created_at","status"]) 
    require_columns(users,  ["user_id","country","signup_date"]) #add cols
    assert_non_empty(orders_raw,"orders")
    assert_non_empty(users,"users")
    assert_unique_key(orders_raw, "order_id")
    assert_unique_key(users, "user_id")

    orders= (
        orders_raw
        .pipe(enforce_schema).assign(
            status_clean=lambda d: apply_mapping(normalize_text(d["status"]), 
            {"paid": "paid", "refund": "refund", "refunded": "refund"}) 
        )
        .pipe(add_missing_flags, cols=["amount", "quantity"])
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )
    
    joined = safe_left_join(
        orders,
        users,
        on="user_id",
        validate="many_to_one", 
        suffixes=("", "_user"), 
        )
    
    assert len(joined) == len(orders), "Row count changed (join explosion?)" 

    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)

    return joined



##################

log = logging.getLogger(__name__)

def load_outputs(*, analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    """Write processed artifacts (idempotent)."""
    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)


def write_run_meta( cfg: ETLConfig, *, orders_raw: pd.DataFrame,
                    users: pd.DataFrame, analytics: pd.DataFrame) -> None:
    
    missing_created_at = int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else None
    
    country_match_rate = (
        1.0 - float(analytics["country"].isna().mean())
        if "country" in analytics.columns
        else None
    )

    meta = {
        "rows_in_orders_raw": int(len(orders_raw)),
        "rows_in_users": int(len(users)),
        "rows_out_analytics": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()} 
    }

    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    log.info("Loading inputs")
    orders_raw, users = load_inputs(cfg)
    log.info("Transforming (orders=%s, users=%s)", len(orders_raw), len(users))
    analytics = transform(orders_raw, users)
    log.info("Writing outputs to %s", cfg.out_analytics.parent)
    load_outputs(analytics=analytics, users=users, cfg=cfg)
    log.info("Writing run metadata: %s", cfg.run_meta)
    write_run_meta(cfg, orders_raw=orders_raw, users=users, analytics=analytics)












    








