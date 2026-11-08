"""Multi-source ETL pipeline for Olist Delivery & Customer Reviews analysis.

Pipeline steps:
1. Extract & Stage: Load raw Olist CSVs into SQLite staging tables (or fall back
   to the committed cleaned CSV when raw Kaggle files are not present locally).
2. Transform: Join orders, reviews, and customers in SQLite via sql/delivery_analysis.sql,
   filter out negative delivery durations, and persist both Data/cleaned_delivery_analysis.csv
   and the SQLite analytical table `cleaned_delivery_analysis`.
3. Partitioned Parquet Export: Write a PyArrow Parquet dataset partitioned by
   `purchase_ym` (YYYY-MM) to Data/parquet/ and save a manifest in reports/parquet_manifest.json.
4. Data Quality & Reconciliation Checks: Validate schema, nulls, review score bounds (1..5),
   date ordering, multi-review order consistency, and cross-store reconciliation across
   SQLite, CSV, and partitioned Parquet; write reports/validation_report.json.
5. Summary Metrics & Chart: Refresh reports/metrics.json and reports/delivery_review_summary.png.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import json
import logging
import shutil
import sqlite3
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import load_to_sqlite
import summarize_results

ROOT = Path(__file__).resolve().parent

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "customer_state",
    "review_score",
    "purchase_date",
    "actual_delivery_date",
    "estimated_delivery_date",
    "actual_delivery_days",
    "delay_days",
    "delivery_status",
]

ANALYTICAL_TABLE = "cleaned_delivery_analysis"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("olist_pipeline")


def has_raw_csvs(data_dir: Path) -> bool:
    """Return True if all three raw Olist source CSVs exist in data_dir."""
    return all((data_dir / name).is_file() for name in load_to_sqlite.FILES)


def extract_and_stage(
    data_dir: Path,
    db_path: Path,
    mode: str = "auto",
) -> dict:
    """Stage raw Olist CSVs into SQLite, or fall back to cleaned CSV when requested."""
    raw_available = has_raw_csvs(data_dir)
    if mode == "from-raw" and not raw_available:
        missing = [name for name in load_to_sqlite.FILES if not (data_dir / name).is_file()]
        raise FileNotFoundError(
            f"Missing raw Olist CSVs in {data_dir}: {', '.join(missing)}. "
            "Place the Kaggle CSVs in Data/ or run with --from-cleaned."
        )

    effective_mode = "from-raw" if (mode == "from-raw" or (mode == "auto" and raw_available)) else "from-cleaned"
    staging_counts: dict[str, int] = {}

    if effective_mode == "from-raw":
        logger.info("Extracting 3 raw Olist CSVs from %s into SQLite (%s)", data_dir, db_path.name)
        with closing(sqlite3.connect(db_path)) as conn, conn:
            for filename, table in load_to_sqlite.FILES.items():
                csv_path = data_dir / filename
                df_raw = pd.read_csv(csv_path)
                df_raw.to_sql(table, conn, if_exists="replace", index=False)
                staging_counts[table] = int(len(df_raw))
                logger.info("  Staged %-22s : %7d rows", table, len(df_raw))
    else:
        cleaned_csv = data_dir / "cleaned_delivery_analysis.csv"
        if not cleaned_csv.is_file():
            raise FileNotFoundError(f"Cleaned CSV not found at {cleaned_csv}")
        logger.info("Raw Kaggle CSVs not used; staging from existing %s", cleaned_csv)
        df_clean = pd.read_csv(cleaned_csv)
        with closing(sqlite3.connect(db_path)) as conn, conn:
            df_clean.to_sql(ANALYTICAL_TABLE, conn, if_exists="replace", index=False)
            staging_counts[ANALYTICAL_TABLE] = int(len(df_clean))
            logger.info("  Staged %-22s : %7d rows", ANALYTICAL_TABLE, len(df_clean))

    return {
        "mode": effective_mode,
        "staging_tables": staging_counts,
        "total_staged_rows": int(sum(staging_counts.values())),
    }


def transform_delivery_data(
    data_dir: Path,
    db_path: Path,
    sql_path: Path,
    mode: str,
) -> tuple[pd.DataFrame, dict]:
    """Run SQL join + pandas cleaning and save analytical CSV and SQLite table."""
    cleaned_csv = data_dir / "cleaned_delivery_analysis.csv"

    with closing(sqlite3.connect(db_path)) as conn, conn:
        if mode == "from-raw":
            query = sql_path.read_text(encoding="utf-8")
            logger.info("Running SQL join from %s", sql_path.relative_to(ROOT))
            df_joined = pd.read_sql_query(query, conn)
            raw_joined_rows = int(len(df_joined))
            negative_delivery_rows = int((df_joined["actual_delivery_days"] < 0).sum())
            df_clean = df_joined[df_joined["actual_delivery_days"] >= 0].copy()
            logger.info(
                "SQL join returned %d rows; removed %d negative-duration rows -> %d clean rows",
                raw_joined_rows,
                negative_delivery_rows,
                len(df_clean),
            )
            df_clean[REQUIRED_COLUMNS].to_csv(cleaned_csv, index=False)
            logger.info("Saved cleaned CSV to %s", cleaned_csv.relative_to(ROOT))
        else:
            df_clean = pd.read_sql_query(f"SELECT * FROM {ANALYTICAL_TABLE}", conn)
            raw_joined_rows = int(len(df_clean))
            negative_delivery_rows = int((df_clean["actual_delivery_days"] < 0).sum())
            df_clean = df_clean[df_clean["actual_delivery_days"] >= 0].copy()
            logger.info("Loaded %d clean analytical rows from SQLite", len(df_clean))

        df_clean[REQUIRED_COLUMNS].to_sql(ANALYTICAL_TABLE, conn, if_exists="replace", index=False)
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{ANALYTICAL_TABLE}_order_id ON {ANALYTICAL_TABLE}(order_id)")
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{ANALYTICAL_TABLE}_status ON {ANALYTICAL_TABLE}(delivery_status)"
        )

    transform_stats = {
        "sql_joined_rows_before_filter": raw_joined_rows,
        "negative_delivery_days_filtered": negative_delivery_rows,
        "clean_analytical_rows": int(len(df_clean)),
        "unique_orders": int(df_clean["order_id"].nunique()),
        "multi_review_extra_rows": int(len(df_clean) - df_clean["order_id"].nunique()),
    }
    return df_clean, transform_stats


def write_partitioned_parquet(
    df: pd.DataFrame,
    parquet_dir: Path,
    reports_dir: Path,
) -> dict:
    """Write dataset partitioned by purchase_ym (YYYY-MM) and save a manifest JSON."""
    if parquet_dir.exists():
        shutil.rmtree(parquet_dir)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    df_pq = df[REQUIRED_COLUMNS].copy()
    df_pq["purchase_ym"] = df_pq["purchase_date"].astype(str).str.slice(0, 7)

    table = pa.Table.from_pandas(df_pq, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(parquet_dir),
        partition_cols=["purchase_ym"],
        compression="snappy",
    )

    partition_counts = (
        df_pq.groupby("purchase_ym", as_index=False)
        .agg(
            rows=("order_id", "size"),
            unique_orders=("order_id", "nunique"),
            delayed_rows=("delivery_status", lambda s: int((s == "Delayed").sum())),
            avg_review_score=("review_score", lambda s: round(float(s.mean()), 4)),
        )
        .sort_values("purchase_ym")
    )

    parquet_files = list(parquet_dir.rglob("*.parquet"))
    total_bytes = sum(f.stat().st_size for f in parquet_files)

    try:
        rel_dataset_path = str(parquet_dir.relative_to(ROOT)).replace("\\", "/")
    except ValueError:
        rel_dataset_path = str(parquet_dir).replace("\\", "/")

    manifest = {
        "dataset_path": rel_dataset_path,
        "partition_key": "purchase_ym",
        "compression": "snappy",
        "partition_count": int(len(partition_counts)),
        "parquet_file_count": int(len(parquet_files)),
        "total_rows": int(len(df_pq)),
        "total_size_bytes": int(total_bytes),
        "purchase_ym_min": str(partition_counts["purchase_ym"].min()),
        "purchase_ym_max": str(partition_counts["purchase_ym"].max()),
        "partitions": partition_counts.to_dict(orient="records"),
    }

    reports_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = reports_dir / "parquet_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info(
        "Wrote partitioned Parquet (%d partitions, %d rows, %.1f KB) -> %s",
        manifest["partition_count"],
        manifest["total_rows"],
        total_bytes / 1024,
        manifest_path.name,
    )
    return manifest


def validate_pipeline(
    df: pd.DataFrame,
    db_path: Path,
    csv_path: Path,
    parquet_dir: Path,
    staging_info: dict,
    transform_stats: dict,
    reports_dir: Path,
) -> dict:
    """Run data quality and cross-store reconciliation checks and write validation_report.json."""
    checks: list[dict] = []

    def add_check(name: str, passed: bool, details: dict) -> None:
        checks.append({"check": name, "passed": bool(passed), "details": details})

    # 1. Required columns present
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    add_check(
        "required_columns_present",
        len(missing_cols) == 0,
        {"expected_columns": REQUIRED_COLUMNS, "missing_columns": missing_cols},
    )

    # 2. No nulls in critical fields
    null_counts = {col: int(df[col].isna().sum()) for col in REQUIRED_COLUMNS}
    total_nulls = sum(null_counts.values())
    add_check(
        "no_null_critical_fields",
        total_nulls == 0,
        {"total_nulls": total_nulls, "nulls_by_column": null_counts},
    )

    # 3. Review score in 1..5
    invalid_scores = int((~df["review_score"].between(1, 5)).sum())
    add_check(
        "review_score_range_1_to_5",
        invalid_scores == 0,
        {
            "min_score": int(df["review_score"].min()),
            "max_score": int(df["review_score"].max()),
            "out_of_range_rows": invalid_scores,
        },
    )

    # 4. Delivery dates vs purchase dates (non-negative delivery duration)
    negative_days = int((df["actual_delivery_days"] < 0).sum())
    date_order_violations = int((df["actual_delivery_date"] < df["purchase_date"]).sum())
    add_check(
        "delivery_date_not_before_purchase_date",
        negative_days == 0 and date_order_violations == 0,
        {
            "negative_actual_delivery_days_rows": negative_days,
            "date_order_violation_rows": date_order_violations,
            "min_actual_delivery_days": float(df["actual_delivery_days"].min()),
            "max_actual_delivery_days": float(df["actual_delivery_days"].max()),
        },
    )

    # 5. Valid delivery status domain
    observed_statuses = sorted(df["delivery_status"].dropna().unique().tolist())
    add_check(
        "valid_delivery_status_domain",
        set(observed_statuses) == {"Delayed", "On-Time"},
        {"observed_values": observed_statuses},
    )

    # 6. Order-level delivery_status consistency across multiple reviews
    conflicting_orders = int((df.groupby("order_id")["delivery_status"].nunique() > 1).sum())
    orders_with_multiple_reviews = int((df.groupby("order_id").size() > 1).sum())
    add_check(
        "order_delivery_status_consistent_across_reviews",
        conflicting_orders == 0,
        {
            "unique_orders": int(df["order_id"].nunique()),
            "joined_records": int(len(df)),
            "orders_with_multiple_reviews": orders_with_multiple_reviews,
            "extra_review_rows_retained": int(len(df) - df["order_id"].nunique()),
            "conflicting_status_orders": conflicting_orders,
        },
    )

    # 7. Monthly partition coverage check (2016-09 .. 2018-08; 2016-11 is a known gap in Olist)
    observed_months = sorted(df["purchase_date"].astype(str).str.slice(0, 7).unique().tolist())
    all_months_in_span = [
        p.strftime("%Y-%m")
        for p in pd.period_range(start=observed_months[0], end=observed_months[-1], freq="M")
    ]
    missing_months = [m for m in all_months_in_span if m not in set(observed_months)]
    add_check(
        "monthly_partition_coverage_audit",
        missing_months == ["2016-11"] and len(observed_months) == 23,
        {
            "purchase_ym_start": observed_months[0],
            "purchase_ym_end": observed_months[-1],
            "calendar_months_in_span": len(all_months_in_span),
            "present_partition_months": len(observed_months),
            "missing_months_in_span": missing_months,
            "note": "2016-11 has 0 delivered orders in the upstream Olist dataset.",
        },
    )

    # 8. Cross-store reconciliation: SQLite analytical table vs CSV vs Partitioned Parquet
    with closing(sqlite3.connect(db_path)) as conn:
        sqlite_row_count = int(
            conn.execute(f"SELECT COUNT(*) FROM {ANALYTICAL_TABLE}").fetchone()[0]
        )
        sqlite_unique_orders = int(
            conn.execute(f"SELECT COUNT(DISTINCT order_id) FROM {ANALYTICAL_TABLE}").fetchone()[0]
        )
        sqlite_group_rows = dict(
            conn.execute(
                f"SELECT delivery_status, COUNT(*) FROM {ANALYTICAL_TABLE} GROUP BY delivery_status"
            ).fetchall()
        )
        sqlite_group_avg = {
            k: round(float(v), 4)
            for k, v in conn.execute(
                f"SELECT delivery_status, AVG(review_score) FROM {ANALYTICAL_TABLE} GROUP BY delivery_status"
            ).fetchall()
        }

    df_csv = pd.read_csv(csv_path)
    df_pq = pq.read_table(str(parquet_dir)).to_pandas()

    csv_rows = int(len(df_csv))
    pq_rows = int(len(df_pq))
    csv_unique_orders = int(df_csv["order_id"].nunique())
    pq_unique_orders = int(df_pq["order_id"].nunique())

    on_time_avg = round(float(df[df["delivery_status"] == "On-Time"]["review_score"].mean()), 2)
    delayed_avg = round(float(df[df["delivery_status"] == "Delayed"]["review_score"].mean()), 2)

    stores_agree = (
        sqlite_row_count == csv_rows == pq_rows == 96353
        and sqlite_unique_orders == csv_unique_orders == pq_unique_orders == 95824
        and int(sqlite_group_rows.get("On-Time", 0)) == 88653
        and int(sqlite_group_rows.get("Delayed", 0)) == 7700
        and on_time_avg == 4.29
        and delayed_avg == 2.57
    )
    add_check(
        "cross_store_reconciliation_sqlite_csv_parquet",
        stores_agree,
        {
            "sqlite_rows": sqlite_row_count,
            "csv_rows": csv_rows,
            "parquet_rows": pq_rows,
            "sqlite_unique_orders": sqlite_unique_orders,
            "csv_unique_orders": csv_unique_orders,
            "parquet_unique_orders": pq_unique_orders,
            "sqlite_rows_by_status": sqlite_group_rows,
            "sqlite_avg_review_by_status": sqlite_group_avg,
            "on_time_avg_review_rounded": on_time_avg,
            "delayed_avg_review_rounded": delayed_avg,
        },
    )

    all_passed = all(c["passed"] for c in checks)
    report = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "all_checks_passed": all_passed,
        "checks_total": len(checks),
        "checks_passed": sum(1 for c in checks if c["passed"]),
        "checks_failed": sum(1 for c in checks if not c["passed"]),
        "staging": staging_info,
        "transform": transform_stats,
        "checks": checks,
    }

    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / "validation_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info(
        "Validation complete: %d/%d checks passed -> %s",
        report["checks_passed"],
        report["checks_total"],
        report_path.name,
    )
    if not all_passed:
        failed_names = [c["check"] for c in checks if not c["passed"]]
        raise RuntimeError(f"Data quality checks failed: {failed_names}")

    return report


def run_pipeline(
    mode: str = "auto",
    data_dir: Path = ROOT / "Data",
    db_path: Path = ROOT / "olist.db",
    sql_path: Path = ROOT / "sql" / "delivery_analysis.sql",
    parquet_dir: Path = ROOT / "Data" / "parquet",
    reports_dir: Path = ROOT / "reports",
) -> dict:
    """Execute the full Olist ETL, Parquet partitioning, validation, and summary pipeline."""
    t0 = time.perf_counter()
    logger.info("Starting Olist ETL pipeline (mode=%s)", mode)

    staging_info = extract_and_stage(data_dir=data_dir, db_path=db_path, mode=mode)
    df_clean, transform_stats = transform_delivery_data(
        data_dir=data_dir,
        db_path=db_path,
        sql_path=sql_path,
        mode=staging_info["mode"],
    )
    parquet_manifest = write_partitioned_parquet(
        df=df_clean,
        parquet_dir=parquet_dir,
        reports_dir=reports_dir,
    )
    validation_report = validate_pipeline(
        df=df_clean,
        db_path=db_path,
        csv_path=data_dir / "cleaned_delivery_analysis.csv",
        parquet_dir=parquet_dir,
        staging_info=staging_info,
        transform_stats=transform_stats,
        reports_dir=reports_dir,
    )

    if data_dir.resolve() == (ROOT / "Data").resolve() and reports_dir.resolve() == (ROOT / "reports").resolve():
        logger.info("Refreshing summary metrics and chart via summarize_results.py")
        summarize_results.main()

    elapsed = round(time.perf_counter() - t0, 2)
    logger.info("Pipeline finished successfully in %.2fs", elapsed)
    return {
        "elapsed_seconds": elapsed,
        "staging": staging_info,
        "transform": transform_stats,
        "parquet": parquet_manifest,
        "validation": validation_report,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Olist delivery & customer review ETL pipeline."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--from-raw",
        action="store_const",
        dest="mode",
        const="from-raw",
        help="Require and ingest the 3 raw Olist CSVs from Data/ into SQLite.",
    )
    group.add_argument(
        "--from-cleaned",
        action="store_const",
        dest="mode",
        const="from-cleaned",
        help="Run pipeline starting from the committed Data/cleaned_delivery_analysis.csv.",
    )
    parser.set_defaults(mode="auto")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "Data", help="Path to Data directory.")
    parser.add_argument("--db-path", type=Path, default=ROOT / "olist.db", help="Path to SQLite database.")
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        default=ROOT / "Data" / "parquet",
        help="Output directory for partitioned Parquet dataset.",
    )
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=ROOT / "reports",
        help="Output directory for validation and manifest JSON reports.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run_pipeline(
        mode=args.mode,
        data_dir=args.data_dir,
        db_path=args.db_path,
        parquet_dir=args.parquet_dir,
        reports_dir=args.reports_dir,
    )


if __name__ == "__main__":
    main()
