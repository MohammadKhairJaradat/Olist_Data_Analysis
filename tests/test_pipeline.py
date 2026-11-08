"""Light unit and data-quality tests for the Olist ETL pipeline."""
from pathlib import Path
import json
import pandas as pd
import run_pipeline

ROOT = Path(__file__).resolve().parent.parent


def test_cleaned_csv_schema_and_metrics():
    csv_path = ROOT / "Data" / "cleaned_delivery_analysis.csv"
    assert csv_path.is_file()
    df = pd.read_csv(csv_path)
    assert list(df.columns) == run_pipeline.REQUIRED_COLUMNS
    assert len(df) == 96353
    assert df["order_id"].nunique() == 95824
    assert df["review_score"].between(1, 5).all()
    assert (df["actual_delivery_days"] >= 0).all()
    assert set(df["delivery_status"].unique()) == {"On-Time", "Delayed"}


def test_validation_report_and_parquet_manifest():
    val_path = ROOT / "reports" / "validation_report.json"
    pq_path = ROOT / "reports" / "parquet_manifest.json"
    assert val_path.is_file()
    assert pq_path.is_file()

    val = json.loads(val_path.read_text(encoding="utf-8"))
    assert val["all_checks_passed"] is True
    assert val["checks_failed"] == 0
    assert val["transform"]["clean_analytical_rows"] == 96353
    assert val["transform"]["unique_orders"] == 95824

    pq_manifest = json.loads(pq_path.read_text(encoding="utf-8"))
    assert pq_manifest["partition_key"] == "purchase_ym"
    assert pq_manifest["total_rows"] == 96353
    assert pq_manifest["partition_count"] == 23


def test_from_cleaned_fallback_mode(tmp_path):
    db_path = tmp_path / "test_olist.db"
    pq_dir = tmp_path / "parquet"
    rep_dir = tmp_path / "reports"
    result = run_pipeline.run_pipeline(
        mode="from-cleaned",
        data_dir=ROOT / "Data",
        db_path=db_path,
        parquet_dir=pq_dir,
        reports_dir=rep_dir,
    )
    assert result["staging"]["mode"] == "from-cleaned"
    assert result["validation"]["all_checks_passed"] is True
    assert result["parquet"]["total_rows"] == 96353

