import argparse
import hashlib
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict

import pandas as pd
import yaml

from .ingest import SyntheticConfig, load_raw_data
from .validate import (
    validate_schema,
    summarize_data_quality,
    validate_business_rules,
    run_data_quality_checks,
)
from .transform import add_derived_fields
from .kpi import compute_daily_kpis, compute_carrier_kpis, compute_region_kpis

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# --------------------------------------------------------------------------- #
# Config & logging
# --------------------------------------------------------------------------- #
def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(cfg: Dict[str, Any]) -> None:
    log_cfg = cfg.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")

    if log_cfg.get("save_pipeline_logs", True):
        pipeline_log = cfg.get("outputs", {}).get(
            "pipeline_log", "outputs/logs/pipeline.log"
        )
        os.makedirs(os.path.dirname(pipeline_log), exist_ok=True)
        fh = logging.FileHandler(pipeline_log)
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        fh.setFormatter(fmt)
        logging.getLogger().addHandler(fh)


def _config_hash(cfg: Dict[str, Any]) -> str:
    s = json.dumps(cfg, sort_keys=True)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _write_metadata(meta: Dict[str, Any], path: str, start_ts: float) -> None:
    meta["start_time_utc"] = datetime.utcfromtimestamp(start_ts).isoformat()
    meta["end_time_utc"] = datetime.utcnow().isoformat()
    meta["duration_sec"] = time.time() - start_ts
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, default=str)


# --------------------------------------------------------------------------- #
# Simple anomaly detection (rolling z-score)
# --------------------------------------------------------------------------- #
def detect_kpi_anomalies(
    kpi_daily: pd.DataFrame,
    window_days: int = 28,
    min_history: int = 14,
    z_low: float = 1.5,
    z_high: float = 3.0,
) -> pd.DataFrame:
    if kpi_daily.empty:
        return pd.DataFrame(
            columns=[
                "delivery_date",
                "kpi_name",
                "actual_value",
                "rolling_mean",
                "rolling_std",
                "z_score",
                "severity",
            ]
        )

    df = kpi_daily.sort_values("delivery_date").reset_index(drop=True)
    kpi_cols = [
        "otif",
        "fill_rate",
        "backorder_rate",
        "late_percent",
        "avg_lead_time",
        "sla_violation_rate",
    ]
    records = []

    for kpi in kpi_cols:
        if kpi not in df.columns:
            continue
        s = df[kpi]
        rolled = s.shift(1).rolling(window=window_days, min_periods=min_history)
        means = rolled.mean()
        stds = rolled.std(ddof=0)

        for idx, row in df.iterrows():
            mean = means.iloc[idx]
            std = stds.iloc[idx]
            actual = row[kpi]
            date = row["delivery_date"]

            if pd.isna(mean) or pd.isna(std) or std == 0 or pd.isna(actual):
                continue

            z = (actual - mean) / std
            az = abs(z)
            if az < z_low:
                continue

            severity = "HIGH" if az >= z_high else "LOW"

            records.append(
                {
                    "delivery_date": date,
                    "kpi_name": kpi,
                    "actual_value": float(actual),
                    "rolling_mean": float(mean),
                    "rolling_std": float(std),
                    "z_score": float(z),
                    "severity": severity,
                }
            )

    return pd.DataFrame.from_records(records)


# --------------------------------------------------------------------------- #
# Simple alerting – based on anomalies
# --------------------------------------------------------------------------- #
def trigger_alerts(
    anomalies: pd.DataFrame,
    alerts_log_path: str,
    high_threshold: int = 3,
    window_days: int = 7,
) -> None:
    """Write alert if HIGH anomalies exceed threshold in the lookback window."""
    if anomalies.empty:
        return

    df = anomalies.copy()
    df["delivery_date"] = pd.to_datetime(df["delivery_date"])
    cutoff = df["delivery_date"].max() - pd.Timedelta(days=window_days)
    recent = df[df["delivery_date"] >= cutoff]
    high_count = int((recent["severity"] == "HIGH").sum())

    if high_count <= high_threshold:
        return

    alert = {
        "type": "HIGH_ANOMALY_LOAD",
        "severity": "HIGH",
        "message": (
            f"More than {high_threshold} HIGH-severity KPI anomalies detected "
            f"in the last {window_days} days (count={high_count})."
        ),
        "context": {"high_count": high_count, "window_days": window_days},
    }

    os.makedirs(os.path.dirname(alerts_log_path), exist_ok=True)
    with open(alerts_log_path, "a", encoding="utf-8") as f:
        entry = {"timestamp": datetime.utcnow().isoformat(), "alert": alert}
        f.write(json.dumps(entry) + "\n")

    logging.warning(f"[ALERT] {alert['type']} – {alert['message']}")


# --------------------------------------------------------------------------- #
# Minimal HTML report generator (KPI + anomalies)
# --------------------------------------------------------------------------- #
def generate_simple_report(
    kpi_path: str, anomalies_path: str, output_path: str
) -> None:
    """Minimal HTML report built from KPI and anomaly outputs."""
    if os.path.exists(kpi_path):
        kpi_df = pd.read_csv(kpi_path, parse_dates=["delivery_date"])
    else:
        kpi_df = pd.DataFrame()

    if os.path.exists(anomalies_path):
        anom_df = pd.read_csv(anomalies_path, parse_dates=["delivery_date"])
    else:
        anom_df = pd.DataFrame()

    latest_kpi = (
        kpi_df.sort_values("delivery_date").tail(7) if not kpi_df.empty else None
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("<html><head><title>Supply Chain Operations Report</title></head><body>\n")
        f.write("<h1>Supply Chain Operations Report</h1>\n")
        f.write(f"<p>Generated at {datetime.utcnow().isoformat()}</p>\n")

        f.write("<h2>KPI Summary (last 7 days)</h2>\n")
        if latest_kpi is not None and not latest_kpi.empty:
            avg_otif = latest_kpi["otif"].mean() * 100
            avg_fill = latest_kpi["fill_rate"].mean() * 100
            avg_late = latest_kpi["late_percent"].mean() * 100
            f.write("<ul>\n")
            f.write(f"<li>OTIF (avg): {avg_otif:.1f}%</li>\n")
            f.write(f"<li>Fill rate (avg): {avg_fill:.1f}%</li>\n")
            f.write(f"<li>Late % (avg): {avg_late:.1f}%</li>\n")
            f.write("</ul>\n")
        else:
            f.write("<p>No KPI data available.</p>\n")

        f.write("<h2>Recent Anomalies</h2>\n")
        if not anom_df.empty:
            anom_df = anom_df.sort_values("delivery_date", ascending=False).head(10)
            f.write(
                "<table border='1' cellpadding='4'>"
                "<tr><th>Date</th><th>KPI</th><th>Actual</th>"
                "<th>Baseline</th><th>z-score</th><th>Severity</th></tr>\n"
            )
            for _, row in anom_df.iterrows():
                f.write(
                    "<tr>"
                    f"<td>{row['delivery_date'].date()}</td>"
                    f"<td>{row['kpi_name']}</td>"
                    f"<td>{row['actual_value']:.4f}</td>"
                    f"<td>{row['rolling_mean']:.4f}</td>"
                    f"<td>{row['z_score']:.2f}</td>"
                    f"<td>{row['severity']}</td>"
                    "</tr>\n"
                )
            f.write("</table>\n")
        else:
            f.write("<p>No anomalies detected.</p>\n")

        f.write("</body></html>\n")


# --------------------------------------------------------------------------- #
# Main orchestration
# --------------------------------------------------------------------------- #
def run_pipeline(config_path: str) -> None:
    cfg = load_config(config_path)
    setup_logging(cfg)

    run_id = hashlib.sha1(str(time.time()).encode()).hexdigest()[:10]
    run_start = time.time()
    logging.info("Starting pipeline run", extra={"run_id": run_id, "config": config_path})

    data_cfg = cfg["data"]
    outputs_cfg = cfg["outputs"]
    dq_cfg = cfg["data_quality"]
    monitoring_cfg = cfg.get("monitoring", {})
    alert_cfg = cfg.get("alert_thresholds", {})
    runtime_cfg = cfg.get("runtime", {})

    meta: Dict[str, Any] = {
        "run_id": run_id,
        "config_hash": _config_hash(cfg),
        "stages": {},
    }

    # 1. Ingest
    t_ingest = time.time()
    syn_cfg = SyntheticConfig(
        n_days=data_cfg["n_days"],
        n_orders_per_day=data_cfg["n_orders_per_day"],
        random_seed=data_cfg["random_seed"],
    )
    df_raw = load_raw_data(data_cfg["raw_path"], syn_cfg)
    meta["stages"]["ingest"] = {
        "rows": int(len(df_raw)),
        "duration_sec": time.time() - t_ingest,
    }
    logging.info("Loaded raw data", extra={"rows": len(df_raw)})

    # 2. Validate & data quality
    t_validate = time.time()
    schema_errors = validate_schema(df_raw)
    dq_summary = summarize_data_quality(df_raw, dq_cfg["max_missing_pct"])
    business_issues = validate_business_rules(df_raw)
    dq_full = run_data_quality_checks(
        df_raw,
        dq_cfg,
        outputs_cfg.get("data_quality_summary", "outputs/data_quality_summary.json"),
    )

    meta["stages"]["validate"] = {
        "duration_sec": time.time() - t_validate,
        "schema_errors": schema_errors,
        "dq_summary": dq_summary,
        "business_issues": business_issues,
    }
    logging.info("Data quality summary", extra={"dq": dq_summary})
    logging.info("Business rule issues", extra={"issues": business_issues})

    if (not dq_full.get("within_threshold", True)) and runtime_cfg.get("fail_fast", True):
        logging.error("Data quality threshold exceeded. Aborting run.")
        _write_metadata(
            meta,
            outputs_cfg.get("run_metadata", "outputs/run_metadata.json"),
            run_start,
        )
        raise RuntimeError("Data quality checks failed")

    # 3. Transform
    t_transform = time.time()
    df_transformed = add_derived_fields(
        df_raw,
        earliest=dq_cfg.get("earliest_date"),
        latest=dq_cfg.get("latest_date"),
    )
    meta["stages"]["transform"] = {
        "rows": int(len(df_transformed)),
        "duration_sec": time.time() - t_transform,
    }
    logging.info("Transformed data", extra={"rows": len(df_transformed)})

    # 4. KPI
    t_kpi = time.time()
    kpi_daily = compute_daily_kpis(df_transformed)
    _ = compute_carrier_kpis(df_transformed)
    _ = compute_region_kpis(df_transformed)

    kpi_path = outputs_cfg["kpi_daily"]
    os.makedirs(os.path.dirname(kpi_path), exist_ok=True)
    kpi_daily.to_csv(kpi_path, index=False)

    meta["stages"]["kpi"] = {
        "rows": int(len(kpi_daily)),
        "duration_sec": time.time() - t_kpi,
    }

    # 5. Anomaly detection
    t_anom = time.time()
    anomalies_df = detect_kpi_anomalies(
        kpi_daily,
        window_days=monitoring_cfg.get("rolling_window_days", 28),
        min_history=monitoring_cfg.get("min_history_days", 14),
        z_low=monitoring_cfg.get("z_threshold_low", 1.5),
        z_high=monitoring_cfg.get("z_threshold_high", 3.0),
    )
    anomalies_path = outputs_cfg["anomalies"]
    os.makedirs(os.path.dirname(anomalies_path), exist_ok=True)
    anomalies_df.to_csv(anomalies_path, index=False)

    meta["stages"]["anomaly"] = {
        "rows": int(len(anomalies_df)),
        "high_severity_count": int(
            (anomalies_df["severity"] == "HIGH").sum() if not anomalies_df.empty else 0
        ),
        "duration_sec": time.time() - t_anom,
    }
    logging.info(
        "Anomaly detection completed",
        extra={"anomaly_count": len(anomalies_df)},
    )

    # 6. Recommendations – placeholder (empty file with stable schema)
    t_rec = time.time()
    rec_cols = [
        "priority",
        "region",
        "carrier",
        "issue_type",
        "root_cause",
        "recommended_action",
        "expected_operational_impact",
    ]
    rec_df = pd.DataFrame(columns=rec_cols)
    rec_path = outputs_cfg.get("recommendations", "outputs/recommendations.csv")
    os.makedirs(os.path.dirname(rec_path), exist_ok=True)
    rec_df.to_csv(rec_path, index=False)

    meta["stages"]["recommend"] = {
        "rows": int(len(rec_df)),
        "duration_sec": time.time() - t_rec,
    }

    # 7. Report
    t_report = time.time()
    report_path = outputs_cfg["report_html"]
    generate_simple_report(kpi_path, anomalies_path, report_path)
    meta["stages"]["report"] = {"duration_sec": time.time() - t_report}

    # 8. Alerts
    t_alerts = time.time()
    trigger_alerts(
        anomalies_df,
        alerts_log_path=outputs_cfg["alerts_log"],
        high_threshold=alert_cfg.get("high_anomaly_count", 3),
        window_days=monitoring_cfg.get("anomaly_window_days", 7),
    )
    meta["stages"]["alerts"] = {"duration_sec": time.time() - t_alerts}

    # 9. BI exports
    bi_dir = outputs_cfg.get("bi_dir", "outputs/bi")
    os.makedirs(bi_dir, exist_ok=True)
    kpi_daily.to_csv(os.path.join(bi_dir, "kpi_daily.csv"), index=False)
    anomalies_df.to_csv(os.path.join(bi_dir, "anomalies.csv"), index=False)

    risk_path = outputs_cfg.get("risk_predictions", "outputs/risk_predictions.csv")
    risk_cols = ["order_id", "date", "risk_score", "risk_bucket"]
    if not os.path.exists(risk_path):
        pd.DataFrame(columns=risk_cols).to_csv(risk_path, index=False)
    pd.read_csv(risk_path).to_csv(
        os.path.join(bi_dir, "risk_predictions.csv"), index=False
    )

    # 10. Run metadata
    meta_path = outputs_cfg.get("run_metadata", "outputs/run_metadata.json")
    _write_metadata(meta, meta_path, run_start)
    logging.info("Pipeline run completed", extra={"run_id": run_id})


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Supply Chain Automation & Analytics Pipeline"
    )
    parser.add_argument(
        "--config", type=str, required=True, help="Path to YAML config file"
    )
    args = parser.parse_args()
    run_pipeline(args.config)


if __name__ == "__main__":
    main()
