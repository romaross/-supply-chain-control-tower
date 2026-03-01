from typing import Any, Dict, List

import json
import os

import pandas as pd


REQUIRED_COLUMNS = [
    "order_id",
    "order_date",
    "customer",
    "region",
    "plant",
    "carrier",
    "transport_mode",
    "promised_date",
    "actual_delivery_date",
    "quantity",
    "shipped_quantity",
    "status",
]


def validate_schema(df: pd.DataFrame) -> List[str]:
    errors: List[str] = []
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")
    return errors


def validate_business_rules(df: pd.DataFrame) -> Dict[str, int]:
    issues: Dict[str, int] = {}

    if "quantity" in df.columns:
        issues["negative_quantity"] = int((df["quantity"] < 0).sum())
    if "shipped_quantity" in df.columns:
        issues["negative_shipped_quantity"] = int((df["shipped_quantity"] < 0).sum())
        issues["shipped_gt_ordered"] = int((df["shipped_quantity"] > df.get("quantity", 0)).sum())

    if "promised_date" in df.columns and "actual_delivery_date" in df.columns:
        issues["promised_after_actual"] = int((df["promised_date"] > df["actual_delivery_date"]).sum())

    valid_status = {"delivered", "partial", "backorder", "cancelled"}
    issues["invalid_status"] = int((~df["status"].isin(valid_status)).sum())

    return issues


def summarize_data_quality(df: pd.DataFrame, max_missing_pct: float) -> Dict[str, float]:
    critical = [
        "order_id",
        "order_date",
        "region",
        "carrier",
        "promised_date",
        "actual_delivery_date",
        "quantity",
        "shipped_quantity",
        "status",
    ]
    missing_pct = df[critical].isna().mean() * 100
    summary: Dict[str, float] = missing_pct.to_dict()
    summary["max_missing_pct"] = float(missing_pct.max())
    summary["within_threshold"] = summary["max_missing_pct"] <= max_missing_pct
    return summary


def run_data_quality_checks(df: pd.DataFrame, dq_cfg: Dict[str, Any], output_path: str) -> Dict[str, Any]:
    """Compatibility wrapper used by the pipeline.

    Uses summarize_data_quality and writes the summary to JSON so that the
    pipeline can decide whether to fail fast based on the within_threshold flag.
    """
    max_missing = dq_cfg.get("max_missing_pct", 100.0)
    summary = summarize_data_quality(df, max_missing)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    return summary
