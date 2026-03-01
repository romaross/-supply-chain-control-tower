"""Actionability & decision layer.

Maps KPI and risk patterns to concrete operational actions:
- Expedite / re-route shipments
- Adjust safety stock / capacity
- Switch or review carrier
- Proactive customer communication

The logic is intentionally rule-based and explainable.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class ActionConfig:
    top_shipments: int = 10
    high_risk_threshold: float = 0.7  # probability
    medium_risk_threshold: float = 0.4
    high_value_quantity: int = 50
    key_regions: Optional[List[str]] = None


def build_recommended_actions(
    kpi_daily: pd.DataFrame,
    carrier_kpis: pd.DataFrame,
    risk_df: Optional[pd.DataFrame],
    cfg: ActionConfig,
) -> pd.DataFrame:
    """Build a prioritized list of recommended operational actions.

    Returns a DataFrame with columns:
      - level: 'shipment' | 'lane' | 'carrier'
      - target_id: order_id / lane / carrier
      - severity: 'HIGH' | 'MEDIUM'
      - action_type: e.g. 'EXPEDITE', 'CARRIER_REVIEW'
      - description: business-language recommendation
    """

    actions: List[Dict] = []

    # 1) Shipment-level actions from risk predictions
    if risk_df is not None and not risk_df.empty:
        actions.extend(
            _shipment_level_actions(risk_df, cfg)
        )

    # 2) Carrier-level actions from carrier KPIs
    if carrier_kpis is not None and not carrier_kpis.empty:
        actions.extend(
            _carrier_level_actions(carrier_kpis)
        )

    # 3) Lane-level actions will be derived from risk_df if present
    if risk_df is not None and not risk_df.empty:
        actions.extend(
            _lane_level_actions(risk_df)
        )

    actions_df = pd.DataFrame.from_records(actions)
    if actions_df.empty:
        return actions_df

    # sort by severity then by an optional score if present
    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    actions_df["severity_rank"] = actions_df["severity"].map(severity_order).fillna(1)
    actions_df = actions_df.sort_values([
        "severity_rank",
        "score",
    ], ascending=[True, False]).drop(columns=["severity_rank"])

    return actions_df.head(cfg.top_shipments * 2)  # keep list compact


def _shipment_level_actions(risk_df: pd.DataFrame, cfg: ActionConfig) -> List[Dict]:
    df = risk_df.copy()

    # ensure required columns exist
    required_cols = {"order_id", "delay_probability", "risk_bucket", "quantity", "region"}
    missing = required_cols - set(df.columns)
    if missing:
        return []

    if cfg.key_regions:
        df["is_key_region"] = df["region"].isin(cfg.key_regions)
    else:
        df["is_key_region"] = True

    df["is_high_value"] = df["quantity"] >= cfg.high_value_quantity

    # score: probability + bonuses for high value / key region
    df["score"] = df["delay_probability"]
    df.loc[df["is_high_value"], "score"] += 0.1
    df.loc[df["is_key_region"], "score"] += 0.05

    df = df.sort_values("score", ascending=False).head(cfg.top_shipments)

    actions: List[Dict] = []
    for _, row in df.iterrows():
        if row["delay_probability"] >= cfg.high_risk_threshold:
            severity = "HIGH"
            action_type = "EXPEDITE_OR_REROUTE"
            desc = (
                f"Order {row['order_id']} has HIGH delay risk "
                f"({row['delay_probability']:.2f}). Prioritise picking and loading, "
                "consider express routing, and inform customer proactively."
            )
        elif row["delay_probability"] >= cfg.medium_risk_threshold:
            severity = "MEDIUM"
            action_type = "MONITOR_AND_COMMUNICATE"
            desc = (
                f"Order {row['order_id']} has MEDIUM delay risk "
                f"({row['delay_probability']:.2f}). Monitor closely and keep customer "
                "service informed for potential rescheduling."
            )
        else:
            continue

        actions.append(
            {
                "level": "shipment",
                "target_id": row["order_id"],
                "severity": severity,
                "action_type": action_type,
                "score": float(row["score"]),
                "description": desc,
            }
        )

    return actions


def _carrier_level_actions(carrier_kpis: pd.DataFrame) -> List[Dict]:
    actions: List[Dict] = []

    for _, row in carrier_kpis.iterrows():
        if row["total_delivered"] < 100:
            continue  # too low volume

        otif = row["otif"]
        late = row["late_percent"]
        idx = row["carrier_index"]

        if np.isnan(otif) or np.isnan(late) or np.isnan(idx):
            continue

        if idx < 0.9 or late > 0.12:  # 90%+ index expected
            severity = "HIGH" if idx < 0.85 or late > 0.15 else "MEDIUM"
            actions.append(
                {
                    "level": "carrier",
                    "target_id": row["carrier"],
                    "severity": severity,
                    "action_type": "CARRIER_PERFORMANCE_REVIEW",
                    "score": float(1 - idx),  # bigger gap -> higher score
                    "description": (
                        f"Carrier {row['carrier']} shows weak service performance "
                        f"(OTIF {otif*100:.1f}%, Late {late*100:.1f}%, Index {idx*100:.1f}%). "
                        "Schedule a performance review and evaluate reallocating critical volume "
                        "to better-performing carriers."
                    ),
                }
            )

    return actions


def _lane_level_actions(risk_df: pd.DataFrame) -> List[Dict]:
    if "plant" not in risk_df.columns or "region" not in risk_df.columns:
        return []

    df = risk_df.copy()
    df["lane"] = df["plant"] + " → " + df["region"]

    grp = df.groupby("lane")
    actions: List[Dict] = []

    for lane, g in grp:
        total = len(g)
        if total < 50:
            continue
        high_share = (g["risk_bucket"] == "HIGH").mean()
        avg_prob = g["delay_probability"].mean()

        if high_share < 0.2 and avg_prob < 0.5:
            continue

        severity = "HIGH" if high_share > 0.3 else "MEDIUM"
        actions.append(
            {
                "level": "lane",
                "target_id": lane,
                "severity": severity,
                "action_type": "CAPACITY_AND_STOCK_REVIEW",
                "score": float(high_share + avg_prob),
                "description": (
                    f"Lane {lane} has an elevated share of HIGH-risk shipments "
                    f"({high_share*100:.1f}%). Review lane capacity, routing options, "
                    "and safety stock positioning to stabilise service."
                ),
            }
        )

    return actions
