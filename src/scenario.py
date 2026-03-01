"""Scenario and what-if simulation module.

Provides simple, explainable what-if scenarios on top of the transformed
shipment data and KPI computation logic. This is not a full stochastic
simulation engine; it is a planning tool for "what happens if" questions.
"""

from dataclasses import dataclass
from typing import Dict, List

import numpy as np
import pandas as pd

from .kpi import compute_daily_kpis


@dataclass
class ScenarioConfig:
    demand_increase_pct: float = 20.0
    lead_time_shift_days: int = 2
    disrupted_carrier: str = "CARRIER_B"
    disruption_delay_days: int = 3


def run_scenarios(df_transformed: pd.DataFrame, cfg: ScenarioConfig) -> pd.DataFrame:
    """Run a set of simple what-if scenarios and return KPI deltas.

    Scenarios:
      - BASE: as-is KPIs
      - DEMAND_STRESS: +X% demand (by duplicating proportional subset of orders)
      - CARRIER_DISRUPTION: increase delay for one carrier
      - LEADTIME_SHIFT: add days to lead time across the network

    Returns DataFrame with columns:
      scenario, metric, base_value, scenario_value, delta_abs, delta_pct
    """

    base_kpi = compute_daily_kpis(df_transformed)
    base_summary = _summarise_kpis(base_kpi)

    rows: List[Dict] = []

    # 1) Demand stress scenario
    demand_df = _simulate_demand_increase(df_transformed, cfg.demand_increase_pct)
    demand_kpi = compute_daily_kpis(demand_df)
    demand_summary = _summarise_kpis(demand_kpi)
    rows.extend(_compare_summaries("DEMAND_STRESS", base_summary, demand_summary))

    # 2) Carrier disruption scenario
    carrier_df = _simulate_carrier_disruption(
        df_transformed, cfg.disrupted_carrier, cfg.disruption_delay_days
    )
    carrier_kpi = compute_daily_kpis(carrier_df)
    carrier_summary = _summarise_kpis(carrier_kpi)
    rows.extend(_compare_summaries("CARRIER_DISRUPTION", base_summary, carrier_summary))

    # 3) Lead time shift scenario
    lead_df = _simulate_lead_time_shift(df_transformed, cfg.lead_time_shift_days)
    lead_kpi = compute_daily_kpis(lead_df)
    lead_summary = _summarise_kpis(lead_kpi)
    rows.extend(_compare_summaries("LEADTIME_SHIFT", base_summary, lead_summary))

    return pd.DataFrame.from_records(rows)


def _summarise_kpis(kpi_daily: pd.DataFrame) -> Dict[str, float]:
    if kpi_daily.empty:
        return {
            "otif": np.nan,
            "fill_rate": np.nan,
            "backorder_rate": np.nan,
            "late_percent": np.nan,
            "avg_lead_time": np.nan,
            "sla_violation_rate": np.nan,
        }
    latest = kpi_daily.sort_values("delivery_date").iloc[-7:]  # last week
    return {
        "otif": float(latest["otif"].mean()),
        "fill_rate": float(latest["fill_rate"].mean()),
        "backorder_rate": float(latest["backorder_rate"].mean()),
        "late_percent": float(latest["late_percent"].mean()),
        "avg_lead_time": float(latest["avg_lead_time"].mean()),
        "sla_violation_rate": float(latest["sla_violation_rate"].mean()),
    }


def _compare_summaries(scenario_name: str, base: Dict[str, float], scen: Dict[str, float]) -> List[Dict]:
    rows: List[Dict] = []
    for metric, base_val in base.items():
        scen_val = scen.get(metric)
        if base_val is None or np.isnan(base_val) or scen_val is None or np.isnan(scen_val):
            delta_abs = np.nan
            delta_pct = np.nan
        else:
            delta_abs = scen_val - base_val
            delta_pct = (delta_abs / base_val * 100) if base_val != 0 else np.nan
        rows.append(
            {
                "scenario": scenario_name,
                "metric": metric,
                "base_value": base_val,
                "scenario_value": scen_val,
                "delta_abs": delta_abs,
                "delta_pct": delta_pct,
            }
        )
    return rows


def _simulate_demand_increase(df: pd.DataFrame, pct: float) -> pd.DataFrame:
    if pct <= 0:
        return df.copy()

    factor = pct / 100.0
    extra = df.sample(frac=factor, replace=True, random_state=42)
    # For simplicity, treat duplicated orders as additional demand with same characteristics
    return pd.concat([df, extra], ignore_index=True)


def _simulate_carrier_disruption(df: pd.DataFrame, carrier: str, delay_days: int) -> pd.DataFrame:
    if delay_days <= 0:
        return df.copy()
    df2 = df.copy()
    mask = df2["carrier"] == carrier
    df2.loc[mask, "actual_delivery_date"] = df2.loc[mask, "actual_delivery_date"] + pd.Timedelta(
        days=delay_days
    )
    # recompute derived fields that depend on dates will be done by transform in a full run;
    # here we focus on approximate impact
    df2["delivery_date"] = df2["actual_delivery_date"].dt.normalize()
    df2["lead_time_days"] = (df2["actual_delivery_date"] - df2["order_date"]).dt.days
    df2["delay_days"] = (df2["actual_delivery_date"] - df2["promised_date"]).dt.days.clip(lower=0)
    df2["is_late"] = df2["actual_delivery_date"] > df2["promised_date"]
    df2["is_sla_violation"] = df2["delay_days"] > 2
    return df2


def _simulate_lead_time_shift(df: pd.DataFrame, shift_days: int) -> pd.DataFrame:
    if shift_days == 0:
        return df.copy()
    df2 = df.copy()
    df2["actual_delivery_date"] = df2["actual_delivery_date"] + pd.Timedelta(days=shift_days)
    df2["delivery_date"] = df2["actual_delivery_date"].dt.normalize()
    df2["lead_time_days"] = (df2["actual_delivery_date"] - df2["order_date"]).dt.days
    df2["delay_days"] = (df2["actual_delivery_date"] - df2["promised_date"]).dt.days.clip(lower=0)
    df2["is_late"] = df2["actual_delivery_date"] > df2["promised_date"]
    df2["is_sla_violation"] = df2["delay_days"] > 2
    return df2
