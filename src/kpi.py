from typing import Dict

import numpy as np
import pandas as pd


def _safe_div(num: float, den: float) -> float:
    if den == 0:
        return np.nan
    return num / den


def compute_daily_kpis(df: pd.DataFrame) -> pd.DataFrame:
    grp = df.groupby("delivery_date", dropna=False)
    records = []
    for date, g in grp:
        delivered = g[g["is_delivered"]]
        total_orders = len(g)
        total_delivered = len(delivered)

        otif_num = ((delivered["is_on_time"]) & (delivered["is_full"])).sum()
        fill_rate = _safe_div(g["shipped_quantity"].sum(), g["quantity"].sum())
        backorder_rate = _safe_div(g["is_backorder"].sum(), total_orders)
        late_percent = _safe_div(delivered["is_late"].sum(), total_delivered)
        avg_lead_time = delivered["lead_time_days"].mean() if total_delivered > 0 else np.nan
        sla_violation_rate = _safe_div(delivered["is_sla_violation"].sum(), total_delivered)

        records.append(
            {
                "delivery_date": date,
                "otif": _safe_div(otif_num, total_delivered),
                "fill_rate": fill_rate,
                "backorder_rate": backorder_rate,
                "late_percent": late_percent,
                "avg_lead_time": avg_lead_time,
                "sla_violation_rate": sla_violation_rate,
                "total_orders": total_orders,
                "total_delivered": total_delivered,
            }
        )
    return pd.DataFrame.from_records(records).sort_values("delivery_date")


def compute_carrier_kpis(df: pd.DataFrame) -> pd.DataFrame:
    grp = df.groupby("carrier", dropna=False)
    records = []
    for carrier, g in grp:
        delivered = g[g["is_delivered"]]
        total_delivered = len(delivered)
        otif_num = ((delivered["is_on_time"]) & (delivered["is_full"])).sum()
        late_percent = _safe_div(delivered["is_late"].sum(), total_delivered)
        avg_lead_time = delivered["lead_time_days"].mean() if total_delivered > 0 else np.nan
        idx = np.nan
        if total_delivered > 0:
            otif = _safe_div(otif_num, total_delivered)
            idx = 0.5 * otif + 0.5 * (1 - late_percent)
        records.append(
            {
                "carrier": carrier,
                "total_orders": len(g),
                "total_delivered": total_delivered,
                "otif": _safe_div(otif_num, total_delivered),
                "late_percent": late_percent,
                "avg_lead_time": avg_lead_time,
                "carrier_index": idx,
            }
        )
    return pd.DataFrame.from_records(records).sort_values("carrier")


def compute_region_kpis(df: pd.DataFrame) -> pd.DataFrame:
    grp = df.groupby("region", dropna=False)
    records = []
    for region, g in grp:
        delivered = g[g["is_delivered"]]
        total_delivered = len(delivered)
        otif_num = ((delivered["is_on_time"]) & (delivered["is_full"])).sum()
        late_percent = _safe_div(delivered["is_late"].sum(), total_delivered)
        idx = np.nan
        if total_delivered > 0:
            otif = _safe_div(otif_num, total_delivered)
            idx = 0.5 * otif + 0.5 * (1 - late_percent)
        records.append(
            {
                "region": region,
                "total_orders": len(g),
                "total_delivered": total_delivered,
                "otif": _safe_div(otif_num, total_delivered),
                "late_percent": late_percent,
                "region_index": idx,
            }
        )
    return pd.DataFrame.from_records(records).sort_values("region")
