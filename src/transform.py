from typing import Optional

import pandas as pd


def add_derived_fields(df: pd.DataFrame, earliest: Optional[str], latest: Optional[str]) -> pd.DataFrame:
    df = df.copy()

    if earliest:
        df = df[df["order_date"] >= pd.to_datetime(earliest)]
    if latest:
        df = df[df["order_date"] <= pd.to_datetime(latest)]

    df["delivery_date"] = df["actual_delivery_date"].dt.normalize()
    df["promised_date"] = df["promised_date"].dt.normalize()

    df["lead_time_days"] = (df["actual_delivery_date"] - df["order_date"]).dt.days
    df["delay_days"] = (df["actual_delivery_date"] - df["promised_date"]).dt.days.clip(lower=0)

    df["is_delivered"] = df["status"].isin(["delivered", "partial"])
    df["is_full"] = df["shipped_quantity"] >= df["quantity"]
    df["is_on_time"] = df["is_delivered"] & (df["actual_delivery_date"] <= df["promised_date"])
    df["is_late"] = df["is_delivered"] & (df["actual_delivery_date"] > df["promised_date"])
    df["is_backorder"] = df["status"] == "backorder"
    df["is_sla_violation"] = df["delay_days"] > 2

    return df
