import os
from dataclasses import dataclass
from typing import List, Dict

import numpy as np
import pandas as pd


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)


@dataclass
class SyntheticConfig:
    n_days: int
    n_orders_per_day: int
    random_seed: int


def _date_range(n_days: int) -> pd.DatetimeIndex:
    end = pd.Timestamp.today().normalize()
    start = end - pd.Timedelta(days=n_days - 1)
    return pd.date_range(start, end, freq="D")


def _dimensions() -> Dict[str, List[str]]:
    regions = ["EUROPE", "NORTH_AMERICA", "ASIA", "LATAM"]
    plants = {
        "EUROPE": ["EU_DC_1", "EU_DC_2"],
        "NORTH_AMERICA": ["NA_DC_1", "NA_DC_2"],
        "ASIA": ["ASIA_DC_1"],
        "LATAM": ["LATAM_DC_1"],
    }
    carriers = ["CARRIER_A", "CARRIER_B", "CARRIER_C", "POSTAL", "EXPRESS_X"]
    modes = ["ROAD", "AIR", "SEA", "RAIL"]
    return {"regions": regions, "plants": plants, "carriers": carriers, "modes": modes}


def _generate_synthetic(cfg: SyntheticConfig, path: str) -> pd.DataFrame:
    dims = _dimensions()
    dates = _date_range(cfg.n_days)
    np.random.seed(cfg.random_seed)

    records = []
    order_id = 1

    base_demand = {"EUROPE": 250, "NORTH_AMERICA": 200, "ASIA": 150, "LATAM": 120}

    for date in dates:
        dow = date.dayofweek
        dow_factor = 1.2 if dow < 4 else 1.0
        if dow >= 5:
            dow_factor = 0.6

        for region in dims["regions"]:
            lam = base_demand[region] * dow_factor
            n_orders = np.random.poisson(lam=lam)
            for _ in range(n_orders):
                oid = f"ORD_{order_id:08d}"; order_id += 1
                plant = np.random.choice(dims["plants"][region])
                mode = np.random.choice(dims["modes"], p=[0.6, 0.1, 0.25, 0.05])
                if mode == "AIR":
                    carrier = np.random.choice(["EXPRESS_X", "CARRIER_A"], p=[0.7, 0.3])
                elif mode == "SEA":
                    carrier = np.random.choice(["CARRIER_B", "CARRIER_C"], p=[0.6, 0.4])
                else:
                    carrier = np.random.choice(dims["carriers"])

                qty = int(np.random.choice([1, 2, 5, 10, 20, 50], p=[0.3, 0.25, 0.2, 0.15, 0.07, 0.03]))

                # simple lead time per region+mode
                base_lt = 3
                if region == "ASIA" and mode == "SEA":
                    base_lt = 20
                elif mode == "SEA":
                    base_lt = 14
                elif mode == "AIR":
                    base_lt = 2
                elif region == "NORTH_AMERICA" and mode == "ROAD":
                    base_lt = 5

                planned_lt = max(1, int(np.random.normal(base_lt, max(1, base_lt * 0.3))))
                promised = (date + pd.Timedelta(days=planned_lt)).normalize()

                # simple delay model
                base_otif = {"CARRIER_A": 0.97, "CARRIER_B": 0.93, "CARRIER_C": 0.9, "POSTAL": 0.88, "EXPRESS_X": 0.98}.get(carrier, 0.9)
                is_on_time = np.random.rand() < base_otif
                if is_on_time:
                    delay = np.random.randint(-1, 2)
                else:
                    delay = np.random.randint(1, 6)
                actual = (promised + pd.Timedelta(days=delay)).normalize()

                shipped_qty = qty
                status = "delivered"

                records.append(
                    {
                        "order_id": oid,
                        "order_date": date.normalize(),
                        "customer": f"CUST_{np.random.randint(1,5000):05d}",
                        "region": region,
                        "plant": plant,
                        "carrier": carrier,
                        "transport_mode": mode,
                        "promised_date": promised,
                        "actual_delivery_date": actual,
                        "quantity": qty,
                        "shipped_quantity": shipped_qty,
                        "status": status,
                    }
                )

    df = pd.DataFrame.from_records(records)
    df.to_csv(path, index=False)
    return df


def load_raw_data(path: str, cfg: SyntheticConfig) -> pd.DataFrame:
    full_path = os.path.join(PROJECT_ROOT, path) if not os.path.isabs(path) else path
    if not os.path.exists(full_path):
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        df = _generate_synthetic(cfg, full_path)
    else:
        df = pd.read_csv(full_path, parse_dates=["order_date", "promised_date", "actual_delivery_date"])
    return df
