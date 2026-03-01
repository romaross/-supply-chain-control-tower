import os
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
from jinja2 import Template


REPORT_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Supply Chain Operations Report</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; }
    h1, h2, h3 { color: #333; }
    table { border-collapse: collapse; margin-bottom: 24px; }
    th, td { border: 1px solid #ccc; padding: 6px 10px; font-size: 13px; }
    th { background-color: #f2f2f2; }
    .kpi-card { margin-bottom: 16px; }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; }
    .pill-high { background-color: #ffcccc; color: #900; }
    .pill-medium { background-color: #ffe9b3; color: #a66b00; }
    .pill-low { background-color: #d6f5d6; color: #006600; }
  </style>
</head>
<body>
  <h1>Supply Chain Operations Report</h1>
  <p>Generated at {{ generated_at }}</p>

  <h2>Latest Daily KPIs</h2>
  {% if latest_kpi %}
  <div class="kpi-card">
    <p><strong>Date:</strong> {{ latest_kpi.delivery_date }}</p>
    <p><strong>OTIF:</strong> {{ (latest_kpi.otif * 100) | round(2) }}%</p>
    <p><strong>Fill Rate:</strong> {{ (latest_kpi.fill_rate * 100) | round(2) }}%</p>
    <p><strong>Backorder Rate:</strong> {{ (latest_kpi.backorder_rate * 100) | round(2) }}%</p>
    <p><strong>Late Deliveries:</strong> {{ (latest_kpi.late_percent * 100) | round(2) }}%</p>
    <p><strong>Avg Lead Time:</strong> {{ latest_kpi.avg_lead_time | round(2) }} days</p>
    <p><strong>SLA Violation Rate:</strong> {{ (latest_kpi.sla_violation_rate * 100) | round(2) }}%</p>
  </div>
  {% else %}
  <p>No KPI data available.</p>
  {% endif %}

  {% if top_carriers is not none %}
  <h2>Top Carriers (by Carrier Index)</h2>
  <table>
    <tr>
      <th>Carrier</th>
      <th>OTIF</th>
      <th>Late %</th>
      <th>Carrier Index</th>
      <th>Total Delivered</th>
    </tr>
    {% for row in top_carriers %}
    <tr>
      <td>{{ row.carrier }}</td>
      <td>{{ (row.otif * 100) | round(2) }}%</td>
      <td>{{ (row.late_percent * 100) | round(2) }}%</td>
      <td>{{ (row.carrier_index * 100) | round(2) }}%</td>
      <td>{{ row.total_delivered }}</td>
    </tr>
    {% endfor %}
  </table>
  {% endif %}

  {% if top_regions is not none %}
  <h2>Regional Performance</h2>
  <table>
    <tr>
      <th>Region</th>
      <th>OTIF</th>
      <th>Late %</th>
      <th>Region Index</th>
      <th>Total Delivered</th>
    </tr>
    {% for row in top_regions %}
    <tr>
      <td>{{ row.region }}</td>
      <td>{{ (row.otif * 100) | round(2) }}%</td>
      <td>{{ (row.late_percent * 100) | round(2) }}%</td>
      <td>{{ (row.region_index * 100) | round(2) }}%</td>
      <td>{{ row.total_delivered }}</td>
    </tr>
    {% endfor %}
  </table>
  {% endif %}

  {% if risk_overview %}
  <h2>Delay Risk Overview</h2>

  <h3>Global Risk Snapshot</h3>
  <p>
    <strong>Total evaluated shipments:</strong> {{ risk_overview.total_shipments }}<br>
    <strong>High risk:</strong> {{ risk_overview.high_count }} ({{ risk_overview.high_pct | round(1) }}%)<br>
    <strong>Medium risk:</strong> {{ risk_overview.medium_count }} ({{ risk_overview.medium_pct | round(1) }}%)<br>
    <strong>Low risk:</strong> {{ risk_overview.low_count }} ({{ risk_overview.low_pct | round(1) }}%)<br>
  </p>
  {% if risk_overview.prev_high_pct is not none %}
  <p>
    Compared to the previous period, the share of <strong>HIGH</strong> risk shipments
    changed by {{ (risk_overview.high_pct - risk_overview.prev_high_pct) | round(1) }} p.p.
  </p>
  {% endif %}

  {% if risk_by_region %}
  <h3>Risk by Region</h3>
  <table>
    <tr>
      <th>Region</th>
      <th>High %</th>
      <th>Medium %</th>
      <th>Low %</th>
      <th>Total Shipments</th>
    </tr>
    {% for row in risk_by_region %}
    <tr>
      <td>{{ row.region }}</td>
      <td>{{ row.high_pct | round(1) }}%</td>
      <td>{{ row.medium_pct | round(1) }}%</td>
      <td>{{ row.low_pct | round(1) }}%</td>
      <td>{{ row.total_shipments }}</td>
    </tr>
    {% endfor %}
  </table>
  {% endif %}

  {% if risk_by_carrier %}
  <h3>Risk by Carrier</h3>
  <table>
    <tr>
      <th>Carrier</th>
      <th>High %</th>
      <th>Medium %</th>
      <th>Low %</th>
      <th>Total Shipments</th>
    </tr>
    {% for row in risk_by_carrier %}
    <tr>
      <td>{{ row.carrier }}</td>
      <td>{{ row.high_pct | round(1) }}%</td>
      <td>{{ row.medium_pct | round(1) }}%</td>
      <td>{{ row.low_pct | round(1) }}%</td>
      <td>{{ row.total_shipments }}</td>
    </tr>
    {% endfor %}
  </table>
  {% endif %}

  {% if top_lanes %}
  <h3>Top Risky Lanes</h3>
  <p>Origin is approximated by plant; destination by region.</p>
  <table>
    <tr>
      <th>Lane (Plant → Region)</th>
      <th>High %</th>
      <th>Avg Delay Probability</th>
      <th>Total Shipments</th>
    </tr>
    {% for row in top_lanes %}
    <tr>
      <td>{{ row.lane }}</td>
      <td>{{ row.high_pct | round(1) }}%</td>
      <td>{{ row.avg_prob | round(2) }}</td>
      <td>{{ row.total_shipments }}</td>
    </tr>
    {% endfor %}
  </table>
  {% endif %}

  {% if risk_recommendations %}
  <h3>Operational Recommendations</h3>
  <ul>
    {% for rec in risk_recommendations %}
    <li>{{ rec }}</li>
    {% endfor %}
  </ul>
  {% endif %}
  {% endif %}

</body>
</html>
"""


def _build_risk_overview(risk_df: Optional[pd.DataFrame]) -> Dict:
    if risk_df is None or risk_df.empty:
        return {}

    df = risk_df.copy()

    total = len(df)
    high = (df["risk_bucket"] == "HIGH").sum()
    medium = (df["risk_bucket"] == "MEDIUM").sum()
    low = (df["risk_bucket"] == "LOW").sum()

    df_sorted = df.sort_values("order_date") if "order_date" in df.columns else df
    if "order_date" in df_sorted.columns and len(df_sorted) >= 14:
        cutoff = df_sorted["order_date"].max() - pd.Timedelta(days=7)
        recent = df_sorted[df_sorted["order_date"] > cutoff]
        previous = df_sorted[df_sorted["order_date"] <= cutoff].tail(len(recent))
        prev_total = len(previous)
        prev_high_pct = (
            (previous["risk_bucket"] == "HIGH").sum() / prev_total * 100
            if prev_total > 0
            else None
        )
    else:
        prev_high_pct = None

    return {
        "total_shipments": int(total),
        "high_count": int(high),
        "medium_count": int(medium),
        "low_count": int(low),
        "high_pct": high / total * 100 if total > 0 else 0.0,
        "medium_pct": medium / total * 100 if total > 0 else 0.0,
        "low_pct": low / total * 100 if total > 0 else 0.0,
        "prev_high_pct": prev_high_pct,
    }


def _risk_distribution_by(risk_df: Optional[pd.DataFrame], group_col: str) -> Optional[list]:
    if risk_df is None or risk_df.empty or group_col not in risk_df.columns:
        return None

    grp = risk_df.groupby(group_col)
    records = []
    for key, g in grp:
        total = len(g)
        if total == 0:
            continue
        high_pct = (g["risk_bucket"] == "HIGH").mean() * 100
        med_pct = (g["risk_bucket"] == "MEDIUM").mean() * 100
        low_pct = (g["risk_bucket"] == "LOW").mean() * 100
        records.append(
            {
                group_col: key,
                "high_pct": float(high_pct),
                "medium_pct": float(med_pct),
                "low_pct": float(low_pct),
                "total_shipments": int(total),
            }
        )

    if not records:
        return None
    records = sorted(records, key=lambda x: x["high_pct"], reverse=True)
    return records


def _top_risky_lanes(risk_df: Optional[pd.DataFrame], top_n: int = 10) -> Optional[list]:
    if risk_df is None or risk_df.empty:
        return None
    if "plant" not in risk_df.columns or "region" not in risk_df.columns:
        return None

    df = risk_df.copy()
    df["lane"] = df["plant"] + " → " + df["region"]

    grp = df.groupby("lane")
    records = []
    for lane, g in grp:
        total = len(g)
        if total < 20:
            continue
        high_pct = (g["risk_bucket"] == "HIGH").mean() * 100
        avg_prob = g["delay_probability"].mean()
        records.append(
            {
                "lane": lane,
                "high_pct": float(high_pct),
                "avg_prob": float(avg_prob),
                "total_shipments": int(total),
            }
        )

    if not records:
        return None

    records = sorted(records, key=lambda x: (x["high_pct"], x["avg_prob"]), reverse=True)
    return records[:top_n]


def _build_risk_recommendations(
    overview: Dict,
    risk_by_region: Optional[list],
    risk_by_carrier: Optional[list],
    top_lanes: Optional[list],
) -> list:
    recs = []

    if not overview:
        return recs

    if overview["high_pct"] > 20:
        recs.append(
            "High share of HIGH-risk shipments globally (>20%). Consider short-term actions: "
            "freeze non-essential promo volume, increase buffer capacity on critical lanes, "
            "and review lead-time promises for next planning cycle."
        )

    if risk_by_region:
        worst_region = risk_by_region[0]
        if worst_region["high_pct"] > 25:
            recs.append(
                f"Region {worst_region['region']} has elevated HIGH-risk share "
                f"({worst_region['high_pct']:.1f}%). Review local carrier mix, "
                "cutover plans, and regional safety stock levels."
            )

    if risk_by_carrier:
        worst_carrier = risk_by_carrier[0]
        if worst_carrier["high_pct"] > 25:
            recs.append(
                f"Carrier {worst_carrier['carrier']} shows elevated delay risk "
                f"({worst_carrier['high_pct']:.1f}% HIGH risk). Consider "
                "reallocating critical shipments to alternative carriers and "
                "initiating a performance review."
            )

    if top_lanes:
        lane = top_lanes[0]
        recs.append(
            f"Lane {lane['lane']} has {lane['high_pct']:.1f}% HIGH risk and "
            f"average delay probability {lane['avg_prob']:.2f}. Evaluate "
            "capacity and transit-time assumptions, and consider targeted actions "
            "such as temporary express routing or local inventory buffers."
        )

    if not recs:
        recs.append(
            "Risk levels are within normal range. Maintain current carrier and "
            "capacity allocation, and continue monitoring daily risk dashboards."
        )

    return recs


def generate_html_report(
    kpi_daily: pd.DataFrame,
    carrier_kpis: pd.DataFrame,
    region_kpis: pd.DataFrame,
    report_cfg: Dict,
    output_path: str,
    risk_df: Optional[pd.DataFrame] = None,
) -> None:
    latest_kpi = None
    if not kpi_daily.empty:
        latest_kpi = kpi_daily.sort_values("delivery_date").iloc[-1].to_dict()

    top_carriers = None
    if report_cfg.get("include_top_carriers", True) and not carrier_kpis.empty:
        n = report_cfg.get("top_n_carriers", 5)
        top_carriers = (
            carrier_kpis.sort_values("carrier_index", ascending=False)
            .head(n)
            .to_dict(orient="records")
        )

    top_regions = None
    if report_cfg.get("include_regions", True) and not region_kpis.empty:
        n = report_cfg.get("top_n_regions", 5)
        top_regions = (
            region_kpis.sort_values("region_index", ascending=False)
            .head(n)
            .to_dict(orient="records")
        )

    risk_overview = None
    risk_by_region = None
    risk_by_carrier = None
    top_lanes = None
    risk_recommendations = None

    if risk_df is not None and not risk_df.empty:
        risk_overview = _build_risk_overview(risk_df)
        risk_by_region = _risk_distribution_by(risk_df, "region")
        risk_by_carrier = _risk_distribution_by(risk_df, "carrier")
        top_lanes = _top_risky_lanes(risk_df)
        risk_recommendations = _build_risk_recommendations(
            risk_overview,
            risk_by_region,
            risk_by_carrier,
            top_lanes,
        )

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        generated_at=datetime.utcnow().isoformat(),
        latest_kpi=latest_kpi,
        top_carriers=top_carriers,
        top_regions=top_regions,
        risk_overview=risk_overview,
        risk_by_region=risk_by_region,
        risk_by_carrier=risk_by_carrier,
        top_lanes=top_lanes,
        risk_recommendations=risk_recommendations,
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
